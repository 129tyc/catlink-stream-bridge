package bridge

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/129tyc/catlink-stream-bridge/internal/mpegps"
	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
)

func TestSignVector(t *testing.T) {
	if got := signParams(map[string]string{"noncestr": "1700000000000", "token": "token+%?中文"}); got != "E759A1C30317FC00581DF4BAAF17ABA9" {
		t.Fatalf("sign=%s", got)
	}
}

func TestAccountTokenManagerReusesAndRefreshesAccountSession(t *testing.T) {
	var providerCalls, exchangeCalls int
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/session":
			providerCalls++
			_, _ = io.WriteString(response, `{"token":"login-token","apiBase":"`+server.URL+`/"}`)
		case "/token/device/camera/accessToken":
			exchangeCalls++
			_, _ = io.WriteString(response, `{"returnCode":0,"data":{"accessToken":"camera-token"}}`)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	manager := NewAccountTokenManager(AccountConfig{
		ID: "account", SessionProviderURL: server.URL + "/v1/session", APIKey: "key",
	}, server.Client())
	for attempt := 0; attempt < 2; attempt++ {
		if token, err := manager.Token(context.Background()); err != nil || token != "camera-token" {
			t.Fatalf("token attempt %d=%q err=%v", attempt, token, err)
		}
	}
	if providerCalls != 1 || exchangeCalls != 1 {
		t.Fatalf("cached session calls provider=%d exchange=%d", providerCalls, exchangeCalls)
	}
	manager.Invalidate()
	if token, err := manager.Token(context.Background()); err != nil || token != "camera-token" {
		t.Fatalf("refreshed token=%q err=%v", token, err)
	}
	if providerCalls != 2 || exchangeCalls != 2 {
		t.Fatalf("refresh calls provider=%d exchange=%d", providerCalls, exchangeCalls)
	}
}

func TestAccountTokenManagerRefreshesAfterCameraAuthorizationError(t *testing.T) {
	var providerCalls, exchangeCalls int
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/session":
			providerCalls++
			_, _ = io.WriteString(response, `{"token":"login-token","apiBase":"`+server.URL+`/"}`)
		case "/token/device/camera/accessToken":
			exchangeCalls++
			if exchangeCalls == 1 {
				_, _ = io.WriteString(response, `{"returnCode":1002,"data":{}}`)
				return
			}
			_, _ = io.WriteString(response, `{"returnCode":0,"data":{"accessToken":"fresh-camera-token"}}`)
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	manager := NewAccountTokenManager(AccountConfig{
		ID: "account", SessionProviderURL: server.URL + "/v1/session", APIKey: "key",
	}, server.Client())
	if _, err := manager.Token(context.Background()); !errors.Is(err, ErrTalkAuthorization) {
		t.Fatalf("first token error=%v", err)
	}
	if token, err := manager.Token(context.Background()); err != nil || token != "fresh-camera-token" {
		t.Fatalf("fresh token=%q err=%v", token, err)
	}
	if providerCalls != 2 || exchangeCalls != 2 {
		t.Fatalf("refresh calls provider=%d exchange=%d", providerCalls, exchangeCalls)
	}
}

func TestAccountTokenManagerReadsAndRefreshesSessionFile(t *testing.T) {
	sessionPath := filepath.Join(t.TempDir(), "auth-test.json")
	if err := os.WriteFile(sessionPath, []byte(`{"data":{"token":"login-token-a"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	var exchangeCalls int
	var seenTokens []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/token/device/camera/accessToken" {
			response.WriteHeader(http.StatusNotFound)
			return
		}
		exchangeCalls++
		seenTokens = append(seenTokens, request.Header.Get("Token"))
		_, _ = io.WriteString(response, `{"returnCode":0,"data":{"accessToken":"camera-token"}}`)
	}))
	defer server.Close()

	manager := NewAccountTokenManager(AccountConfig{
		ID: "account", SessionFile: sessionPath, APIBase: server.URL,
	}, server.Client())
	if token, err := manager.Token(context.Background()); err != nil || token != "camera-token" {
		t.Fatalf("first token=%q err=%v", token, err)
	}
	if err := os.WriteFile(sessionPath, []byte(`{"data":{"token":"login-token-b"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if token, err := manager.Token(context.Background()); err != nil || token != "camera-token" {
		t.Fatalf("refreshed token=%q err=%v", token, err)
	}
	if exchangeCalls != 2 {
		t.Fatalf("camera token exchange calls=%d, want 2 after session change", exchangeCalls)
	}
	if strings.Join(seenTokens, ",") != "login-token-a,login-token-b" {
		t.Fatalf("session tokens=%v, want [login-token-a login-token-b]", seenTokens)
	}
}

func TestAuthFailureClassification(t *testing.T) {
	for _, message := range []string{
		"EZOpen live-url business code 401",
		"EZOpen playback rejected: unauthorized",
		"camera token status 403",
		"camera token business code 1002",
	} {
		if !isAuthFailure(errors.New(message)) {
			t.Fatalf("auth failure not classified: %s", message)
		}
	}
	if isAuthFailure(errors.New("EZOpen websocket dial: connection reset")) {
		t.Fatal("transport failure classified as auth failure")
	}
}

func TestConfigValidation(t *testing.T) {
	valid := `{
      "accounts":[{"id":"account-a","sessionProviderUrl":"http://session-reader:8090/v1/session","apiKey":"bridge-key"}],
      "devices":[{"id":"device-a","serial":"serial-a","accountId":"account-a"}],
      "routes":[{"name":"camera-a","deviceId":"device-a","channel":1,"quality":"hd","path":"/catlink-a"}]
    }`
	cfg, err := LoadConfig(strings.NewReader(valid))
	if err != nil || cfg.Listen != ":8554" || cfg.HealthListen != ":8080" {
		t.Fatalf("config=%+v err=%v", cfg, err)
	}
	selectorConfig := strings.Replace(valid, `"serial":"serial-a"`, `"selector":{"deviceName":"Living Room"}`, 1)
	if _, err := LoadConfig(strings.NewReader(selectorConfig)); err != nil {
		t.Fatalf("selector-only config rejected: %v", err)
	}
	fileConfig := strings.Replace(valid, `"sessionProviderUrl":"http://session-reader:8090/v1/session","apiKey":"bridge-key"`, `"sessionFile":"/config/.storage/catlink/auth-*.json"`, 1)
	if _, err := LoadConfig(strings.NewReader(fileConfig)); err != nil {
		t.Fatalf("session-file config rejected: %v", err)
	}
	for _, invalid := range []string{
		`{"accounts":[],"devices":[],"routes":[]}`,
		`{"accounts":[{"id":"a"}],"devices":[{"id":"d","serial":"s","accountId":"a"}],"routes":[{"name":"x","deviceId":"d","channel":1,"quality":"hd","path":"/x"}]}`,
		`{"accounts":[{"id":"a","sessionFile":"/config/auth.json","sessionProviderUrl":"http://x","apiKey":"k"}],"devices":[{"id":"d","serial":"s","accountId":"a"}],"routes":[{"name":"x","deviceId":"d","channel":1,"quality":"hd","path":"/x"}]}`,
		`{"accounts":[{"id":"a","sessionProviderUrl":"http://x","apiKey":"k"}],"devices":[{"id":"d","serial":"s","accountId":"a"}],"routes":[{"name":"x","deviceId":"d","channel":3,"quality":"hd","path":"/x"}]}`,
		`{"accounts":[{"id":"a","sessionProviderUrl":"http://x","apiKey":"k"}],"devices":[{"id":"d","serial":"s","accountId":"a"}],"routes":[{"name":"x","deviceId":"d","channel":1,"quality":"hd","path":"//x"}]}`,
		`{"accounts":[{"id":"a","sessionProviderUrl":"http://x","apiKey":"k"}],"devices":[{"id":"d","serial":"s","accountId":"a"}],"routes":[{"name":"x","deviceId":"d","channel":1,"quality":"hd","path":"/x","source":"other"}]}`,
		`{"accounts":[{"id":"a","sessionProviderUrl":"http://x","apiKey":"k"}],"devices":[{"id":"d","serial":"s","accountId":"a","selector":{"deviceName":"x"}}],"routes":[{"name":"x","deviceId":"d","channel":1,"quality":"hd","path":"/x"}]}`,
		`{"accounts":[{"id":"a","sessionProviderUrl":"http://x","apiKey":"k"}],"devices":[{"id":"d","accountId":"a","selector":{"deviceSerial":"visible"}}],"routes":[{"name":"x","deviceId":"d","channel":1,"quality":"hd","path":"/x"}]}`,
	} {
		if _, err := LoadConfig(strings.NewReader(invalid)); err == nil {
			t.Fatalf("invalid config accepted")
		}
	}
}

func TestRealplayVector(t *testing.T) {
	vector := struct {
		Timestamp string `json:"timestamp"`
		Rand      string `json:"rand"`
		Modulus   string `json:"pkd_modulus"`
		PlayURL   string `json:"play_url"`
		Expected  struct {
			Sequence      int    `json:"sequence"`
			Command       string `json:"cmd"`
			URL           string `json:"url"`
			Key           string `json:"key"`
			IV            string `json:"iv"`
			Authorization string `json:"authorization"`
			Token         string `json:"token"`
			WireKey       string `json:"wire_key"`
		} `json:"expected"`
	}{
		Timestamp: "1700000000000",
		Rand:      "0011223344556677",
		Modulus:   strings.Repeat("A", 255) + "B",
		PlayURL:   "/lapp/mock?ssn=abc&auth=1&biz=4&cln=100&lid=00000000-0000-4000-8000-000000000000",
	}
	vector.Expected.Sequence = 0
	vector.Expected.Command = "realplay"
	vector.Expected.URL = vector.PlayURL
	vector.Expected.Key = "bf11ff333f233150c77918a7d6ccc062bf11ff333f233150c77918a7d6ccc062"
	vector.Expected.IV = "fd73dcde7de6432ccb10303c0703b7ce"
	vector.Expected.Authorization = "f3a0d822046fb1a1baf9b4971f34dd0aba4d8ae63f52d52d03424b5889d28a79"
	vector.Expected.Token = "6a45c258e4bdc92100e717bef63edd27"
	vector.Expected.WireKey = "197a2ede0bbfb63a9b477f60248c4be95e03b7a557b8a3386493bc34acc64f65ce5a77fd0f231e8bc88a7e544927a65d5ca35178fbd03787b6a91f5823d4394dfabbd888defcef2762a7879afc214b4177aeb3ca1380ce56a626ce964eb25420b423959bb5f217a78d3ee4fac392080389f5745395a5d6091100a8cea0d3d0c5"
	command, err := createRealplayCommandWithRandom(1700000000000, vector.Rand, vector.Modulus, vector.PlayURL, bytes.NewReader(bytes.Repeat([]byte{0xab}, 128)))
	if err != nil {
		t.Fatalf("command: %v", err)
	}
	for _, field := range []struct {
		name      string
		got, want any
	}{
		{"sequence", command["sequence"], vector.Expected.Sequence},
		{"cmd", command["cmd"], vector.Expected.Command},
		{"url", command["url"], vector.Expected.URL},
		{"authorization", command["authorization"], vector.Expected.Authorization},
		{"token", command["token"], vector.Expected.Token},
		{"key", command["key"], vector.Expected.WireKey},
	} {
		if field.got != field.want {
			t.Fatalf("command field mismatch: %s", field.name)
		}
	}
}

func TestRealplayAcceptsEZOpenEvenPKD(t *testing.T) {
	modulus := strings.Repeat("A", 255) + "C"
	if _, err := createRealplayCommandWithRandom(
		1700000000000, "rand", modulus, "/live", bytes.NewReader(bytes.Repeat([]byte{0xab}, 128)),
	); err != nil {
		t.Fatalf("even PKD rejected: %v", err)
	}
}

func TestEZOpenPlayInfoResponseVariants(t *testing.T) {
	for _, responseBody := range []string{
		`{"code":0,"ext":{"token":"ticket"},"data":"http://example.test/live"}`,
		`{"code":"0","ext":{"token":"ticket"},"data":"http://example.test/live"}`,
		`{"meta":{"code":200},"data":{"token":"ticket","url":"http://example.test/live"}}`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPost || !strings.HasPrefix(request.Header.Get("Content-Type"), "multipart/form-data;") {
				t.Errorf("unexpected request: %s %s", request.Method, request.Header.Get("Content-Type"))
			}
			response.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(response, responseBody)
		}))
		info, err := getEzOpenPlayInfo(context.Background(), server.Client(), server.URL, "ezopen://open.ys7.com/serial/1.hd.live", "camera-token", defaultUserAgent)
		server.Close()
		if err != nil || !strings.HasPrefix(info.connectURL, "ws://") || !strings.Contains(info.playURL, "ssn=ticket") || !strings.Contains(info.playURL, "lid=") {
			t.Fatalf("info=%+v err=%v", info, err)
		}
	}
}

func TestEZOpenIMKHAndHandshake(t *testing.T) {
	var received atomic.Bool
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			response.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(response, `{"code":0,"ext":{"token":"ticket"},"data":"`+server.URL+`/live"}`)
			return
		}
		query := request.URL.Query()
		if query.Get("version") != "0.1" || query.Get("cipherSuites") != "0" ||
			query.Get("sessionID") != "" || len(query["sessionID"]) != 1 {
			t.Errorf("unexpected EZOpen websocket query: %s", request.URL.RawQuery)
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		connection, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		_ = connection.WriteJSON(map[string]any{"version": "1.0", "cipherSuite": 0, "PKD": strings.Repeat("A", 254) + "AB", "rand": "rand"})
		_, _, err = connection.ReadMessage()
		if err != nil {
			return
		}
		received.Store(true)
		header := append([]byte("IMKH"), bytes.Repeat([]byte{0}, 36)...)
		payload := append([]byte{0, 0, 1, 0x40, 1, 1}, bytes.Repeat([]byte{0}, 18)...)
		_ = connection.WriteMessage(websocket.BinaryMessage, append(header, payload...))
	}))
	defer server.Close()
	source := NewEZOpenSource(server.Client(), server.URL, "ezopen://open.ys7.com/serial/1.live")
	if err := source.Start(context.Background(), "camera-token"); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer source.Close()
	message, err := source.Read(context.Background())
	want := append([]byte{0, 0, 1, 0x40, 1, 1}, bytes.Repeat([]byte{0}, 18)...)
	if err != nil || !message.Binary || !bytes.Equal(message.Data, want) || !received.Load() {
		t.Fatalf("message=%+v err=%v received=%v", message, err, received.Load())
	}
}

func TestPCMAWriter(t *testing.T) {
	writer := newPCMAWriter()
	var packets []*rtp.Packet
	var offsets []uint32
	err := writer.Push(100, bytes.Repeat([]byte{1}, 320), func(packet *rtp.Packet, _ uint64, offset uint32) error {
		packets = append(packets, packet)
		offsets = append(offsets, offset)
		return nil
	})
	if err != nil || len(packets) != 2 || packets[0].Timestamp+160 != packets[1].Timestamp || offsets[0] != 0 || offsets[1] != 160 {
		t.Fatalf("packets=%d offsets=%v err=%v", len(packets), offsets, err)
	}
	if err := writer.Push(99, []byte{1}, func(*rtp.Packet, uint64, uint32) error { return nil }); err == nil {
		t.Fatal("backwards audio timestamp accepted")
	}
}

func TestPCMAWriterBoundsFrozenTimestamp(t *testing.T) {
	writer := newPCMAWriter()
	var packetCount int
	err := writer.Push(100, bytes.Repeat([]byte{1}, maxPCMAPendingBytes+1), func(*rtp.Packet, uint64, uint32) error {
		packetCount++
		return nil
	})
	if !errors.Is(err, errPCMAOverflow) {
		t.Fatalf("overflow returned error: %v", err)
	}
	if packetCount != 0 || writer.havePTS || len(writer.pending) != 0 {
		t.Fatalf("overflow was not dropped: packets=%d havePTS=%t pending=%d", packetCount, writer.havePTS, len(writer.pending))
	}
}

func TestPTSUnwrapperHandlesMPEGPeriod(t *testing.T) {
	var unwrap ptsUnwrapper
	got, ok := unwrap.Unwrap(mpegPTSPeriodMS - 10)
	if !ok || got != mpegPTSPeriodMS-10 {
		t.Fatalf("first timestamp=%d", got)
	}
	got, ok = unwrap.Unwrap(3)
	if !ok || got != mpegPTSPeriodMS+3 {
		t.Fatalf("wrapped timestamp=%d", got)
	}
	got, ok = unwrap.Unwrap(mpegPTSPeriodMS - 5)
	if ok {
		t.Fatalf("late pre-wrap timestamp accepted=%d", got)
	}
	got, ok = unwrap.Unwrap(100)
	if !ok || got != mpegPTSPeriodMS+100 {
		t.Fatalf("post-wrap timestamp=%d", got)
	}
}

func TestAudioPTSAlignsToVideoClockAcrossWrap(t *testing.T) {
	got, ok := alignPTSNear(3, mpegPTSPeriodMS-10)
	if !ok || got != mpegPTSPeriodMS+3 {
		t.Fatalf("audio timestamp=%d ok=%t", got, ok)
	}
	got, ok = alignPTSNear(mpegPTSPeriodMS-5, 3)
	if ok {
		t.Fatalf("late pre-wrap timestamp=%d ok=%t", got, ok)
	}
}

func TestPipelineAudioUsesVideoEpochAcrossWrap(t *testing.T) {
	publisher := new(fixturePublisher)
	pipeline := &Pipeline{
		publisher:   publisher,
		publication: &Publication{epochPTS: mpegPTSPeriodMS - 10, epochWall: time.Unix(0, 0)},
		audio:       newPCMAWriter(),
	}
	pipeline.OnElementary(mpegps.ElementaryEvent{
		Kind: mpegps.KindH265NALU, Data: []byte{0, 0, 1, 0x40, 0x01}, CallbackPTSMS: mpegPTSPeriodMS - 10,
	})
	pipeline.OnElementary(mpegps.ElementaryEvent{
		Kind: mpegps.KindPCMAPayload, Data: bytes.Repeat([]byte{1}, 160), CallbackPTSMS: 3,
	})
	if pipeline.audio.lastPTS != mpegPTSPeriodMS+3 {
		t.Fatalf("audio last PTS=%d", pipeline.audio.lastPTS)
	}
}

func TestPipelineDoesNotCountOutOfOrderVideoAsMedia(t *testing.T) {
	pipeline := &Pipeline{}
	validNALU := []byte{0, 0, 1, 0x40, 0x01}
	pipeline.OnElementary(mpegps.ElementaryEvent{Kind: mpegps.KindH265NALU, Data: validNALU, CallbackPTSMS: 100})
	if pipeline.MediaCount() != 1 {
		t.Fatalf("first media count=%d", pipeline.MediaCount())
	}
	pipeline.OnElementary(mpegps.ElementaryEvent{Kind: mpegps.KindH265NALU, Data: validNALU, CallbackPTSMS: 99})
	if pipeline.MediaCount() != 1 {
		t.Fatalf("out-of-order media refreshed count=%d", pipeline.MediaCount())
	}
}

func TestPipelineReadinessClearsWithPublication(t *testing.T) {
	pipeline := &Pipeline{hasVideoOutput: true, videoOutputCount: 1}
	pipeline.clearMediaState()
	if pipeline.HasVideoOutput() {
		t.Fatal("closed publication retained video readiness")
	}
	if pipeline.VideoOutputCount() != 1 {
		t.Fatal("watchdog counter was reset with publication")
	}
}

func TestSignedPTSDelta(t *testing.T) {
	if got := signedPTSDelta(900, 1000); got != -100*time.Millisecond {
		t.Fatalf("delta=%v", got)
	}
}

func TestPipelineRecoversFromMalformedFragment(t *testing.T) {
	publisher := new(fixturePublisher)
	pipeline, err := NewPipeline("/recover", publisher)
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if err := pipeline.Feed([]byte{0, 0, 1, 0xbb, 0, 0}); err != nil {
		t.Fatalf("malformed fragment became route error: %v", err)
	}
	if pipeline.Error() != nil || publisher.publications != 0 {
		t.Fatalf("malformed fragment poisoned pipeline: error=%v publications=%d", pipeline.Error(), publisher.publications)
	}
}

func TestMakeEZOpenURL(t *testing.T) {
	if got := makeEZOpenURL("serial", 2, "hd"); got != "ezopen://open.ys7.com/serial/2.hd.live" {
		t.Fatalf("url=%s", got)
	}
	if _, err := url.Parse("rtsp://127.0.0.1:8554/catlink"); err != nil {
		t.Fatal(err)
	}
}

func TestScalarJSONValueLimitsControlDetails(t *testing.T) {
	if got := diagnosticJSONValue([]byte(`"error"`)); got != "error" {
		t.Fatalf("string value=%q", got)
	}
	if got := diagnosticJSONValue([]byte(`{"error":"https://example.test/?token=secret"}`)); got != "" {
		t.Fatalf("object value=%q", got)
	}
	if got := diagnosticJSONValue([]byte(`"` + strings.Repeat("x", 160) + `"`)); len([]rune(got)) > 129 {
		t.Fatalf("long value length=%d", len(got))
	}
	if got := diagnosticJSONValue([]byte(`"invalid ssn=secret"`)); got != "<redacted>" {
		t.Fatalf("ssn value=%q", got)
	}
}

func TestEZOpenControlErrorHandlesNonStringFields(t *testing.T) {
	err := ezOpenControlError([]byte(`{"statusString":"error","error":{"code":401}}`))
	if !errors.Is(err, errSourceUnavailable) {
		t.Fatalf("error=%v", err)
	}
}
