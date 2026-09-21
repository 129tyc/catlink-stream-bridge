package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
)

func TestTalkURLHelpers(t *testing.T) {
	if got := normalizeTalkRTCURL("https://webrtc.example/rtcgw"); got != "wss://webrtc.example/rtcgw-ws" {
		t.Fatalf("normalized rtc URL=%q", got)
	}
	if got := normalizeTalkRTCURL("wss://webrtc.example/rtcgw-ws"); got != "wss://webrtc.example/rtcgw-ws" {
		t.Fatalf("preserved rtc URL=%q", got)
	}
	if got := makeTalkLink("host.example:8664?ignored=1", "device", 2); got != "tts://host.example:8664/talk?dev=device&chann=2&encodetype=2" {
		t.Fatalf("talk link=%q", got)
	}
	if got := talkJSONCode(json.RawMessage(`"200"`)); got != "200" {
		t.Fatalf("string code=%q", got)
	}
	if got := talkJSONCode(json.RawMessage(`200`)); got != "200" {
		t.Fatalf("number code=%q", got)
	}
}

func TestFetchTalkURLUsesMultipartCameraFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != talkURLPath {
			t.Fatalf("request=%s %s", request.Method, request.URL.Path)
		}
		if !strings.HasPrefix(request.Header.Get("Content-Type"), "multipart/form-data;") {
			t.Fatalf("content type=%q", request.Header.Get("Content-Type"))
		}
		reader, err := request.MultipartReader()
		if err != nil {
			t.Fatal(err)
		}
		fields := readMultipartFields(t, reader)
		if fields["accessToken"] != "camera" || fields["deviceSerial"] != "device" || fields["channelNo"] != "2" {
			t.Fatalf("fields=%v", fields)
		}
		response.Header().Set("Content-Type", "application/json")
		io.WriteString(response, `{"code":"200","msg":"ok","data":{"rtcUrl":"https://webrtc.example/rtcgw","ttsUrl":"tts.example:8664","stream":"talk-stream"}}`)
	}))
	defer server.Close()

	transport := newC07TalkTransport(server.Client(), server.URL)
	session, err := transport.fetchTalkURL(context.Background(), TalkStart{
		CameraToken: "camera",
		Device:      ResolvedDevice{EZOpenSerial: "device"},
		Channel:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if session.RTCURL == "" || session.TTSURL == "" || session.Stream != "talk-stream" {
		t.Fatalf("session=%+v", session)
	}
}

func readMultipartFields(t *testing.T, reader *multipart.Reader) map[string]string {
	t.Helper()
	fields := make(map[string]string)
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			return fields
		}
		if err != nil {
			t.Fatal(err)
		}
		value, err := io.ReadAll(part)
		if err != nil {
			t.Fatal(err)
		}
		fields[part.FormName()] = string(value)
	}
}

func TestPCMAOpusEncoderProducesTwentyMillisecondFrame(t *testing.T) {
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: talkOpusSampleRate,
		Channels:  talkOpusChannels,
	}, "audio", "test")
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := newPCMAOpusEncoder(track)
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.Write(context.Background(), TalkAudio{Payload: make([]byte, 160)}); err != nil {
		t.Fatal(err)
	}
	if encoder.sequence != 1 || encoder.timestamp != talkOpusFrameSamples || len(encoder.pcm) != 0 {
		t.Fatalf("sequence=%d timestamp=%d buffered=%d", encoder.sequence, encoder.timestamp, len(encoder.pcm))
	}
	if len(encoder.packet) != 4000 {
		t.Fatalf("unexpected packet buffer length=%d", len(encoder.packet))
	}
}

func TestAlawDecodeKnownPolarity(t *testing.T) {
	if got := alawToPCM16(0xD5); got <= 0 {
		t.Fatalf("positive A-law sample=%d", got)
	}
	if got := alawToPCM16(0x55); got >= 0 {
		t.Fatalf("negative A-law sample=%d", got)
	}
}

func TestTalkNotificationsCarryTransactions(t *testing.T) {
	for _, test := range []struct {
		name   string
		kind   string
		handle uint64
	}{
		{name: "keepalive", kind: "keepalive"},
		{name: "trickle", kind: "trickle", handle: 22},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := talkNotification(test.kind, 11, test.handle)
			if payload["transaction"] == "" {
				t.Fatal("notification has no transaction")
			}
			if payload["session_id"] != uint64(11) {
				t.Fatalf("session_id=%v", payload["session_id"])
			}
			if test.handle != 0 && payload["handle_id"] != test.handle {
				t.Fatalf("handle_id=%v", payload["handle_id"])
			}
		})
	}
}

func TestTalkWriteStopsAfterGatewayReadCloses(t *testing.T) {
	transport := newC07TalkTransport(nil, "https://open.ys7.com")
	close(transport.readDone)
	if err := transport.WriteAudio(context.Background(), TalkAudio{Payload: []byte{1}}); !errors.Is(err, ErrTalkSessionInactive) {
		t.Fatalf("write error=%v", err)
	}
}

func TestTalkGatewayAuthorizationErrorIsTyped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		io.WriteString(response, `{"code":"1002","msg":"expired","data":{}}`)
	}))
	defer server.Close()
	transport := newC07TalkTransport(server.Client(), server.URL)
	_, err := transport.fetchTalkURL(context.Background(), TalkStart{
		CameraToken: "expired",
		Device:      ResolvedDevice{EZOpenSerial: "device"},
		Channel:     1,
	})
	if !errors.Is(err, ErrTalkAuthorization) {
		t.Fatalf("error=%v", err)
	}
}
