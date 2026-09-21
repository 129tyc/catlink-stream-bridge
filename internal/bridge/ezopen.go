package bridge

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	crand "crypto/rand"
	"crypto/rsa"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultOpenDomain   = "https://open.ys7.com"
	defaultUserAgent    = "catlink-node-bridge"
	maxWebSocketMessage = 2 << 20
	wsReadTimeout       = 45 * time.Second
	wsWriteTimeout      = 10 * time.Second
	wsHandshakeTimeout  = 10 * time.Second
)

type SourceMessage struct {
	Binary bool
	Data   []byte
}

type EZOpenSource struct {
	client     *http.Client
	openDomain string
	ezopen     string
	userAgent  string

	ws          *websocket.Conn
	writeMu     sync.Mutex
	firstBinary bool
	stopOnce    sync.Once
	done        chan struct{}
}

type sourceUnavailableError struct {
	code string
	err  error
}

func (e sourceUnavailableError) Error() string { return e.err.Error() }
func (e sourceUnavailableError) Unwrap() error { return errSourceUnavailable }

func NewEZOpenSource(client *http.Client, openDomain, ezopen string) *EZOpenSource {
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	if openDomain == "" {
		openDomain = defaultOpenDomain
	}
	return &EZOpenSource{
		client: client, openDomain: strings.TrimRight(openDomain, "/"),
		ezopen: ezopen, userAgent: defaultUserAgent, done: make(chan struct{}),
	}
}

func (s *EZOpenSource) Start(ctx context.Context, accessToken string) error {
	info, err := getEzOpenPlayInfo(ctx, s.client, s.openDomain, s.ezopen, accessToken, s.userAgent)
	if err != nil {
		return err
	}
	wsURL := withEZOpenWebSocketQuery(info.connectURL)
	dialer := websocket.Dialer{EnableCompression: false, HandshakeTimeout: 20 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("EZOpen websocket dial: %w", err)
	}
	conn.SetReadLimit(maxWebSocketMessage)
	s.ws = conn
	s.firstBinary = true

	if err := s.readHandshake(ctx, info.playURL); err != nil {
		conn.Close()
		return err
	}
	go s.pingLoop()
	return nil
}

func (s *EZOpenSource) readHandshake(ctx context.Context, playURL string) error {
	if err := s.ws.SetReadDeadline(time.Now().Add(wsHandshakeTimeout)); err != nil {
		return err
	}
	_, payload, err := s.ws.ReadMessage()
	if err != nil {
		return fmt.Errorf("EZOpen handshake read: %w", err)
	}
	var message map[string]json.RawMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return fmt.Errorf("EZOpen handshake JSON: %w", err)
	}
	version, ok := stringField(message, "version")
	if !ok || version == "" || !validCipherSuite(message["cipherSuite"]) {
		return fmt.Errorf("EZOpen handshake fields invalid")
	}
	pkd, ok := stringField(message, "PKD")
	if !ok || pkd == "" {
		return fmt.Errorf("EZOpen handshake PKD missing")
	}
	randValue, ok := stringField(message, "rand")
	if !ok || randValue == "" {
		return fmt.Errorf("EZOpen handshake rand missing")
	}
	command, err := createRealplayCommand(time.Now().UnixMilli(), randValue, pkd, playURL)
	if err != nil {
		return err
	}
	return s.writeJSON(command)
}

func (s *EZOpenSource) Read(ctx context.Context) (SourceMessage, error) {
	if s.ws == nil {
		return SourceMessage{}, fmt.Errorf("EZOpen websocket is not started")
	}
	if err := s.ws.SetReadDeadline(time.Now().Add(wsReadTimeout)); err != nil {
		return SourceMessage{}, err
	}
	kind, payload, err := s.ws.ReadMessage()
	if err != nil {
		return SourceMessage{}, err
	}
	if kind != websocket.BinaryMessage {
		var control map[string]json.RawMessage
		if json.Unmarshal(payload, &control) == nil {
			if controlErr := ezOpenControlError(payload); controlErr != nil {
				return SourceMessage{}, controlErr
			}
		}
		return SourceMessage{}, nil
	}
	if s.firstBinary {
		s.firstBinary = false
		if len(payload) == 40 || len(payload) == 64 {
			if len(payload) >= 4 && string(payload[:4]) == "IMKH" {
				if len(payload) == 40 {
					return SourceMessage{}, nil
				}
				payload = payload[40:]
			}
		}
	}
	if len(payload) == 0 {
		return SourceMessage{}, nil
	}
	return SourceMessage{Binary: true, Data: append([]byte(nil), payload...)}, nil
}

func ezOpenControlError(payload []byte) error {
	var control map[string]json.RawMessage
	if err := json.Unmarshal(payload, &control); err != nil {
		return nil
	}
	status := diagnosticJSONValue(control["statusString"])
	if status == "" || strings.EqualFold(status, "ok") {
		return nil
	}
	details := make([]string, 0, 5)
	for _, field := range []struct {
		name string
		key  string
	}{
		{name: "code", key: "code"},
		{name: "errorCode", key: "errorCode"},
		{name: "msg", key: "msg"},
		{name: "reason", key: "reason"},
		{name: "error", key: "error"},
	} {
		if value := diagnosticJSONValue(control[field.key]); value != "" {
			details = append(details, field.name+"="+value)
		}
	}
	suffix := ""
	if len(details) > 0 {
		suffix = " " + strings.Join(details, " ")
	}
	base := fmt.Errorf("%w: EZOpen statusString=%s%s", errSourceUnavailable, strings.ToLower(status), suffix)
	return sourceUnavailableError{code: diagnosticJSONValue(control["errorCode"]), err: base}
}

func sourceUnavailableCode(err error) string {
	var unavailable sourceUnavailableError
	if errors.As(err, &unavailable) {
		return unavailable.code
	}
	return ""
}

func diagnosticJSONValue(raw json.RawMessage) string {
	value := strings.TrimSpace(string(raw))
	if value == "" || value == "null" || value[0] == '{' || value[0] == '[' {
		return ""
	}
	if value[0] == '"' {
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return "<invalid>"
		}
		text = strings.TrimSpace(text)
		lower := strings.ToLower(text)
		for _, marker := range []string{"token", "ssn", "authorization", "password", "secret", "http://", "https://"} {
			if strings.Contains(lower, marker) {
				return "<redacted>"
			}
		}
		runes := []rune(text)
		if len(runes) > 128 {
			return string(runes[:128]) + "…"
		}
		return text
	}
	if len(value) > 64 {
		return value[:64]
	}
	return value
}

func (s *EZOpenSource) writeJSON(value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.ws.SetWriteDeadline(time.Now().Add(wsWriteTimeout)); err != nil {
		return err
	}
	return s.ws.WriteMessage(websocket.TextMessage, data)
}

func (s *EZOpenSource) pingLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.writeMu.Lock()
			if s.ws != nil {
				_ = s.ws.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
				_ = s.ws.WriteMessage(websocket.PingMessage, nil)
			}
			s.writeMu.Unlock()
		case <-s.done:
			return
		}
	}
}

func (s *EZOpenSource) Close() {
	s.stopOnce.Do(func() {
		close(s.done)
		if s.ws != nil {
			s.writeMu.Lock()
			_ = s.ws.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			_ = s.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bridge stop"))
			_ = s.ws.Close()
			s.writeMu.Unlock()
		}
	})
}

type ezOpenPlayInfo struct {
	connectURL string
	playURL    string
}

func getEzOpenPlayInfo(ctx context.Context, client *http.Client, domain, ezopen, accessToken, userAgent string) (ezOpenPlayInfo, error) {
	var body strings.Builder
	writer := multipart.NewWriter(&body)
	fields := map[string]string{
		"accessToken": accessToken,
		"ezopen":      ezopen,
		"isFlv":       "false",
		"isHttp":      "false",
		"userAgent":   userAgent,
	}
	for _, key := range []string{"accessToken", "ezopen", "isFlv", "isHttp", "userAgent"} {
		if err := writer.WriteField(key, fields[key]); err != nil {
			return ezOpenPlayInfo{}, err
		}
	}
	if err := writer.Close(); err != nil {
		return ezOpenPlayInfo{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, domain+"/api/lapp/live/url/ezopen", strings.NewReader(body.String()))
	if err != nil {
		return ezOpenPlayInfo{}, err
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := client.Do(request)
	if err != nil {
		return ezOpenPlayInfo{}, fmt.Errorf("EZOpen live-url request: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return ezOpenPlayInfo{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return ezOpenPlayInfo{}, fmt.Errorf("EZOpen live-url status %d", response.StatusCode)
	}
	var payload struct {
		Code json.RawMessage `json:"code"`
		Meta struct {
			Code json.RawMessage `json:"code"`
		} `json:"meta"`
		Ext struct {
			Token string `json:"token"`
		} `json:"ext"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return ezOpenPlayInfo{}, fmt.Errorf("EZOpen live-url JSON: %w", err)
	}
	code, present, err := parseBusinessCode(payload.Code)
	if err != nil {
		return ezOpenPlayInfo{}, fmt.Errorf("EZOpen live-url code invalid: %w", err)
	}
	if !present {
		code, present, err = parseBusinessCode(payload.Meta.Code)
		if err != nil {
			return ezOpenPlayInfo{}, fmt.Errorf("EZOpen live-url meta.code invalid: %w", err)
		}
	}
	if code != 0 && code != 200 {
		return ezOpenPlayInfo{}, fmt.Errorf("EZOpen live-url business code %d", code)
	}
	var stream, rawURL string
	if payload.Ext.Token != "" {
		stream = payload.Ext.Token
		if err := json.Unmarshal(payload.Data, &rawURL); err != nil {
			return ezOpenPlayInfo{}, fmt.Errorf("EZOpen live-url data invalid")
		}
	} else {
		var dataObject struct {
			Token string `json:"token"`
			URL   string `json:"url"`
		}
		if err := json.Unmarshal(payload.Data, &dataObject); err != nil {
			return ezOpenPlayInfo{}, fmt.Errorf("EZOpen live-url data invalid")
		}
		stream, rawURL = dataObject.Token, dataObject.URL
	}
	if stream == "" || rawURL == "" {
		return ezOpenPlayInfo{}, fmt.Errorf("EZOpen live-url has no stream")
	}
	playURL := appendRawQuery(rawURL, "ssn", stream)
	playURL = appendRawQuery(playURL, "auth", "1")
	playURL = appendRawQuery(playURL, "biz", "4")
	playURL = appendRawQuery(playURL, "cln", "100")
	parsed, err := url.Parse(playURL)
	if err != nil {
		return ezOpenPlayInfo{}, err
	}
	scheme := "wss"
	if parsed.Scheme == "http" || parsed.Scheme == "ws" {
		scheme = "ws"
	}
	connect := &url.URL{Scheme: scheme, Host: parsed.Host}
	return ezOpenPlayInfo{connectURL: connect.String(), playURL: parsed.EscapedPath() + "?" + parsed.RawQuery + "&lid=" + randomUUIDForSource()}, nil
}

func parseBusinessCode(raw json.RawMessage) (int, bool, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false, nil
	}
	var number int
	if json.Unmarshal(raw, &number) == nil {
		return number, true, nil
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		number, err := strconv.Atoi(strings.TrimSpace(text))
		if err != nil {
			return 0, true, err
		}
		return number, true, nil
	}
	return 0, true, fmt.Errorf("expected numeric or string code")
}

func withEZOpenWebSocketQuery(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	// EZUIKit's WebSocket adapter requires these connection parameters before
	// it accepts the subsequent realplay command.
	parsed.RawQuery = "version=0.1&cipherSuites=0&sessionID="
	return parsed.String()
}

func appendRawQuery(raw, key, value string) string {
	separator := "?"
	if strings.Contains(raw, "?") {
		separator = "&"
	}
	return raw + separator + key + "=" + value
}

func randomUUIDForSource() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		return "00000000-0000-4000-8000-000000000000"
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func stringField(values map[string]json.RawMessage, key string) (string, bool) {
	var value string
	raw, ok := values[key]
	if !ok || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

func validCipherSuite(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var number int
	if json.Unmarshal(raw, &number) == nil {
		return number == 0
	}
	var value string
	return json.Unmarshal(raw, &value) == nil && value == "0"
}

func createRealplayCommand(timestamp int64, randValue, pkd, playURL string) (map[string]any, error) {
	return createRealplayCommandInternal(timestamp, randValue, pkd, playURL, nil)
}

func createRealplayCommandWithRandom(timestamp int64, randValue, pkd, playURL string, random io.Reader) (map[string]any, error) {
	return createRealplayCommandInternal(timestamp, randValue, pkd, playURL, random)
}

func createRealplayCommandInternal(timestamp int64, randValue, pkd, playURL string, random io.Reader) (map[string]any, error) {
	key, err := aesHex(strconv.FormatInt(timestamp, 10), "1234567891234567123456789123456712345678912345671234567891234567", "12345678912345671234567891234567")
	if err != nil {
		return nil, err
	}
	if len(key) < 64 {
		key += key
	}
	iv, err := aesHex(strconv.FormatInt(timestamp, 10), "12345678912345671234567891234567", "12345678912345671234567891234567")
	if err != nil {
		return nil, err
	}
	modulusHex := strings.SplitN(pkd, "|", 2)[0]
	modulus, ok := new(big.Int).SetString(modulusHex, 16)
	if !ok || modulus.Sign() <= 0 || len(modulusHex)%2 != 0 || len(modulusHex) < 256 || len(modulusHex) > 2048 {
		return nil, fmt.Errorf("EZOpen PKD modulus invalid")
	}
	publicKey := &rsa.PublicKey{N: modulus, E: 65537}
	if random == nil {
		random = crand.Reader
	}
	// Preserve the EZOpen compatibility behavior because some deployed PKD
	// values are even, which crypto/rsa quite correctly rejects as non-standard
	// RSA keys. The user-agent string above is also kept for wire compatibility.
	ciphertext, err := deterministicRSAEncryptPKCS1v15(random, publicKey, []byte(iv+":"+key))
	if err != nil {
		return nil, fmt.Errorf("EZOpen RSA command: %w", err)
	}
	authorization, err := aesHex(randValue+":", key, iv)
	if err != nil {
		return nil, err
	}
	token, err := aesHex("", key, iv)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"sequence":      0,
		"cmd":           "realplay",
		"url":           playURL,
		"key":           hex.EncodeToString(ciphertext),
		"authorization": authorization,
		"token":         token,
	}, nil
}

func deterministicRSAEncryptPKCS1v15(random io.Reader, publicKey *rsa.PublicKey, message []byte) ([]byte, error) {
	blockLength := publicKey.Size()
	if len(message) > blockLength-11 {
		return nil, rsa.ErrMessageTooLong
	}
	em := make([]byte, blockLength)
	em[1] = 2
	padding := em[2 : blockLength-len(message)-1]
	for index := range padding {
		var value [1]byte
		for value[0] == 0 {
			if _, err := io.ReadFull(random, value[:]); err != nil {
				return nil, err
			}
		}
		padding[index] = value[0]
	}
	em[blockLength-len(message)-1] = 0
	copy(em[blockLength-len(message):], message)
	value := new(big.Int).Exp(new(big.Int).SetBytes(em), big.NewInt(int64(publicKey.E)), publicKey.N)
	encoded := value.Bytes()
	result := make([]byte, blockLength)
	copy(result[blockLength-len(encoded):], encoded)
	return result, nil
}

func aesHex(plaintext, keyHex, ivHex string) (string, error) {
	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return "", err
	}
	iv, err := hex.DecodeString(ivHex)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	if len(iv) != block.BlockSize() {
		return "", fmt.Errorf("invalid AES IV")
	}
	padding := block.BlockSize() - len(plaintext)%block.BlockSize()
	data := append([]byte(plaintext), make([]byte, padding)...)
	for i := len(data) - padding; i < len(data); i++ {
		data[i] = byte(padding)
	}
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(data, data)
	return hex.EncodeToString(data), nil
}
