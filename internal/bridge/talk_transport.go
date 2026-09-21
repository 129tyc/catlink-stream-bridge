package bridge

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/gopus"
)

const (
	talkURLPath           = "/api/lapp/live/talk/url"
	talkWebSocketProtocol = "rtcgw-protocol"
	talkPlugin            = "rtcgw.plugin.tts"
	talkKeepalivePeriod   = 25 * time.Second
	talkOpusSampleRate    = 48000
	talkOpusChannels      = 2
	talkOpusFrameSamples  = 960
	talkOpusFramePCM      = talkOpusFrameSamples * talkOpusChannels
)

var talkTransactionID atomic.Uint64

var (
	ErrTalkAuthorization   = errors.New("CATLINK talk authorization failed")
	ErrTalkSessionInactive = errors.New("CATLINK talk session inactive")
)

// NewC07TalkTransportFactory creates the production pure-Go talk transport.
// The factory does not own CATLINK login state; TalkStart contains the
// already-resolved camera token and the transport obtains a fresh talk URL
// for each new WebRTC session.
func NewC07TalkTransportFactory(client *http.Client, openDomain string) TalkTransportFactory {
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	return func(TalkStart) TalkTransport {
		return newC07TalkTransport(client, strings.TrimRight(openDomain, "/"))
	}
}

type talkURLSession struct {
	RTCURL string
	TTSURL string
	Stream string
}

type talkGatewayMessage struct {
	RTCGW       string          `json:"rtcgw"`
	Transaction string          `json:"transaction"`
	SessionID   uint64          `json:"session_id"`
	HandleID    uint64          `json:"handle_id"`
	Data        json.RawMessage `json:"data"`
	JSEP        json.RawMessage `json:"jsep"`
	Error       struct {
		Code   json.RawMessage `json:"code"`
		Reason string          `json:"reason"`
	} `json:"error"`
	PluginData struct {
		Plugin string          `json:"plugin"`
		Data   json.RawMessage `json:"data"`
	} `json:"plugindata"`
}

type talkPendingRequest struct {
	response chan talkGatewayMessage
}

type c07TalkTransport struct {
	client     *http.Client
	openDomain string

	mu      sync.Mutex
	ws      *websocket.Conn
	peer    *webrtc.PeerConnection
	track   *webrtc.TrackLocalStaticRTP
	encoder *pcmaOpusEncoder
	session uint64
	handle  uint64

	writeMu sync.Mutex

	pendingMu sync.Mutex
	pending   map[string]talkPendingRequest

	readDone  chan struct{}
	readErrMu sync.Mutex
	readErr   error

	remoteJSEP   chan json.RawMessage
	remoteICE    chan webrtc.ICECandidateInit
	talkUp       chan struct{}
	asyncErr     chan error
	sessionErr   chan error
	closed       chan struct{}
	closeOnce    sync.Once
	stopMu       sync.Mutex
	stopped      bool
	stopResult   error
	connected    chan struct{}
	connectedOne sync.Once
	stateErr     chan error
}

func newC07TalkTransport(client *http.Client, openDomain string) *c07TalkTransport {
	return &c07TalkTransport{
		client:     client,
		openDomain: openDomain,
		pending:    make(map[string]talkPendingRequest),
		readDone:   make(chan struct{}),
		remoteJSEP: make(chan json.RawMessage, 4),
		remoteICE:  make(chan webrtc.ICECandidateInit, 32),
		talkUp:     make(chan struct{}, 1),
		asyncErr:   make(chan error, 1),
		sessionErr: make(chan error, 1),
		closed:     make(chan struct{}),
		connected:  make(chan struct{}),
		stateErr:   make(chan error, 1),
	}
}

func (t *c07TalkTransport) Start(ctx context.Context, start TalkStart) (err error) {
	if start.CameraToken == "" || start.Device.EZOpenSerial == "" || start.Channel < 1 {
		return fmt.Errorf("talk start parameters incomplete")
	}
	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), talkStopTimeout)
			_ = t.cleanup(cleanupCtx)
			cancel()
		}
	}()

	session, err := t.fetchTalkURL(ctx, start)
	if err != nil {
		return err
	}
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{talkWebSocketProtocol}
	dialer.HandshakeTimeout = talkStartTimeout
	ws, _, err := dialer.DialContext(ctx, normalizeTalkRTCURL(session.RTCURL), nil)
	if err != nil {
		return fmt.Errorf("talk gateway websocket: %w", err)
	}
	t.mu.Lock()
	t.ws = ws
	t.mu.Unlock()
	go t.readLoop()

	talkToken := session.Stream
	if talkToken == "" {
		talkToken = start.TalkStreamToken
	}
	if talkToken == "" {
		talkToken = start.CameraToken
	}
	create, err := t.request(ctx, map[string]any{
		"rtcgw":       "create",
		"token":       talkToken,
		"device":      start.Device.EZOpenSerial,
		"channel":     start.Channel,
		"authBizType": "TALK",
	})
	if err != nil {
		return fmt.Errorf("talk gateway create: %w", err)
	}
	sessionID := gatewayMessageID(create.SessionID, create.Data)
	if sessionID == 0 {
		return fmt.Errorf("talk gateway create returned no session")
	}
	t.mu.Lock()
	t.session = sessionID
	t.mu.Unlock()
	go t.keepAlive()
	attach, err := t.request(ctx, map[string]any{
		"rtcgw":      "attach",
		"session_id": sessionID,
		"plugin":     talkPlugin,
		"opaque_id":  fmt.Sprintf("catlink-talk-%d", time.Now().UnixNano()),
	})
	if err != nil {
		return fmt.Errorf("talk gateway attach: %w", err)
	}
	handleID := gatewayMessageID(attach.HandleID, attach.Data)
	if handleID == 0 {
		return fmt.Errorf("talk gateway attach returned no handle")
	}
	t.mu.Lock()
	t.handle = handleID
	t.mu.Unlock()

	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return fmt.Errorf("register WebRTC codecs: %w", err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine))
	peer, err := api.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	})
	if err != nil {
		return fmt.Errorf("create WebRTC peer: %w", err)
	}
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeOpus,
			ClockRate: talkOpusSampleRate,
			Channels:  talkOpusChannels,
		},
		"audio", "catlink-talk",
	)
	if err != nil {
		peer.Close()
		return fmt.Errorf("create talk audio track: %w", err)
	}
	if _, err := peer.AddTrack(track); err != nil {
		peer.Close()
		return fmt.Errorf("add talk audio track: %w", err)
	}
	encoder, err := newPCMAOpusEncoder(track)
	if err != nil {
		peer.Close()
		return err
	}
	t.mu.Lock()
	t.peer = peer
	t.track = track
	t.encoder = encoder
	t.mu.Unlock()

	peer.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go drainRemoteTalkTrack(remote)
	})
	peer.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		payload := talkNotification("trickle", sessionID, handleID)
		if candidate == nil {
			payload["candidate"] = map[string]any{"completed": true}
		} else {
			payload["candidate"] = candidate.ToJSON()
		}
		if sendErr := t.sendJSON(context.Background(), payload); sendErr != nil {
			t.reportAsyncError(sendErr)
		}
	})
	peer.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			t.connectedOne.Do(func() { close(t.connected) })
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			t.reportSessionError(fmt.Errorf("WebRTC connection state %s", state))
			select {
			case t.stateErr <- fmt.Errorf("WebRTC connection state %s", state):
			default:
			}
		}
	})
	offer, err := peer.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create talk offer: %w", err)
	}
	if err := peer.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set talk offer: %w", err)
	}
	local := peer.LocalDescription()
	if local == nil {
		return fmt.Errorf("talk local description missing")
	}
	if _, err := t.request(ctx, map[string]any{
		"rtcgw":      "message",
		"session_id": sessionID,
		"handle_id":  handleID,
		"body": map[string]any{
			"request":     "start",
			"url":         makeTalkLink(session.TTSURL, start.Device.EZOpenSerial, start.Channel),
			"codec":       "opus",
			"dir":         "sendrecv",
			"audio_debug": 1,
			"url_version": "1",
		},
		"jsep": map[string]any{"type": local.Type.String(), "sdp": local.SDP},
	}); err != nil {
		return fmt.Errorf("talk gateway start: %w", err)
	}

	remoteSet := false
	ttsUp := false
	connected := false
	pendingRemoteICE := make([]webrtc.ICECandidateInit, 0, 4)
	for !(remoteSet && ttsUp && connected) {
		select {
		case raw := <-t.remoteJSEP:
			var description webrtc.SessionDescription
			if err := json.Unmarshal(raw, &description); err != nil {
				return fmt.Errorf("decode talk remote JSEP: %w", err)
			}
			if err := peer.SetRemoteDescription(description); err != nil {
				return fmt.Errorf("set talk remote JSEP: %w", err)
			}
			remoteSet = true
			for _, candidate := range pendingRemoteICE {
				if err := peer.AddICECandidate(candidate); err != nil {
					return fmt.Errorf("add talk remote ICE: %w", err)
				}
			}
			pendingRemoteICE = pendingRemoteICE[:0]
			draining := true
			for draining {
				select {
				case candidate := <-t.remoteICE:
					if err := peer.AddICECandidate(candidate); err != nil {
						return fmt.Errorf("add talk remote ICE: %w", err)
					}
				default:
					draining = false
				}
			}
		case candidate := <-t.remoteICE:
			if remoteSet {
				if err := peer.AddICECandidate(candidate); err != nil {
					return fmt.Errorf("add talk remote ICE: %w", err)
				}
			} else {
				pendingRemoteICE = append(pendingRemoteICE, candidate)
			}
		case <-t.talkUp:
			ttsUp = true
		case <-t.connected:
			connected = true
		case err := <-t.stateErr:
			return err
		case err := <-t.sessionErr:
			return err
		case <-t.readDone:
			return fmt.Errorf("talk gateway read: %w", t.readError())
		case err := <-t.asyncErr:
			return fmt.Errorf("talk gateway transport: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	go t.monitorSession(peer)
	return nil
}

func (t *c07TalkTransport) WriteAudio(ctx context.Context, audio TalkAudio) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case err := <-t.sessionErr:
		return err
	case err := <-t.asyncErr:
		return err
	case <-t.readDone:
		return ErrTalkSessionInactive
	default:
	}
	t.mu.Lock()
	encoder := t.encoder
	t.mu.Unlock()
	if encoder == nil {
		return ErrTalkTransportUnavailable
	}
	return encoder.Write(ctx, audio)
}

func (t *c07TalkTransport) monitorSession(peer *webrtc.PeerConnection) {
	for {
		select {
		case raw := <-t.remoteJSEP:
			var description webrtc.SessionDescription
			if err := json.Unmarshal(raw, &description); err != nil {
				t.reportSessionError(fmt.Errorf("decode talk remote JSEP: %w", err))
				return
			}
			if err := peer.SetRemoteDescription(description); err != nil {
				t.reportSessionError(fmt.Errorf("set talk remote JSEP: %w", err))
				return
			}
		case candidate := <-t.remoteICE:
			if err := peer.AddICECandidate(candidate); err != nil {
				t.reportSessionError(fmt.Errorf("add talk remote ICE: %w", err))
				return
			}
		case <-t.talkUp:
		case err := <-t.stateErr:
			t.reportSessionError(err)
			return
		case err := <-t.asyncErr:
			t.reportSessionError(err)
			return
		case <-t.readDone:
			t.reportSessionError(fmt.Errorf("talk gateway read: %w", t.readError()))
			return
		case <-t.closed:
			return
		}
	}
}

func (t *c07TalkTransport) Stop(ctx context.Context) error {
	t.stopMu.Lock()
	if t.stopped {
		err := t.stopResult
		t.stopMu.Unlock()
		return err
	}
	t.stopped = true
	t.stopMu.Unlock()
	err := t.cleanup(ctx)
	t.stopMu.Lock()
	t.stopResult = err
	t.stopMu.Unlock()
	return err
}

func (t *c07TalkTransport) Close() {
	t.closeOnce.Do(func() {
		close(t.closed)
		t.mu.Lock()
		peer := t.peer
		ws := t.ws
		t.peer = nil
		t.ws = nil
		t.mu.Unlock()
		if peer != nil {
			_ = peer.Close()
		}
		if ws != nil {
			t.writeMu.Lock()
			_ = ws.Close()
			t.writeMu.Unlock()
		}
	})
}

func (t *c07TalkTransport) cleanup(ctx context.Context) error {
	t.mu.Lock()
	ws := t.ws
	session := t.session
	handle := t.handle
	t.mu.Unlock()
	if ws == nil {
		t.Close()
		return nil
	}
	var firstErr error
	if session != 0 && handle != 0 {
		if _, err := t.request(ctx, map[string]any{
			"rtcgw":      "detach",
			"session_id": session,
			"handle_id":  handle,
		}); err != nil {
			firstErr = err
		}
	}
	if session != 0 {
		if _, err := t.request(ctx, map[string]any{
			"rtcgw":      "destroy",
			"session_id": session,
		}); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	t.Close()
	return firstErr
}

func (t *c07TalkTransport) fetchTalkURL(ctx context.Context, start TalkStart) (talkURLSession, error) {
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for key, value := range map[string]string{
		"accessToken":  start.CameraToken,
		"deviceSerial": start.Device.EZOpenSerial,
		"channelNo":    strconv.Itoa(start.Channel),
	} {
		if err := form.WriteField(key, value); err != nil {
			return talkURLSession{}, fmt.Errorf("build talk URL form: %w", err)
		}
	}
	if err := form.Close(); err != nil {
		return talkURLSession{}, fmt.Errorf("close talk URL form: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.openDomain+talkURLPath, &body)
	if err != nil {
		return talkURLSession{}, err
	}
	request.Header.Set("Content-Type", form.FormDataContentType())
	request.Header.Set("Accept", "application/json")
	response, err := t.client.Do(request)
	if err != nil {
		return talkURLSession{}, sanitizedTalkTransportError(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 128<<10))
	if err != nil {
		return talkURLSession{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
			return talkURLSession{}, fmt.Errorf("%w: talk URL status %d", ErrTalkAuthorization, response.StatusCode)
		}
		return talkURLSession{}, fmt.Errorf("talk URL status %d", response.StatusCode)
	}
	var payload struct {
		Code json.RawMessage `json:"code"`
		Msg  string          `json:"msg"`
		Data struct {
			RTCURL string `json:"rtcUrl"`
			TTSURL string `json:"ttsUrl"`
			Stream string `json:"stream"`
		} `json:"data"`
	}
	if err := json.Unmarshal(responseBody, &payload); err != nil {
		return talkURLSession{}, fmt.Errorf("talk URL response invalid")
	}
	if talkJSONCode(payload.Code) != "200" {
		if isTalkAuthorizationCode(talkJSONCode(payload.Code)) {
			return talkURLSession{}, fmt.Errorf("%w: talk URL business code %s", ErrTalkAuthorization, talkJSONCode(payload.Code))
		}
		if payload.Msg == "" {
			return talkURLSession{}, fmt.Errorf("talk URL business code %s", talkJSONCode(payload.Code))
		}
		return talkURLSession{}, fmt.Errorf("talk URL rejected: %s", payload.Msg)
	}
	if payload.Data.RTCURL == "" || payload.Data.TTSURL == "" {
		return talkURLSession{}, fmt.Errorf("talk URL response missing gateway fields")
	}
	return talkURLSession{
		RTCURL: payload.Data.RTCURL,
		TTSURL: payload.Data.TTSURL,
		Stream: payload.Data.Stream,
	}, nil
}

func (t *c07TalkTransport) request(ctx context.Context, payload map[string]any) (talkGatewayMessage, error) {
	transaction := nextTalkTransaction()
	payload["transaction"] = transaction
	response := make(chan talkGatewayMessage, 1)
	t.pendingMu.Lock()
	t.pending[transaction] = talkPendingRequest{response: response}
	t.pendingMu.Unlock()
	if err := t.sendJSON(ctx, payload); err != nil {
		t.removePending(transaction)
		return talkGatewayMessage{}, err
	}
	select {
	case message := <-response:
		if message.RTCGW != "success" && message.RTCGW != "ack" {
			return talkGatewayMessage{}, gatewayMessageError(message)
		}
		return message, nil
	case <-t.readDone:
		return talkGatewayMessage{}, fmt.Errorf("talk gateway read: %w", t.readError())
	case <-t.closed:
		return talkGatewayMessage{}, errors.New("talk transport closed")
	case <-ctx.Done():
		t.removePending(transaction)
		return talkGatewayMessage{}, ctx.Err()
	}
}

func (t *c07TalkTransport) sendJSON(ctx context.Context, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	t.mu.Lock()
	ws := t.ws
	t.mu.Unlock()
	if ws == nil {
		return errors.New("talk gateway websocket is not connected")
	}
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	select {
	case <-t.closed:
		return errors.New("talk transport closed")
	default:
	}
	deadline := time.Now().Add(5 * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := ws.SetWriteDeadline(deadline); err != nil {
		return err
	}
	err = ws.WriteMessage(websocket.TextMessage, data)
	_ = ws.SetWriteDeadline(time.Time{})
	return err
}

func (t *c07TalkTransport) readLoop() {
	defer close(t.readDone)
	t.mu.Lock()
	ws := t.ws
	t.mu.Unlock()
	if ws == nil {
		t.setReadError(errors.New("talk gateway websocket is not connected"))
		return
	}
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			t.setReadError(err)
			return
		}
		var message talkGatewayMessage
		if err := json.Unmarshal(data, &message); err != nil {
			continue
		}
		if message.RTCGW == "hangup" || message.RTCGW == "timeout" || message.RTCGW == "detached" {
			t.reportSessionError(gatewayMessageError(message))
		}
		if message.RTCGW == "error" {
			t.reportSessionError(gatewayMessageError(message))
		}
		if message.RTCGW == "trickle" {
			var candidate struct {
				Completed bool            `json:"completed"`
				Candidate json.RawMessage `json:"candidate"`
			}
			if json.Unmarshal(data, &candidate) == nil && !candidate.Completed && len(candidate.Candidate) > 0 && string(candidate.Candidate) != "null" {
				var ice webrtc.ICECandidateInit
				if json.Unmarshal(candidate.Candidate, &ice) == nil {
					select {
					case t.remoteICE <- ice:
					case <-t.closed:
						return
					}
				}
			}
			continue
		}
		if len(message.JSEP) > 0 && string(message.JSEP) != "null" {
			select {
			case t.remoteJSEP <- message.JSEP:
			case <-t.closed:
				return
			}
		}
		if talkHasPluginData(message, "ttsup") {
			select {
			case t.talkUp <- struct{}{}:
			default:
			}
		}
		if message.Transaction != "" {
			t.pendingMu.Lock()
			pending, ok := t.pending[message.Transaction]
			if ok {
				delete(t.pending, message.Transaction)
			}
			t.pendingMu.Unlock()
			if ok {
				select {
				case pending.response <- message:
				case <-t.closed:
					return
				}
			}
		}
	}
}

func (t *c07TalkTransport) keepAlive() {
	ticker := time.NewTicker(talkKeepalivePeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := t.sendJSON(context.Background(), talkNotification("keepalive", t.sessionID(), 0)); err != nil {
				t.reportAsyncError(err)
			}
		case <-t.closed:
			return
		case <-t.readDone:
			return
		}
	}
}

func (t *c07TalkTransport) sessionID() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.session
}

func (t *c07TalkTransport) setReadError(err error) {
	t.readErrMu.Lock()
	t.readErr = err
	t.readErrMu.Unlock()
}

func (t *c07TalkTransport) readError() error {
	t.readErrMu.Lock()
	defer t.readErrMu.Unlock()
	if t.readErr == nil {
		return errors.New("connection closed")
	}
	return t.readErr
}

func (t *c07TalkTransport) reportAsyncError(err error) {
	select {
	case t.asyncErr <- err:
	default:
	}
}

func (t *c07TalkTransport) reportSessionError(err error) {
	select {
	case t.sessionErr <- err:
	default:
	}
}

func (t *c07TalkTransport) removePending(transaction string) {
	t.pendingMu.Lock()
	delete(t.pending, transaction)
	t.pendingMu.Unlock()
}

func gatewayMessageID(value uint64, raw json.RawMessage) uint64 {
	if value != 0 {
		return value
	}
	var data struct {
		ID uint64 `json:"id"`
	}
	if json.Unmarshal(raw, &data) == nil {
		return data.ID
	}
	return 0
}

func gatewayMessageError(message talkGatewayMessage) error {
	code := strings.Trim(string(message.Error.Code), `"`)
	if code == "" {
		code = "unknown"
	}
	reason := message.Error.Reason
	if reason == "" {
		reason = "gateway rejected request"
	}
	if isTalkAuthorizationCode(code) || strings.Contains(strings.ToLower(reason), "unauthor") || strings.Contains(strings.ToLower(reason), "token") {
		return fmt.Errorf("%w: gateway error %s: %s", ErrTalkAuthorization, code, reason)
	}
	return fmt.Errorf("gateway error %s: %s", code, reason)
}

func isTalkAuthorizationCode(code string) bool {
	return code == "401" || code == "403" || code == "1002"
}

func talkHasPluginData(message talkGatewayMessage, value string) bool {
	if message.PluginData.Plugin != talkPlugin && message.PluginData.Plugin != "" {
		return false
	}
	var data map[string]any
	if json.Unmarshal(message.PluginData.Data, &data) != nil {
		return false
	}
	return data["rtcgw"] == value || data["result"] == value
}

func nextTalkTransaction() string {
	return fmt.Sprintf("catlink-%d-%d", time.Now().UnixNano(), talkTransactionID.Add(1))
}

func talkNotification(kind string, session, handle uint64) map[string]any {
	payload := map[string]any{
		"rtcgw":       kind,
		"transaction": nextTalkTransaction(),
		"session_id":  session,
	}
	if handle != 0 {
		payload["handle_id"] = handle
	}
	return payload
}

func normalizeTalkRTCURL(value string) string {
	if strings.Contains(value, "ws") {
		return value
	}
	return strings.Replace(strings.Replace(value, "https", "wss", 1), "rtcgw", "rtcgw-ws", 1)
}

func makeTalkLink(ttsURL, serial string, channel int) string {
	value := ttsURL
	if !strings.HasPrefix(value, "tts://") {
		value = "tts://" + value
	}
	if index := strings.IndexByte(value, '?'); index >= 0 {
		value = value[:index]
	}
	return fmt.Sprintf("%s/talk?dev=%s&chann=%d&encodetype=2", value, serial, channel)
}

func talkJSONCode(raw json.RawMessage) string {
	value := strings.TrimSpace(string(raw))
	return strings.Trim(value, `"`)
}

func sanitizedTalkTransportError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("talk URL transport failure")
}

func drainRemoteTalkTrack(track *webrtc.TrackRemote) {
	for {
		if _, _, err := track.ReadRTP(); err != nil {
			return
		}
	}
}

type pcmaOpusEncoder struct {
	mu        sync.Mutex
	encoder   *gopus.Encoder
	track     *webrtc.TrackLocalStaticRTP
	pcm       []int16
	packet    []byte
	sequence  uint16
	timestamp uint32
	ssrc      uint32
	started   bool
}

func newPCMAOpusEncoder(track *webrtc.TrackLocalStaticRTP) (*pcmaOpusEncoder, error) {
	encoder, err := gopus.NewEncoder(gopus.EncoderConfig{
		SampleRate:  talkOpusSampleRate,
		Channels:    talkOpusChannels,
		Application: gopus.ApplicationVoIP,
	})
	if err != nil {
		return nil, fmt.Errorf("create pure-Go Opus encoder: %w", err)
	}
	if err := encoder.SetBitrate(24000); err != nil {
		return nil, fmt.Errorf("configure pure-Go Opus encoder: %w", err)
	}
	var ssrcBytes [4]byte
	if _, err := rand.Read(ssrcBytes[:]); err != nil {
		return nil, fmt.Errorf("generate talk SSRC: %w", err)
	}
	return &pcmaOpusEncoder{
		encoder: encoder,
		track:   track,
		pcm:     make([]int16, 0, talkOpusFramePCM*2),
		packet:  make([]byte, 4000),
		ssrc:    uint32(ssrcBytes[0])<<24 | uint32(ssrcBytes[1])<<16 | uint32(ssrcBytes[2])<<8 | uint32(ssrcBytes[3]),
	}, nil
}

func (e *pcmaOpusEncoder) Write(ctx context.Context, audio TalkAudio) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started && len(e.pcm) == 0 {
		e.timestamp = audio.RTPTime * 6
	}
	if !e.started {
		e.timestamp = audio.RTPTime * 6
		e.started = true
	}
	for _, value := range audio.Payload {
		if err := ctx.Err(); err != nil {
			return err
		}
		sample := alawToPCM16(value)
		for range 6 {
			e.pcm = append(e.pcm, sample, sample)
		}
	}
	for len(e.pcm) >= talkOpusFramePCM {
		encoded, err := e.encoder.EncodeInt16(e.pcm[:talkOpusFramePCM], e.packet)
		if err != nil {
			return fmt.Errorf("encode PCMA talk audio: %w", err)
		}
		packet := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				SequenceNumber: e.sequence,
				Timestamp:      e.timestamp,
				SSRC:           e.ssrc,
				Marker:         e.sequence == 0,
			},
			Payload: e.packet[:encoded],
		}
		if err := e.track.WriteRTP(packet); err != nil {
			return err
		}
		e.sequence++
		e.timestamp += talkOpusFrameSamples
		copy(e.pcm, e.pcm[talkOpusFramePCM:])
		e.pcm = e.pcm[:len(e.pcm)-talkOpusFramePCM]
	}
	return nil
}

func alawToPCM16(value byte) int16 {
	value ^= 0x55
	segment := int((value & 0x70) >> 4)
	sample := int(value&0x0f) << 4
	if segment == 0 {
		sample += 8
	} else {
		sample += 0x108
		sample <<= segment - 1
	}
	if value&0x80 != 0 {
		return int16(sample)
	}
	return int16(-sample)
}
