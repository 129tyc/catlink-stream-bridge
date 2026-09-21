package bridge

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/pion/rtp"
)

func TestRTSPPublisherLoopbackTCP(t *testing.T) {
	transport := newRecordingTalkTransport()
	publisher := NewRTSPPublisher("127.0.0.1:0", func(TalkStart) TalkTransport { return transport })
	var listener net.Listener
	publisher.server.Listen = func(network, address string) (net.Listener, error) {
		var err error
		listener, err = net.Listen(network, address)
		return listener, err
	}
	if err := publisher.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer publisher.Shutdown()
	publication, err := publisher.Open("/loop", nil, nil, nil, 1000, &TalkTarget{
		AccountID: "account",
		Device:    ResolvedDevice{EZOpenSerial: "serial"},
		Channel:   1,
		SnapshotProvider: newTalkSnapshotProvider(func(_ context.Context, target TalkTarget) (TalkStart, error) {
			return TalkStart{CameraToken: "camera", Device: target.Device, Channel: target.Channel}, nil
		}, nil),
	})
	if err != nil {
		t.Fatalf("open publication: %v", err)
	}
	defer publisher.ClosePublication(publication)

	serverURL, err := url.Parse("rtsp://" + listener.Addr().String() + "/loop")
	if err != nil {
		t.Fatal(err)
	}
	rtspClient := gortsplib.Client{Scheme: serverURL.Scheme, Host: serverURL.Host, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
	protocol := gortsplib.ProtocolTCP
	rtspClient.Protocol = &protocol
	if err := rtspClient.Start(); err != nil {
		t.Fatalf("client start: %v", err)
	}
	defer rtspClient.Close()
	desc, _, err := rtspClient.Describe(&base.URL{Scheme: serverURL.Scheme, Host: serverURL.Host, Path: serverURL.Path})
	if err != nil || len(desc.Medias) != 3 {
		t.Fatalf("describe medias=%d err=%v", len(desc.Medias), err)
	}
	// The publisher advertises the backchannel even when the first DESCRIBE
	// did not request it, matching cameras that go2rtc can auto-discover.
	rtspClient.RequestBackChannels = true
	var backchannel *description.Media
	for _, media := range desc.Medias {
		if media.IsBackChannel {
			backchannel = media
		}
	}
	if backchannel == nil {
		t.Fatal("backchannel media missing")
	}
	if err := rtspClient.SetupAll(desc.BaseURL, desc.Medias); err != nil {
		t.Fatalf("setup: %v", err)
	}
	video, err := publication.EncodeVideo([][]byte{{0x26, 0x01, 0x01}})
	if err != nil {
		t.Fatalf("encode video: %v", err)
	}
	videoPackets := make(chan struct{}, 1)
	audioPackets := make(chan struct{}, 1)
	rtspClient.OnPacketRTPAny(func(_ *description.Media, forma format.Format, _ *rtp.Packet) {
		switch forma.PayloadType() {
		case 96:
			select {
			case videoPackets <- struct{}{}:
			default:
			}
		case 8:
			select {
			case audioPackets <- struct{}{}:
			default:
			}
		}
	})
	if _, err := rtspClient.Play(nil); err != nil {
		t.Fatalf("play: %v", err)
	}
	if err := rtspClient.WritePacketRTP(backchannel, &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 8, Timestamp: 0}, Payload: []byte{1, 2, 3}}); err != nil {
		t.Fatalf("write backchannel: %v", err)
	}
	select {
	case audio := <-transport.writes:
		if string(audio.Payload) != string([]byte{1, 2, 3}) {
			t.Fatalf("backchannel payload=%v", audio.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("backchannel RTP was not forwarded")
	}
	for _, packet := range video {
		packet.Timestamp = publication.VideoTimestamp(1000)
	}
	if err := publisher.WriteVideoAccessUnit(publication, video, time.Now(), true); err != nil {
		t.Fatalf("write video: %v", err)
	}
	audio := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 8, Timestamp: 8000}, Payload: make([]byte, 160)}
	if err := publisher.WriteAudio(publication, audio, time.Now()); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	select {
	case <-videoPackets:
	case <-time.After(time.Second):
		t.Fatal("did not receive video RTP")
	}
	select {
	case <-audioPackets:
	case <-time.After(time.Second):
		t.Fatal("did not receive audio RTP")
	}
}

func TestRTSPPublisherAllowsRepeatedBackchannelSetup(t *testing.T) {
	transport := newRecordingTalkTransport()
	publisher := NewRTSPPublisher("127.0.0.1:0", func(TalkStart) TalkTransport { return transport })
	var listener net.Listener
	publisher.server.Listen = func(network, address string) (net.Listener, error) {
		var err error
		listener, err = net.Listen(network, address)
		return listener, err
	}
	if err := publisher.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer publisher.Shutdown()
	publication, err := publisher.Open("/repeat", nil, nil, nil, 1000, &TalkTarget{
		AccountID: "account",
		Device:    ResolvedDevice{EZOpenSerial: "serial"},
		Channel:   1,
		SnapshotProvider: newTalkSnapshotProvider(func(_ context.Context, target TalkTarget) (TalkStart, error) {
			return TalkStart{CameraToken: "camera", Device: target.Device, Channel: target.Channel}, nil
		}, nil),
	})
	if err != nil {
		t.Fatalf("open publication: %v", err)
	}
	defer publisher.ClosePublication(publication)

	address := listener.Addr().String()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)

	response := sendRawRTSP(t, conn, reader, fmt.Sprintf("DESCRIBE rtsp://%s/repeat RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n", address))
	if response.status != 200 {
		t.Fatalf("describe status=%d body=%s", response.status, response.body)
	}
	session := ""
	backchannelTransport := ""
	for sequence, trackID := range []string{"0", "1", "2"} {
		response = sendRawRTSP(t, conn, reader, fmt.Sprintf("SETUP rtsp://%s/repeat/trackID=%s RTSP/1.0\r\nCSeq: %d\r\nTransport: RTP/AVP/TCP;unicast;interleaved=%d-%d\r\n%s\r\n", address, trackID, sequence+2, sequence*2, sequence*2+1, sessionHeader(session)))
		if response.status != 200 {
			t.Fatalf("setup track=%s status=%d body=%s", trackID, response.status, response.body)
		}
		if session == "" {
			session = strings.Split(response.headers["session"], ";")[0]
		}
		if trackID == "2" {
			backchannelTransport = response.headers["transport"]
		}
	}

	response = sendRawRTSP(t, conn, reader, fmt.Sprintf("SETUP rtsp://%s/repeat/trackID=2 RTSP/1.0\r\nCSeq: 5\r\nTransport: RTP/AVP/TCP;unicast;interleaved=4-5\r\nSession: %s\r\n\r\n", address, session))
	if response.status != 200 {
		t.Fatalf("repeated backchannel setup status=%d body=%s", response.status, response.body)
	}
	if !strings.Contains(response.headers["transport"], "interleaved=4-5") {
		t.Fatalf("repeated setup transport=%q", response.headers["transport"])
	}
	if response.headers["transport"] != backchannelTransport {
		t.Fatalf("repeated setup changed transport from %q to %q", backchannelTransport, response.headers["transport"])
	}

	response = sendRawRTSP(t, conn, reader, fmt.Sprintf("PLAY rtsp://%s/repeat/ RTSP/1.0\r\nCSeq: 7\r\nSession: %s\r\n\r\n", address, session))
	if response.status != 200 {
		t.Fatalf("play status=%d body=%s", response.status, response.body)
	}

	packet, err := (&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 8, SequenceNumber: 1, Timestamp: 160}, Payload: []byte{1, 2, 3}}).Marshal()
	if err != nil {
		t.Fatalf("marshal backchannel RTP: %v", err)
	}
	frame := make([]byte, 4+len(packet))
	frame[0] = '$'
	frame[1] = 4
	binary.BigEndian.PutUint16(frame[2:4], uint16(len(packet)))
	copy(frame[4:], packet)
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write backchannel RTP: %v", err)
	}
	select {
	case audio := <-transport.writes:
		if string(audio.Payload) != string([]byte{1, 2, 3}) {
			t.Fatalf("backchannel payload=%v", audio.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("repeated setup backchannel RTP was not forwarded")
	}

	response = sendRawRTSP(t, conn, reader, fmt.Sprintf("SETUP rtsp://%s/repeat/trackID=2 RTSP/1.0\r\nCSeq: 8\r\nTransport: RTP/AVP/TCP;unicast;interleaved=6-7\r\nSession: %s\r\n\r\n", address, session))
	if response.status != 400 {
		t.Fatalf("different-pair repeated setup status=%d body=%s", response.status, response.body)
	}
}

type rawRTSPResponse struct {
	status  int
	headers map[string]string
	body    string
}

func sendRawRTSP(t *testing.T, conn net.Conn, reader *bufio.Reader, request string) rawRTSPResponse {
	t.Helper()
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatalf("write RTSP request: %v", err)
	}
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read RTSP status: %v", err)
	}
	parts := strings.SplitN(strings.TrimSpace(statusLine), " ", 3)
	if len(parts) < 2 {
		t.Fatalf("invalid RTSP status line %q", statusLine)
	}
	status, err := strconv.Atoi(parts[1])
	if err != nil {
		t.Fatalf("invalid RTSP status code in %q: %v", statusLine, err)
	}
	headers := make(map[string]string)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read RTSP header: %v", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			t.Fatalf("invalid RTSP header %q", line)
		}
		headers[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(value)
	}
	length, _ := strconv.Atoi(headers["content-length"])
	body := make([]byte, length)
	if _, err := io.ReadFull(reader, body); err != nil {
		t.Fatalf("read RTSP body: %v", err)
	}
	return rawRTSPResponse{status: status, headers: headers, body: string(body)}
}

func sessionHeader(session string) string {
	if session == "" {
		return ""
	}
	return "Session: " + session + "\r\n"
}

func TestRTSPPublisherBindsSessionToFirstPublication(t *testing.T) {
	first := &Publication{stream: new(gortsplib.ServerStream)}
	second := &Publication{stream: new(gortsplib.ServerStream)}
	session := new(gortsplib.ServerSession)
	publisher := &RTSPPublisher{
		paths:    map[string]*Publication{"/camera": first},
		bindings: make(map[*gortsplib.ServerSession]*Publication),
	}

	_, stream, err := publisher.OnSetup(&gortsplib.ServerHandlerOnSetupCtx{Session: session, Path: "/camera"})
	if err != nil || stream != first.stream {
		t.Fatalf("first setup stream=%p err=%v", stream, err)
	}
	publisher.paths["/camera"] = second
	_, stream, err = publisher.OnSetup(&gortsplib.ServerHandlerOnSetupCtx{Session: session, Path: "/camera"})
	if err != nil || stream != first.stream {
		t.Fatalf("bound setup switched stream=%p err=%v", stream, err)
	}
}

func TestRTSPPublisherReplacesPublicationAtExplicitBoundary(t *testing.T) {
	publisher := NewRTSPPublisher("127.0.0.1:0")
	if err := publisher.Start(); err != nil {
		t.Fatalf("start publisher: %v", err)
	}
	defer publisher.Shutdown()
	publication, err := publisher.Open("/boundary", nil, nil, nil, 0, nil)
	if err != nil {
		t.Fatalf("open publication: %v", err)
	}
	if publisher.lookup("/boundary") != publication {
		t.Fatal("publication was not registered")
	}
	live, err := publisher.OpenOrReplace("/boundary", nil, nil, nil, 1, nil)
	if err != nil {
		t.Fatalf("replace publication: %v", err)
	}
	if live == publication || publisher.lookup("/boundary") != live {
		t.Fatal("replacement did not become the active publication")
	}
	publisher.ClosePublication(live)
	if publisher.lookup("/boundary") != nil {
		t.Fatal("closed publication remained registered")
	}
}
