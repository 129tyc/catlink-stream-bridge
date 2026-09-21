package bridge

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph265"
	"github.com/pion/rtp"
)

type Publication struct {
	stream      *gortsplib.ServerStream
	video       *description.Media
	audio       *description.Media
	backchannel *description.Media
	talkTarget  *TalkTarget
	videoEnc    rtph265.Encoder
	epochPTS    uint64
	epochWall   time.Time
	videoBase   uint32
}

func (p *Publication) EncodeVideo(nalus [][]byte) ([]*rtp.Packet, error) {
	return p.videoEnc.Encode(nalus)
}

func (p *Publication) VideoTimestamp(pts uint64) uint32 {
	if pts < p.epochPTS {
		return p.videoBase
	}
	return p.videoBase + uint32((pts-p.epochPTS)*90)
}

func (p *Publication) NTPForVideo(pts uint64) time.Time {
	return p.epochWall.Add(signedPTSDelta(pts, p.epochPTS))
}

func (p *Publication) NTPForAudio(pts uint64, offset uint32) time.Time {
	return p.epochWall.Add(signedPTSDelta(pts, p.epochPTS)).Add(time.Duration(offset) * time.Second / 8000)
}

func signedPTSDelta(value, base uint64) time.Duration {
	if value >= base {
		return time.Duration(value-base) * time.Millisecond
	}
	return -time.Duration(base-value) * time.Millisecond
}

type RTSPPublisher struct {
	server    *gortsplib.Server
	mu        sync.RWMutex
	paths     map[string]*Publication
	bindings  map[*gortsplib.ServerSession]*Publication
	sessions  map[*gortsplib.ServerSession]*rtspSessionPublication
	playBound map[*gortsplib.ServerSession]struct{}
	talks     *TalkRegistry
}

type rtspSessionPublication struct {
	stream     *gortsplib.ServerStream
	videoSetup bool
	audioSetup bool
	videoReady bool
}

// OnRequest advertises the RTSP backchannel during the initial DESCRIBE.
// gortsplib otherwise hides IsBackChannel media unless the client already
// knows to send the ONVIF backchannel Require header. go2rtc discovers this
// capability from the SDP, so advertising it here makes the bridge behave
// like cameras that expose their sendonly track unconditionally.
func (p *RTSPPublisher) OnRequest(_ *gortsplib.ServerConn, request *base.Request) {
	if request == nil || request.Method != base.Describe {
		return
	}
	for _, value := range request.Header["Require"] {
		if value == "www.onvif.org/ver20/backchannel" {
			return
		}
	}
	request.Header["Require"] = append(request.Header["Require"], "www.onvif.org/ver20/backchannel")
}

func NewRTSPPublisher(listen string, factory ...TalkTransportFactory) *RTSPPublisher {
	if listen == "" {
		listen = ":8554"
	}
	var talkFactory TalkTransportFactory
	if len(factory) > 0 {
		talkFactory = factory[0]
	}
	publisher := &RTSPPublisher{
		paths:     make(map[string]*Publication),
		bindings:  make(map[*gortsplib.ServerSession]*Publication),
		sessions:  make(map[*gortsplib.ServerSession]*rtspSessionPublication),
		playBound: make(map[*gortsplib.ServerSession]struct{}),
		talks:     NewTalkRegistry(talkFactory),
	}
	publisher.server = &gortsplib.Server{
		Handler:        publisher,
		RTSPAddress:    listen,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    60 * time.Second,
		WriteQueueSize: 512,
		MaxPacketSize:  1472,
	}
	return publisher
}

func (p *RTSPPublisher) Start() error { return p.server.Start() }

func (p *RTSPPublisher) Wait() error { return p.server.Wait() }

func (p *RTSPPublisher) Shutdown() {
	p.talks.Close()
	p.server.Close()
}

func (p *RTSPPublisher) Open(path string, vps, sps, pps []byte, epochPTS uint64, talkTarget *TalkTarget) (*Publication, error) {
	return p.open(path, vps, sps, pps, epochPTS, talkTarget, false)
}

func (p *RTSPPublisher) EncodeVideo(publication *Publication, nalus [][]byte) ([]*rtp.Packet, error) {
	if publication == nil {
		return nil, fmt.Errorf("publication is nil")
	}
	return publication.EncodeVideo(nalus)
}

func (p *RTSPPublisher) OpenOrReplace(path string, vps, sps, pps []byte, epochPTS uint64, talkTarget *TalkTarget) (*Publication, error) {
	return p.open(path, vps, sps, pps, epochPTS, talkTarget, true)
}

func (p *RTSPPublisher) open(path string, vps, sps, pps []byte, epochPTS uint64, talkTarget *TalkTarget, replace bool) (*Publication, error) {
	videoFormat := &format.H265{PayloadTyp: 96, VPS: append([]byte(nil), vps...), SPS: append([]byte(nil), sps...), PPS: append([]byte(nil), pps...)}
	audioFormat := &format.G711{PayloadTyp: 8, MULaw: false, SampleRate: 8000, ChannelCount: 1}
	video := &description.Media{Type: description.MediaTypeVideo, Formats: []format.Format{videoFormat}}
	audio := &description.Media{Type: description.MediaTypeAudio, Formats: []format.Format{audioFormat}}
	backchannelFormat := &format.G711{PayloadTyp: 8, MULaw: false, SampleRate: 8000, ChannelCount: 1}
	backchannel := &description.Media{Type: description.MediaTypeAudio, IsBackChannel: true, Formats: []format.Format{backchannelFormat}}
	stream := &gortsplib.ServerStream{
		Server: p.server,
		Desc:   &description.Session{Medias: []*description.Media{video, audio, backchannel}},
	}
	if err := stream.Initialize(); err != nil {
		return nil, err
	}
	baseVideo, err := randomUint32()
	if err != nil {
		stream.Close()
		return nil, err
	}
	encoder := rtph265.Encoder{PayloadType: 96, PayloadMaxSize: 1200}
	if err := encoder.Init(); err != nil {
		stream.Close()
		return nil, err
	}
	publication := &Publication{
		stream:      stream,
		video:       video,
		audio:       audio,
		backchannel: backchannel,
		talkTarget:  cloneTalkTarget(talkTarget),
		videoEnc:    encoder,
		epochPTS:    epochPTS,
		epochWall:   time.Now(),
		videoBase:   baseVideo,
	}
	p.mu.Lock()
	old := p.paths[path]
	if old != nil && !replace {
		p.mu.Unlock()
		stream.Close()
		return nil, fmt.Errorf("RTSP path is already published")
	}
	p.paths[path] = publication
	var oldSessions []*gortsplib.ServerSession
	var oldSessionStreams []*gortsplib.ServerStream
	if old != nil {
		for session, bound := range p.bindings {
			if bound == old {
				oldSessions = append(oldSessions, session)
				if sessionPublication := p.sessions[session]; sessionPublication != nil {
					oldSessionStreams = append(oldSessionStreams, sessionPublication.stream)
				}
				delete(p.bindings, session)
				delete(p.sessions, session)
				delete(p.playBound, session)
			}
		}
	}
	p.mu.Unlock()
	for _, session := range oldSessions {
		session.Close()
	}
	for _, sessionStream := range oldSessionStreams {
		sessionStream.Close()
	}
	if old != nil {
		old.stream.Close()
	}
	return publication, nil
}

func (p *RTSPPublisher) SetTalkTarget(path string, target *TalkTarget) {
	p.mu.Lock()
	if publication := p.paths[path]; publication != nil {
		publication.talkTarget = cloneTalkTarget(target)
	}
	p.mu.Unlock()
}

func (p *RTSPPublisher) ClosePublication(publication *Publication) {
	if publication == nil {
		return
	}
	p.mu.Lock()
	var sessions []*gortsplib.ServerSession
	var sessionStreams []*gortsplib.ServerStream
	for path, current := range p.paths {
		if current == publication {
			delete(p.paths, path)
			break
		}
	}
	for session, bound := range p.bindings {
		if bound == publication {
			sessions = append(sessions, session)
			if sessionPublication := p.sessions[session]; sessionPublication != nil {
				sessionStreams = append(sessionStreams, sessionPublication.stream)
			}
			delete(p.bindings, session)
			delete(p.sessions, session)
			delete(p.playBound, session)
		}
	}
	p.mu.Unlock()
	for _, session := range sessions {
		session.Close()
	}
	for _, sessionStream := range sessionStreams {
		sessionStream.Close()
	}
	publication.stream.Close()
}

func (p *RTSPPublisher) WriteVideo(publication *Publication, packet *rtp.Packet, ntp time.Time) error {
	return p.WriteVideoAccessUnit(publication, []*rtp.Packet{packet}, ntp, false)
}

func (p *RTSPPublisher) WriteVideoAccessUnit(publication *Publication, packets []*rtp.Packet, ntp time.Time, keyframe bool) error {
	p.mu.RLock()
	targets := make([]*gortsplib.ServerSession, 0)
	for session, bound := range p.bindings {
		if bound != publication {
			continue
		}
		if sessionPublication := p.sessions[session]; sessionPublication != nil &&
			sessionPublication.videoSetup && (sessionPublication.videoReady || keyframe) &&
			session.State() == gortsplib.ServerSessionStatePlay {
			targets = append(targets, session)
		}
	}
	p.mu.RUnlock()
	if len(targets) == 0 && p.server == nil {
		for _, packet := range packets {
			if err := publication.stream.WritePacketRTPWithNTP(publication.video, packet, ntp); err != nil {
				return err
			}
		}
		return nil
	}
	failed := make([]*gortsplib.ServerSession, 0)
	for _, target := range targets {
		success := true
		for _, packet := range packets {
			if err := target.WritePacketRTPWithNTP(publication.video, packet, ntp); err != nil {
				failed = append(failed, target)
				success = false
				break
			}
		}
		if keyframe && success {
			p.mu.Lock()
			if sessionPublication := p.sessions[target]; sessionPublication != nil {
				sessionPublication.videoReady = true
			}
			p.mu.Unlock()
		}
	}
	for _, target := range failed {
		target.Close()
	}
	return nil
}

func (p *RTSPPublisher) WriteAudio(publication *Publication, packet *rtp.Packet, ntp time.Time) error {
	p.mu.RLock()
	targets := make([]*gortsplib.ServerSession, 0)
	for session, bound := range p.bindings {
		if bound != publication {
			continue
		}
		if sessionPublication := p.sessions[session]; sessionPublication != nil &&
			sessionPublication.audioSetup && (!sessionPublication.videoSetup || sessionPublication.videoReady) &&
			session.State() == gortsplib.ServerSessionStatePlay {
			targets = append(targets, session)
		}
	}
	p.mu.RUnlock()
	if len(targets) == 0 && p.server == nil {
		return publication.stream.WritePacketRTPWithNTP(publication.audio, packet, ntp)
	}
	failed := make([]*gortsplib.ServerSession, 0)
	for _, target := range targets {
		if err := target.WritePacketRTPWithNTP(publication.audio, packet, ntp); err != nil {
			failed = append(failed, target)
		}
	}
	for _, target := range failed {
		target.Close()
	}
	return nil
}

func (p *RTSPPublisher) lookup(path string) *Publication {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.paths[path]
}

func (p *RTSPPublisher) OnDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	publication := p.lookup(ctx.Path)
	if publication == nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, publication.stream, nil
}

func (p *RTSPPublisher) OnSetup(ctx *gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	p.mu.Lock()
	if publication := p.bindings[ctx.Session]; publication != nil {
		session := p.sessions[ctx.Session]
		p.mu.Unlock()
		if session != nil {
			return &base.Response{StatusCode: base.StatusOK}, session.stream, nil
		}
		return &base.Response{StatusCode: base.StatusOK}, publication.stream, nil
	}
	publication := p.paths[ctx.Path]
	if publication == nil {
		p.mu.Unlock()
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	if p.server == nil {
		p.bindings[ctx.Session] = publication
		p.mu.Unlock()
		return &base.Response{StatusCode: base.StatusOK}, publication.stream, nil
	}
	sessionStream := &gortsplib.ServerStream{Server: p.server, Desc: publication.stream.Desc}
	if err := sessionStream.Initialize(); err != nil {
		p.mu.Unlock()
		return &base.Response{StatusCode: base.StatusInternalServerError}, nil, err
	}
	p.bindings[ctx.Session] = publication
	p.sessions[ctx.Session] = &rtspSessionPublication{stream: sessionStream}
	p.mu.Unlock()
	return &base.Response{StatusCode: base.StatusOK}, sessionStream, nil
}

func (p *RTSPPublisher) OnPlay(ctx *gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	p.mu.Lock()
	publication := p.bindings[ctx.Session]
	if publication == nil {
		p.mu.Unlock()
		return &base.Response{StatusCode: base.StatusBadRequest}, nil
	}
	_, alreadyBound := p.playBound[ctx.Session]
	talkTarget := cloneTalkTarget(publication.talkTarget)
	backchannel := publication.backchannel
	if !alreadyBound {
		p.playBound[ctx.Session] = struct{}{}
		if session := p.sessions[ctx.Session]; session != nil {
			medias := ctx.Session.Medias()
			for _, media := range medias {
				if media == publication.video {
					session.videoSetup = true
				}
				if media == publication.audio {
					session.audioSetup = true
				}
			}
			session.videoReady = !session.videoSetup
		}
	}
	p.mu.Unlock()
	if !alreadyBound && backchannel != nil && talkTarget != nil {
		ctx.Session.OnPacketRTPAny(func(media *description.Media, forma format.Format, packet *rtp.Packet) {
			if media != backchannel || !isTalkBackchannelFormat(forma, packet) {
				return
			}
			p.talks.Ingest(*talkTarget, ctx.Session, packet)
		})
	}
	return &base.Response{StatusCode: base.StatusOK}, nil
}

func (p *RTSPPublisher) OnSessionClose(ctx *gortsplib.ServerHandlerOnSessionCloseCtx) {
	p.talks.ReleaseSession(ctx.Session)
	p.mu.Lock()
	sessionPublication := p.sessions[ctx.Session]
	delete(p.bindings, ctx.Session)
	delete(p.sessions, ctx.Session)
	delete(p.playBound, ctx.Session)
	p.mu.Unlock()
	if sessionPublication != nil {
		sessionPublication.stream.Close()
	}
}

func (p *RTSPPublisher) OnStreamWriteError(ctx *gortsplib.ServerHandlerOnStreamWriteErrorCtx) {
	ctx.Session.Close()
}

func randomUint32() (uint32, error) {
	var value [4]byte
	if _, err := rand.Read(value[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(value[:]), nil
}

func randomUint16() (uint16, error) {
	var value [2]byte
	if _, err := rand.Read(value[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(value[:]), nil
}

func cloneTalkTarget(target *TalkTarget) *TalkTarget {
	if target == nil {
		return nil
	}
	copy := *target
	return &copy
}

func isTalkBackchannelFormat(forma format.Format, packet *rtp.Packet) bool {
	if packet == nil || packet.PayloadType != 8 {
		return false
	}
	g711, ok := forma.(*format.G711)
	return ok && !g711.MULaw && g711.SampleRate == 8000 && g711.ChannelCount == 1
}
