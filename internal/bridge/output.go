package bridge

import (
	"sync"
	"time"

	"github.com/pion/rtp"
)

const (
	outputVideoStaleAfter          = 5 * time.Second
	outputSourceVideoRecoveryAfter = 45 * time.Second
	outputStableLiveInterval       = 30 * time.Second
)

type routeOutput struct {
	publisher *RTSPPublisher
	health    *Health
	route     string
	path      string

	mu             sync.RWMutex
	publicationMu  sync.Mutex
	publication    *Publication
	mode           string
	talkTarget     *TalkTarget
	lastVideoMedia time.Time
	pausedSince    time.Time
	liveSince      time.Time
	hasAudioMedia  bool
	videoStale     bool
	ownerID        uint64
	stableLive     bool
}

func newRouteOutput(path, route string, publisher *RTSPPublisher, health *Health) *routeOutput {
	output := &routeOutput{
		publisher:   publisher,
		health:      health,
		route:       route,
		path:        path,
		mode:        "paused",
		pausedSince: time.Now(),
	}
	health.SetRoute(route, false)
	return output
}

func (o *routeOutput) close() {
	o.publicationMu.Lock()
	defer o.publicationMu.Unlock()
	o.mu.Lock()
	publication := o.publication
	o.publication = nil
	o.ownerID = 0
	o.mu.Unlock()
	if publication != nil {
		o.publisher.ClosePublication(publication)
	}
	o.health.SetRoute(o.route, false)
}

func (o *routeOutput) setTalkTarget(target *TalkTarget) {
	o.mu.Lock()
	o.talkTarget = cloneTalkTarget(target)
	o.mu.Unlock()
	o.publisher.SetTalkTarget(o.path, target)
}

// pauseFor stops sending media while retaining an already published stream.
// A later valid source may replace that publication after it has a complete
// codec configuration and keyframe. No synthetic media is generated here.
func (o *routeOutput) pauseFor(ownerID uint64, expected *Publication) {
	o.publicationMu.Lock()
	defer o.publicationMu.Unlock()
	o.mu.Lock()
	if (ownerID != 0 && o.ownerID != 0 && ownerID != o.ownerID) ||
		(expected != nil && expected != o.publication) {
		o.mu.Unlock()
		return
	}
	if o.mode == "live" && !o.liveSince.IsZero() && time.Since(o.liveSince) >= outputStableLiveInterval {
		o.stableLive = true
	}
	if o.mode != "paused" || o.pausedSince.IsZero() {
		o.pausedSince = time.Now()
	}
	o.ownerID = 0
	o.mode = "paused"
	o.lastVideoMedia = time.Time{}
	o.videoStale = false
	o.hasAudioMedia = false
	o.liveSince = time.Time{}
	ready := o.publication != nil
	o.mu.Unlock()
	o.health.SetRoute(o.route, ready)
}

func (o *routeOutput) consumeStableLive() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	stable := o.stableLive
	o.stableLive = false
	return stable
}

func (o *routeOutput) isLive() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.mode == "live"
}

func (o *routeOutput) readyForLocalOptimization(now time.Time) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.mode != "live" || o.liveSince.IsZero() || now.Sub(o.liveSince) < outputStableLiveInterval {
		return false
	}
	if o.lastVideoMedia.IsZero() || now.Sub(o.lastVideoMedia) >= outputVideoStaleAfter {
		return false
	}
	return o.hasAudioMedia
}

func (o *routeOutput) observeVideoFreshness(now, attemptStarted time.Time) (stale, recover, changed bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mode == "live" {
		progressSince := o.lastVideoMedia
		if progressSince.IsZero() {
			progressSince = o.liveSince
		}
		if !progressSince.IsZero() {
			stale = now.Sub(progressSince) >= outputVideoStaleAfter
			recover = now.Sub(progressSince) >= outputSourceVideoRecoveryAfter
		}
	} else {
		graceSince := o.pausedSince
		if graceSince.IsZero() || graceSince.Before(attemptStarted) {
			graceSince = attemptStarted
		}
		recover = !graceSince.IsZero() && now.Sub(graceSince) >= outputSourceVideoRecoveryAfter
	}
	changed = stale != o.videoStale
	o.videoStale = stale
	return stale, recover, changed
}

func (o *routeOutput) openLive(ownerID uint64, vps, sps, pps []byte, epochPTS uint64) (*Publication, error) {
	o.publicationMu.Lock()
	defer o.publicationMu.Unlock()
	o.mu.RLock()
	target := cloneTalkTarget(o.talkTarget)
	o.mu.RUnlock()
	publication, err := o.publisher.OpenOrReplace(o.path, vps, sps, pps, epochPTS, target)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	o.publication = publication
	o.ownerID = ownerID
	o.mode = "live"
	o.lastVideoMedia = time.Time{}
	o.pausedSince = time.Time{}
	o.liveSince = time.Time{}
	o.hasAudioMedia = false
	o.videoStale = false
	o.mu.Unlock()
	o.health.SetRoute(o.route, true)
	return publication, nil
}

type routeMediaPublisher struct {
	output    *routeOutput
	sessionID uint64
}

func (p routeMediaPublisher) Open(_ string, vps, sps, pps []byte, epochPTS uint64, _ *TalkTarget) (*Publication, error) {
	return p.output.openLive(p.sessionID, vps, sps, pps, epochPTS)
}

func (p routeMediaPublisher) ClosePublication(publication *Publication) {
	p.output.pauseFor(p.sessionID, publication)
}

func (p routeMediaPublisher) WriteVideo(publication *Publication, packet *rtp.Packet, ntp time.Time) error {
	p.output.mu.Lock()
	defer p.output.mu.Unlock()
	if p.output.publication != publication || p.output.mode != "live" {
		return nil
	}
	if err := p.output.publisher.WriteVideo(publication, packet, ntp); err != nil {
		return err
	}
	if p.output.liveSince.IsZero() {
		p.output.liveSince = time.Now()
	}
	p.output.lastVideoMedia = time.Now()
	return nil
}

func (p routeMediaPublisher) WriteVideoAccessUnit(publication *Publication, packets []*rtp.Packet, ntp time.Time, keyframe bool) error {
	p.output.mu.Lock()
	defer p.output.mu.Unlock()
	if p.output.publication != publication || p.output.mode != "live" {
		return nil
	}
	if err := p.output.publisher.WriteVideoAccessUnit(publication, packets, ntp, keyframe); err != nil {
		return err
	}
	if len(packets) > 0 {
		if p.output.liveSince.IsZero() {
			p.output.liveSince = time.Now()
		}
		p.output.lastVideoMedia = time.Now()
	}
	return nil
}

func (p routeMediaPublisher) WriteAudio(publication *Publication, packet *rtp.Packet, ntp time.Time) error {
	p.output.mu.Lock()
	defer p.output.mu.Unlock()
	if p.output.publication != publication || p.output.mode != "live" {
		return nil
	}
	if err := p.output.publisher.WriteAudio(publication, packet, ntp); err != nil {
		return err
	}
	p.output.hasAudioMedia = true
	return nil
}

func (p routeMediaPublisher) EncodeVideo(publication *Publication, nalus [][]byte) ([]*rtp.Packet, error) {
	p.output.mu.Lock()
	defer p.output.mu.Unlock()
	if p.output.publication != publication || p.output.mode != "live" {
		return nil, nil
	}
	return publication.EncodeVideo(nalus)
}
