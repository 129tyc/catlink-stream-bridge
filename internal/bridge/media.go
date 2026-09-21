package bridge

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/129tyc/catlink-stream-bridge/internal/mpegps"
	"github.com/pion/rtp"
)

type MediaPublisher interface {
	Open(path string, vps, sps, pps []byte, epochPTS uint64, talkTarget *TalkTarget) (*Publication, error)
	ClosePublication(publication *Publication)
	EncodeVideo(publication *Publication, nalus [][]byte) ([]*rtp.Packet, error)
	WriteVideo(publication *Publication, packet *rtp.Packet, ntp time.Time) error
	WriteAudio(publication *Publication, packet *rtp.Packet, ntp time.Time) error
}

type videoAccessUnitWriter interface {
	WriteVideoAccessUnit(publication *Publication, packets []*rtp.Packet, ntp time.Time, keyframe bool) error
}

type Pipeline struct {
	publisher MediaPublisher
	path      string
	decoder   *mpegps.Decoder

	publication      *Publication
	vps, sps, pps    []byte
	auNalus          [][]byte
	auBytes          int
	auPTS            uint64
	haveAU           bool
	lastVideoPTS     uint64
	haveVideoPTS     bool
	mediaCount       uint64
	outputCount      uint64
	videoPTS         ptsUnwrapper
	audioPTS         ptsUnwrapper
	hasOutput        bool
	videoOutputCount uint64
	hasVideoOutput   bool
	fatal            error
	audio            *PCMAWriter
	talkTarget       *TalkTarget
}

var errConfigChanged = fmt.Errorf("H265 configuration changed")
var errPCMATimestampBackwards = fmt.Errorf("PCMA timestamp moved backwards")
var errPCMAOverflow = fmt.Errorf("PCMA pending data exceeded limit")

const (
	maxAccessUnitBytes  = 4 * 1024 * 1024
	maxAccessUnitNALUs  = 4096
	maxPCMAPendingBytes = 1 * 1024 * 1024
	mpegPTSPeriodMS     = uint64(95443717)
	mpegPTSHalfPeriodMS = uint64(47721858)
)

type ptsUnwrapper struct {
	last   uint64
	offset uint64
	have   bool
}

func (u *ptsUnwrapper) Unwrap(value uint64) (uint64, bool) {
	if u.have {
		switch {
		case value < u.last && u.last-value > mpegPTSHalfPeriodMS:
			u.offset += mpegPTSPeriodMS
		case value < u.last:
			return 0, false
		case value > u.last && value-u.last > mpegPTSHalfPeriodMS:
			// A late pre-wrap callback arrived after this track already
			// crossed the wrap. Do not turn it into a second epoch.
			return 0, false
		}
	}
	u.last = value
	u.have = true
	return value + u.offset, true
}

func NewPipeline(path string, publisher MediaPublisher, talkTarget ...*TalkTarget) (*Pipeline, error) {
	var target *TalkTarget
	if len(talkTarget) > 0 {
		target = talkTarget[0]
	}
	pipeline := &Pipeline{path: path, publisher: publisher, talkTarget: target}
	decoder, err := mpegps.NewDecoder(pipeline)
	if err != nil {
		return nil, err
	}
	pipeline.decoder = decoder
	return pipeline, nil
}

func (p *Pipeline) Feed(data []byte) error {
	if p.fatal != nil {
		return p.fatal
	}
	for len(data) > 0 {
		chunk := data
		if len(chunk) > 256*1024 {
			chunk = chunk[:256*1024]
		}
		result, err := p.decoder.Feed(chunk)
		if err != nil {
			return err
		}
		if result.Fatal {
			// Phase 0 deliberately resets its parser on malformed or unsupported
			// input. A live camera can produce an occasional bad fragment; recover
			// this route in place instead of terminating the source or process.
			p.clearMediaState()
			return nil
		}
		if p.fatal != nil {
			return p.fatal
		}
		data = data[len(chunk):]
	}
	return nil
}

func (p *Pipeline) CloseAbrupt() {
	_ = p.decoder.CloseAbrupt()
	p.dropPublication()
	p.auNalus = nil
	p.auBytes = 0
	p.haveAU = false
	p.lastVideoPTS = 0
	p.haveVideoPTS = false
	p.hasOutput = false
	if p.audio != nil {
		p.audio.Reset()
	}
}

func (p *Pipeline) Error() error { return p.fatal }

func (p *Pipeline) MediaCount() uint64       { return p.mediaCount }
func (p *Pipeline) OutputCount() uint64      { return p.outputCount }
func (p *Pipeline) HasOutput() bool          { return p.hasOutput }
func (p *Pipeline) VideoOutputCount() uint64 { return p.videoOutputCount }
func (p *Pipeline) HasVideoOutput() bool     { return p.hasVideoOutput }

func (p *Pipeline) OnElementary(event mpegps.ElementaryEvent) {
	if p.fatal != nil {
		return
	}
	switch event.Kind {
	case mpegps.KindH265NALU:
		pts, ok := p.videoPTS.Unwrap(event.CallbackPTSMS)
		if !ok {
			return
		}
		if p.haveVideoPTS && pts < p.lastVideoPTS {
			return
		}
		p.lastVideoPTS = pts
		p.haveVideoPTS = true
		nalu, err := normalizeNalu(event.Data)
		if err != nil {
			// Phase 0 already classified this as a video event. If the
			// elementary payload is still unusable, discard the current
			// publication/AU and wait for a later clean keyframe.
			p.clearMediaState()
			return
		}
		if !p.haveAU {
			p.auPTS = pts
			p.haveAU = true
		} else if pts != p.auPTS {
			if err := p.emitAU(); err != nil {
				if errors.Is(err, errConfigChanged) {
					p.fatal = err
					return
				}
				p.clearMediaState()
				return
			}
			p.auPTS = pts
			p.auNalus = nil
			p.auBytes = 0
		}
		if len(p.auNalus) >= maxAccessUnitNALUs || p.auBytes+len(nalu) > maxAccessUnitBytes {
			// Drop a pathological access unit but keep the upstream session
			// alive. The next timestamp starts a clean access unit.
			p.auNalus = nil
			p.auBytes = 0
			p.haveAU = false
			return
		}
		p.auNalus = append(p.auNalus, nalu)
		p.auBytes += len(nalu)
		p.mediaCount++
	case mpegps.KindPCMAPayload:
		pts, ok := p.audioPTS.Unwrap(event.CallbackPTSMS)
		if !ok {
			return
		}
		if p.haveVideoPTS {
			pts, ok = alignPTSNear(pts, p.lastVideoPTS)
			if !ok {
				return
			}
		}
		if p.publication == nil {
			p.mediaCount++
			return
		}
		if p.audio == nil {
			p.audio = newPCMAWriter()
		}
		if p.audio.havePTS && pts < p.audio.lastPTS {
			return
		}
		if err := p.audio.Push(pts, event.Data, func(packet *rtp.Packet, sourcePTS uint64, offset uint32) error {
			err := p.publisher.WriteAudio(p.publication, packet, p.publication.NTPForAudio(sourcePTS, offset))
			if err == nil {
				p.hasOutput = true
				p.outputCount++
			}
			return err
		}); err != nil {
			if errors.Is(err, errPCMATimestampBackwards) {
				// A camera can briefly report an out-of-order audio timestamp.
				// Drop only that callback and retain the RTP cursor so the next
				// forward timestamp remains continuous.
				return
			}
			if errors.Is(err, errPCMAOverflow) {
				return
			}
			p.fatal = err
			return
		}
		p.mediaCount++
	}
}

func (p *Pipeline) OnReset(event mpegps.ResetEvent) {
	p.clearMediaState()
	p.fatal = nil
}

func (p *Pipeline) clearMediaState() {
	p.dropPublication()
	p.vps = nil
	p.sps = nil
	p.pps = nil
	p.auNalus = nil
	p.auBytes = 0
	p.haveAU = false
	p.audio = nil
	p.lastVideoPTS = 0
	p.haveVideoPTS = false
	p.hasOutput = false
	p.hasVideoOutput = false
}

func (p *Pipeline) emitAU() error {
	if len(p.auNalus) == 0 {
		return nil
	}
	var mediaNalus [][]byte
	irap := false
	for _, nalu := range p.auNalus {
		if len(nalu) < 2 {
			return fmt.Errorf("H265 NALU is too short")
		}
		typ := (nalu[0] >> 1) & 0x3f
		switch typ {
		case 32:
			if p.publication != nil && !equalBytes(p.vps, nalu) {
				return errConfigChanged
			}
			p.vps = append([]byte(nil), nalu...)
		case 33:
			if p.publication != nil && !equalBytes(p.sps, nalu) {
				return errConfigChanged
			}
			p.sps = append([]byte(nil), nalu...)
		case 34:
			if p.publication != nil && !equalBytes(p.pps, nalu) {
				return errConfigChanged
			}
			p.pps = append([]byte(nil), nalu...)
		case 16, 17, 18, 19, 20, 21, 22, 23:
			irap = true
			mediaNalus = append(mediaNalus, nalu)
		default:
			mediaNalus = append(mediaNalus, nalu)
		}
	}
	if p.publication == nil {
		if len(p.vps) == 0 || len(p.sps) == 0 || len(p.pps) == 0 || !irap {
			return nil
		}
		publication, err := p.publisher.Open(p.path, p.vps, p.sps, p.pps, p.auPTS, p.talkTarget)
		if err != nil {
			return err
		}
		p.publication = publication
		p.audio = newPCMAWriter()
	}
	if len(mediaNalus) == 0 {
		return nil
	}
	packets, err := p.publisher.EncodeVideo(p.publication, mediaNalus)
	if err != nil {
		return err
	}
	ntp := p.publication.NTPForVideo(p.auPTS)
	for _, packet := range packets {
		packet.Timestamp = p.publication.VideoTimestamp(p.auPTS)
	}
	if writer, ok := p.publisher.(videoAccessUnitWriter); ok {
		if err := writer.WriteVideoAccessUnit(p.publication, packets, ntp, irap); err != nil {
			return err
		}
		p.hasOutput = true
		p.outputCount += uint64(len(packets))
		p.videoOutputCount += uint64(len(packets))
		p.hasVideoOutput = true
		return nil
	}
	for _, packet := range packets {
		if err := p.publisher.WriteVideo(p.publication, packet, ntp); err != nil {
			return err
		}
		p.hasOutput = true
		p.outputCount++
		p.videoOutputCount++
		p.hasVideoOutput = true
	}
	return nil
}

func alignPTSNear(value, reference uint64) (uint64, bool) {
	best := int64(value)
	referenceSigned := int64(reference)
	bestDistance := absInt64Difference(best, referenceSigned)
	for _, candidate := range []int64{
		int64(value) - int64(mpegPTSPeriodMS),
		int64(value) + int64(mpegPTSPeriodMS),
	} {
		distance := absInt64Difference(candidate, referenceSigned)
		if distance < bestDistance {
			best, bestDistance = candidate, distance
		}
	}
	// Do not emit an audio packet that is materially older than the current
	// video clock. It is usually a late pre-wrap callback.
	if best < 0 || best+2000 < referenceSigned {
		return 0, false
	}
	return uint64(best), true
}

func absInt64Difference(left, right int64) uint64 {
	if left >= right {
		return uint64(left - right)
	}
	return uint64(right - left)
}

func (p *Pipeline) dropPublication() {
	if p.publication != nil {
		p.publisher.ClosePublication(p.publication)
		p.publication = nil
	}
}

func normalizeNalu(data []byte) ([]byte, error) {
	prefix := 0
	if len(data) >= 4 && data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 1 {
		prefix = 4
	} else if len(data) >= 3 && data[0] == 0 && data[1] == 0 && data[2] == 1 {
		prefix = 3
	} else {
		return nil, fmt.Errorf("H265 NALU has no Annex-B prefix")
	}
	end := len(data)
	for end > prefix+2 && data[end-1] == 0 {
		end--
	}
	if end < prefix+2 {
		return nil, fmt.Errorf("H265 NALU has no payload")
	}
	return append([]byte(nil), data[prefix:end]...), nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type PCMAWriter struct {
	baseTS    uint32
	firstPTS  uint64
	lastPTS   uint64
	nextTS    uint32
	runOffset uint32
	pending   []byte
	havePTS   bool
	sequence  uint16
}

func newPCMAWriter() *PCMAWriter {
	var random [6]byte
	_, _ = rand.Read(random[:])
	return &PCMAWriter{baseTS: binary.BigEndian.Uint32(random[:4]), sequence: binary.BigEndian.Uint16(random[4:])}
}

func (w *PCMAWriter) Reset() {
	w.pending = nil
	w.havePTS = false
	w.runOffset = 0
}

func (w *PCMAWriter) Push(pts uint64, data []byte, write func(*rtp.Packet, uint64, uint32) error) error {
	if !w.havePTS {
		w.havePTS = true
		w.firstPTS = pts
		w.lastPTS = pts
		w.nextTS = w.baseTS
	} else if pts < w.lastPTS {
		return errPCMATimestampBackwards
	} else if pts != w.lastPTS {
		target := w.baseTS + uint32((pts-w.firstPTS)*8)
		if err := w.flush(write); err != nil {
			return err
		}
		if target > w.nextTS {
			w.nextTS = target
		}
		w.lastPTS = pts
		w.runOffset = 0
	}
	if len(data) > maxPCMAPendingBytes || len(w.pending)+len(data) > maxPCMAPendingBytes {
		// Do not let a frozen audio timestamp turn into an unbounded
		// per-route buffer. Drop this audio run and wait for the next one.
		w.pending = nil
		w.havePTS = false
		w.runOffset = 0
		return errPCMAOverflow
	}
	w.pending = append(w.pending, data...)
	for len(w.pending) >= 160 {
		payload := append([]byte(nil), w.pending[:160]...)
		w.pending = w.pending[160:]
		if err := w.emit(payload, w.lastPTS, write); err != nil {
			return err
		}
	}
	return nil
}

func (w *PCMAWriter) flush(write func(*rtp.Packet, uint64, uint32) error) error {
	if len(w.pending) == 0 {
		return nil
	}
	payload := append([]byte(nil), w.pending...)
	w.pending = nil
	return w.emit(payload, w.lastPTS, write)
}

func (w *PCMAWriter) emit(payload []byte, pts uint64, write func(*rtp.Packet, uint64, uint32) error) error {
	packet := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 8, SequenceNumber: w.sequence, Timestamp: w.nextTS}, Payload: payload}
	w.sequence++
	offset := w.runOffset
	w.nextTS += uint32(len(payload))
	w.runOffset += uint32(len(payload))
	return write(packet, pts, offset)
}
