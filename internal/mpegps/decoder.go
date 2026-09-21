// Package mpegps adapts the pinned gomedia MPEG-PS decoder into a bounded,
// owned elementary-event stream for the later RTSP pipeline.
package mpegps

import (
	"errors"
	"reflect"
	"sync/atomic"

	"github.com/yapingcat/gomedia/go-mpeg2"
)

const (
	maxInputChunkBytes      = 256 * 1024
	maxRawSinceH265Event    = 4 * 1024 * 1024
	maxRawSincePCMAEvent    = 1 * 1024 * 1024
	maxSuccessfulPESPayload = 64 * 1024
	acceptedVideoIDMin      = 0xE0
	acceptedVideoIDMax      = 0xEF
	acceptedAudioIDMin      = 0xC0
	acceptedAudioIDMax      = 0xDF
)

var (
	// ErrReentrant is returned when a Decoder method is called while the same
	// Decoder is already executing.
	ErrReentrant = errors.New("decoder reentrant call")
	// ErrNilSink is returned when NewDecoder receives a nil or typed-nil sink.
	ErrNilSink           = errors.New("decoder sink is nil")
	errTestFlushed       = errors.New("decoder test parser already flushed")
	errFlushPrecondition = errors.New("decoder flush test precondition failed")
)

type ElementaryKind uint8

const (
	KindH265NALU ElementaryKind = iota + 1
	KindPCMAPayload
)

type ElementaryEvent struct {
	Epoch         uint64
	Order         uint64
	Kind          ElementaryKind
	Data          []byte
	CallbackPTSMS uint64
	CallbackDTSMS uint64
	NALType       uint8
	LayerID       uint8
}

type AuditSnapshot struct {
	H265ESID     uint8
	PCMAESID     uint8
	PSMVersion   uint8
	PSMValidated bool
	PTSSeen      uint64
	DTSExplicit  uint64
	DTSFallback  uint64
}

type ResetReason string

const (
	ResetParserError        ResetReason = "parser_error"
	ResetUnsupportedProfile ResetReason = "unsupported_profile"
	ResetMissingPTS         ResetReason = "missing_pts"
	ResetLimit              ResetReason = "limit"
	ResetPanic              ResetReason = "panic"
	ResetAbruptClose        ResetReason = "abrupt_close"
	ResetInvalidPES         ResetReason = "invalid_pes"
	ResetClassifier         ResetReason = "classifier"
)

type ResetEvent struct {
	Epoch  uint64
	Order  uint64
	Reason ResetReason
}

type FeedResult struct {
	NeedMore bool
	Fatal    bool
}

type Sink interface {
	OnElementary(ElementaryEvent)
	OnReset(ResetEvent)
}

type profileState struct {
	accepted  bool
	h265ID    uint8
	pcmaID    uint8
	auxiliary bool
	version   uint8
}

type pesTimestamp struct {
	pts uint64
	dts uint64
}

type Decoder struct {
	sink Sink

	demux        *mpeg2.PSDemuxer
	inCall       atomic.Bool
	inSink       bool
	closed       bool
	flushed      bool
	flushing     bool
	lastNeedMore bool

	epoch   uint64
	order   uint64
	profile profileState
	audit   AuditSnapshot
	pesTime map[uint8]pesTimestamp

	h265Bytes uint64
	pcmaBytes uint64
	feedH265  bool
	feedPCMA  bool
	flushH265 bool
	flushPCMA bool

	fatalReason ResetReason
}

func NewDecoder(sink Sink) (*Decoder, error) {
	if isNilSink(sink) {
		return nil, ErrNilSink
	}
	d := &Decoder{sink: sink, epoch: 1, pesTime: make(map[uint8]pesTimestamp)}
	d.installDemuxer()
	return d, nil
}

func isNilSink(sink Sink) bool {
	if sink == nil {
		return true
	}
	v := reflect.ValueOf(sink)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func (d *Decoder) installDemuxer() {
	d.demux = mpeg2.NewPSDemuxer()
	d.demux.OnPacket = d.onPacket
	d.demux.OnFrame = d.onFrame
}

func (d *Decoder) Feed(data []byte) (result FeedResult, err error) {
	if !d.inCall.CompareAndSwap(false, true) {
		return FeedResult{}, ErrReentrant
	}
	defer d.inCall.Store(false)
	if d.flushed {
		return FeedResult{}, errTestFlushed
	}

	d.closed = false
	d.feedH265 = false
	d.feedPCMA = false

	if len(data) > maxInputChunkBytes {
		d.latch(ResetLimit)
		d.commitReset()
		return FeedResult{Fatal: true}, nil
	}
	if d.exceeds(d.h265Bytes, uint64(len(data)), maxRawSinceH265Event) ||
		d.exceeds(d.pcmaBytes, uint64(len(data)), maxRawSincePCMAEvent) {
		d.latch(ResetLimit)
		d.commitReset()
		return FeedResult{Fatal: true}, nil
	}
	d.h265Bytes += uint64(len(data))
	d.pcmaBytes += uint64(len(data))

	var parseErr error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				if d.inSink {
					panic(recovered)
				}
				d.latch(ResetPanic)
			}
		}()
		parseErr = d.demux.Input(data)
	}()

	if d.fatalReason != "" {
		d.commitReset()
		return FeedResult{Fatal: true}, nil
	}
	if parseErr != nil {
		if parserNeedMore(parseErr) {
			result.NeedMore = true
		} else {
			d.latch(ResetParserError)
			d.commitReset()
			return FeedResult{Fatal: true}, nil
		}
	}
	d.lastNeedMore = result.NeedMore

	if d.feedH265 {
		d.h265Bytes = 0
	}
	if d.feedPCMA {
		d.pcmaBytes = 0
	}
	return result, nil
}

func (d *Decoder) exceeds(current, incoming, limit uint64) bool {
	return current >= limit || incoming >= limit-current
}

func parserNeedMore(err error) bool {
	parsed, ok := err.(mpeg2.Error)
	return ok && parsed.NeedMore()
}

func (d *Decoder) CloseAbrupt() error {
	if !d.inCall.CompareAndSwap(false, true) {
		return ErrReentrant
	}
	defer d.inCall.Store(false)
	if d.closed {
		return nil
	}
	d.closed = true
	d.reset(ResetAbruptClose)
	return nil
}

func (d *Decoder) Audit() (AuditSnapshot, error) {
	if !d.inCall.CompareAndSwap(false, true) {
		return AuditSnapshot{}, ErrReentrant
	}
	defer d.inCall.Store(false)
	return d.audit, nil
}

// flushTailForTest is intentionally unexported. It exists only for a private
// file-fixture test to observe gomedia's residual callback behavior. Live
// routes must use CloseAbrupt instead of Flush.
func (d *Decoder) flushTailForTest() error {
	if !d.inCall.CompareAndSwap(false, true) {
		return ErrReentrant
	}
	defer d.inCall.Store(false)
	if d.flushed || d.closed || d.lastNeedMore || d.fatalReason != "" || !d.profile.accepted || d.demux == nil {
		return errFlushPrecondition
	}
	d.flushH265 = false
	d.flushPCMA = false
	d.flushing = true
	d.demux.Flush()
	d.flushing = false
	d.flushed = true
	d.demux = nil
	return nil
}

func (d *Decoder) onPacket(pkg mpeg2.Display, decodeResult error) {
	if d.fatalReason != "" || decodeResult != nil && parserNeedMore(decodeResult) {
		return
	}
	if decodeResult != nil {
		d.latch(ResetParserError)
		return
	}

	switch packet := pkg.(type) {
	case *mpeg2.PSPackHeader:
		if packet.IsMpeg1 {
			d.latch(ResetUnsupportedProfile)
		}
	case *mpeg2.Program_stream_map:
		d.handlePSM(packet)
	case *mpeg2.PesPacket:
		d.handlePES(packet)
	}
}

func (d *Decoder) handlePSM(packet *mpeg2.Program_stream_map) {
	if d.fatalReason != "" {
		return
	}
	if packet.Current_next_indicator != 1 || len(packet.Stream_map) < 2 || len(packet.Stream_map) > 3 {
		d.latch(ResetUnsupportedProfile)
		return
	}

	var h265ID, pcmaID uint8
	var h265Found, pcmaFound, auxiliaryFound bool
	for _, stream := range packet.Stream_map {
		switch mpeg2.PS_STREAM_TYPE(stream.Stream_type) {
		case mpeg2.PS_STREAM_H265:
			if h265Found || stream.Elementary_stream_id < acceptedVideoIDMin || stream.Elementary_stream_id > acceptedVideoIDMax {
				d.latch(ResetUnsupportedProfile)
				return
			}
			h265Found = true
			h265ID = stream.Elementary_stream_id
		case mpeg2.PS_STREAM_G711A:
			if pcmaFound || stream.Elementary_stream_id < acceptedAudioIDMin || stream.Elementary_stream_id > acceptedAudioIDMax {
				d.latch(ResetUnsupportedProfile)
				return
			}
			pcmaFound = true
			pcmaID = stream.Elementary_stream_id
		case 0:
			if auxiliaryFound || stream.Elementary_stream_id != 0xBD {
				d.latch(ResetUnsupportedProfile)
				return
			}
			auxiliaryFound = true
		default:
			d.latch(ResetUnsupportedProfile)
			return
		}
	}
	if !h265Found || !pcmaFound || h265ID == pcmaID || auxiliaryFound != (len(packet.Stream_map) == 3) {
		d.latch(ResetUnsupportedProfile)
		return
	}

	if d.profile.accepted && (d.profile.h265ID != h265ID || d.profile.pcmaID != pcmaID || d.profile.auxiliary != auxiliaryFound) {
		d.latch(ResetUnsupportedProfile)
		return
	}
	d.profile = profileState{accepted: true, h265ID: h265ID, pcmaID: pcmaID, auxiliary: auxiliaryFound, version: packet.Program_stream_map_version}
	d.audit.H265ESID = h265ID
	d.audit.PCMAESID = pcmaID
	d.audit.PSMVersion = packet.Program_stream_map_version
	d.audit.PSMValidated = true
}

func (d *Decoder) handlePES(packet *mpeg2.PesPacket) {
	if d.fatalReason != "" {
		return
	}
	if !d.profile.accepted {
		d.latch(ResetUnsupportedProfile)
		return
	}
	if packet.Stream_id != d.profile.h265ID && packet.Stream_id != d.profile.pcmaID {
		d.latch(ResetUnsupportedProfile)
		return
	}
	if packet.PES_packet_length == 0 ||
		(packet.PES_packet_length != 0 && packet.PES_packet_length < 3+uint16(packet.PES_header_data_length)) {
		d.latch(ResetInvalidPES)
		return
	}
	if !headerCoversFlags(packet) || len(packet.Pes_payload) >= maxSuccessfulPESPayload {
		d.latch(ResetInvalidPES)
		return
	}
	if packet.PTS_DTS_flags&0x03 == 1 {
		d.latch(ResetInvalidPES)
		return
	}
	inheritedTimestamp := false
	if packet.PTS_DTS_flags&0x02 == 0 {
		previous, ok := d.pesTime[packet.Stream_id]
		if !ok {
			d.latch(ResetMissingPTS)
			return
		}
		// C07 splits large video access units across multiple PES packets. The
		// continuation packets omit PTS/DTS; keep the timestamp on the PES
		// object before gomedia consumes its payload.
		packet.Pts = previous.pts
		packet.Dts = previous.dts
		inheritedTimestamp = true
	}
	d.pesTime[packet.Stream_id] = pesTimestamp{pts: packet.Pts, dts: packet.Dts}
	if !inheritedTimestamp {
		d.audit.PTSSeen++
		if packet.PTS_DTS_flags&0x03 == 0x03 {
			d.audit.DTSExplicit++
		} else {
			d.audit.DTSFallback++
		}
	}
}

func headerCoversFlags(packet *mpeg2.PesPacket) bool {
	required := 0
	switch packet.PTS_DTS_flags & 0x03 {
	case 0x02:
		required += 5
	case 0x03:
		required += 10
	}
	if packet.ESCR_flag != 0 {
		required += 6
	}
	if packet.ES_rate_flag != 0 {
		required += 3
	}
	if packet.DSM_trick_mode_flag != 0 {
		required++
	}
	if packet.Additional_copy_info_flag != 0 {
		required++
	}
	if packet.PES_CRC_flag != 0 {
		required += 2
	}
	return int(packet.PES_header_data_length) >= required
}

func (d *Decoder) onFrame(frame []byte, codec mpeg2.PS_STREAM_TYPE, ptsMS, dtsMS uint64) {
	if d.fatalReason != "" {
		return
	}
	if d.flushing {
		switch codec {
		case mpeg2.PS_STREAM_H265:
			d.flushH265 = true
		case mpeg2.PS_STREAM_G711A:
			d.flushPCMA = true
		}
		return
	}
	if !d.profile.accepted || len(frame) == 0 {
		d.latch(ResetUnsupportedProfile)
		return
	}

	event := ElementaryEvent{
		Epoch:         d.epoch,
		Order:         d.order + 1,
		Data:          append([]byte(nil), frame...),
		CallbackPTSMS: ptsMS,
		CallbackDTSMS: dtsMS,
	}
	switch codec {
	case mpeg2.PS_STREAM_H265:
		nalType, layerID, ok := classifyH265(event.Data)
		if !ok {
			d.latch(ResetClassifier)
			return
		}
		event.Kind = KindH265NALU
		event.NALType = nalType
		event.LayerID = layerID
		d.feedH265 = true
	case mpeg2.PS_STREAM_G711A:
		event.Kind = KindPCMAPayload
		d.feedPCMA = true
	default:
		d.latch(ResetUnsupportedProfile)
		return
	}

	d.order = event.Order
	d.inSink = true
	d.sink.OnElementary(event)
	d.inSink = false
}

func (d *Decoder) latch(reason ResetReason) {
	if d.fatalReason == "" {
		d.fatalReason = reason
	}
}

func (d *Decoder) commitReset() {
	if d.fatalReason == "" {
		return
	}
	d.reset(d.fatalReason)
}

func (d *Decoder) reset(reason ResetReason) {
	d.epoch++
	d.order = 0
	d.profile = profileState{}
	d.audit = AuditSnapshot{}
	d.pesTime = make(map[uint8]pesTimestamp)
	d.h265Bytes = 0
	d.pcmaBytes = 0
	d.feedH265 = false
	d.feedPCMA = false
	d.flushH265 = false
	d.flushPCMA = false
	d.lastNeedMore = false
	d.fatalReason = ""
	d.installDemuxer()

	d.inSink = true
	d.sink.OnReset(ResetEvent{Epoch: d.epoch, Reason: reason})
	d.inSink = false
}

func classifyH265(data []byte) (nalType, layerID uint8, ok bool) {
	prefix := 0
	if len(data) >= 4 && data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 1 {
		prefix = 4
	} else if len(data) >= 3 && data[0] == 0 && data[1] == 0 && data[2] == 1 {
		prefix = 3
	} else {
		return 0, 0, false
	}
	if len(data) < prefix+2 {
		return 0, 0, false
	}
	if len(data) >= prefix+3 && data[prefix] == 0 && data[prefix+1] == 0 && data[prefix+2] == 1 {
		return 0, 0, false
	}
	header0, header1 := data[prefix], data[prefix+1]
	if header0&0x80 != 0 || header1&0x07 == 0 {
		return 0, 0, false
	}
	layerID = ((header0 & 0x01) << 5) | ((header1 >> 3) & 0x1F)
	if layerID != 0 {
		return 0, layerID, false
	}
	return (header0 >> 1) & 0x3F, layerID, true
}
