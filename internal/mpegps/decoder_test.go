package mpegps

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/yapingcat/gomedia/go-mpeg2"
)

type recordingSink struct {
	events []ElementaryEvent
	resets []ResetEvent
}

func (s *recordingSink) OnElementary(event ElementaryEvent) {
	s.events = append(s.events, event)
}

func (s *recordingSink) OnReset(event ResetEvent) {
	s.resets = append(s.resets, event)
}

func syntheticPS(t *testing.T) []byte {
	t.Helper()
	var ps []byte
	mux := mpeg2.NewPsMuxer()
	mux.OnPacket = func(packet []byte) {
		ps = append(ps, packet...)
	}
	videoID := mux.AddStream(mpeg2.PS_STREAM_H265)
	audioID := mux.AddStream(mpeg2.PS_STREAM_G711A)
	video := []byte{
		0, 0, 0, 1, 0x40, 0x01, 0x0c, // VPS
		0, 0, 0, 1, 0x42, 0x01, 0x01, // SPS
		0, 0, 0, 1, 0x44, 0x01, 0x01, // PPS
		0, 0, 0, 1, 0x26, 0x01, 0x80, // IDR
	}
	audio := bytes.Repeat([]byte{0xD5}, 640)
	if err := mux.Write(videoID, video, 90000, 90000); err != nil {
		t.Fatalf("write video: %v", err)
	}
	if err := mux.Write(audioID, audio, 90000, 90000); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := mux.Write(audioID, audio, 99000, 99000); err != nil {
		t.Fatalf("write second audio: %v", err)
	}
	return ps
}

func handAuthoredPS() []byte {
	return []byte{
		// Program stream map: H.265 0xe0 and G.711A 0xc0.
		0x00, 0x00, 0x01, 0xbc, 0x00, 0x12, 0xe0, 0xff, 0x00, 0x00, 0x00, 0x08,
		0x24, 0xe0, 0x00, 0x00, 0x90, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// Video PES at 90,000 ticks with two NAL units.
		0x00, 0x00, 0x01, 0xe0, 0x00, 0x14, 0x80, 0x80, 0x05, 0x21, 0x00, 0x05,
		0xbf, 0x21, 0x00, 0x00, 0x01, 0x40, 0x01, 0x0c, 0x00, 0x00, 0x01, 0x26,
		0x01, 0x80,
		// First PCMA PES at the same timestamp.
		0x00, 0x00, 0x01, 0xc0, 0x00, 0x0c, 0x80, 0x80, 0x05, 0x21, 0x00, 0x05,
		0xbf, 0x21, 0xd5, 0xd5, 0xd5, 0xd5,
		// Second video PES at 99,000 ticks flushes the prior video tail.
		0x00, 0x00, 0x01, 0xe0, 0x00, 0x0e, 0x80, 0x80, 0x05, 0x23, 0x00, 0x07,
		0x05, 0x71, 0x00, 0x00, 0x01, 0x44, 0x01, 0x01,
		// Second PCMA PES at 99,000 ticks flushes the prior audio payload.
		0x00, 0x00, 0x01, 0xc0, 0x00, 0x0c, 0x80, 0x80, 0x05, 0x23, 0x00, 0x07,
		0x05, 0x71, 0xd5, 0xd5, 0xd5, 0xd5,
	}
}

func handAuthoredAuxiliaryPS() []byte {
	return []byte{
		// Program stream map: the two media entries plus CATLINK's 0xbd auxiliary.
		0x00, 0x00, 0x01, 0xbc, 0x00, 0x16, 0xe0, 0xff, 0x00, 0x00, 0x00, 0x0c,
		0x24, 0xe0, 0x00, 0x00, 0x90, 0xc0, 0x00, 0x00, 0x00, 0xbd, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		// CommonPesPacket is intentionally not exposed as a media event.
		0x00, 0x00, 0x01, 0xbd, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04,
	}
}

func handAuthoredSamePTSAudioPS() []byte {
	return []byte{
		0x00, 0x00, 0x01, 0xbc, 0x00, 0x12, 0xe0, 0xff, 0x00, 0x00, 0x00, 0x08,
		0x24, 0xe0, 0x00, 0x00, 0x90, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// Two audio PES packets share PTS 90,000 and must be concatenated.
		0x00, 0x00, 0x01, 0xc0, 0x00, 0x0a, 0x80, 0x80, 0x05, 0x21, 0x00, 0x05,
		0xbf, 0x21, 0xd5, 0xd5,
		0x00, 0x00, 0x01, 0xc0, 0x00, 0x0a, 0x80, 0x80, 0x05, 0x21, 0x00, 0x05,
		0xbf, 0x21, 0xaa, 0xaa,
		// A new PTS flushes the four-byte same-PTS aggregate.
		0x00, 0x00, 0x01, 0xc0, 0x00, 0x0a, 0x80, 0x80, 0x05, 0x23, 0x00, 0x07,
		0x05, 0x71, 0xff, 0xff,
	}
}

func TestHandAuthoredPSGoldenEvents(t *testing.T) {
	data := handAuthoredPS()
	sink := feedWithChunks(t, data, 1)
	if len(sink.events) != 3 {
		t.Fatalf("events=%d, want 3", len(sink.events))
	}
	want := []struct {
		kind ElementaryKind
		pts  uint64
		typ  uint8
		data []byte
	}{
		{kind: KindH265NALU, pts: 1000, typ: 32, data: []byte{0, 0, 1, 0x40, 1, 0x0c}},
		{kind: KindH265NALU, pts: 1000, typ: 19, data: []byte{0, 0, 1, 0x26, 1, 0x80}},
		{kind: KindPCMAPayload, pts: 1000, data: []byte{0xd5, 0xd5, 0xd5, 0xd5}},
	}
	for index, event := range sink.events {
		if event.Kind != want[index].kind || event.CallbackPTSMS != want[index].pts || event.CallbackDTSMS != want[index].pts || !bytes.Equal(event.Data, want[index].data) {
			t.Fatalf("event %d mismatch: kind=%d pts=%d len=%d", index, event.Kind, event.CallbackPTSMS, len(event.Data))
		}
		if event.Kind == KindH265NALU && event.NALType != want[index].typ {
			t.Fatalf("event %d nal type=%d, want %d", index, event.NALType, want[index].typ)
		}
		if event.Kind == KindH265NALU && event.LayerID != 0 {
			t.Fatalf("event %d layer=%d, want 0", index, event.LayerID)
		}
		if event.Kind == KindPCMAPayload && (event.NALType != 0 || event.LayerID != 0) {
			t.Fatalf("event %d audio classifier fields=%d/%d", index, event.NALType, event.LayerID)
		}
		if event.Epoch != 1 || event.Order != uint64(index+1) {
			t.Fatalf("event %d epoch/order=%d/%d", index, event.Epoch, event.Order)
		}
	}
}

func TestFlushTailForTestDiscardsResidualCallbacks(t *testing.T) {
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	result, err := decoder.Feed(handAuthoredPS())
	if err != nil || result.Fatal || result.NeedMore {
		t.Fatalf("feed result=%+v err=%v", result, err)
	}
	eventCount := len(sink.events)
	if err := decoder.flushTailForTest(); err != nil {
		t.Fatalf("flush tail: %v", err)
	}
	if len(sink.events) != eventCount {
		t.Fatalf("flush delivered %d new events", len(sink.events)-eventCount)
	}
	if !decoder.flushH265 || !decoder.flushPCMA {
		t.Fatalf("flush did not observe both residual codec callbacks")
	}
	if decoder.demux != nil || !decoder.flushed {
		t.Fatal("flush did not destroy parser")
	}
	if err := decoder.flushTailForTest(); err != errFlushPrecondition {
		t.Fatalf("second flush error=%v, want %v", err, errFlushPrecondition)
	}
}

func TestFlushTailForTestPreconditions(t *testing.T) {
	decoder, err := NewDecoder(new(recordingSink))
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	if err := decoder.flushTailForTest(); err != errFlushPrecondition {
		t.Fatalf("unaccepted flush error=%v, want %v", err, errFlushPrecondition)
	}
	if _, err := decoder.Feed([]byte{0, 0, 1, 0xbc}); err != nil {
		t.Fatalf("feed: %v", err)
	}
	if err := decoder.flushTailForTest(); err != errFlushPrecondition {
		t.Fatalf("NeedMore flush error=%v, want %v", err, errFlushPrecondition)
	}
}

func TestAbruptCloseResetsEpochWithoutFlush(t *testing.T) {
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	data := handAuthoredPS()
	firstAudio := bytes.Index(data, []byte{0, 0, 1, 0xc0})
	if firstAudio < 0 {
		t.Fatal("golden audio PES not found")
	}
	result, err := decoder.Feed(data[:firstAudio])
	if err != nil || result.Fatal || len(sink.events) == 0 {
		t.Fatalf("feed result=%+v err=%v events=%d", result, err, len(sink.events))
	}
	eventCount := len(sink.events)
	if err := decoder.CloseAbrupt(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(sink.events) != eventCount {
		t.Fatalf("abrupt close flushed a residual event: before=%d after=%d", eventCount, len(sink.events))
	}
	if len(sink.resets) != 1 || sink.resets[0].Reason != ResetAbruptClose || sink.resets[0].Epoch != 2 || sink.resets[0].Order != 0 {
		t.Fatalf("reset count=%d", len(sink.resets))
	}
	if err := decoder.CloseAbrupt(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if len(sink.resets) != 1 {
		t.Fatalf("second close emitted another reset: count=%d", len(sink.resets))
	}
}

func feedWithChunks(t *testing.T, data []byte, chunkSize int) *recordingSink {
	t.Helper()
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	for offset := 0; offset < len(data); {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		result, err := decoder.Feed(data[offset:end])
		if err != nil {
			t.Fatalf("feed: %v", err)
		}
		if result.Fatal {
			t.Fatalf("unexpected fatal result at offset %d", offset)
		}
		offset = end
	}
	return sink
}

func eventSignature(events []ElementaryEvent) []string {
	result := make([]string, 0, len(events))
	for _, event := range events {
		result = append(result, fmt.Sprintf("%d/%d/%d/%d/%d/%d/%d/%x", event.Epoch,
			event.Order, event.Kind, event.CallbackPTSMS, event.CallbackDTSMS,
			event.NALType, event.LayerID,
			event.Data))
	}
	return result
}

func TestSyntheticPSFragmentationIsStable(t *testing.T) {
	data := syntheticPS(t)
	baseline := feedWithChunks(t, data, 4096)
	if len(baseline.events) == 0 {
		t.Fatal("synthetic fixture produced no events")
	}
	var h265, pcma int
	for _, event := range baseline.events {
		switch event.Kind {
		case KindH265NALU:
			h265++
		case KindPCMAPayload:
			pcma++
		}
	}
	if h265 == 0 || pcma == 0 {
		t.Fatalf("synthetic fixture codecs: h265=%d pcma=%d", h265, pcma)
	}
	for _, size := range []int{1, 7, 188, 1388, 2776} {
		got := feedWithChunks(t, data, size)
		if want, have := eventSignature(baseline.events), eventSignature(got.events); !equalStrings(want, have) {
			t.Fatalf("chunk size %d changed event sequence: want %d events, got %d", size, len(want), len(have))
		}
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestPESContinuationKeepsAccessUnitTimestamp(t *testing.T) {
	data := handAuthoredPS()
	withPTS := []byte{0x00, 0x00, 0x01, 0xe0, 0x00, 0x0e, 0x80, 0x80, 0x05, 0x23, 0x00, 0x07, 0x05, 0x71, 0x00, 0x00, 0x01, 0x44, 0x01, 0x01}
	withoutPTS := []byte{0x00, 0x00, 0x01, 0xe0, 0x00, 0x09, 0x80, 0x00, 0x00, 0x00, 0x00, 0x01, 0x44, 0x01, 0x01}
	nextVideo := []byte{0x00, 0x00, 0x01, 0xe0, 0x00, 0x0e, 0x80, 0x80, 0x05, 0x23, 0x00, 0x07, 0x05, 0x71, 0x00, 0x00, 0x01, 0x02, 0x01, 0x01}
	finalVideo := []byte{0x00, 0x00, 0x01, 0xe0, 0x00, 0x0e, 0x80, 0x80, 0x05, 0x23, 0x00, 0x07, 0x05, 0x71, 0x00, 0x00, 0x01, 0x02, 0x01, 0x02}
	if !bytes.Contains(data, withPTS) {
		t.Fatal("continuation fixture pattern not found")
	}
	replacement := append(append(append([]byte{}, withoutPTS...), nextVideo...), finalVideo...)
	data = bytes.Replace(data, withPTS, replacement, 1)
	sink := feedWithChunks(t, data, 1)
	if len(sink.resets) != 0 {
		t.Fatalf("resets=%d", len(sink.resets))
	}
	inherited := false
	for _, event := range sink.events {
		if event.Kind == KindH265NALU && event.NALType == 34 && event.CallbackPTSMS == 1000 {
			inherited = true
			break
		}
	}
	if !inherited {
		t.Fatalf("continuation PPS did not inherit PTS: events=%d", len(sink.events))
	}
}

func TestClassifyH265Header(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		ok    bool
		typ   uint8
		layer uint8
	}{
		{name: "three byte prefix", data: []byte{0, 0, 1, 0x40, 0x01, 0xAA}, ok: true, typ: 32},
		{name: "four byte prefix", data: []byte{0, 0, 0, 1, 0x26, 0x01, 0x80}, ok: true, typ: 19},
		{name: "trailing zero preserved", data: []byte{0, 0, 1, 0x40, 0x01, 0, 0}, ok: true, typ: 32},
		{name: "short header", data: []byte{0, 0, 1, 0x40}, ok: false},
		{name: "forbidden bit", data: []byte{0, 0, 1, 0xC0, 0x01}, ok: false},
		{name: "zero temporal id", data: []byte{0, 0, 1, 0x40, 0x00}, ok: false},
		{name: "nonzero layer", data: []byte{0, 0, 1, 0x40, 0x09}, ok: false, layer: 1},
		{name: "empty nalu", data: []byte{0, 0, 1, 0, 0, 1}, ok: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotType, gotLayer, gotOK := classifyH265(tc.data)
			if gotOK != tc.ok || gotType != tc.typ || gotLayer != tc.layer {
				t.Fatalf("got type=%d layer=%d ok=%t", gotType, gotLayer, gotOK)
			}
		})
	}
}

func TestSamePTSAudioIsAggregated(t *testing.T) {
	sink := feedWithChunks(t, handAuthoredSamePTSAudioPS(), 1)
	if len(sink.events) != 1 {
		t.Fatalf("audio events=%d", len(sink.events))
	}
	if sink.events[0].Kind != KindPCMAPayload || !bytes.Equal(sink.events[0].Data, []byte{0xd5, 0xd5, 0xaa, 0xaa}) {
		t.Fatalf("audio kind=%d len=%d", sink.events[0].Kind, len(sink.events[0].Data))
	}
}

func TestInvalidPESHeaderIsRejected(t *testing.T) {
	decoder, err := NewDecoder(new(recordingSink))
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	decoder.profile = profileState{accepted: true, h265ID: 0xE0, pcmaID: 0xC0}
	decoder.handlePES(&mpeg2.PesPacket{
		Stream_id:              0xE0,
		PES_packet_length:      20,
		PES_header_data_length: 0,
		PTS_DTS_flags:          0x02,
		Pes_payload:            []byte{1, 2, 3},
	})
	if decoder.fatalReason != ResetInvalidPES {
		t.Fatalf("fatal reason = %q", decoder.fatalReason)
	}
}

func TestPSMProfileAllowsOnlyKnownAuxiliaryMapping(t *testing.T) {
	tests := []struct {
		name      string
		streams   []*mpeg2.Elementary_stream_elem
		wantFatal bool
	}{
		{
			name: "media only",
			streams: []*mpeg2.Elementary_stream_elem{
				{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: 0xE0},
				{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: 0xC0},
			},
		},
		{
			name: "known auxiliary",
			streams: []*mpeg2.Elementary_stream_elem{
				{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: 0xE0},
				{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: 0xC0},
				{Stream_type: 0x00, Elementary_stream_id: 0xBD},
			},
		},
		{
			name: "wrong auxiliary id",
			streams: []*mpeg2.Elementary_stream_elem{
				{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: 0xE0},
				{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: 0xC0},
				{Stream_type: 0x00, Elementary_stream_id: 0xBE},
			},
			wantFatal: true,
		},
		{
			name: "wrong auxiliary type",
			streams: []*mpeg2.Elementary_stream_elem{
				{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: 0xE0},
				{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: 0xC0},
				{Stream_type: uint8(mpeg2.PS_STREAM_AAC), Elementary_stream_id: 0xBD},
			},
			wantFatal: true,
		},
		{
			name: "second auxiliary",
			streams: []*mpeg2.Elementary_stream_elem{
				{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: 0xE0},
				{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: 0xC0},
				{Stream_type: 0x00, Elementary_stream_id: 0xBD},
				{Stream_type: 0x00, Elementary_stream_id: 0xBD},
			},
			wantFatal: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoder, err := NewDecoder(new(recordingSink))
			if err != nil {
				t.Fatalf("new decoder: %v", err)
			}
			decoder.handlePSM(&mpeg2.Program_stream_map{
				Current_next_indicator: 1,
				Stream_map:             tc.streams,
			})
			if got := decoder.fatalReason != ""; got != tc.wantFatal {
				t.Fatalf("fatal=%t, want %t (%q)", got, tc.wantFatal, decoder.fatalReason)
			}
		})
	}
}

func TestCurrentNextAndMediaESIDValidation(t *testing.T) {
	valid := func(videoID, audioID uint8) []*mpeg2.Elementary_stream_elem {
		return []*mpeg2.Elementary_stream_elem{
			{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: videoID},
			{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: audioID},
		}
	}
	for _, tc := range []struct {
		name    string
		current uint8
		streams []*mpeg2.Elementary_stream_elem
		want    bool
	}{
		{name: "video lower boundary", current: 1, streams: valid(0xe0, 0xc0), want: true},
		{name: "video upper boundary", current: 1, streams: valid(0xef, 0xdf), want: true},
		{name: "video below range", current: 1, streams: valid(0xdf, 0xc0)},
		{name: "audio above range", current: 1, streams: valid(0xe0, 0xe0)},
		{name: "not current", current: 0, streams: valid(0xe0, 0xc0)},
		{name: "duplicate video", current: 1, streams: append(valid(0xe0, 0xc0), &mpeg2.Elementary_stream_elem{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: 0xe1})},
		{name: "duplicate audio", current: 1, streams: append(valid(0xe0, 0xc0), &mpeg2.Elementary_stream_elem{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: 0xc1})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder, err := NewDecoder(new(recordingSink))
			if err != nil {
				t.Fatalf("new decoder: %v", err)
			}
			decoder.handlePSM(&mpeg2.Program_stream_map{Current_next_indicator: tc.current, Stream_map: tc.streams})
			if got := decoder.fatalReason == ""; got != tc.want {
				t.Fatalf("accepted=%t, want %t, reason=%q", got, tc.want, decoder.fatalReason)
			}
		})
	}
}

func TestAuxiliaryCommonPESDoesNotProduceMediaState(t *testing.T) {
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	result, err := decoder.Feed(handAuthoredAuxiliaryPS())
	if err != nil || result.Fatal || result.NeedMore {
		t.Fatalf("feed result=%+v err=%v", result, err)
	}
	if len(sink.events) != 0 || len(sink.resets) != 0 || decoder.order != 0 || decoder.audit.PTSSeen != 0 {
		t.Fatalf("auxiliary changed media state: events=%d resets=%d order=%d audit=%+v", len(sink.events), len(sink.resets), decoder.order, decoder.audit)
	}
}

func TestPSMMappingAddOrRemoveAuxiliaryIsFatal(t *testing.T) {
	decoder, err := NewDecoder(new(recordingSink))
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	media := []*mpeg2.Elementary_stream_elem{
		{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: 0xE0},
		{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: 0xC0},
	}
	decoder.handlePSM(&mpeg2.Program_stream_map{Current_next_indicator: 1, Stream_map: media})
	decoder.handlePSM(&mpeg2.Program_stream_map{Current_next_indicator: 1, Stream_map: append(media, &mpeg2.Elementary_stream_elem{Stream_type: 0x00, Elementary_stream_id: 0xBD})})
	if decoder.fatalReason != ResetUnsupportedProfile {
		t.Fatalf("add auxiliary fatal reason=%q", decoder.fatalReason)
	}

	decoder, err = NewDecoder(new(recordingSink))
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	withAux := append(media, &mpeg2.Elementary_stream_elem{Stream_type: 0x00, Elementary_stream_id: 0xBD})
	decoder.handlePSM(&mpeg2.Program_stream_map{Current_next_indicator: 1, Stream_map: withAux})
	decoder.handlePSM(&mpeg2.Program_stream_map{Current_next_indicator: 1, Stream_map: media})
	if decoder.fatalReason != ResetUnsupportedProfile {
		t.Fatalf("remove auxiliary fatal reason=%q", decoder.fatalReason)
	}
}

func acceptedDecoder(t *testing.T) *Decoder {
	t.Helper()
	decoder, err := NewDecoder(new(recordingSink))
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	decoder.handlePSM(&mpeg2.Program_stream_map{
		Current_next_indicator:     1,
		Program_stream_map_version: 1,
		Stream_map: []*mpeg2.Elementary_stream_elem{
			{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: 0xE0},
			{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: 0xC0},
		},
	})
	if decoder.fatalReason != "" {
		t.Fatalf("accepted profile became fatal: %q", decoder.fatalReason)
	}
	return decoder
}

func TestPSMVersionAndMappingRules(t *testing.T) {
	decoder := acceptedDecoder(t)
	decoder.handlePSM(&mpeg2.Program_stream_map{
		Current_next_indicator:     1,
		Program_stream_map_version: 2,
		Stream_map: []*mpeg2.Elementary_stream_elem{
			{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: 0xE0},
			{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: 0xC0},
		},
	})
	if decoder.fatalReason != "" || decoder.audit.PSMVersion != 2 {
		t.Fatalf("same mapping version change: fatal=%q audit=%+v", decoder.fatalReason, decoder.audit)
	}
	decoder.handlePSM(&mpeg2.Program_stream_map{
		Current_next_indicator: 1,
		Stream_map: []*mpeg2.Elementary_stream_elem{
			{Stream_type: uint8(mpeg2.PS_STREAM_H265), Elementary_stream_id: 0xE1},
			{Stream_type: uint8(mpeg2.PS_STREAM_G711A), Elementary_stream_id: 0xC0},
		},
	})
	if decoder.fatalReason != ResetUnsupportedProfile {
		t.Fatalf("mapping change fatal=%q", decoder.fatalReason)
	}
}

func TestNeedMorePacketDoesNotReadOrCountFields(t *testing.T) {
	decoder := acceptedDecoder(t)
	decoder.onPacket(&mpeg2.PesPacket{
		Stream_id:              0xff,
		PES_packet_length:      0,
		PES_header_data_length: 255,
		PTS_DTS_flags:          0,
		Pes_payload:            bytes.Repeat([]byte{1}, maxSuccessfulPESPayload),
	}, fakeNeedMoreError{})
	if decoder.fatalReason != "" || decoder.audit.PTSSeen != 0 {
		t.Fatalf("NeedMore changed state: fatal=%q audit=%+v", decoder.fatalReason, decoder.audit)
	}
}

func TestFailedPacketLatchesParserErrorWithoutReadingFields(t *testing.T) {
	decoder := acceptedDecoder(t)
	decoder.onPacket(&mpeg2.PesPacket{
		Stream_id:              0xff,
		PES_packet_length:      0,
		PES_header_data_length: 255,
		PTS_DTS_flags:          3,
		Pes_payload:            bytes.Repeat([]byte{1}, maxSuccessfulPESPayload),
	}, fakeParserError{})
	if decoder.fatalReason != ResetParserError || decoder.audit.PTSSeen != 0 {
		t.Fatalf("failed packet changed state: fatal=%q audit=%+v", decoder.fatalReason, decoder.audit)
	}
}

type fakeNeedMoreError struct{}

func (fakeNeedMoreError) Error() string          { return "need more" }
func (fakeNeedMoreError) NeedMore() bool         { return true }
func (fakeNeedMoreError) ParserError() bool      { return false }
func (fakeNeedMoreError) StreamIdNotFound() bool { return false }

type fakeParserError struct{}

func (fakeParserError) Error() string          { return "parser error" }
func (fakeParserError) NeedMore() bool         { return false }
func (fakeParserError) ParserError() bool      { return true }
func (fakeParserError) StreamIdNotFound() bool { return false }

func TestPESGateAndAuditCounters(t *testing.T) {
	tests := []struct {
		name   string
		packet *mpeg2.PesPacket
		reason ResetReason
	}{
		{
			name:   "zero length",
			packet: &mpeg2.PesPacket{Stream_id: 0xE0, PES_packet_length: 0, PES_header_data_length: 5, PTS_DTS_flags: 2, Pes_payload: []byte{1}},
			reason: ResetInvalidPES,
		},
		{
			name:   "short length",
			packet: &mpeg2.PesPacket{Stream_id: 0xE0, PES_packet_length: 7, PES_header_data_length: 5, PTS_DTS_flags: 2, Pes_payload: []byte{1}},
			reason: ResetInvalidPES,
		},
		{
			name:   "short optional header",
			packet: &mpeg2.PesPacket{Stream_id: 0xE0, PES_packet_length: 7, PES_header_data_length: 4, PTS_DTS_flags: 2, Pes_payload: []byte{1}},
			reason: ResetInvalidPES,
		},
		{
			name:   "missing pts",
			packet: &mpeg2.PesPacket{Stream_id: 0xE0, PES_packet_length: 4, PES_header_data_length: 1, Pes_payload: []byte{1}},
			reason: ResetMissingPTS,
		},
		{
			name:   "oversized payload",
			packet: &mpeg2.PesPacket{Stream_id: 0xE0, PES_packet_length: 0xffff, PES_header_data_length: 5, PTS_DTS_flags: 2, Pes_payload: bytes.Repeat([]byte{1}, maxSuccessfulPESPayload)},
			reason: ResetInvalidPES,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoder := acceptedDecoder(t)
			decoder.handlePES(tc.packet)
			if decoder.fatalReason != tc.reason {
				t.Fatalf("fatal=%q, want %q", decoder.fatalReason, tc.reason)
			}
			if decoder.audit.PTSSeen != 0 {
				t.Fatalf("invalid packet counted: %+v", decoder.audit)
			}
		})
	}

	decoder := acceptedDecoder(t)
	decoder.handlePES(&mpeg2.PesPacket{
		Stream_id: 0xE0, PES_packet_length: 8, PES_header_data_length: 5,
		PTS_DTS_flags: 2, Pes_payload: []byte{1},
	})
	decoder.handlePES(&mpeg2.PesPacket{
		Stream_id: 0xC0, PES_packet_length: 13, PES_header_data_length: 10,
		PTS_DTS_flags: 3, Pes_payload: []byte{1},
	})
	if decoder.fatalReason != "" || decoder.audit.PTSSeen != 2 || decoder.audit.DTSFallback != 1 || decoder.audit.DTSExplicit != 1 {
		t.Fatalf("valid audit=%+v fatal=%q", decoder.audit, decoder.fatalReason)
	}
}

func TestPESContinuationInheritsStreamTimestamp(t *testing.T) {
	decoder := acceptedDecoder(t)
	first := &mpeg2.PesPacket{
		Stream_id: 0xE0, PES_packet_length: 8, PES_header_data_length: 5,
		PTS_DTS_flags: 2, Pts: 90000, Dts: 90000, Pes_payload: []byte{1},
	}
	decoder.handlePES(first)
	continuation := &mpeg2.PesPacket{
		Stream_id: 0xE0, PES_packet_length: 4, PES_header_data_length: 1,
		Pes_payload: []byte{2},
	}
	decoder.handlePES(continuation)
	if decoder.fatalReason != "" || continuation.Pts != first.Pts || continuation.Dts != first.Dts {
		t.Fatalf("continuation timestamp=%d/%d fatal=%q", continuation.Pts, continuation.Dts, decoder.fatalReason)
	}
	if decoder.audit.PTSSeen != 1 || decoder.audit.DTSFallback != 1 || decoder.audit.DTSExplicit != 0 {
		t.Fatalf("audit=%+v", decoder.audit)
	}
}

func TestHeaderCoversFlags(t *testing.T) {
	tests := []struct {
		name   string
		packet *mpeg2.PesPacket
		want   bool
	}{
		{name: "pts exact", packet: &mpeg2.PesPacket{PTS_DTS_flags: 2, PES_header_data_length: 5}, want: true},
		{name: "pts short", packet: &mpeg2.PesPacket{PTS_DTS_flags: 2, PES_header_data_length: 4}},
		{name: "pts dts exact", packet: &mpeg2.PesPacket{PTS_DTS_flags: 3, PES_header_data_length: 10}, want: true},
		{name: "pts dts short", packet: &mpeg2.PesPacket{PTS_DTS_flags: 3, PES_header_data_length: 9}},
		{name: "all optional exact", packet: &mpeg2.PesPacket{
			PTS_DTS_flags: 2, PES_header_data_length: 18, ESCR_flag: 1,
			ES_rate_flag: 1, DSM_trick_mode_flag: 1, Additional_copy_info_flag: 1,
			PES_CRC_flag: 1,
		}, want: true},
		{name: "all optional short", packet: &mpeg2.PesPacket{
			PTS_DTS_flags: 2, PES_header_data_length: 17, ESCR_flag: 1,
			ES_rate_flag: 1, DSM_trick_mode_flag: 1, Additional_copy_info_flag: 1,
			PES_CRC_flag: 1,
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := headerCoversFlags(tc.packet); got != tc.want {
				t.Fatalf("got %t, want %t", got, tc.want)
			}
		})
	}
}

func TestWatchdogRejectsThresholdBeforeParser(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*Decoder)
	}{
		{name: "h265", set: func(decoder *Decoder) { decoder.h265Bytes = maxRawSinceH265Event - 1 }},
		{name: "pcma", set: func(decoder *Decoder) { decoder.pcmaBytes = maxRawSincePCMAEvent - 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := new(recordingSink)
			decoder, err := NewDecoder(sink)
			if err != nil {
				t.Fatalf("new decoder: %v", err)
			}
			tc.set(decoder)
			decoder.demux = nil
			result, err := decoder.Feed([]byte{0})
			if err != nil || !result.Fatal || len(sink.resets) != 1 || sink.resets[0].Reason != ResetLimit {
				t.Fatalf("result=%+v err=%v reset count=%d", result, err, len(sink.resets))
			}
		})
	}
}

func TestWatchdogExactBoundaryAndChunkLimit(t *testing.T) {
	decoder := acceptedDecoder(t)
	decoder.h265Bytes = maxRawSinceH265Event
	result, err := decoder.Feed(nil)
	if err != nil || !result.Fatal {
		t.Fatalf("exact watchdog result=%+v err=%v", result, err)
	}

	decoder, err = NewDecoder(new(recordingSink))
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	result, err = decoder.Feed(make([]byte, maxInputChunkBytes))
	if err != nil || result.Fatal || !result.NeedMore || decoder.h265Bytes != maxInputChunkBytes || decoder.pcmaBytes != maxInputChunkBytes {
		t.Fatalf("max chunk result=%+v err=%v", result, err)
	}
	if decoder.sink.(*recordingSink).resets != nil {
		t.Fatalf("max chunk emitted a reset")
	}

	decoder, err = NewDecoder(new(recordingSink))
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	decoder.h265Bytes = maxRawSinceH265Event - 2
	decoder.pcmaBytes = maxRawSincePCMAEvent - 2
	result, err = decoder.Feed([]byte{0})
	if err != nil || result.Fatal || !result.NeedMore || decoder.h265Bytes != maxRawSinceH265Event-1 || decoder.pcmaBytes != maxRawSincePCMAEvent-1 {
		t.Fatalf("below-limit result=%+v err=%v counters=%d/%d", result, err, decoder.h265Bytes, decoder.pcmaBytes)
	}

	decoder, err = NewDecoder(new(recordingSink))
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	result, err = decoder.Feed(make([]byte, maxInputChunkBytes+1))
	if err != nil || !result.Fatal {
		t.Fatalf("oversized chunk result=%+v err=%v", result, err)
	}
}

func TestNewDecoderRejectsNilSink(t *testing.T) {
	if _, err := NewDecoder(nil); err != ErrNilSink {
		t.Fatalf("nil sink error=%v", err)
	}
	var sink *recordingSink
	if _, err := NewDecoder(sink); err != ErrNilSink {
		t.Fatalf("typed nil sink error=%v", err)
	}
}

func TestPreProfileAndUnmappedPESAreFatal(t *testing.T) {
	decoder, err := NewDecoder(new(recordingSink))
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	decoder.handlePES(&mpeg2.PesPacket{Stream_id: 0xE0})
	if decoder.fatalReason != ResetUnsupportedProfile {
		t.Fatalf("pre-profile reason=%q", decoder.fatalReason)
	}
	for index, id := range []uint8{0xFC, 0xFD, 0xFE, 0xC1, 0xE1} {
		decoder := acceptedDecoder(t)
		decoder.handlePES(&mpeg2.PesPacket{Stream_id: id})
		if decoder.fatalReason != ResetUnsupportedProfile {
			t.Fatalf("unmapped id index=%d reason=%q", index, decoder.fatalReason)
		}
	}
}

func TestMPEG1AndFirstFatalWins(t *testing.T) {
	decoder := acceptedDecoder(t)
	decoder.onPacket(&mpeg2.PSPackHeader{IsMpeg1: true}, nil)
	decoder.handlePES(&mpeg2.PesPacket{Stream_id: 0xE0})
	if decoder.fatalReason != ResetUnsupportedProfile {
		t.Fatalf("fatal reason=%q", decoder.fatalReason)
	}
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	mpeg1Pack := []byte{0, 0, 1, 0xba, 0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	result, err := decoder.Feed(mpeg1Pack)
	if err != nil || !result.Fatal || len(sink.resets) != 1 || sink.resets[0].Reason != ResetUnsupportedProfile {
		t.Fatalf("mpeg1 feed result=%+v err=%v reset count=%d", result, err, len(sink.resets))
	}
}

func TestDataOwnershipAndRouteIsolation(t *testing.T) {
	leftSink := new(recordingSink)
	left, err := NewDecoder(leftSink)
	if err != nil {
		t.Fatalf("new left decoder: %v", err)
	}
	left.profile = profileState{accepted: true, h265ID: 0xE0, pcmaID: 0xC0}
	rightSink := new(recordingSink)
	right, err := NewDecoder(rightSink)
	if err != nil {
		t.Fatalf("new right decoder: %v", err)
	}
	right.profile = left.profile
	frame := []byte{0, 0, 1, 0x40, 1, 0xaa}
	left.onFrame(frame, mpeg2.PS_STREAM_H265, 1, 1)
	frame[5] = 0xbb
	right.onFrame([]byte{0, 0, 1, 0x40, 1, 0xcc}, mpeg2.PS_STREAM_H265, 1, 1)
	if leftSink.events[0].Data[5] != 0xaa || rightSink.events[0].Data[5] != 0xcc {
		t.Fatalf("event data was not owned")
	}
	left.latch(ResetParserError)
	left.commitReset()
	if len(rightSink.resets) != 0 || len(rightSink.events) != 1 {
		t.Fatalf("route isolation failed: right events=%d resets=%d", len(rightSink.events), len(rightSink.resets))
	}
}

func TestPanicBecomesReset(t *testing.T) {
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	result, err := decoder.Feed([]byte{0, 0, 1, 0xbb, 0, 0})
	if err != nil || !result.Fatal || len(sink.resets) != 1 || sink.resets[0].Reason != ResetPanic {
		t.Fatalf("result=%+v err=%v reset count=%d", result, err, len(sink.resets))
	}
}

func TestFatalSuppressesLaterCallbacksAndPreservesFirstCause(t *testing.T) {
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	decoder.profile = profileState{accepted: true, h265ID: 0xE0, pcmaID: 0xC0}
	decoder.onFrame([]byte{0, 0, 1, 0x40, 1, 0xaa}, mpeg2.PS_STREAM_H265, 1, 1)
	decoder.latch(ResetClassifier)
	decoder.onFrame([]byte{0, 0, 1, 0x40, 1, 0xbb}, mpeg2.PS_STREAM_H265, 2, 2)
	decoder.latch(ResetParserError)
	if len(sink.events) != 1 || sink.events[0].Data[5] != 0xaa || decoder.fatalReason != ResetClassifier {
		t.Fatalf("event count=%d first kind=%d fatal=%q", len(sink.events), sink.events[0].Kind, decoder.fatalReason)
	}
	decoder.commitReset()
	if len(sink.resets) != 1 || sink.resets[0].Reason != ResetClassifier || sink.resets[0].Epoch != 2 {
		t.Fatalf("reset count=%d", len(sink.resets))
	}
}

type fatalOnEventSink struct {
	decoder *Decoder
	count   int
	resets  []ResetEvent
}

func (s *fatalOnEventSink) OnElementary(ElementaryEvent) {
	s.count++
	s.decoder.latch(ResetClassifier)
}

func (s *fatalOnEventSink) OnReset(event ResetEvent) { s.resets = append(s.resets, event) }

func TestFeedCanDeliverEventsAndReturnNeedMore(t *testing.T) {
	data := handAuthoredPS()
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	result, err := decoder.Feed(data[:len(data)-1])
	if err != nil || result.Fatal || !result.NeedMore || len(sink.events) == 0 {
		t.Fatalf("result=%+v err=%v events=%d", result, err, len(sink.events))
	}
}

func TestFeedFatalTakesPrecedenceOverNeedMore(t *testing.T) {
	sink := &fatalOnEventSink{}
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	sink.decoder = decoder
	data := handAuthoredPS()
	result, err := decoder.Feed(data[:len(data)-1])
	if err != nil || !result.Fatal || result.NeedMore || sink.count == 0 || len(sink.resets) != 1 || sink.resets[0].Reason != ResetClassifier {
		t.Fatalf("result=%+v err=%v callbacks=%d reset count=%d", result, err, sink.count, len(sink.resets))
	}
}

func TestResetStartsNewEpochAndOrder(t *testing.T) {
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	decoder.profile = profileState{accepted: true, h265ID: 0xE0, pcmaID: 0xC0}
	decoder.onFrame([]byte{0, 0, 1, 0x40, 1, 0xaa}, mpeg2.PS_STREAM_H265, 1, 1)
	decoder.latch(ResetLimit)
	decoder.commitReset()
	decoder.profile = profileState{accepted: true, h265ID: 0xE0, pcmaID: 0xC0}
	decoder.onFrame([]byte{0, 0, 1, 0x40, 1, 0xbb}, mpeg2.PS_STREAM_H265, 2, 2)
	if len(sink.events) != 2 || sink.events[0].Epoch != 1 || sink.events[0].Order != 1 || sink.events[1].Epoch != 2 || sink.events[1].Order != 1 {
		t.Fatalf("events epoch/order=%d/%d,%d/%d", sink.events[0].Epoch, sink.events[0].Order, sink.events[1].Epoch, sink.events[1].Order)
	}
}

func TestCodecWatchdogsResetOnlyAfterMatchingEvent(t *testing.T) {
	data := handAuthoredPS()
	firstAudio := bytes.Index(data, []byte{0, 0, 1, 0xc0})
	if firstAudio < 0 {
		t.Fatal("golden audio PES not found")
	}
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	result, err := decoder.Feed(data[:firstAudio])
	if err != nil || result.Fatal || len(sink.events) == 0 || decoder.h265Bytes != 0 || decoder.pcmaBytes == 0 {
		t.Fatalf("result=%+v err=%v events=%d counters=%d/%d", result, err, len(sink.events), decoder.h265Bytes, decoder.pcmaBytes)
	}
}

func TestPCMAWatchdogResetsSymmetrically(t *testing.T) {
	sink := new(recordingSink)
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	decoder.h265Bytes = 17
	decoder.pcmaBytes = 23
	data := handAuthoredSamePTSAudioPS()
	result, err := decoder.Feed(data)
	if err != nil || result.Fatal || len(sink.events) != 1 || decoder.pcmaBytes != 0 || decoder.h265Bytes != 17+uint64(len(data)) {
		t.Fatalf("result=%+v err=%v events=%d counters=%d/%d", result, err, len(sink.events), decoder.h265Bytes, decoder.pcmaBytes)
	}
}

func TestFailureOnOneRouteDoesNotChangeAnother(t *testing.T) {
	actions := []struct {
		name string
		run  func(*Decoder)
	}{
		{name: "parser error", run: func(decoder *Decoder) { decoder.onPacket(nil, fakeParserError{}); decoder.commitReset() }},
		{name: "unsupported profile", run: func(decoder *Decoder) {
			decoder.handlePSM(&mpeg2.Program_stream_map{Current_next_indicator: 0})
			decoder.commitReset()
		}},
		{name: "missing pts", run: func(decoder *Decoder) {
			decoder.profile = profileState{accepted: true, h265ID: 0xE0, pcmaID: 0xC0}
			decoder.handlePES(&mpeg2.PesPacket{Stream_id: 0xE0, PES_packet_length: 4, PES_header_data_length: 1, Pes_payload: []byte{1}})
			decoder.commitReset()
		}},
		{name: "limit", run: func(decoder *Decoder) { decoder.h265Bytes = maxRawSinceH265Event; _, _ = decoder.Feed(nil) }},
		{name: "panic", run: func(decoder *Decoder) { _, _ = decoder.Feed([]byte{0, 0, 1, 0xbb, 0, 0}) }},
		{name: "abrupt close", run: func(decoder *Decoder) { _ = decoder.CloseAbrupt() }},
	}
	for _, tc := range actions {
		t.Run(tc.name, func(t *testing.T) {
			leftSink := new(recordingSink)
			left, err := NewDecoder(leftSink)
			if err != nil {
				t.Fatalf("new left decoder: %v", err)
			}
			rightSink := new(recordingSink)
			right, err := NewDecoder(rightSink)
			if err != nil {
				t.Fatalf("new right decoder: %v", err)
			}
			right.profile = profileState{accepted: true, h265ID: 0xE0, pcmaID: 0xC0}
			right.onFrame([]byte{0, 0, 1, 0x40, 1, 0xcc}, mpeg2.PS_STREAM_H265, 1, 1)
			tc.run(left)
			if len(rightSink.events) != 1 || len(rightSink.resets) != 0 || rightSink.events[0].Epoch != 1 || rightSink.events[0].Order != 1 {
				t.Fatalf("right changed: event count=%d reset count=%d epoch/order=%d/%d", len(rightSink.events), len(rightSink.resets), rightSink.events[0].Epoch, rightSink.events[0].Order)
			}
		})
	}
}

func TestReentrantCallsAreRejected(t *testing.T) {
	sink := &reentrantSink{}
	decoder, err := NewDecoder(sink)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	sink.decoder = decoder
	decoder.profile = profileState{accepted: true, h265ID: 0xE0, pcmaID: 0xC0}
	decoder.inCall.Store(true)
	decoder.onFrame([]byte{0, 0, 1, 0x40, 0x01, 0xAA}, mpeg2.PS_STREAM_H265, 1, 1)
	decoder.inCall.Store(false)
	if sink.err != ErrReentrant {
		t.Fatalf("reentrant error = %v", sink.err)
	}
	before := decoderStateForTest(decoder)
	decoder.inCall.Store(true)
	if _, err := decoder.Feed(nil); err != ErrReentrant {
		t.Fatalf("reentrant feed error=%v", err)
	}
	if err := decoder.CloseAbrupt(); err != ErrReentrant {
		t.Fatalf("reentrant close error=%v", err)
	}
	if _, err := decoder.Audit(); err != ErrReentrant {
		t.Fatalf("reentrant audit error=%v", err)
	}
	if after := decoderStateForTest(decoder); !reflect.DeepEqual(before, after) {
		t.Fatalf("reentrant call changed decoder state")
	}
	decoder.inCall.Store(false)
}

type decoderStateSnapshot struct {
	epoch        uint64
	order        uint64
	profile      profileState
	audit        AuditSnapshot
	h265Bytes    uint64
	pcmaBytes    uint64
	feedH265     bool
	feedPCMA     bool
	lastNeedMore bool
	closed       bool
	flushed      bool
	fatalReason  ResetReason
	demux        *mpeg2.PSDemuxer
}

func decoderStateForTest(decoder *Decoder) decoderStateSnapshot {
	return decoderStateSnapshot{
		epoch: decoder.epoch, order: decoder.order, profile: decoder.profile,
		audit: decoder.audit, h265Bytes: decoder.h265Bytes, pcmaBytes: decoder.pcmaBytes,
		feedH265: decoder.feedH265, feedPCMA: decoder.feedPCMA,
		lastNeedMore: decoder.lastNeedMore, closed: decoder.closed, flushed: decoder.flushed,
		fatalReason: decoder.fatalReason, demux: decoder.demux,
	}
}

type reentrantSink struct {
	decoder *Decoder
	err     error
}

func (s *reentrantSink) OnElementary(ElementaryEvent) {
	_, s.err = s.decoder.Audit()
}

func (s *reentrantSink) OnReset(ResetEvent) {}
