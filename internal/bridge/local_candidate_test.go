package bridge

import (
	"testing"

	"github.com/129tyc/catlink-stream-bridge/internal/mpegps"
)

func TestLocalCandidateGateRequiresVideoKeyframeAndAudio(t *testing.T) {
	gate := &localCandidateGate{}
	params := []mpegps.ElementaryEvent{
		{Kind: mpegps.KindH265NALU, Data: []byte{0, 0, 1, 0x40, 1}},
		{Kind: mpegps.KindH265NALU, Data: []byte{0, 0, 1, 0x42, 1}},
		{Kind: mpegps.KindH265NALU, Data: []byte{0, 0, 1, 0x44, 1}},
	}
	for _, event := range params {
		gate.OnElementary(event)
	}
	if gate.ready {
		t.Fatal("parameter sets alone must not make a candidate ready")
	}
	gate.OnElementary(mpegps.ElementaryEvent{Kind: mpegps.KindPCMAPayload, Data: []byte{1}})
	if gate.ready {
		t.Fatal("audio plus parameter sets without a keyframe must not make a candidate ready")
	}
	gate.OnElementary(mpegps.ElementaryEvent{Kind: mpegps.KindH265NALU, Data: []byte{0, 0, 1, 0x26, 1}})
	if !gate.ready {
		t.Fatal("candidate should be ready after parameter sets, audio, and an IRAP")
	}
}
