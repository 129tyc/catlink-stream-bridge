package bridge

import (
	"bytes"
	"io"
	"testing"

	"github.com/pion/rtp"
)

func TestReadLocalRTPPayloadHandlesPrefixAndFragmentedFrame(t *testing.T) {
	packet := &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: 7, Timestamp: 9000},
		Payload: []byte{0, 0, 1, 0xba, 1, 2, 3},
	}
	raw, err := packet.Marshal()
	if err != nil {
		t.Fatalf("marshal RTP: %v", err)
	}
	frame := append([]byte{'$', 1, byte(len(raw) >> 8), byte(len(raw))}, raw...)
	input := append([]byte{0x01, 0x02, 0x03}, frame...)
	reader := &fragmentedReader{data: input, chunkSize: 2}
	got, err := readLocalRTPPayload(reader)
	if err != nil {
		t.Fatalf("read RTP: %v", err)
	}
	if !bytes.Equal(got, packet.Payload) {
		t.Fatalf("payload=%x want=%x", got, packet.Payload)
	}
}

func TestReadLocalRTPPayloadStripsCatlinkPSFragmentHeaders(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		body   []byte
	}{
		{name: "complete audio or pack", prefix: []byte{0x0d}, body: []byte{0, 0, 1, 0xc0, 1, 2}},
		{name: "fragment start", prefix: []byte{0x1c, 0x80}, body: []byte{0, 0, 1, 0xba, 3, 4}},
		{name: "fragment middle", prefix: []byte{0x1c, 0x00}, body: []byte{5, 6, 7}},
		{name: "fragment end", prefix: []byte{0x1c, 0x40}, body: []byte{8, 9, 10}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packet := &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96}, Payload: append(tt.prefix, tt.body...)}
			raw, err := packet.Marshal()
			if err != nil {
				t.Fatalf("marshal RTP: %v", err)
			}
			frame := append([]byte{'$', 0, byte(len(raw) >> 8), byte(len(raw))}, raw...)
			got, err := readLocalRTPPayload(bytes.NewReader(frame))
			if err != nil {
				t.Fatalf("read RTP: %v", err)
			}
			if !bytes.Equal(got, tt.body) {
				t.Fatalf("payload=%x want=%x", got, tt.body)
			}
		})
	}
}

func TestStripLocalPSFragmentHeaderLeavesBarePSAlone(t *testing.T) {
	bare := []byte{0, 0, 1, 0xba, 1, 2, 3}
	if got := stripLocalPSFragmentHeader(bare); !bytes.Equal(got, bare) {
		t.Fatalf("bare PS changed: %x", got)
	}
}

func TestReadLocalRTPPayloadRejectsOversizedPrefix(t *testing.T) {
	input := bytes.Repeat([]byte{0x01}, localRTPPrefixLimit+1)
	if _, err := readLocalRTPPayload(bytes.NewReader(input)); err == nil {
		t.Fatal("oversized prefix was accepted")
	}
}

type fragmentedReader struct {
	data      []byte
	chunkSize int
}

func (r *fragmentedReader) Read(target []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := r.chunkSize
	if n > len(target) {
		n = len(target)
	}
	if n > len(r.data) {
		n = len(r.data)
	}
	copy(target[:n], r.data[:n])
	r.data = r.data[n:]
	return n, nil
}
