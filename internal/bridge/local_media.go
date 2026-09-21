package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pion/rtp"
)

const (
	localInterleavedMagic      = byte('$')
	localInterleavedHeaderSize = 4
	localInterleavedMaxPayload = 1<<16 - 1
	localRTPPrefixLimit        = 4096
	localReadDeadline          = outputSourceVideoRecoveryAfter
)

var errLocalRTPUnavailable = errors.New("local RTP stream unavailable")

type localRTPSource struct {
	conn      net.Conn
	closeOnce sync.Once
}

func newLocalRTPSource(conn net.Conn) *localRTPSource {
	return &localRTPSource{conn: conn}
}

func (s *localRTPSource) Read(ctx context.Context) (SourceMessage, error) {
	if s.conn == nil {
		return SourceMessage{}, fmt.Errorf("%w: connection is nil", errLocalRTPUnavailable)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = s.conn.SetReadDeadline(deadline)
	} else {
		_ = s.conn.SetReadDeadline(time.Now().Add(localReadDeadline))
	}
	payload, err := readLocalRTPPayload(s.conn)
	if err != nil {
		if ctx.Err() != nil {
			return SourceMessage{}, ctx.Err()
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return SourceMessage{}, fmt.Errorf("%w: read timeout", errLocalRTPUnavailable)
		}
		return SourceMessage{}, fmt.Errorf("%w: %v", errLocalRTPUnavailable, err)
	}
	return SourceMessage{Binary: true, Data: payload}, nil
}

func (s *localRTPSource) Close() {
	if s.conn == nil {
		return
	}
	s.closeOnce.Do(func() { _ = s.conn.Close() })
}

func readLocalRTPPayload(reader io.Reader) ([]byte, error) {
	var prefix [1]byte
	for skipped := 0; skipped <= localRTPPrefixLimit; skipped++ {
		if _, err := io.ReadFull(reader, prefix[:]); err != nil {
			return nil, err
		}
		if prefix[0] != localInterleavedMagic {
			continue
		}
		var headerTail [localInterleavedHeaderSize - 1]byte
		if _, err := io.ReadFull(reader, headerTail[:]); err != nil {
			return nil, err
		}
		payloadLength := int(headerTail[1])<<8 | int(headerTail[2])
		if payloadLength <= 0 || payloadLength > localInterleavedMaxPayload {
			return nil, fmt.Errorf("invalid local RTP payload length %d", payloadLength)
		}
		payload := make([]byte, payloadLength)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, err
		}
		var packet rtp.Packet
		if err := packet.Unmarshal(payload); err != nil {
			return nil, fmt.Errorf("parse local RTP packet: %w", err)
		}
		return stripLocalPSFragmentHeader(packet.Payload), nil
	}
	return nil, fmt.Errorf("local RTP prefix exceeded %d bytes", localRTPPrefixLimit)
}

// stripLocalPSFragmentHeader removes the small CATLINK/EZVIZ local-media
// envelope that sits inside the RTP payload. It is not an H.265 FU-A header:
// the bytes after it are MPEG-PS, and the PS PES length excludes the envelope.
func stripLocalPSFragmentHeader(payload []byte) []byte {
	if len(payload) >= 5 && payload[0] == 0x0d &&
		payload[1] == 0x00 && payload[2] == 0x00 && payload[3] == 0x01 {
		return append([]byte(nil), payload[1:]...)
	}
	if len(payload) >= 3 && payload[0] == 0x1c &&
		(payload[1] == 0x00 || payload[1] == 0x40 || payload[1] == 0x80) {
		return append([]byte(nil), payload[2:]...)
	}
	return append([]byte(nil), payload...)
}
