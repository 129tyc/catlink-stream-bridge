package bridge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/pion/rtp"
)

type recordingTalkTransport struct {
	started chan struct{}
	stopped chan struct{}
	writes  chan TalkAudio
	start   func(context.Context, TalkStart) error
	mu      sync.Mutex
	closed  bool
}

func newRecordingTalkTransport() *recordingTalkTransport {
	return &recordingTalkTransport{
		started: make(chan struct{}),
		stopped: make(chan struct{}),
		writes:  make(chan TalkAudio, 16),
	}
}

func (t *recordingTalkTransport) Start(ctx context.Context, params TalkStart) error {
	if t.start != nil {
		if err := t.start(ctx, params); err != nil {
			return err
		}
	}
	select {
	case <-t.started:
	default:
		close(t.started)
	}
	return nil
}

func (t *recordingTalkTransport) WriteAudio(_ context.Context, audio TalkAudio) error {
	t.writes <- audio
	return nil
}

func (t *recordingTalkTransport) Stop(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.closed = true
		close(t.stopped)
	}
	return nil
}

func (t *recordingTalkTransport) Close() {}

func testTalkTarget() TalkTarget {
	return TalkTarget{
		AccountID: "account",
		Device:    ResolvedDevice{EZOpenSerial: "device"},
		Channel:   1,
		SnapshotProvider: newTalkSnapshotProvider(func(_ context.Context, target TalkTarget) (TalkStart, error) {
			return TalkStart{CameraToken: "camera", Device: target.Device, Channel: target.Channel}, nil
		}, nil),
	}
}

func testRTP(payload []byte, timestamp uint32) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{PayloadType: 8, Timestamp: timestamp}, Payload: payload}
}

func TestTalkRegistryStartsAndForwardsPCMA(t *testing.T) {
	transport := newRecordingTalkTransport()
	registry := NewTalkRegistry(func(params TalkStart) TalkTransport {
		if params.Device.EZOpenSerial != "device" || params.Channel != 1 {
			t.Fatalf("unexpected talk target: %+v", params)
		}
		if params.CameraToken != "camera" || params.TalkStreamToken != "" {
			t.Fatalf("unexpected talk token boundary: %+v", params)
		}
		return transport
	})
	defer registry.Close()

	session := new(gortsplib.ServerSession)
	registry.Ingest(testTalkTarget(), session, testRTP([]byte{1, 2, 3}, 0))
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("talk transport did not start")
	}
	select {
	case audio := <-transport.writes:
		if string(audio.Payload) != string([]byte{1, 2, 3}) {
			t.Fatalf("payload=%v", audio.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("talk payload was not forwarded")
	}

	registry.ReleaseSession(session)
	select {
	case <-transport.stopped:
	case <-time.After(time.Second):
		t.Fatal("talk transport did not stop")
	}
}

func TestTalkRegistryUsesGenerationFencing(t *testing.T) {
	registry := NewTalkRegistry(func(TalkStart) TalkTransport {
		transport := newRecordingTalkTransport()
		return transport
	})
	defer registry.Close()

	target := testTalkTarget()
	firstSession := new(gortsplib.ServerSession)
	registry.Ingest(target, firstSession, testRTP([]byte{1}, 0))
	registry.mu.Lock()
	oldEntry := registry.entries[talkKey{accountID: "account", serial: "device", channel: 1}]
	registry.mu.Unlock()
	registry.ReleaseSession(firstSession)
	select {
	case <-oldEntry.done:
	case <-time.After(time.Second):
		t.Fatal("old talk worker did not finish before new owner")
	}

	secondSession := new(gortsplib.ServerSession)
	registry.Ingest(target, secondSession, testRTP([]byte{2}, 1))

	registry.mu.Lock()
	current := registry.entries[talkKey{accountID: "account", serial: "device", channel: 1}]
	registry.mu.Unlock()
	if current == nil || current.lease.owner != secondSession || current.lease.generation != 2 {
		t.Fatalf("old cleanup affected new owner: %+v", current)
	}
	registry.ReleaseSession(secondSession)
}

func TestTalkRegistryReleasesInactiveLeaseWithoutClosingReader(t *testing.T) {
	transport := newRecordingTalkTransport()
	registry := NewTalkRegistry(func(TalkStart) TalkTransport { return transport })
	registry.leaseDuration = 30 * time.Millisecond
	defer registry.Close()

	session := new(gortsplib.ServerSession)
	registry.Ingest(testTalkTarget(), session, testRTP([]byte{1}, 0))
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("talk transport did not start")
	}
	select {
	case <-transport.stopped:
	case <-time.After(time.Second):
		t.Fatal("inactive talk lease did not stop")
	}
	registry.mu.Lock()
	_, stillOwned := registry.entries[talkKey{accountID: "account", serial: "device", channel: 1}]
	registry.mu.Unlock()
	if stillOwned {
		t.Fatal("inactive lease still owns the talk slot")
	}
}

func TestTalkRegistryBoundsQueueByBytesAndDuration(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	registry := NewTalkRegistry(func(TalkStart) TalkTransport {
		transport := newRecordingTalkTransport()
		transport.start = func(ctx context.Context, _ TalkStart) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		close(started)
		return transport
	})
	defer registry.Close()

	session := new(gortsplib.ServerSession)
	target := testTalkTarget()
	for i := 0; i < 5; i++ {
		registry.Ingest(target, session, testRTP(make([]byte, 1024), uint32(i)))
	}
	<-started
	registry.mu.Lock()
	entry := registry.entries[talkKey{accountID: "account", serial: "device", channel: 1}]
	queueBytes := entry.queueBytes
	queueLen := len(entry.queue)
	registry.mu.Unlock()
	if queueBytes > talkQueueMaxBytes || queueLen > 4 {
		t.Fatalf("queue bytes=%d len=%d", queueBytes, queueLen)
	}
	close(release)
	registry.ReleaseSession(session)
}

func TestTalkRegistryTransportUnavailableDoesNotPanic(t *testing.T) {
	registry := NewTalkRegistry(func(TalkStart) TalkTransport {
		return talkErrorTransport{err: ErrTalkTransportUnavailable}
	})
	registry.leaseDuration = 20 * time.Millisecond
	defer registry.Close()

	registry.Ingest(testTalkTarget(), new(gortsplib.ServerSession), testRTP([]byte{1}, 0))
	time.Sleep(40 * time.Millisecond)
	if registry.closing {
		t.Fatal("registry closed after transport failure")
	}
}

func TestTalkRegistryRefreshesSnapshotAfterStartFailure(t *testing.T) {
	var snapshots atomic.Int32
	snapshotReady := make(chan struct{}, 2)
	registry := NewTalkRegistry(func(TalkStart) TalkTransport {
		return talkErrorTransport{err: errors.New("temporary start failure")}
	})
	defer registry.Close()
	target := testTalkTarget()
	target.SnapshotProvider = newTalkSnapshotProvider(func(_ context.Context, target TalkTarget) (TalkStart, error) {
		n := snapshots.Add(1)
		snapshotReady <- struct{}{}
		return TalkStart{CameraToken: fmt.Sprintf("camera-%d", n), Device: target.Device, Channel: target.Channel}, nil
	}, nil)
	registry.Ingest(target, new(gortsplib.ServerSession), testRTP([]byte{1}, 0))
	for i := 0; i < 2; i++ {
		select {
		case <-snapshotReady:
		case <-time.After(2500 * time.Millisecond):
			t.Fatalf("snapshot %d was not requested", i+1)
		}
	}
}

func TestTalkRegistryInvalidatesSnapshotOnAuthorizationFailure(t *testing.T) {
	invalidated := make(chan struct{})
	registry := NewTalkRegistry(func(TalkStart) TalkTransport {
		return talkErrorTransport{err: fmt.Errorf("%w: expired", ErrTalkAuthorization)}
	})
	defer registry.Close()
	target := testTalkTarget()
	target.SnapshotProvider = newTalkSnapshotProvider(func(_ context.Context, target TalkTarget) (TalkStart, error) {
		return TalkStart{CameraToken: "camera", Device: target.Device, Channel: target.Channel}, nil
	}, func() {
		select {
		case <-invalidated:
		default:
			close(invalidated)
		}
	})
	registry.Ingest(target, new(gortsplib.ServerSession), testRTP([]byte{1}, 0))
	select {
	case <-invalidated:
	case <-time.After(time.Second):
		t.Fatal("authorization failure did not invalidate snapshot")
	}
}

func TestTalkRegistryStopsFailedTransportBeforeRetry(t *testing.T) {
	transport := newRecordingTalkTransport()
	transport.start = func(context.Context, TalkStart) error { return nil }
	writeFailed := false
	registry := NewTalkRegistry(func(TalkStart) TalkTransport {
		return &failOnceTalkTransport{transport: transport, failed: &writeFailed}
	})
	defer registry.Close()
	target := testTalkTarget()
	registry.Ingest(target, new(gortsplib.ServerSession), testRTP([]byte{1}, 0))
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("transport did not start")
	}
	select {
	case <-transport.stopped:
	case <-time.After(time.Second):
		t.Fatal("failed transport was not stopped before retry")
	}
}

func TestTalkRegistryIsolatesTransportFailureByKey(t *testing.T) {
	good := newRecordingTalkTransport()
	badStopped := make(chan struct{})
	registry := NewTalkRegistry(func(params TalkStart) TalkTransport {
		if params.Device.EZOpenSerial == "bad-device" {
			return stopRecordingErrorTransport{stopped: badStopped}
		}
		return good
	})
	defer registry.Close()
	goodTarget := testTalkTarget()
	badTarget := testTalkTarget()
	badTarget.Device.EZOpenSerial = "bad-device"
	goodSession := new(gortsplib.ServerSession)
	badSession := new(gortsplib.ServerSession)
	registry.Ingest(badTarget, badSession, testRTP([]byte{1}, 0))
	registry.Ingest(goodTarget, goodSession, testRTP([]byte{2}, 0))
	select {
	case <-good.writes:
	case <-time.After(time.Second):
		t.Fatal("healthy key did not receive audio")
	}
	select {
	case <-badStopped:
	case <-time.After(time.Second):
		t.Fatal("failed key was not stopped")
	}
	registry.ReleaseSession(goodSession)
	registry.ReleaseSession(badSession)
}

type talkErrorTransport struct{ err error }

func (t talkErrorTransport) Start(context.Context, TalkStart) error { return t.err }
func (t talkErrorTransport) WriteAudio(context.Context, TalkAudio) error {
	return errors.New("write failed")
}
func (talkErrorTransport) Stop(context.Context) error { return nil }
func (talkErrorTransport) Close()                     {}

type failOnceTalkTransport struct {
	transport *recordingTalkTransport
	failed    *bool
}

func (t *failOnceTalkTransport) Start(ctx context.Context, params TalkStart) error {
	return t.transport.Start(ctx, params)
}

func (t *failOnceTalkTransport) WriteAudio(_ context.Context, _ TalkAudio) error {
	if !*t.failed {
		*t.failed = true
		return errors.New("write failed")
	}
	return nil
}

func (t *failOnceTalkTransport) Stop(ctx context.Context) error { return t.transport.Stop(ctx) }
func (t *failOnceTalkTransport) Close()                         {}

type stopRecordingErrorTransport struct{ stopped chan struct{} }

func (t stopRecordingErrorTransport) Start(context.Context, TalkStart) error { return nil }
func (t stopRecordingErrorTransport) WriteAudio(context.Context, TalkAudio) error {
	return errors.New("write failed")
}
func (t stopRecordingErrorTransport) Stop(context.Context) error {
	select {
	case <-t.stopped:
	default:
		close(t.stopped)
	}
	return nil
}
func (t stopRecordingErrorTransport) Close() {}
