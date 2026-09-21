package bridge

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/pion/rtp"
)

const (
	talkInputLease       = 5 * time.Second
	talkQueueMaxBytes    = 4 * 1024
	talkQueueMaxDuration = 500 * time.Millisecond
	talkStartTimeout     = 10 * time.Second
	talkWriteTimeout     = 2 * time.Second
	talkStopTimeout      = 2 * time.Second
	talkRetryBackoff     = time.Second
)

var ErrTalkTransportUnavailable = errors.New("CATLINK talk transport unavailable")
var ErrTalkSnapshotUnavailable = errors.New("CATLINK talk snapshot unavailable")

type ResolvedDevice struct {
	APIID              string
	DeviceName         string
	Model              string
	DeviceType         string
	VisibleSerial      string
	EZOpenSerial       string
	LocalIP            string
	LocalCmdPort       int
	LocalStreamPort    int
	CASIP              string
	CASPort            int
	LocalOperationCode string
	LocalKey           string
	LocalIsEncrypt     bool
}

type AccountSessionSnapshot struct {
	CameraToken string
	Device      ResolvedDevice
	Generation  uint64
}

type TalkTarget struct {
	AccountID        string
	Device           ResolvedDevice
	Channel          int
	SnapshotProvider TalkSnapshotProvider
}

type TalkAudio struct {
	Payload []byte
	RTPTime uint32
	Marker  bool
}

type TalkStart struct {
	CameraToken     string
	TalkStreamToken string
	Device          ResolvedDevice
	Channel         int
}

type TalkSnapshotProvider interface {
	Snapshot(context.Context, TalkTarget) (TalkStart, error)
}

type TalkSnapshotInvalidator interface {
	Invalidate()
}

type talkSnapshotProvider struct {
	snapshot   func(context.Context, TalkTarget) (TalkStart, error)
	invalidate func()
}

func (p talkSnapshotProvider) Snapshot(ctx context.Context, target TalkTarget) (TalkStart, error) {
	return p.snapshot(ctx, target)
}

func (p talkSnapshotProvider) Invalidate() {
	if p.invalidate != nil {
		p.invalidate()
	}
}

func newTalkSnapshotProvider(snapshot func(context.Context, TalkTarget) (TalkStart, error), invalidate func()) TalkSnapshotProvider {
	return talkSnapshotProvider{snapshot: snapshot, invalidate: invalidate}
}

type TalkTransport interface {
	Start(context.Context, TalkStart) error
	WriteAudio(context.Context, TalkAudio) error
	Stop(context.Context) error
	Close()
}

type TalkTransportFactory func(TalkStart) TalkTransport

type talkKey struct {
	accountID string
	serial    string
	channel   int
}

type talkLease struct {
	key        talkKey
	owner      *gortsplib.ServerSession
	generation uint64
}

type talkEntry struct {
	registry *TalkRegistry
	lease    talkLease
	target   TalkTarget
	snapshot TalkSnapshotProvider

	stop chan struct{}
	wake chan struct{}
	done chan struct{}

	queue      []TalkAudio
	queueBytes int
	expiresAt  time.Time
	closed     bool
}

type TalkRegistry struct {
	mu            sync.Mutex
	entries       map[talkKey]*talkEntry
	generations   map[talkKey]uint64
	closing       bool
	now           func() time.Time
	leaseDuration time.Duration
	factory       TalkTransportFactory
	waitOnce      sync.Once
}

func NewTalkRegistry(factory TalkTransportFactory) *TalkRegistry {
	if factory == nil {
		factory = func(TalkStart) TalkTransport { return unavailableTalkTransport{} }
	}
	return &TalkRegistry{
		entries:       make(map[talkKey]*talkEntry),
		generations:   make(map[talkKey]uint64),
		now:           time.Now,
		leaseDuration: talkInputLease,
		factory:       factory,
	}
}

func (r *TalkRegistry) Ingest(target TalkTarget, session *gortsplib.ServerSession, packet *rtp.Packet) {
	if session == nil || packet == nil || len(packet.Payload) == 0 {
		return
	}
	if target.AccountID == "" || target.Device.EZOpenSerial == "" || target.Channel < 1 {
		return
	}
	if target.SnapshotProvider == nil {
		return
	}
	audio := TalkAudio{Payload: append([]byte(nil), packet.Payload...), RTPTime: packet.Timestamp, Marker: packet.Marker}
	key := talkKey{accountID: target.AccountID, serial: target.Device.EZOpenSerial, channel: target.Channel}
	now := r.now()

	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return
	}
	entry := r.entries[key]
	if entry != nil && entry.closed {
		r.mu.Unlock()
		return
	}
	if entry != nil && !now.Before(entry.expiresAt) {
		r.retireLocked(entry)
		r.mu.Unlock()
		return
	}
	if entry != nil && entry.lease.owner != session {
		r.mu.Unlock()
		return
	}
	if entry == nil {
		entry = &talkEntry{
			registry: r,
			lease:    talkLease{key: key, owner: session, generation: r.nextGenerationLocked(key)},
			target:   target,
			snapshot: target.SnapshotProvider,
			stop:     make(chan struct{}),
			wake:     make(chan struct{}, 1),
			done:     make(chan struct{}),
		}
		r.entries[key] = entry
		go entry.run()
	}
	entry.expiresAt = now.Add(r.leaseDuration)
	entry.enqueueLocked(audio)
	r.mu.Unlock()
	entry.signal()
}

func (r *TalkRegistry) nextGenerationLocked(key talkKey) uint64 {
	r.generations[key]++
	return r.generations[key]
}

func (e *talkEntry) enqueueLocked(audio TalkAudio) {
	if len(audio.Payload) > talkQueueMaxBytes {
		return
	}
	for e.queueBytes+len(audio.Payload) > talkQueueMaxBytes || e.queueDurationLocked()+audioDuration(audio) > talkQueueMaxDuration {
		if len(e.queue) == 0 {
			return
		}
		e.queueBytes -= len(e.queue[0].Payload)
		e.queue = e.queue[1:]
	}
	e.queue = append(e.queue, audio)
	e.queueBytes += len(audio.Payload)
}

func (e *talkEntry) queueDurationLocked() time.Duration {
	return time.Duration(e.queueBytes) * time.Second / 8000
}

func audioDuration(audio TalkAudio) time.Duration {
	return time.Duration(len(audio.Payload)) * time.Second / 8000
}

func (e *talkEntry) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *talkEntry) run() {
	defer close(e.done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-e.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	var transport TalkTransport
	retryAt := time.Time{}
	defer func() {
		if transport != nil {
			ctx, cancel := context.WithTimeout(context.Background(), talkStopTimeout)
			_ = transport.Stop(ctx)
			cancel()
			transport.Close()
		}
		e.registry.finish(e)
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		if !e.current() {
			return
		}
		if e.expired() {
			e.registry.retire(e)
			return
		}
		if transport == nil && !retryAt.IsZero() && e.registry.now().Before(retryAt) {
			waitUntil := retryAt
			if expiry := e.expiry(); expiry.Before(waitUntil) {
				waitUntil = expiry
			}
			e.waitUntil(waitUntil)
			continue
		}
		if transport == nil {
			startCtx, startCancel := context.WithTimeout(ctx, talkStartTimeout)
			start, err := e.snapshot.Snapshot(startCtx, e.target)
			startCancel()
			if err != nil {
				if errors.Is(err, ErrTalkAuthorization) {
					if invalidator, ok := e.snapshot.(TalkSnapshotInvalidator); ok {
						invalidator.Invalidate()
					}
				}
				retryAt = e.registry.now().Add(talkRetryBackoff)
				continue
			}
			transport = e.registry.factory(start)
			if transport == nil {
				transport = unavailableTalkTransport{}
			}
			startCtx, startCancel = context.WithTimeout(ctx, talkStartTimeout)
			err = transport.Start(startCtx, start)
			startCancel()
			if err != nil {
				if errors.Is(err, ErrTalkAuthorization) {
					if invalidator, ok := e.snapshot.(TalkSnapshotInvalidator); ok {
						invalidator.Invalidate()
					}
				}
				transport.Close()
				transport = nil
				retryAt = e.registry.now().Add(talkRetryBackoff)
				if errors.Is(err, ErrTalkTransportUnavailable) {
					retryAt = e.registry.now().Add(30 * time.Second)
				}
				continue
			}
			retryAt = time.Time{}
		}

		audio, ok := e.pop()
		if !ok {
			e.waitUntil(e.expiry())
			continue
		}
		writeCtx, writeCancel := context.WithTimeout(ctx, talkWriteTimeout)
		err := transport.WriteAudio(writeCtx, audio)
		writeCancel()
		if err == nil {
			continue
		}
		if errors.Is(err, ErrTalkAuthorization) {
			if invalidator, ok := e.snapshot.(TalkSnapshotInvalidator); ok {
				invalidator.Invalidate()
			}
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), talkStopTimeout)
		_ = transport.Stop(stopCtx)
		stopCancel()
		transport.Close()
		transport = nil
		retryAt = e.registry.now().Add(talkRetryBackoff)
	}
}

func (e *talkEntry) current() bool {
	e.registry.mu.Lock()
	defer e.registry.mu.Unlock()
	return e.registry.entries[e.lease.key] == e && !e.closed && !e.registry.closing
}

func (e *talkEntry) expired() bool {
	e.registry.mu.Lock()
	defer e.registry.mu.Unlock()
	return !e.registry.now().Before(e.expiresAt)
}

func (e *talkEntry) expiry() time.Time {
	e.registry.mu.Lock()
	defer e.registry.mu.Unlock()
	return e.expiresAt
}

func (e *talkEntry) pop() (TalkAudio, bool) {
	e.registry.mu.Lock()
	defer e.registry.mu.Unlock()
	if e.closed || len(e.queue) == 0 {
		return TalkAudio{}, false
	}
	audio := e.queue[0]
	e.queueBytes -= len(audio.Payload)
	e.queue = e.queue[1:]
	return audio, true
}

func (e *talkEntry) waitUntil(deadline time.Time) {
	duration := time.Until(deadline)
	if duration <= 0 {
		return
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-e.wake:
	case <-e.stop:
	case <-timer.C:
	}
}

func (r *TalkRegistry) retire(entry *talkEntry) {
	r.mu.Lock()
	r.retireLocked(entry)
	r.mu.Unlock()
}

func (r *TalkRegistry) retireLocked(entry *talkEntry) {
	if entry == nil || entry.closed {
		return
	}
	entry.closed = true
	close(entry.stop)
}

func (r *TalkRegistry) finish(entry *talkEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries[entry.lease.key] == entry {
		delete(r.entries, entry.lease.key)
	}
}

func (r *TalkRegistry) ReleaseSession(session *gortsplib.ServerSession) {
	if session == nil {
		return
	}
	r.mu.Lock()
	for _, entry := range r.entries {
		if entry.lease.owner == session {
			r.retireLocked(entry)
		}
	}
	r.mu.Unlock()
}

func (r *TalkRegistry) Close() {
	r.waitOnce.Do(func() {
		r.mu.Lock()
		r.closing = true
		entries := make([]*talkEntry, 0, len(r.entries))
		for _, entry := range r.entries {
			entries = append(entries, entry)
			r.retireLocked(entry)
		}
		r.mu.Unlock()
		deadline := time.NewTimer(talkStopTimeout + time.Second)
		defer deadline.Stop()
		for _, entry := range entries {
			select {
			case <-entry.done:
			case <-deadline.C:
				return
			}
		}
	})
}

type unavailableTalkTransport struct{}

func (unavailableTalkTransport) Start(context.Context, TalkStart) error {
	return ErrTalkTransportUnavailable
}
func (unavailableTalkTransport) WriteAudio(context.Context, TalkAudio) error {
	return ErrTalkTransportUnavailable
}
func (unavailableTalkTransport) Stop(context.Context) error { return nil }
func (unavailableTalkTransport) Close()                     {}
