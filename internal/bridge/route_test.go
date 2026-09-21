package bridge

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type testAccountTokenSource struct {
	invalidations atomic.Int32
}

func (a *testAccountTokenSource) Token(context.Context) (string, error) { return "test-token", nil }

func (a *testAccountTokenSource) ResolveDevice(context.Context, *DeviceSelector) (string, error) {
	return "test-serial", nil
}

func (a *testAccountTokenSource) Invalidate() { a.invalidations.Add(1) }

type failingStreamSource struct {
	starts chan<- struct{}
}

func (s *failingStreamSource) Start(context.Context, string) error {
	select {
	case s.starts <- struct{}{}:
	default:
	}
	return errors.New("synthetic upstream disconnect")
}

func (*failingStreamSource) Read(context.Context) (SourceMessage, error) {
	return SourceMessage{}, errors.New("not started")
}

func (*failingStreamSource) Close() {}

func TestRouteRunnerRetriesLocallyAndKeepsHealthAlive(t *testing.T) {
	account := new(testAccountTokenSource)
	health := NewHealth([]string{"camera"})
	health.SetRTSP(true)
	starts := make(chan struct{}, 4)
	runner := NewRouteRunner(
		RouteConfig{Name: "camera", DeviceID: "device", Channel: 1, Quality: "hd", Path: "/camera"},
		DeviceConfig{ID: "device", Serial: "serial", AccountID: "account"},
		(*AccountTokenManager)(nil), "https://open.example", http.DefaultClient, nil, health,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	runner.account = account
	runner.newSource = func(*http.Client, string, string) streamSource {
		return &failingStreamSource{starts: starts}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runner.Run(ctx)
		close(done)
	}()

	select {
	case <-starts:
	case <-time.After(time.Second):
		t.Fatal("route did not start")
	}
	select {
	case <-starts:
	case <-time.After(3 * time.Second):
		t.Fatal("route did not retry after upstream failure")
	}

	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	health.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("health status=%d", response.Code)
	}
	if account.invalidations.Load() != 0 {
		t.Fatalf("transient upstream failure invalidated account=%d", account.invalidations.Load())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("route did not stop after context cancellation")
	}
}

func TestRouteRunnerDoesNotAdvertiseColdRouteAsReady(t *testing.T) {
	account := new(testAccountTokenSource)
	health := NewHealth([]string{"camera"})
	health.SetRTSP(true)
	publisher := NewRTSPPublisher("127.0.0.1:0")
	if err := publisher.Start(); err != nil {
		t.Fatalf("start publisher: %v", err)
	}
	defer publisher.Shutdown()
	runner := NewRouteRunner(
		RouteConfig{Name: "camera", DeviceID: "device", Channel: 1, Quality: "hd", Path: "/camera"},
		DeviceConfig{ID: "device", Serial: "serial", AccountID: "account"},
		(*AccountTokenManager)(nil), "https://open.example", http.DefaultClient, publisher, health,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	runner.account = account
	starts := make(chan struct{}, 1)
	runner.newSource = func(*http.Client, string, string) streamSource {
		return &failingStreamSource{starts: starts}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runner.Run(ctx)
		close(done)
	}()
	select {
	case <-starts:
	case <-time.After(time.Second):
		t.Fatal("route did not start")
	}
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	health.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status=%d, want %d before first real publication", response.Code, http.StatusServiceUnavailable)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("route did not stop")
	}
}

func TestRouteOutputPausesWithoutDroppingPublication(t *testing.T) {
	health := NewHealth([]string{"camera"})
	health.SetRTSP(true)
	publisher := NewRTSPPublisher("127.0.0.1:0")
	if err := publisher.Start(); err != nil {
		t.Fatalf("start publisher: %v", err)
	}
	defer publisher.Shutdown()

	output := newRouteOutput("/camera", "camera", publisher, health)
	if publisher.lookup("/camera") != nil {
		t.Fatal("cold route unexpectedly published a stream")
	}
	publication, err := output.openLive(1, nil, nil, nil, 0)
	if err != nil {
		t.Fatalf("open publication: %v", err)
	}
	output.pauseFor(1, publication)
	if publisher.lookup("/camera") != publication {
		t.Fatal("pausing the route dropped the publication")
	}
	output.mu.RLock()
	mode := output.mode
	output.mu.RUnlock()
	if mode != "paused" {
		t.Fatalf("mode=%q, want paused", mode)
	}
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	health.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("ready status=%d while publication is retained", response.Code)
	}
	output.close()
}

func TestRouteOutputPauseStartsFreshRecoveryWindow(t *testing.T) {
	health := NewHealth([]string{"camera"})
	output := &routeOutput{
		health:  health,
		mode:    "live",
		ownerID: 1,
	}
	output.pauseFor(1, nil)
	pausedAt := output.pausedSince
	if pausedAt.IsZero() {
		t.Fatal("pause did not record its recovery start")
	}
	attemptStarted := pausedAt.Add(-time.Minute)
	if _, recover, _ := output.observeVideoFreshness(pausedAt.Add(outputSourceVideoRecoveryAfter-time.Millisecond), attemptStarted); recover {
		t.Fatal("pause recovery window ended at the old attempt start")
	}
	if _, recover, _ := output.observeVideoFreshness(pausedAt.Add(outputSourceVideoRecoveryAfter), attemptStarted); !recover {
		t.Fatal("pause recovery window did not expire")
	}
}

func TestRouteOutputStaleDoesNotTriggerRecovery(t *testing.T) {
	output := &routeOutput{
		mode:           "live",
		lastVideoMedia: time.Unix(100, 0),
		liveSince:      time.Unix(100, 0),
	}
	now := time.Unix(100, 0).Add(outputVideoStaleAfter)
	stale, recover, changed := output.observeVideoFreshness(now, time.Time{})
	if !stale || recover || !changed {
		t.Fatalf("stale=%t recover=%t changed=%t, want stale-only transition", stale, recover, changed)
	}
	if _, recover, changed = output.observeVideoFreshness(now.Add(time.Second), time.Time{}); recover || changed {
		t.Fatalf("repeated stale check recover=%t changed=%t", recover, changed)
	}
	stale, recover, changed = output.observeVideoFreshness(now.Add(outputSourceVideoRecoveryAfter), time.Time{})
	if !stale || !recover || changed {
		t.Fatalf("hard timeout stale=%t recover=%t changed=%t", stale, recover, changed)
	}
}

func TestScheduledBackoffUsesOptimizationIntervals(t *testing.T) {
	want := []time.Duration{
		5 * time.Second,
		10 * time.Second,
		1 * time.Minute,
		1 * time.Hour,
		6 * time.Hour,
		24 * time.Hour,
		24 * time.Hour,
	}
	var current time.Duration
	for _, expected := range want {
		current = nextScheduledBackoff(current, localProbeBackoffSchedule)
		if current != expected {
			t.Fatalf("backoff=%s, want %s", current, expected)
		}
	}
}

func TestRouteRunnerSchedulesLocalProbeIndependently(t *testing.T) {
	runner := &RouteRunner{}
	now := time.Now()
	runner.scheduleLocalProbe(now)
	first := runner.localProbeAt()
	if first.Before(now.Add(5*time.Second)) || first.After(now.Add(5*time.Second+time.Millisecond)) {
		t.Fatalf("first local probe=%s", first.Sub(now))
	}
	runner.scheduleLocalProbe(now)
	second := runner.localProbeAt()
	if second.Before(now.Add(10*time.Second)) || second.After(now.Add(10*time.Second+time.Millisecond)) {
		t.Fatalf("second local probe=%s", second.Sub(now))
	}
	runner.clearLocalProbe()
	if !runner.localProbeAt().IsZero() {
		t.Fatal("local probe was not cleared")
	}
}

func TestSourceCooldownUsesProvidedClock(t *testing.T) {
	runner := &RouteRunner{}
	now := time.Unix(100, 0)
	runner.cloudCooldownUntil = now.Add(10 * time.Second)
	if got := runner.sourceCooldownWait("cloud", now); got != 10*time.Second {
		t.Fatalf("cloud cooldown=%s, want 10s", got)
	}
	if got := runner.sourceCooldownWait("cloud", now.Add(10*time.Second)); got != 0 {
		t.Fatalf("expired cloud cooldown=%s, want 0", got)
	}
}

func TestSourceCooldownAppliesToExplicitSources(t *testing.T) {
	runner := &RouteRunner{}
	now := time.Unix(100, 0)
	runner.applySourceCooldown("cloud", sourceUnavailableErrorForTest("5416"), now)
	if got := runner.sourceCooldownWait("cloud", now); got != cloudPlaybackCooldown {
		t.Fatalf("cloud cooldown=%s, want %s", got, cloudPlaybackCooldown)
	}
	runner.applySourceCooldown("local", errLocalResourceUnavailable, now)
	if got := runner.sourceCooldownWait("local", now); got != localResourceCooldown {
		t.Fatalf("local cooldown=%s, want %s", got, localResourceCooldown)
	}
}

func sourceUnavailableErrorForTest(code string) error {
	return sourceUnavailableError{code: code, err: errSourceUnavailable}
}
