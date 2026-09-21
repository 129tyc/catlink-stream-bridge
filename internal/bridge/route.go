package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var errSourceUnavailable = errors.New("stream source unavailable")
var errLocalProbe = errors.New("local candidate ready")
var errLocalResourceUnavailable = errors.New("local resource unavailable")

var localProbeBackoffSchedule = []time.Duration{
	5 * time.Second,
	10 * time.Second,
	1 * time.Minute,
	1 * time.Hour,
	6 * time.Hour,
	24 * time.Hour,
}

const routeRepairBackoffMax = 60 * time.Second

const (
	cloudPlaybackCooldown = 5 * time.Minute
	localResourceCooldown = 60 * time.Second
)

type RouteRunner struct {
	config             RouteConfig
	device             DeviceConfig
	account            accountTokenSource
	openDomain         string
	client             *http.Client
	publisher          *RTSPPublisher
	health             *Health
	logger             *slog.Logger
	newSource          func(*http.Client, string, string) streamSource
	sourceMode         string
	probeMu            sync.Mutex
	nextLocalProbe     time.Time
	localProbeBackoff  time.Duration
	lastAttemptSource  string
	attemptSequence    uint64
	localCooldownUntil time.Time
	cloudCooldownUntil time.Time
	pendingCandidate   *localCandidate
}

type preparedLocalCandidate struct {
	candidate *localCandidate
	pipe      *Pipeline
	sessionID uint64
}

func (r *RouteRunner) nextAttemptID() uint64 {
	r.attemptSequence++
	return r.attemptSequence
}

type accountTokenSource interface {
	Token(context.Context) (string, error)
	ResolveDevice(context.Context, *DeviceSelector) (string, error)
	Invalidate()
}

type accountSessionSource interface {
	SessionForDevice(context.Context, DeviceConfig) (AccountSessionSnapshot, error)
}

type streamSource interface {
	Start(context.Context, string) error
	Read(context.Context) (SourceMessage, error)
	Close()
}

func NewRouteRunner(config RouteConfig, device DeviceConfig, account *AccountTokenManager, openDomain string, client *http.Client, publisher *RTSPPublisher, health *Health, logger *slog.Logger) *RouteRunner {
	return &RouteRunner{
		config: config, device: device, account: account, openDomain: openDomain,
		client: client, publisher: publisher, health: health, logger: logger,
		sourceMode: func() string {
			if config.Source == "" || config.Source == "cloud" {
				return "cloud"
			}
			if config.Source == "auto" {
				return "local"
			}
			return config.Source
		}(),
		newSource: func(client *http.Client, openDomain, ezopen string) streamSource {
			return NewEZOpenSource(client, openDomain, ezopen)
		},
	}
}

func (r *RouteRunner) Run(ctx context.Context) {
	defer func() {
		if r.pendingCandidate != nil {
			r.pendingCandidate.source.Close()
			r.pendingCandidate = nil
		}
	}()
	var output *routeOutput
	if r.publisher != nil && r.health != nil {
		output = newRouteOutput(r.config.Path, r.config.Name, r.publisher, r.health)
		defer output.close()
	}
	repairBackoff := time.Second
	for {
		if ctx.Err() != nil {
			if r.pendingCandidate != nil {
				r.pendingCandidate.source.Close()
				r.pendingCandidate = nil
			}
			return
		}
		if wait := r.sourceCooldownWait(r.sourceMode, time.Now()); wait > 0 {
			if !sleepContext(ctx, wait) {
				return
			}
			continue
		}
		attemptStartedSource := r.sourceMode
		err := r.runAttempt(ctx, output)
		failedSource := r.lastAttemptSource
		if failedSource == "" {
			failedSource = attemptStartedSource
		}
		if ctx.Err() != nil {
			return
		}
		if output != nil && output.consumeStableLive() {
			repairBackoff = time.Second
			if r.sourceMode == "local" {
				r.clearLocalProbe()
			}
		}
		if errors.Is(err, errLocalProbe) {
			r.logger.Info("local optimization ready", "route", r.config.Name, "previousSource", failedSource, "nextSource", "local")
			r.sourceMode = "local"
			continue
		}
		if output == nil && r.health != nil {
			r.health.SetRoute(r.config.Name, false)
		}
		if isAuthFailure(err) {
			r.invalidateAuthFailure(err)
		}
		now := time.Now()
		r.applySourceCooldown(failedSource, err, now)
		if r.config.Source == "auto" {
			if failedSource == "local" {
				if r.sourceMode == "local" {
					r.sourceMode = r.chooseRepairSource("cloud", now)
				}
				if r.sourceMode == "cloud" {
					r.scheduleLocalProbe(now)
				}
			} else if failedSource == "cloud" && r.sourceMode == "cloud" {
				// A failed cloud foreground attempt is a repair event, not an
				// optimization probe. Give local one foreground repair attempt.
				r.sourceMode = r.chooseRepairSource("local", now)
			}
		}
		if errors.Is(err, errSourceUnavailable) {
			r.logger.Info("route source unavailable", "route", r.config.Name, "failedSource", failedSource, "nextSource", r.sourceMode, "cause", err, "backoff", repairBackoff)
		} else {
			r.logger.Warn("route backoff", "route", r.config.Name, "failedSource", failedSource, "nextSource", r.sourceMode, "error", routeErrorClass(err), "cause", err, "backoff", repairBackoff)
		}
		if !sleepContext(ctx, repairBackoff) {
			return
		}
		if repairBackoff < routeRepairBackoffMax {
			repairBackoff *= 2
			if repairBackoff > routeRepairBackoffMax {
				repairBackoff = routeRepairBackoffMax
			}
		}
	}
}

func (r *RouteRunner) runAttempt(ctx context.Context, output *routeOutput) error {
	attemptID := r.nextAttemptID()
	r.lastAttemptSource = r.sourceMode
	pendingCandidate := r.pendingCandidate
	if r.sourceMode == "local" && pendingCandidate != nil {
		r.pendingCandidate = nil
	}
	acquisitionCtx := ctx
	if r.sourceMode == "local" {
		var cancel context.CancelFunc
		acquisitionCtx, cancel = context.WithTimeout(ctx, localSDKSetupTimeout)
		defer cancel()
	}
	var token string
	var serial string
	var talkTarget *TalkTarget
	var attemptGeneration uint64
	if account, ok := r.account.(accountSessionSource); ok {
		var snapshot AccountSessionSnapshot
		var err error
		if pendingCandidate != nil {
			snapshot = AccountSessionSnapshot{
				CameraToken: pendingCandidate.token,
				Device:      pendingCandidate.device,
				Generation:  pendingCandidate.generation,
			}
			attemptGeneration = snapshot.Generation
		} else {
			snapshot, err = account.SessionForDevice(acquisitionCtx, r.device)
			if err != nil {
				return err
			}
			attemptGeneration = snapshot.Generation
		}
		if r.sourceMode == "local" && pendingCandidate == nil {
			if manager, ok := r.account.(*AccountTokenManager); ok {
				localDevice, localGeneration, localErr := manager.ResolveLocalDeviceWithGeneration(acquisitionCtx, r.device, snapshot.Device)
				if localErr != nil {
					if r.config.Source == "auto" {
						if isAuthFailure(localErr) {
							manager.InvalidateGeneration(localGeneration)
							refreshed, refreshErr := account.SessionForDevice(ctx, r.device)
							if refreshErr != nil {
								return refreshErr
							}
							snapshot = refreshed
							attemptGeneration = refreshed.Generation
							localDevice, retryGeneration, retryErr := manager.ResolveLocalDeviceWithGeneration(acquisitionCtx, r.device, snapshot.Device)
							if retryErr == nil {
								snapshot.Device = localDevice
								attemptGeneration = retryGeneration
								localErr = nil
							} else {
								localErr = retryErr
							}
						}
						if localErr != nil {
							r.sourceMode = "cloud"
							r.scheduleLocalProbe(time.Now())
						}
					} else {
						return localErr
					}
				} else {
					snapshot.Device = localDevice
					if localGeneration != 0 {
						attemptGeneration = localGeneration
					}
				}
			}
		}
		token = snapshot.CameraToken
		serial = snapshot.Device.EZOpenSerial
		talkTarget = &TalkTarget{
			AccountID: r.device.AccountID,
			Device:    snapshot.Device,
			Channel:   r.config.Channel,
			SnapshotProvider: newTalkSnapshotProvider(func(snapshotCtx context.Context, target TalkTarget) (TalkStart, error) {
				fresh, snapshotErr := account.SessionForDevice(snapshotCtx, r.device)
				if snapshotErr != nil {
					return TalkStart{}, snapshotErr
				}
				return TalkStart{
					CameraToken: fresh.CameraToken,
					Device:      fresh.Device,
					Channel:     target.Channel,
				}, nil
			}, func() {
				if manager, ok := r.account.(*AccountTokenManager); ok {
					if manager.sessionFile != "" {
						manager.Invalidate()
					} else {
						manager.InvalidateGeneration(snapshot.Generation)
					}
					return
				}
				r.account.Invalidate()
			}),
		}
	} else {
		var err error
		token, err = r.account.Token(ctx)
		if err != nil {
			return err
		}
		serial = r.device.Serial
		if serial == "" {
			serial, err = r.account.ResolveDevice(ctx, r.device.Selector)
			if err != nil {
				return err
			}
		}
		talkTarget = &TalkTarget{
			AccountID: r.device.AccountID,
			Device:    ResolvedDevice{EZOpenSerial: serial},
			Channel:   r.config.Channel,
			SnapshotProvider: newTalkSnapshotProvider(func(snapshotCtx context.Context, target TalkTarget) (TalkStart, error) {
				freshToken, tokenErr := r.account.Token(snapshotCtx)
				if tokenErr != nil {
					return TalkStart{}, tokenErr
				}
				freshSerial := r.device.Serial
				if freshSerial == "" {
					freshSerial, tokenErr = r.account.ResolveDevice(snapshotCtx, r.device.Selector)
					if tokenErr != nil {
						return TalkStart{}, tokenErr
					}
				}
				return TalkStart{
					CameraToken: freshToken,
					Device:      ResolvedDevice{EZOpenSerial: freshSerial},
					Channel:     target.Channel,
				}, nil
			}, r.account.Invalidate),
		}
	}
	ezopen := makeEZOpenURL(serial, r.config.Channel, r.config.Quality)
	source := r.newSource(r.client, r.openDomain, ezopen)
	sourceStarted := pendingCandidate == nil
	if pendingCandidate != nil {
		source = pendingCandidate.source
	} else if r.sourceMode == "local" {
		resolved := talkTarget.Device
		if resolved.LocalIP != "" && resolved.LocalCmdPort > 0 && resolved.LocalStreamPort > 0 && resolved.LocalOperationCode != "" && resolved.LocalKey != "" {
			source = NewLocalSDKSource(LocalDeviceSession{
				Serial:        resolved.EZOpenSerial,
				Host:          resolved.LocalIP,
				CommandPort:   resolved.LocalCmdPort,
				StreamPort:    resolved.LocalStreamPort,
				OperationCode: resolved.LocalOperationCode,
				Key:           resolved.LocalKey,
				IsEncrypt:     resolved.LocalIsEncrypt,
			}, r.config.Channel, r.config.Quality)
		} else if r.config.Source == "auto" {
			r.sourceMode = "cloud"
			r.scheduleLocalProbe(time.Now())
		} else {
			return fmt.Errorf("local source metadata unavailable")
		}
	}
	r.lastAttemptSource = r.sourceMode
	var mediaPublisher MediaPublisher = r.publisher
	if output != nil {
		output.setTalkTarget(talkTarget)
		mediaPublisher = routeMediaPublisher{output: output, sessionID: attemptID}
	}
	pipe, err := NewPipeline(r.config.Path, mediaPublisher, talkTarget)
	if err != nil {
		if pendingCandidate != nil {
			source.Close()
		}
		return err
	}
	if pendingCandidate != nil && len(pendingCandidate.buffer) > 0 {
		if err := pipe.Feed(pendingCandidate.buffer); err != nil {
			pipe.CloseAbrupt()
			source.Close()
			return err
		}
	}
	startCtx := ctx
	if r.sourceMode == "local" {
		startCtx = acquisitionCtx
	}
	if sourceStarted {
		if err := source.Start(startCtx, token); err != nil {
			pipe.CloseAbrupt()
			source.Close()
			return wrapAccountGenerationError(attemptGeneration, err)
		}
	}
	defer func() { source.Close() }()
	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()
	candidateCtx, cancelCandidate := context.WithCancel(attemptCtx)
	candidateDone := make(chan struct{})
	candidateReady := make(chan *preparedLocalCandidate, 1)
	var candidateActive atomic.Bool
	var candidateCloudFailed atomic.Bool
	foregroundSource := source
	prepareCandidate := func(candidate *localCandidate) (*preparedLocalCandidate, error) {
		if candidate == nil {
			return nil, fmt.Errorf("local candidate is nil")
		}
		candidateSessionID := r.nextAttemptID()
		candidatePublisher := routeMediaPublisher{output: output, sessionID: candidateSessionID}
		candidatePipe, err := NewPipeline(r.config.Path, candidatePublisher, talkTarget)
		if err != nil {
			return nil, err
		}
		if len(candidate.buffer) > 0 {
			if err := candidatePipe.Feed(candidate.buffer); err != nil {
				candidatePipe.CloseAbrupt()
				return nil, err
			}
		}
		return &preparedLocalCandidate{candidate: candidate, pipe: candidatePipe, sessionID: candidateSessionID}, nil
	}
	if r.config.Source == "auto" && r.sourceMode == "cloud" && !r.localProbeAt().IsZero() {
		go func() {
			defer close(candidateDone)
			wait := time.Until(r.localProbeAt())
			if wait < 0 {
				wait = 0
			}
			timer := time.NewTimer(wait)
			defer timer.Stop()
			for {
				select {
				case <-timer.C:
					if candidateCloudFailed.Load() || candidateCtx.Err() != nil {
						return
					}
					if candidateCloudFailed.Load() || output == nil || !output.readyForLocalOptimization(time.Now()) || !r.sourceEligible("local", time.Now()) {
						waitNext := time.Second
						if cooldownWait := r.sourceCooldownWait("local", time.Now()); cooldownWait > waitNext {
							waitNext = cooldownWait
						}
						timer.Reset(waitNext)
						continue
					}
					candidateActive.Store(true)
					candidate, err := r.probeLocalCandidate(candidateCtx)
					candidateActive.Store(false)
					if err == nil {
						prepared, prepareErr := prepareCandidate(candidate)
						if prepareErr != nil {
							candidate.source.Close()
							r.scheduleLocalProbe(time.Now())
							waitNext := time.Second
							if next := r.localProbeAt(); !next.IsZero() {
								if wait := time.Until(next); wait > waitNext {
									waitNext = wait
								}
							}
							timer.Reset(waitNext)
							continue
						}
						candidateReady <- prepared
						// Wake the foreground reader. The main attempt goroutine
						// performs the final source swap. The candidate pipeline has
						// already taken ownership of the output before the wake-up.
						foregroundSource.Close()
						return
					}
					if candidateCtx.Err() != nil && !candidateCloudFailed.Load() {
						return
					}
					if candidateCloudFailed.Load() {
						now := time.Now()
						r.applySourceCooldown("local", err, now)
						r.scheduleLocalProbe(now)
						return
					}
					r.applySourceCooldown("local", err, time.Now())
					var candidateErr localCandidateError
					var accountErr accountGenerationError
					if isAuthFailure(err) && errors.As(err, &candidateErr) {
						if manager, ok := r.account.(*AccountTokenManager); ok && candidateErr.generation != 0 {
							manager.InvalidateGeneration(candidateErr.generation)
						}
					} else if isAuthFailure(err) && errors.As(err, &accountErr) {
						if manager, ok := r.account.(*AccountTokenManager); ok && accountErr.generation != 0 {
							manager.InvalidateGeneration(accountErr.generation)
						}
					}
					r.scheduleLocalProbe(time.Now())
					waitNext := time.Second
					if next := r.localProbeAt(); !next.IsZero() {
						if wait := time.Until(next); wait > waitNext {
							waitNext = wait
						}
					}
					timer.Reset(waitNext)
				case <-candidateCtx.Done():
					return
				}
			}
		}()
	} else {
		close(candidateDone)
	}
	defer func() {
		cancelCandidate()
		<-candidateDone
		select {
		case prepared := <-candidateReady:
			prepared.pipe.CloseAbrupt()
			prepared.candidate.source.Close()
		default:
		}
	}()
	waitCandidateAfterForegroundFailure := func() {
		if candidateActive.Load() {
			candidateCloudFailed.Store(true)
			select {
			case <-candidateDone:
			case <-time.After(localSDKSetupTimeout):
				cancelCandidate()
				<-candidateDone
			}
			return
		}
		cancelCandidate()
		<-candidateDone
	}
	promoteCandidate := func(prepared *preparedLocalCandidate) error {
		if prepared == nil || prepared.candidate == nil || prepared.pipe == nil {
			return fmt.Errorf("local candidate is nil")
		}
		oldSource := source
		oldPipe := pipe
		candidate := prepared.candidate
		oldPipe.CloseAbrupt()
		oldSource.Close()
		source = candidate.source
		pipe = prepared.pipe
		attemptID = prepared.sessionID
		r.lastAttemptSource = "local"
		r.sourceMode = "local"
		return nil
	}
	startWatch := func(activeSource streamSource, ownerID uint64) (chan struct{}, chan struct{}) {
		watchStop := make(chan struct{})
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			started := time.Now()
			for {
				select {
				case <-ticker.C:
					if output == nil {
						continue
					}
					stale, recover, changed := output.observeVideoFreshness(time.Now(), started)
					if changed {
						if stale {
							r.logger.Warn("video stale", "route", r.config.Name, "source", r.sourceMode, "after", outputVideoStaleAfter)
						} else {
							r.logger.Info("video fresh", "route", r.config.Name, "source", r.sourceMode)
						}
					}
					if recover {
						r.logger.Warn("video recovery timeout", "route", r.config.Name, "source", r.sourceMode, "after", outputSourceVideoRecoveryAfter)
						output.pauseFor(ownerID, nil)
						activeSource.Close()
						return
					}
				case <-attemptCtx.Done():
					activeSource.Close()
					return
				case <-watchStop:
					return
				}
			}
		}()
		return watchStop, watchDone
	}
	watchStop, watchDone := startWatch(source, attemptID)
	stopWatch := func() {
		select {
		case <-watchStop:
		default:
			close(watchStop)
		}
		<-watchDone
	}
	defer stopWatch()

	r.logger.Info("route started", "route", r.config.Name, "source", r.sourceMode, "channel", r.config.Channel, "quality", r.config.Quality, "sessionID", attemptID)
	for {
		message, err := source.Read(attemptCtx)
		if err != nil {
			waitCandidateAfterForegroundFailure()
			select {
			case candidate := <-candidateReady:
				stopWatch()
				if promoteErr := promoteCandidate(candidate); promoteErr != nil {
					candidate.candidate.source.Close()
					candidate.pipe.CloseAbrupt()
					pipe.CloseAbrupt()
					return promoteErr
				}
				watchStop, watchDone = startWatch(source, attemptID)
				continue
			default:
			}
			if output != nil {
				output.pauseFor(attemptID, nil)
			}
			pipe.CloseAbrupt()
			return wrapAccountGenerationError(attemptGeneration, err)
		}
		if message.Binary && len(message.Data) > 0 {
			if err := pipe.Feed(message.Data); err != nil {
				waitCandidateAfterForegroundFailure()
				select {
				case candidate := <-candidateReady:
					stopWatch()
					if promoteErr := promoteCandidate(candidate); promoteErr != nil {
						candidate.candidate.source.Close()
						candidate.pipe.CloseAbrupt()
						pipe.CloseAbrupt()
						return promoteErr
					}
					watchStop, watchDone = startWatch(source, attemptID)
					continue
				default:
				}
				if output != nil {
					output.pauseFor(attemptID, nil)
				}
				pipe.CloseAbrupt()
				return err
			}
		}
	}
}

func makeEZOpenURL(serial string, channel int, quality string) string {
	suffix := ".live"
	if quality == "hd" {
		suffix = ".hd.live"
	}
	return fmt.Sprintf("ezopen://open.ys7.com/%s/%d%s", serial, channel, suffix)
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func isAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	value := strings.ToLower(err.Error())
	return strings.Contains(value, "status 401") || strings.Contains(value, "status 403") ||
		strings.Contains(value, "business code 401") || strings.Contains(value, "business code 403") ||
		strings.Contains(value, "business code 1002") ||
		strings.Contains(value, "code 401") || strings.Contains(value, "code 403") || strings.Contains(value, "code 1002") ||
		strings.Contains(value, "result=401") || strings.Contains(value, "result=403") || strings.Contains(value, "result=1002") ||
		strings.Contains(value, "unauthorized") ||
		(strings.Contains(value, "token") && (strings.Contains(value, "invalid") || strings.Contains(value, "expired") || strings.Contains(value, "unauthorized")))
}

func (r *RouteRunner) invalidateAuthFailure(err error) {
	manager, ok := r.account.(*AccountTokenManager)
	if !ok {
		r.account.Invalidate()
		return
	}
	var generationErr accountGenerationError
	if errors.As(err, &generationErr) && generationErr.generation != 0 {
		manager.InvalidateGeneration(generationErr.generation)
		return
	}
	var candidateErr localCandidateError
	if errors.As(err, &candidateErr) && candidateErr.generation != 0 {
		manager.InvalidateGeneration(candidateErr.generation)
	}
}

func routeErrorClass(err error) string {
	if err == nil {
		return "unknown"
	}
	if isAuthFailure(err) {
		return "authorization"
	}
	if errors.Is(err, errSourceUnavailable) {
		return "source_unavailable"
	}
	return "upstream_or_media"
}

func nextScheduledBackoff(current time.Duration, schedule []time.Duration) time.Duration {
	if len(schedule) == 0 {
		return current
	}
	if current <= 0 {
		return schedule[0]
	}
	for _, candidate := range schedule {
		if candidate > current {
			return candidate
		}
	}
	return schedule[len(schedule)-1]
}

func (r *RouteRunner) localProbeAt() time.Time {
	r.probeMu.Lock()
	defer r.probeMu.Unlock()
	return r.nextLocalProbe
}

func (r *RouteRunner) localProbeDue(now time.Time) bool {
	at := r.localProbeAt()
	return !at.IsZero() && !now.Before(at)
}

func (r *RouteRunner) scheduleLocalProbe(now time.Time) {
	r.probeMu.Lock()
	defer r.probeMu.Unlock()
	r.localProbeBackoff = nextScheduledBackoff(r.localProbeBackoff, localProbeBackoffSchedule)
	r.nextLocalProbe = now.Add(r.localProbeBackoff)
}

func (r *RouteRunner) clearLocalProbe() {
	r.probeMu.Lock()
	r.localProbeBackoff = 0
	r.nextLocalProbe = time.Time{}
	r.probeMu.Unlock()
}

func (r *RouteRunner) applySourceCooldown(source string, err error, now time.Time) {
	r.probeMu.Lock()
	defer r.probeMu.Unlock()
	if source == "cloud" && sourceUnavailableCode(err) == "5416" {
		r.cloudCooldownUntil = now.Add(cloudPlaybackCooldown)
	}
	if source == "local" && errors.Is(err, errLocalResourceUnavailable) {
		r.localCooldownUntil = now.Add(localResourceCooldown)
	}
}

func (r *RouteRunner) sourceCooldownWait(source string, now time.Time) time.Duration {
	r.probeMu.Lock()
	defer r.probeMu.Unlock()
	var until time.Time
	if source == "local" {
		until = r.localCooldownUntil
	} else if source == "cloud" {
		until = r.cloudCooldownUntil
	}
	if until.IsZero() || !now.Before(until) {
		return 0
	}
	return until.Sub(now)
}

func (r *RouteRunner) sourceEligible(source string, now time.Time) bool {
	return r.sourceCooldownWait(source, now) == 0
}

func (r *RouteRunner) chooseRepairSource(preferred string, now time.Time) string {
	if r.sourceEligible(preferred, now) {
		return preferred
	}
	other := "cloud"
	if preferred == "cloud" {
		other = "local"
	}
	if r.sourceEligible(other, now) {
		return other
	}
	if r.sourceCooldownWait(preferred, now) <= r.sourceCooldownWait(other, now) {
		return preferred
	}
	return other
}
