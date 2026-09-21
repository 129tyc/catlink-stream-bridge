package bridge

import (
	"context"
	"fmt"

	"github.com/129tyc/catlink-stream-bridge/internal/mpegps"
)

type localCandidateError struct {
	generation uint64
	err        error
}

type localCandidate struct {
	source     streamSource
	token      string
	device     ResolvedDevice
	generation uint64
	buffer     []byte
}

func (e localCandidateError) Error() string { return e.err.Error() }
func (e localCandidateError) Unwrap() error { return e.err }

type localCandidateGate struct {
	vps, sps, pps []byte
	audioReady    bool
	keyframeReady bool
	ready         bool
}

func (g *localCandidateGate) OnElementary(event mpegps.ElementaryEvent) {
	if event.Kind == mpegps.KindPCMAPayload {
		g.audioReady = true
		if g.keyframeReady && len(g.vps) > 0 && len(g.sps) > 0 && len(g.pps) > 0 {
			g.ready = true
		}
		return
	}
	if event.Kind != mpegps.KindH265NALU {
		return
	}
	nalu, err := normalizeNalu(event.Data)
	if err != nil || len(nalu) < 2 {
		return
	}
	typ := (nalu[0] >> 1) & 0x3f
	switch typ {
	case 32:
		g.vps = append([]byte(nil), nalu...)
	case 33:
		g.sps = append([]byte(nil), nalu...)
	case 34:
		g.pps = append([]byte(nil), nalu...)
	case 16, 17, 18, 19, 20, 21, 22, 23:
		g.keyframeReady = true
		if g.audioReady && len(g.vps) > 0 && len(g.sps) > 0 && len(g.pps) > 0 {
			g.ready = true
		}
	}
}

func (g *localCandidateGate) OnReset(mpegps.ResetEvent) {}

func (r *RouteRunner) probeLocalCandidate(ctx context.Context) (*localCandidate, error) {
	account, ok := r.account.(accountSessionSource)
	if !ok {
		return nil, fmt.Errorf("local candidate requires account session source")
	}
	manager, ok := r.account.(*AccountTokenManager)
	if !ok {
		return nil, fmt.Errorf("local candidate requires account token manager")
	}
	probeCtx, cancel := context.WithTimeout(ctx, localSDKSetupTimeout)
	defer cancel()
	snapshot, err := account.SessionForDevice(probeCtx, r.device)
	if err != nil {
		return nil, err
	}
	resolved, generation, err := manager.ResolveLocalDeviceWithGeneration(probeCtx, r.device, snapshot.Device)
	if err != nil {
		return nil, localCandidateError{generation: generation, err: err}
	}
	source := NewLocalSDKSource(LocalDeviceSession{
		Serial:        resolved.EZOpenSerial,
		Host:          resolved.LocalIP,
		CommandPort:   resolved.LocalCmdPort,
		StreamPort:    resolved.LocalStreamPort,
		CASIP:         resolved.CASIP,
		CASPort:       resolved.CASPort,
		OperationCode: resolved.LocalOperationCode,
		Key:           resolved.LocalKey,
		IsEncrypt:     resolved.LocalIsEncrypt,
	}, r.config.Channel, r.config.Quality)
	if err := source.Start(probeCtx, snapshot.CameraToken); err != nil {
		source.Close()
		return nil, localCandidateError{generation: generation, err: err}
	}
	keepSource := false
	var buffer []byte
	defer func() {
		if !keepSource {
			source.Close()
		}
	}()
	gate := &localCandidateGate{}
	decoder, err := mpegps.NewDecoder(gate)
	if err != nil {
		return nil, localCandidateError{generation: generation, err: err}
	}
	defer decoder.CloseAbrupt()
	for {
		message, err := source.Read(probeCtx)
		if err != nil {
			return nil, localCandidateError{generation: generation, err: err}
		}
		if !message.Binary || len(message.Data) == 0 {
			continue
		}
		if len(buffer)+len(message.Data) > 4*1024*1024 {
			return nil, localCandidateError{generation: generation, err: fmt.Errorf("local candidate buffer exceeded limit")}
		}
		buffer = append(buffer, message.Data...)
		result, err := decoder.Feed(message.Data)
		if err != nil {
			return nil, localCandidateError{generation: generation, err: err}
		}
		if result.Fatal {
			return nil, localCandidateError{generation: generation, err: fmt.Errorf("local candidate MPEG-PS parser reset")}
		}
		if gate.ready {
			keepSource = true
			return &localCandidate{source: source, token: snapshot.CameraToken, device: resolved, generation: generation, buffer: buffer}, nil
		}
	}
}
