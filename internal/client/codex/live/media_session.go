package live

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/pion/webrtc/v4"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	"golang.org/x/net/proxy"
)

var errMediaCapacity = errors.New("realtime media relay is at capacity")

type mediaSessionError struct {
	stage string
	cause error
}

func (e *mediaSessionError) Error() string { return e.stage }
func (e *mediaSessionError) Unwrap() error { return e.cause }

type pionMediaSession struct {
	downstream, upstream                       *webrtc.PeerConnection
	downToUp, upToDown                         *mediaDataPipe
	ctx                                        context.Context
	cancel                                     context.CancelCauseFunc
	mu                                         sync.Mutex
	closed, committed, answerStarted, notified bool
	answerReady                                bool
	setup                                      context.Context
	stopSetup, stopLifetime                    func() bool
	failure                                    error
	onFailure                                  func(error)
	finished                                   chan struct{}
	closeOnce                                  sync.Once
	closeErr                                   error
	workers                                    sync.WaitGroup
	releaseSlot                                func()
	proxyDialer                                proxy.ContextDialer
	localOffer                                 string
	tunnels                                    []*tcpCandidateTunnel
	downstreamChannel                          *webrtc.DataChannel
}

// The session retains only lifetime after Commit. The setup request may own a
// body or Gin values, so its context and cancellation hook are cleared then.
func (r *pionMediaRelay) NewSession(setup, lifetime context.Context, clientOffer, proxyURL string) (*pionMediaSession, string, error) {
	if setup == nil || lifetime == nil {
		return nil, "", errors.New("media relay requires setup and lifetime contexts")
	}
	if err := context.Cause(setup); err != nil {
		return nil, "", err
	}
	if err := context.Cause(lifetime); err != nil {
		return nil, "", err
	}
	if len(clientOffer) == 0 || len(clientOffer) > maxCallBodySize {
		return nil, "", errors.New("invalid media SDP offer size")
	}
	built, mode, errProxy := proxyutil.BuildDialer(proxyURL)
	if errProxy != nil {
		return nil, "", &mediaSessionError{stage: "invalid media proxy", cause: errProxy}
	}
	var proxyDialer proxy.ContextDialer
	if mode == proxyutil.ModeProxy {
		var ok bool
		proxyDialer, ok = built.(proxy.ContextDialer)
		if !ok {
			return nil, "", errors.New("media proxy must support cancellation")
		}
	}
	release, available := r.limiter.acquire(r.maxSessions)
	if !available {
		return nil, "", errMediaCapacity
	}
	downstream, errDownstream := r.downstreamAPI.NewPeerConnection(r.configuration)
	if errDownstream != nil {
		release()
		return nil, "", &mediaSessionError{stage: "create downstream media peer", cause: errDownstream}
	}
	api, configuration := r.upstreamAPI, r.configuration
	if proxyDialer != nil {
		api = r.proxyUpstreamAPI
		configuration.ICEServers = nil
	}
	upstream, errUpstream := api.NewPeerConnection(configuration)
	if errUpstream != nil {
		_ = downstream.Close()
		release()
		return nil, "", &mediaSessionError{stage: "create upstream media peer", cause: errUpstream}
	}
	ctx, cancel := context.WithCancelCause(lifetime)
	s := &pionMediaSession{downstream: downstream, upstream: upstream, ctx: ctx, cancel: cancel,
		setup: setup, finished: make(chan struct{}), releaseSlot: release, proxyDialer: proxyDialer}
	s.downToUp = newMediaDataPipe(ctx.Done(), s.fail)
	s.upToDown = newMediaDataPipe(ctx.Done(), s.fail)
	s.mu.Lock()
	s.stopSetup = context.AfterFunc(setup, func() { s.failWith(context.Cause(setup), true) })
	s.stopLifetime = context.AfterFunc(lifetime, func() { s.fail(context.Cause(lifetime)) })
	s.mu.Unlock()
	s.installMediaCallbacks()
	fail := func(stage string, cause error) (*pionMediaSession, string, error) {
		err := &mediaSessionError{stage: stage, cause: cause}
		s.fail(err)
		_ = s.Close()
		return nil, "", context.Cause(s.ctx)
	}
	if err := downstream.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: clientOffer}); err != nil {
		return fail("invalid downstream media offer", err)
	}
	if err := s.installAudioTracks(); err != nil {
		return fail("configure media audio tracks", err)
	}
	channel, errChannel := upstream.CreateDataChannel(realtimeDataChannelLabel, nil)
	if errChannel != nil {
		return fail("create media event channel", errChannel)
	}
	s.downToUp.setDestination(channel)
	s.bindMediaSource(channel, s.upToDown)
	gathered := webrtc.GatheringCompletePromise(upstream)
	offer, errOffer := upstream.CreateOffer(nil)
	if errOffer != nil {
		return fail("create upstream media offer", errOffer)
	}
	if err := upstream.SetLocalDescription(offer); err != nil {
		return fail("set upstream media offer", err)
	}
	if err := s.waitGathering(setup, gathered); err != nil {
		return fail("gather upstream media candidates", err)
	}
	description := upstream.LocalDescription()
	if description == nil || strings.TrimSpace(description.SDP) == "" {
		return fail("upstream media offer is empty", nil)
	}
	s.localOffer = description.SDP
	return s, description.SDP, nil
}

func (s *pionMediaSession) AcceptUpstreamAnswer(ctx context.Context, answer string) (string, error) {
	if ctx == nil {
		return "", errors.New("media answer requires a setup context")
	}
	s.mu.Lock()
	if s.closed || s.answerStarted {
		s.mu.Unlock()
		return "", errors.New("media answer is no longer available")
	}
	s.answerStarted = true
	s.mu.Unlock()
	fail := func(stage string, cause error) (string, error) {
		err := &mediaSessionError{stage: stage, cause: cause}
		s.fail(err)
		_ = s.Close()
		return "", context.Cause(s.ctx)
	}
	if len(answer) > maxCallBodySize {
		return fail("upstream media answer is too large", errCallBodyTooLarge)
	}
	if s.proxyDialer != nil {
		rewritten, tunnels, err := prepareProxiedUpstreamAnswer(s.ctx, answer, s.localOffer, s.proxyDialer, s.fail)
		if err != nil {
			return fail("prepare upstream media proxy", err)
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = closeCandidateTunnels(tunnels)
			return "", context.Cause(s.ctx)
		}
		s.tunnels = tunnels
		s.mu.Unlock()
		answer = rewritten
	}
	if err := s.upstream.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		return fail("invalid upstream media answer", err)
	}
	gathered := webrtc.GatheringCompletePromise(s.downstream)
	localAnswer, errAnswer := s.downstream.CreateAnswer(nil)
	if errAnswer != nil {
		return fail("create downstream media answer", errAnswer)
	}
	if err := s.downstream.SetLocalDescription(localAnswer); err != nil {
		return fail("set downstream media answer", err)
	}
	if err := s.waitGathering(ctx, gathered); err != nil {
		return fail("gather downstream media candidates", err)
	}
	description := s.downstream.LocalDescription()
	if description == nil || strings.TrimSpace(description.SDP) == "" {
		return fail("downstream media answer is empty", nil)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return "", context.Cause(s.ctx)
	}
	s.answerReady = true
	s.mu.Unlock()
	return description.SDP, nil
}

func (s *pionMediaSession) waitGathering(ctx context.Context, gathered <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	case <-gathered:
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return context.Cause(s.ctx)
}

// publish must perform only local registration, never network operations.
func (s *pionMediaSession) Commit(publish func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.committed || !s.answerReady {
		return errors.New("media session cannot be committed")
	}
	if err := context.Cause(s.ctx); err != nil {
		return err
	}
	if err := context.Cause(s.setup); err != nil {
		return err
	}
	if err := publish(); err != nil {
		return err
	}
	s.committed = true
	s.setup = nil
	if s.stopSetup != nil {
		s.stopSetup()
		s.stopSetup = nil
	}
	return nil
}

func (s *pionMediaSession) run(worker func()) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.workers.Add(1)
	s.mu.Unlock()
	go func() { defer s.workers.Done(); worker() }()
}

func (s *pionMediaSession) fail(err error) { s.failWith(err, false) }
func (s *pionMediaSession) failWith(err error, setupOnly bool) {
	if err == nil {
		err = context.Canceled
	}
	s.mu.Lock()
	if s.closed || s.failure != nil || (setupOnly && s.committed) {
		s.mu.Unlock()
		return
	}
	s.failure = err
	s.cancel(err)
	s.mu.Unlock()
	_ = s.Close()
	s.notifyFailure()
}

func (s *pionMediaSession) SetCloseHandler(handler func(error)) {
	s.mu.Lock()
	s.onFailure = handler
	s.mu.Unlock()
	s.notifyFailure()
}

func (s *pionMediaSession) notifyFailure() {
	s.mu.Lock()
	handler, err := s.onFailure, s.failure
	if handler == nil || err == nil || s.notified {
		s.mu.Unlock()
		return
	}
	s.notified = true
	s.mu.Unlock()
	go func() { <-s.finished; handler(err) }()
}

func (s *pionMediaSession) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		stopSetup, stopLifetime := s.stopSetup, s.stopLifetime
		s.stopSetup, s.stopLifetime, s.setup = nil, nil, nil
		tunnels := s.tunnels
		s.tunnels = nil
		s.mu.Unlock()
		if stopSetup != nil {
			stopSetup()
		}
		if stopLifetime != nil {
			stopLifetime()
		}
		s.cancel(context.Canceled)
		s.closeErr = errors.Join(closeCandidateTunnels(tunnels), s.downstream.Close(), s.upstream.Close())
		s.downToUp.wait()
		s.upToDown.wait()
		s.workers.Wait()
		s.releaseSlot()
		close(s.finished)
	})
	return s.closeErr
}
