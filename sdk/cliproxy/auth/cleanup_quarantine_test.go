package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type cleanupQuarantineExecutor struct {
	provider string

	firstStarted   chan struct{}
	firstFinished  chan struct{}
	cleanupStarted chan string
	cleanupRelease chan struct{}
	releaseOnce    sync.Once

	executeCalls atomic.Int32
	streamCalls  atomic.Int32
	countCalls   atomic.Int32
}

func newCleanupQuarantineExecutor(provider string) *cleanupQuarantineExecutor {
	return &cleanupQuarantineExecutor{
		provider:       provider,
		firstStarted:   make(chan struct{}),
		firstFinished:  make(chan struct{}),
		cleanupStarted: make(chan string, 1),
		cleanupRelease: make(chan struct{}),
	}
}

func (e *cleanupQuarantineExecutor) Identifier() string { return e.provider }

func (e *cleanupQuarantineExecutor) Execute(ctx context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e.executeCalls.Add(1) == 1 {
		close(e.firstStarted)
		<-ctx.Done()
		close(e.firstFinished)
		return cliproxyexecutor.Response{}, ctx.Err()
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.RuntimeInstanceID())}, nil
}

func (e *cleanupQuarantineExecutor) ExecuteStream(ctx context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	if e.streamCalls.Add(1) == 1 {
		close(e.firstStarted)
		go func() {
			<-ctx.Done()
			close(e.firstFinished)
			close(chunks)
		}()
		return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
	}
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(auth.RuntimeInstanceID())}
	chunks <- cliproxyexecutor.SuccessfulStreamTerminalChunk()
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *cleanupQuarantineExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *cleanupQuarantineExecutor) CountTokens(ctx context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e.countCalls.Add(1) == 1 {
		close(e.firstStarted)
		<-ctx.Done()
		close(e.firstFinished)
		return cliproxyexecutor.Response{}, ctx.Err()
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.RuntimeInstanceID())}, nil
}

func (e *cleanupQuarantineExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (e *cleanupQuarantineExecutor) CloseAuthInstanceExecutionSessions(_ string, instanceID string, _ string) {
	e.cleanupStarted <- instanceID
	<-e.cleanupRelease
}

func (e *cleanupQuarantineExecutor) releaseCleanup() {
	e.releaseOnce.Do(func() { close(e.cleanupRelease) })
}

type cleanupQuarantineResult struct {
	payload string
	err     error
}

type cleanupQuarantineUpdateResult struct {
	auth *Auth
	err  error
}

type cleanupQuarantineInvocation func(context.Context, *Manager, cliproxyexecutor.Request, cliproxyexecutor.Options) (string, error)

func TestManagerExecuteWaitsForRetiredInstanceCleanup(t *testing.T) {
	runCleanupQuarantineReselectionTest(t, true, func(ctx context.Context, manager *Manager, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (string, error) {
		response, errExecute := manager.Execute(ctx, []string{"cleanup-quarantine"}, req, opts)
		return string(response.Payload), errExecute
	}, func(executor *cleanupQuarantineExecutor) int32 {
		return executor.executeCalls.Load()
	})
}

func TestManagerExecuteStreamWaitsForRetiredInstanceCleanup(t *testing.T) {
	runCleanupQuarantineReselectionTest(t, false, func(ctx context.Context, manager *Manager, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (string, error) {
		result, errStream := manager.ExecuteStream(ctx, []string{"cleanup-quarantine"}, req, opts)
		if errStream != nil {
			return "", errStream
		}
		var payload []byte
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				return "", chunk.Err
			}
			payload = append(payload, chunk.Payload...)
		}
		return string(payload), nil
	}, func(executor *cleanupQuarantineExecutor) int32 {
		return executor.streamCalls.Load()
	})
}

func TestManagerExecuteCountWaitsForRetiredInstanceCleanup(t *testing.T) {
	runCleanupQuarantineReselectionTest(t, false, func(ctx context.Context, manager *Manager, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (string, error) {
		response, errCount := manager.ExecuteCount(ctx, []string{"cleanup-quarantine"}, req, opts)
		return string(response.Payload), errCount
	}, func(executor *cleanupQuarantineExecutor) int32 {
		return executor.countCalls.Load()
	})
}

func runCleanupQuarantineReselectionTest(t *testing.T, strictAffinity bool, invoke cleanupQuarantineInvocation, calls func(*cleanupQuarantineExecutor) int32) {
	t.Helper()
	const provider = "cleanup-quarantine"
	authID := schedulerTestID(t, "auth")
	model := schedulerTestID(t, "model")

	var selector Selector = &RoundRobinSelector{}
	var affinitySelector *SessionAffinitySelector
	if strictAffinity {
		failover := false
		affinitySelector = NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
			Fallback: &RoundRobinSelector{},
			TTL:      time.Minute,
			Failover: &failover,
		})
		t.Cleanup(affinitySelector.Stop)
		selector = affinitySelector
	}

	manager := NewManager(nil, selector, nil)
	if strictAffinity {
		failover := false
		manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{
			SessionAffinity:         true,
			SessionAffinityFailover: &failover,
		}})
	}
	manager.SetRetryConfig(0, 0, 1)
	executor := newCleanupQuarantineExecutor(provider)
	t.Cleanup(executor.releaseCleanup)
	manager.RegisterExecutor(executor)
	registerSchedulerModels(t, provider, model, authID)

	selected, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: provider,
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "before-cleanup",
		},
		Metadata: map[string]any{"type": provider},
	})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	oldInstanceID := selected.RuntimeInstanceID()
	cleanupDone := selected.RuntimeInstanceCleanupDone()
	if cleanupDone == nil {
		t.Fatal("runtime instance cleanup signal is nil")
	}

	headers := make(http.Header)
	headers.Set("X-Session-ID", authID)
	payload := []byte(`{}`)
	req := cliproxyexecutor.Request{Model: model, Payload: payload}
	opts := cliproxyexecutor.Options{Headers: headers, OriginalRequest: payload}
	operationDone := make(chan cleanupQuarantineResult, 1)
	go func() {
		responsePayload, errInvoke := invoke(t.Context(), manager, req, opts)
		operationDone <- cleanupQuarantineResult{payload: responsePayload, err: errInvoke}
	}()
	waitForCleanupTestSignal(t, executor.firstStarted, "initial execution did not start")

	replacement := selected.Clone()
	replacement.Attributes[SourceHashAttributeKey] = "after-cleanup"
	updateDone := make(chan cleanupQuarantineUpdateResult, 1)
	go func() {
		updated, errUpdate := manager.Update(t.Context(), replacement)
		updateDone <- cleanupQuarantineUpdateResult{auth: updated, err: errUpdate}
	}()

	select {
	case cleanupInstanceID := <-executor.cleanupStarted:
		if cleanupInstanceID != oldInstanceID {
			t.Fatalf("cleanup instance = %q, want %q", cleanupInstanceID, oldInstanceID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replacement cleanup did not start")
	}
	waitForCleanupTestSignal(t, executor.firstFinished, "retired execution did not stop")
	select {
	case <-cleanupDone:
		t.Fatal("runtime instance cleanup signal closed while quarantine was active")
	default:
	}
	select {
	case result := <-operationDone:
		t.Fatalf("operation completed during cleanup quarantine: payload=%q err=%v", result.payload, result.err)
	case <-time.After(100 * time.Millisecond):
	}
	if gotCalls := calls(executor); gotCalls != 1 {
		t.Fatalf("calls during cleanup quarantine = %d, want 1", gotCalls)
	}

	executor.releaseCleanup()
	var updated *Auth
	select {
	case result := <-updateDone:
		if result.err != nil {
			t.Fatalf("Update() error = %v", result.err)
		}
		updated = result.auth
	case <-time.After(5 * time.Second):
		t.Fatal("replacement cleanup did not finish")
	}
	select {
	case <-cleanupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime instance cleanup signal did not close")
	}
	select {
	case result := <-operationDone:
		if result.err != nil {
			t.Fatalf("operation error = %v", result.err)
		}
		if updated == nil || result.payload != updated.RuntimeInstanceID() {
			t.Fatalf("operation payload = %q, want replacement instance %q", result.payload, updated.RuntimeInstanceID())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not retry after cleanup quarantine")
	}
	if gotCalls := calls(executor); gotCalls != 2 {
		t.Fatalf("calls after replacement = %d, want 2", gotCalls)
	}

	if affinitySelector != nil {
		cacheKey := provider + "::header:" + authID + "::" + model
		if boundAuthID, ok := affinitySelector.cache.Get(cacheKey); !ok || boundAuthID != authID {
			t.Fatalf("session affinity binding = (%q, %t), want %q", boundAuthID, ok, authID)
		}
	}
}

func TestWaitForRetiredAuthInstanceCleanupRespectsContextCancellation(t *testing.T) {
	auth := &Auth{instanceState: &authInstanceState{}}
	auth.retireInstance()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if errWait := waitForRetiredAuthInstanceCleanup(ctx, auth); !errors.Is(errWait, context.Canceled) {
		t.Fatalf("wait error = %v, want context canceled", errWait)
	}
}

func TestRequestRoundStateRetiredAttemptDoesNotConsumeCredentialBudget(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	state := newRequestRoundState()
	retired := &Auth{ID: "retired", instanceState: &authInstanceState{}}
	next := &Auth{ID: "next", instanceState: &authInstanceState{}}
	state.tried[retired.ID] = struct{}{}
	state.markAttempted(retired)
	allowed := manager.roundPickAllowed(state, 1)
	if allowed(next) {
		t.Fatal("credential budget allowed a second auth before retiring the first attempt")
	}
	retired.retireInstance()
	state.forgetRetiredAttempt(retired)
	if !allowed(next) {
		t.Fatal("retired attempt consumed credential retry budget")
	}
}

func waitForCleanupTestSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal(failure)
	}
}
