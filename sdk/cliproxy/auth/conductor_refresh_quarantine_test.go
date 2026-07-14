package auth

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type refreshLeaseTestExecutor struct {
	provider string

	refreshStarted  chan struct{}
	refreshCanceled chan error
	refreshRelease  chan struct{}
	cleanupActive   chan bool
	refreshActive   atomic.Bool
}

func (e *refreshLeaseTestExecutor) Identifier() string { return e.provider }

func (*refreshLeaseTestExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*refreshLeaseTestExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	chunks := make(chan cliproxyexecutor.StreamChunk)
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *refreshLeaseTestExecutor) Refresh(ctx context.Context, _ *Auth) (*Auth, error) {
	e.refreshActive.Store(true)
	close(e.refreshStarted)
	<-ctx.Done()
	cause := context.Cause(ctx)
	e.refreshCanceled <- cause
	<-e.refreshRelease
	e.refreshActive.Store(false)
	return nil, cause
}

func (*refreshLeaseTestExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*refreshLeaseTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (e *refreshLeaseTestExecutor) CloseAuthInstanceExecutionSessions(string, string, string) {
	e.cleanupActive <- e.refreshActive.Load()
}

func TestManagerAutoRefreshHoldsInstanceLeaseThroughRetirement(t *testing.T) {
	const (
		authID   = "refresh-runtime-lease"
		provider = "refresh-runtime-lease-provider"
	)
	manager := NewManager(nil, nil, nil)
	executor := &refreshLeaseTestExecutor{
		provider:        provider,
		refreshStarted:  make(chan struct{}),
		refreshCanceled: make(chan error, 1),
		refreshRelease:  make(chan struct{}),
		cleanupActive:   make(chan bool, 1),
	}
	manager.RegisterExecutor(executor)
	if _, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: provider,
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "before-refresh",
		},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	job, marked := manager.markRefreshPending(authID, time.Now())
	if !marked {
		t.Fatal("markRefreshPending() = false, want true")
	}

	refreshDone := make(chan struct{})
	go func() {
		manager.refreshAuthJob(t.Context(), job)
		close(refreshDone)
	}()
	waitForRuntimeQuarantineSignal(t, executor.refreshStarted, "refresh did not start")

	replacement, ok := manager.GetByID(authID)
	if !ok || replacement == nil {
		t.Fatal("refresh auth not found")
	}
	replacement.Attributes[SourceHashAttributeKey] = "after-refresh"
	type updateResult struct {
		auth *Auth
		err  error
	}
	updateDone := make(chan updateResult, 1)
	go func() {
		updated, errUpdate := manager.Update(t.Context(), replacement)
		updateDone <- updateResult{auth: updated, err: errUpdate}
	}()

	select {
	case cause := <-executor.refreshCanceled:
		if !errors.Is(cause, errRuntimeAuthInstanceRetired) {
			t.Fatalf("refresh context cause = %v, want runtime retirement", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replacement did not cancel the refresh runtime context")
	}
	close(executor.refreshRelease)
	waitForRuntimeQuarantineSignal(t, refreshDone, "refresh did not release its runtime lease")

	var updated *Auth
	select {
	case result := <-updateDone:
		if result.err != nil {
			t.Fatalf("Update() error = %v", result.err)
		}
		updated = result.auth
	case <-time.After(5 * time.Second):
		t.Fatal("replacement cleanup did not finish after refresh release")
	}
	select {
	case active := <-executor.cleanupActive:
		if active {
			t.Fatal("instance cleanup ran while Refresh was still using the retired instance")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("instance cleanup did not run")
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || updated == nil || current.RuntimeInstanceID() != updated.RuntimeInstanceID() {
		t.Fatalf("current auth = %#v, updated auth = %#v", current, updated)
	}
	if current.LastError != nil || !current.NextRefreshAfter.IsZero() || !current.LastRefreshedAt.IsZero() {
		t.Fatalf("retirement cancellation was recorded as a refresh result: %#v", current)
	}
}

func TestManagerRefreshCleanupWaitIsBounded(t *testing.T) {
	const (
		authID   = "bounded-refresh-cleanup"
		provider = "bounded-refresh-cleanup-provider"
	)
	manager := NewManager(nil, nil, nil)
	manager.refreshCleanupWait = 20 * time.Millisecond
	executor := &refreshLeaseTestExecutor{
		provider:        provider,
		refreshStarted:  make(chan struct{}),
		refreshCanceled: make(chan error, 1),
		refreshRelease:  make(chan struct{}),
		cleanupActive:   make(chan bool, 1),
	}
	manager.RegisterExecutor(executor)
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: provider, Status: StatusActive}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	job, marked := manager.markRefreshPending(authID, time.Now())
	if !marked {
		t.Fatal("markRefreshPending() = false")
	}
	refreshDone := make(chan struct{})
	go func() {
		manager.refreshAuthJob(t.Context(), job)
		close(refreshDone)
	}()
	waitForRuntimeQuarantineSignal(t, executor.refreshStarted, "refresh did not start")

	replacement, ok := manager.GetByID(authID)
	if !ok || replacement == nil {
		t.Fatal("refresh auth not found")
	}
	replacement.Label = "replacement"
	started := time.Now()
	if _, errUpdate := manager.Update(t.Context(), replacement); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Update() blocked for %s after refresh ignored cancellation", elapsed)
	}
	select {
	case active := <-executor.cleanupActive:
		if !active {
			t.Fatal("refresh unexpectedly exited before bounded cleanup elapsed")
		}
	case <-time.After(time.Second):
		t.Fatal("bounded refresh cleanup did not continue")
	}
	close(executor.refreshRelease)
	waitForRuntimeQuarantineSignal(t, refreshDone, "refresh did not exit after release")
}

type directHTTPQuarantineExecutor struct {
	provider string

	cleanupStarted chan struct{}
	cleanupRelease chan struct{}
	cleanupOnce    sync.Once
	prepareCalls   atomic.Int32
	httpCalls      atomic.Int32
}

func (e *directHTTPQuarantineExecutor) Identifier() string { return e.provider }

func (*directHTTPQuarantineExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*directHTTPQuarantineExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	chunks := make(chan cliproxyexecutor.StreamChunk)
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (*directHTTPQuarantineExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (*directHTTPQuarantineExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *directHTTPQuarantineExecutor) PrepareRequest(req *http.Request, _ *Auth) error {
	e.prepareCalls.Add(1)
	req.Header.Set("Authorization", "Bearer replacement-token")
	return nil
}

func (e *directHTTPQuarantineExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	e.httpCalls.Add(1)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("ok")),
	}, nil
}

func (e *directHTTPQuarantineExecutor) CloseAuthInstanceExecutionSessions(string, string, string) {
	e.cleanupOnce.Do(func() { close(e.cleanupStarted) })
	<-e.cleanupRelease
}

func TestManagerDirectHTTPPathsRejectCurrentInstanceDuringCleanupQuarantine(t *testing.T) {
	const (
		authID   = "direct-http-cleanup-quarantine"
		provider = "direct-http-cleanup-quarantine-provider"
	)
	manager := NewManager(nil, nil, nil)
	executor := &directHTTPQuarantineExecutor{
		provider:       provider,
		cleanupStarted: make(chan struct{}),
		cleanupRelease: make(chan struct{}),
	}
	manager.RegisterExecutor(executor)
	selected, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: provider,
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "before-cleanup",
		},
	})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	replacement := selected.Clone()
	replacement.Attributes[SourceHashAttributeKey] = "after-cleanup"
	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(t.Context(), replacement)
		updateDone <- errUpdate
	}()
	waitForRuntimeQuarantineSignal(t, executor.cleanupStarted, "replacement cleanup did not start")
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || current.RuntimeInstanceID() == selected.RuntimeInstanceID() {
		t.Fatalf("replacement auth = %#v", current)
	}

	request, errRequest := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("NewRequestWithContext() error = %v", errRequest)
	}
	assertRuntimeQuarantineError(t, "InjectCredentials", manager.InjectCredentials(request.Clone(t.Context()), authID))
	assertRuntimeQuarantineError(t, "PrepareHttpRequest", manager.PrepareHttpRequest(t.Context(), current, request.Clone(t.Context())))
	_, errNew := manager.NewHttpRequest(t.Context(), current, http.MethodGet, "https://example.com", nil, nil)
	assertRuntimeQuarantineError(t, "NewHttpRequest", errNew)
	_, errHTTP := manager.HttpRequest(t.Context(), current, request.Clone(t.Context()))
	assertRuntimeQuarantineError(t, "HttpRequest", errHTTP)
	if prepares := executor.prepareCalls.Load(); prepares != 0 {
		t.Fatalf("PrepareRequest calls during quarantine = %d, want 0", prepares)
	}
	if calls := executor.httpCalls.Load(); calls != 0 {
		t.Fatalf("HttpRequest calls during quarantine = %d, want 0", calls)
	}

	close(executor.cleanupRelease)
	select {
	case errUpdate := <-updateDone:
		if errUpdate != nil {
			t.Fatalf("Update() error = %v", errUpdate)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replacement cleanup did not finish")
	}
	if errInject := manager.InjectCredentials(request.Clone(t.Context()), authID); errInject != nil {
		t.Fatalf("InjectCredentials() after cleanup error = %v", errInject)
	}
	if errPrepare := manager.PrepareHttpRequest(t.Context(), current, request.Clone(t.Context())); errPrepare != nil {
		t.Fatalf("PrepareHttpRequest() after cleanup error = %v", errPrepare)
	}
	if _, errNew = manager.NewHttpRequest(t.Context(), current, http.MethodGet, "https://example.com", nil, nil); errNew != nil {
		t.Fatalf("NewHttpRequest() after cleanup error = %v", errNew)
	}
	response, errHTTP := manager.HttpRequest(t.Context(), current, request.Clone(t.Context()))
	if errHTTP != nil {
		t.Fatalf("HttpRequest() after cleanup error = %v", errHTTP)
	}
	if response == nil || response.Body == nil {
		t.Fatal("HttpRequest() after cleanup returned no response body")
	}
	if errClose := response.Body.Close(); errClose != nil {
		t.Fatalf("response body close error = %v", errClose)
	}
	if prepares := executor.prepareCalls.Load(); prepares != 3 {
		t.Fatalf("PrepareRequest calls after cleanup = %d, want 3", prepares)
	}
	if calls := executor.httpCalls.Load(); calls != 1 {
		t.Fatalf("HttpRequest calls after cleanup = %d, want 1", calls)
	}
}

func assertRuntimeQuarantineError(t *testing.T, operation string, err error) {
	t.Helper()
	var authErr *Error
	if !errors.As(err, &authErr) || authErr == nil || authErr.Code != "auth_instance_retired" || !authErr.Retryable {
		t.Fatalf("%s error = %#v, want retryable auth_instance_retired", operation, err)
	}
}

func waitForRuntimeQuarantineSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal(failure)
	}
}
