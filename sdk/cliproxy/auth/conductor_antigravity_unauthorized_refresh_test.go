package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type antigravityUnauthorizedRefreshExecutor struct {
	mu sync.Mutex

	executeCalls  []string
	streamCalls   []string
	countCalls    []string
	refreshCalls  int
	invalid       map[string]struct{}
	refreshFail   bool
	refreshError  error
	refreshToken  string
	refreshStart  chan struct{}
	refreshGate   chan struct{}
	refreshOnce   sync.Once
	streamRelease *cliproxyexecutor.RequestBodyReleaseController
}

type antigravityRefreshRetryError struct {
	delay time.Duration
	code  int
}

func (e antigravityRefreshRetryError) Error() string { return "refresh rate limited" }
func (e antigravityRefreshRetryError) StatusCode() int {
	if e.code != 0 {
		return e.code
	}
	return http.StatusTooManyRequests
}
func (e antigravityRefreshRetryError) RetryAfter() *time.Duration { return &e.delay }

func (*antigravityUnauthorizedRefreshExecutor) Identifier() string { return "antigravity" }

func (e *antigravityUnauthorizedRefreshExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executeCalls = append(e.executeCalls, auth.ID)
	if _, invalid := e.invalid[authAccessToken(auth)]; invalid {
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"}
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID + ":" + authAccessToken(auth))}, nil
}

func (e *antigravityUnauthorizedRefreshExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.mu.Lock()
	e.streamCalls = append(e.streamCalls, auth.ID)
	_, invalid := e.invalid[authAccessToken(auth)]
	payload := []byte(auth.ID + ":" + authAccessToken(auth))
	streamRelease := e.streamRelease
	e.mu.Unlock()
	if invalid {
		if streamRelease != nil {
			streamRelease.Release()
		}
		return nil, &Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"}
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: payload}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *antigravityUnauthorizedRefreshExecutor) CountTokens(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.countCalls = append(e.countCalls, auth.ID)
	if _, invalid := e.invalid[authAccessToken(auth)]; invalid {
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"}
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID + ":" + authAccessToken(auth))}, nil
}

func (e *antigravityUnauthorizedRefreshExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	e.mu.Lock()
	e.refreshCalls++
	refreshError := e.refreshError
	refreshFail := e.refreshFail
	refreshToken := e.refreshToken
	refreshStart := e.refreshStart
	refreshGate := e.refreshGate
	e.mu.Unlock()

	if refreshStart != nil {
		e.refreshOnce.Do(func() { close(refreshStart) })
	}
	if refreshGate != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-refreshGate:
		}
	}
	if refreshError != nil {
		return nil, refreshError
	}
	if refreshFail {
		return nil, &Error{HTTPStatus: http.StatusUnauthorized, Message: "refresh token invalid"}
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["access_token"] = refreshToken
	return auth, nil
}

func (*antigravityUnauthorizedRefreshExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func newAntigravityUnauthorizedFixture(t *testing.T, refreshFail bool) (*Manager, *antigravityUnauthorizedRefreshExecutor, *Auth, *Auth, string) {
	t.Helper()
	model := "antigravity-refresh-" + uuid.NewString()
	primary := &Auth{ID: "aa-primary-" + uuid.NewString(), Provider: "antigravity", Metadata: map[string]any{"access_token": "stale", "refresh_token": "refresh"}}
	backup := &Auth{ID: "bb-backup-" + uuid.NewString(), Provider: "antigravity", Metadata: map[string]any{"access_token": "backup", "refresh_token": "backup-refresh"}}
	executor := &antigravityUnauthorizedRefreshExecutor{
		invalid:      map[string]struct{}{"stale": {}},
		refreshFail:  refreshFail,
		refreshToken: "fresh",
	}
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(executor)

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(primary.ID, "antigravity", []*registry.ModelInfo{{ID: model}})
	reg.RegisterClient(backup.ID, "antigravity", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		reg.UnregisterClient(primary.ID)
		reg.UnregisterClient(backup.ID)
	})
	if _, err := manager.Register(context.Background(), primary); err != nil {
		t.Fatalf("register primary: %v", err)
	}
	if _, err := manager.Register(context.Background(), backup); err != nil {
		t.Fatalf("register backup: %v", err)
	}
	return manager, executor, primary, backup, model
}

func TestAntigravityUnauthorizedExecuteRefreshesBeforeFallback(t *testing.T) {
	manager, executor, primary, _, model := newAntigravityUnauthorizedFixture(t, false)
	resp, err := manager.Execute(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := string(resp.Payload); got != primary.ID+":fresh" {
		t.Fatalf("payload = %q", got)
	}
	if executor.refreshCalls != 1 || len(executor.executeCalls) != 2 || executor.executeCalls[0] != primary.ID || executor.executeCalls[1] != primary.ID {
		t.Fatalf("refresh=%d execute=%v", executor.refreshCalls, executor.executeCalls)
	}
}

func TestAntigravityUnauthorizedStreamRefreshesBeforeFallback(t *testing.T) {
	manager, executor, primary, _, model := newAntigravityUnauthorizedFixture(t, false)
	stream, err := manager.ExecuteStream(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	chunk := <-stream.Chunks
	if chunk.Err != nil || string(chunk.Payload) != primary.ID+":fresh" {
		t.Fatalf("chunk = %#v", chunk)
	}
	if executor.refreshCalls != 1 || len(executor.streamCalls) != 2 || executor.streamCalls[0] != primary.ID || executor.streamCalls[1] != primary.ID {
		t.Fatalf("refresh=%d stream=%v", executor.refreshCalls, executor.streamCalls)
	}
}

func TestAntigravityUnauthorizedCountRefreshesBeforeFallback(t *testing.T) {
	manager, executor, primary, _, model := newAntigravityUnauthorizedFixture(t, false)
	resp, err := manager.ExecuteCount(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteCount error: %v", err)
	}
	if got := string(resp.Payload); got != primary.ID+":fresh" {
		t.Fatalf("payload = %q", got)
	}
	if executor.refreshCalls != 1 || len(executor.countCalls) != 2 || executor.countCalls[0] != primary.ID || executor.countCalls[1] != primary.ID {
		t.Fatalf("refresh=%d count=%v", executor.refreshCalls, executor.countCalls)
	}
}

func TestAntigravityUnauthorizedRefreshFailureFallsBack(t *testing.T) {
	manager, executor, primary, backup, model := newAntigravityUnauthorizedFixture(t, true)
	resp, err := manager.Execute(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := string(resp.Payload); got != backup.ID+":backup" {
		t.Fatalf("payload = %q", got)
	}
	if executor.refreshCalls != 1 || len(executor.executeCalls) != 2 || executor.executeCalls[0] != primary.ID || executor.executeCalls[1] != backup.ID {
		t.Fatalf("refresh=%d execute=%v", executor.refreshCalls, executor.executeCalls)
	}
}

func TestAntigravityUnauthorizedRefreshPropagatesRetryAfter(t *testing.T) {
	manager, executor, _, backup, model := newAntigravityUnauthorizedFixture(t, false)
	delay := 7 * time.Second
	executor.refreshError = antigravityRefreshRetryError{delay: delay}
	if errDelete := manager.Delete(context.Background(), backup.ID); errDelete != nil {
		t.Fatalf("delete backup: %v", errDelete)
	}

	_, errExecute := manager.Execute(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	var statusErr cliproxyexecutor.StatusError
	var retryErr interface{ RetryAfter() *time.Duration }
	if !errors.As(errExecute, &statusErr) || statusErr.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("Execute() error = %#v, want 429 refresh error", errExecute)
	}
	if !errors.As(errExecute, &retryErr) || retryErr.RetryAfter() == nil || *retryErr.RetryAfter() != delay {
		t.Fatalf("Execute() error = %#v, want Retry-After %s", errExecute, delay)
	}
}

func TestAntigravityUnauthorizedRefreshRetryAfterControlsTransientCooldown(t *testing.T) {
	manager, executor, primary, backup, model := newAntigravityUnauthorizedFixture(t, false)
	delay := 7 * time.Second
	executor.refreshError = antigravityRefreshRetryError{delay: delay, code: http.StatusServiceUnavailable}
	if errDelete := manager.Delete(context.Background(), backup.ID); errDelete != nil {
		t.Fatalf("delete backup: %v", errDelete)
	}

	startedAt := time.Now()
	_, errExecute := manager.Execute(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(errExecute, &statusErr) || statusErr.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("Execute() error = %#v, want 503 refresh error", errExecute)
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil {
		t.Fatal("primary auth missing after refresh failure")
	}
	minimum := startedAt.Add(delay)
	maximum := time.Now().Add(delay)
	if current.NextRetryAfter.Before(minimum) || current.NextRetryAfter.After(maximum) {
		t.Fatalf("NextRetryAfter = %s, want between %s and %s", current.NextRetryAfter, minimum, maximum)
	}
}

func TestAntigravityUnauthorizedStreamRefreshRetryAfterControlsTransientCooldown(t *testing.T) {
	manager, executor, primary, backup, model := newAntigravityUnauthorizedFixture(t, false)
	delay := 7 * time.Second
	executor.refreshError = antigravityRefreshRetryError{delay: delay, code: http.StatusServiceUnavailable}
	if errDelete := manager.Delete(context.Background(), backup.ID); errDelete != nil {
		t.Fatalf("delete backup: %v", errDelete)
	}

	startedAt := time.Now()
	_, errStream := manager.ExecuteStream(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(errStream, &statusErr) || statusErr.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("ExecuteStream() error = %#v, want 503 refresh error", errStream)
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil {
		t.Fatal("primary auth missing after stream refresh failure")
	}
	minimum := startedAt.Add(delay)
	maximum := time.Now().Add(delay)
	if current.NextRetryAfter.Before(minimum) || current.NextRetryAfter.After(maximum) {
		t.Fatalf("NextRetryAfter = %s, want between %s and %s", current.NextRetryAfter, minimum, maximum)
	}
}

func TestAntigravityUnauthorizedRefreshCancellationDoesNotMarkCredentialFailure(t *testing.T) {
	manager, executor, primary, _, model := newAntigravityUnauthorizedFixture(t, false)
	executor.refreshStart = make(chan struct{})
	executor.refreshGate = make(chan struct{})
	var releaseOnce sync.Once
	releaseRefresh := func() { releaseOnce.Do(func() { close(executor.refreshGate) }) }
	t.Cleanup(releaseRefresh)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		errCh <- errExecute
	}()
	select {
	case <-executor.refreshStart:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	cancel()

	select {
	case errExecute := <-errCh:
		if !errors.Is(errExecute, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context.Canceled", errExecute)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not stop after cancellation")
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil {
		t.Fatal("primary auth missing after cancellation")
	}
	if current.LastError != nil || current.Unavailable || current.Status == StatusError {
		t.Fatalf("canceled refresh polluted auth state: %#v", current)
	}
}

func TestAntigravityUnauthorizedStreamRefreshCancellationDoesNotMarkCredentialFailure(t *testing.T) {
	manager, executor, primary, _, model := newAntigravityUnauthorizedFixture(t, false)
	executor.refreshStart = make(chan struct{})
	executor.refreshGate = make(chan struct{})
	var releaseOnce sync.Once
	releaseRefresh := func() { releaseOnce.Do(func() { close(executor.refreshGate) }) }
	t.Cleanup(releaseRefresh)

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, errStream := manager.ExecuteStream(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		errCh <- errStream
	}()
	select {
	case <-executor.refreshStart:
	case <-time.After(time.Second):
		t.Fatal("stream refresh did not start")
	}
	cancel()

	select {
	case errStream := <-errCh:
		if !errors.Is(errStream, context.Canceled) {
			t.Fatalf("ExecuteStream() error = %v, want context.Canceled", errStream)
		}
	case <-time.After(time.Second):
		t.Fatal("ExecuteStream() did not stop after cancellation")
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil {
		t.Fatal("primary auth missing after stream cancellation")
	}
	if current.LastError != nil || current.Unavailable || current.Status == StatusError {
		t.Fatalf("canceled stream refresh polluted auth state: %#v", current)
	}
}

func TestAntigravityUnauthorizedStreamWithoutReplayRecordsFailure(t *testing.T) {
	manager, executor, primary, backup, model := newAntigravityUnauthorizedFixture(t, false)
	if errDelete := manager.Delete(context.Background(), backup.ID); errDelete != nil {
		t.Fatalf("delete backup: %v", errDelete)
	}
	controller := cliproxyexecutor.NewRequestBodyReleaseController(1024, []byte("<released>"))
	executor.streamRelease = controller
	payload := []byte(`{"model":"` + model + `","stream":true}`)
	_, errStream := manager.ExecuteStream(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{
		Model:   model,
		Payload: payload,
	}, cliproxyexecutor.Options{
		Stream:          true,
		OriginalRequest: payload,
		Metadata: map[string]any{
			cliproxyexecutor.BodyReleaseControllerMetadataKey: controller,
		},
	})
	if errStream == nil {
		t.Fatal("ExecuteStream() error = nil, want unauthorized failure")
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil || current.LastError == nil || current.LastError.HTTPStatus != http.StatusUnauthorized {
		t.Fatalf("unreplayable unauthorized failure was not recorded: %#v", current)
	}
}

func TestAntigravityRequestRefreshLockWaitIsCancellable(t *testing.T) {
	manager, executor, primary, _, _ := newAntigravityUnauthorizedFixture(t, false)
	executor.refreshStart = make(chan struct{})
	executor.refreshGate = make(chan struct{})
	var releaseOnce sync.Once
	releaseRefresh := func() { releaseOnce.Do(func() { close(executor.refreshGate) }) }
	t.Cleanup(releaseRefresh)

	firstErr := make(chan error, 1)
	go func() {
		_, errRefresh := manager.refreshAntigravityForRequest(context.Background(), primary.ID, "stale")
		firstErr <- errRefresh
	}()
	select {
	case <-executor.refreshStart:
	case <-time.After(time.Second):
		t.Fatal("first refresh did not start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	secondErr := make(chan error, 1)
	go func() {
		_, errRefresh := manager.refreshAntigravityForRequest(ctx, primary.ID, "stale")
		secondErr <- errRefresh
	}()
	cancel()
	select {
	case errRefresh := <-secondErr:
		if !errors.Is(errRefresh, context.Canceled) {
			t.Fatalf("second refresh error = %v, want context.Canceled", errRefresh)
		}
	case <-time.After(time.Second):
		t.Fatal("second refresh did not stop after cancellation")
	}

	releaseRefresh()
	select {
	case errRefresh := <-firstErr:
		if errRefresh != nil {
			t.Fatalf("first refresh error: %v", errRefresh)
		}
	case <-time.After(time.Second):
		t.Fatal("first refresh did not finish")
	}
}

func TestAntigravityUnauthorizedRefreshRetriesOnlyOnce(t *testing.T) {
	manager, executor, primary, backup, model := newAntigravityUnauthorizedFixture(t, false)
	executor.invalid["fresh"] = struct{}{}
	resp, err := manager.Execute(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := string(resp.Payload); got != backup.ID+":backup" {
		t.Fatalf("payload = %q", got)
	}
	if executor.refreshCalls != 1 {
		t.Fatalf("refresh calls = %d, want 1", executor.refreshCalls)
	}
	want := []string{primary.ID, primary.ID, backup.ID}
	if len(executor.executeCalls) != len(want) {
		t.Fatalf("execute calls = %v", executor.executeCalls)
	}
	for i := range want {
		if executor.executeCalls[i] != want[i] {
			t.Fatalf("execute calls = %v", executor.executeCalls)
		}
	}
}

func TestAntigravityInvalidGrantDisablesWholeAuth(t *testing.T) {
	manager, executor, primary, backup, model := newAntigravityUnauthorizedFixture(t, false)
	executor.refreshError = &Error{HTTPStatus: http.StatusBadRequest, Message: `{"error":"invalid_grant"}`}

	resp, err := manager.Execute(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if got := string(resp.Payload); got != backup.ID+":backup" {
		t.Fatalf("payload = %q", got)
	}
	disabled, ok := manager.GetByID(primary.ID)
	if !ok || disabled == nil {
		t.Fatal("disabled auth missing")
	}
	if !disabled.Disabled || disabled.Status != StatusDisabled || disabled.StatusMessage != "invalid_grant" {
		t.Fatalf("disabled auth state = %#v", disabled)
	}
	if value, _ := disabled.Metadata["disabled"].(bool); !value {
		t.Fatalf("disabled metadata = %#v", disabled.Metadata["disabled"])
	}

	beforeRefresh := executor.refreshCalls
	beforeExecute := len(executor.executeCalls)
	resp, err = manager.Execute(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("second Execute error: %v", err)
	}
	if got := string(resp.Payload); got != backup.ID+":backup" {
		t.Fatalf("second payload = %q", got)
	}
	if executor.refreshCalls != beforeRefresh {
		t.Fatalf("refresh calls = %d, want %d", executor.refreshCalls, beforeRefresh)
	}
	if got := executor.executeCalls[beforeExecute:]; len(got) != 1 || got[0] != backup.ID {
		t.Fatalf("second execute calls = %v, want backup only", got)
	}
}
