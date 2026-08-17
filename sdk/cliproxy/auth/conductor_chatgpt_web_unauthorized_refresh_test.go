package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type chatGPTWebUnauthorizedRefreshExecutor struct {
	executeCalls       []string
	countCalls         []string
	streamCalls        []string
	prepareCalls       int
	refreshCalls       int
	prepareErr         error
	refreshErr         error
	persistError       bool
	beforeRefresh      func()
	afterRefresh       func()
	refreshContextHook func(context.Context) error
	refreshHook        func(*Auth, *Auth)
	validateCalls      atomic.Int32
	validateHook       func(string, *Auth, *Auth) (*Auth, error)
}

type chatGPTWebRequestRefreshError struct {
	persist bool
	outcome string
}

type chatGPTWebDeadRequestError struct {
	lifecycle *chatgptwebauth.AuthError
}

func (err chatGPTWebDeadRequestError) Error() string { return err.lifecycle.Error() }
func (chatGPTWebDeadRequestError) StatusCode() int   { return http.StatusForbidden }
func (err chatGPTWebDeadRequestError) ChatGPTWebLifecycleError() *chatgptwebauth.AuthError {
	return err.lifecycle
}

type chatGPTWebUnauthorizedChunkExecutor struct {
	*chatGPTWebUnauthorizedRefreshExecutor
	releaseUnauthorized <-chan struct{}
}

type chatGPTWebDistinctUnauthorizedRefreshExecutor struct {
	release      <-chan struct{}
	refreshCalls atomic.Int32
}

type chatGPTWebRefreshWithoutValidation struct {
	ProviderExecutor
}

func (*chatGPTWebRefreshWithoutValidation) Identifier() string { return "chatgpt-web" }

func (*chatGPTWebRefreshWithoutValidation) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	updated := auth.Clone()
	updated.Metadata["access_token"] = "unverified-replacement"
	return updated, nil
}

func (*chatGPTWebDistinctUnauthorizedRefreshExecutor) Identifier() string { return "chatgpt-web" }

func (*chatGPTWebDistinctUnauthorizedRefreshExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*chatGPTWebDistinctUnauthorizedRefreshExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (*chatGPTWebDistinctUnauthorizedRefreshExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*chatGPTWebDistinctUnauthorizedRefreshExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (executor *chatGPTWebDistinctUnauthorizedRefreshExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	executor.refreshCalls.Add(1)
	select {
	case <-executor.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	updated := auth.Clone()
	updated.Metadata["access_token"] = "fresh-" + auth.ID
	return updated, nil
}

func (*chatGPTWebDistinctUnauthorizedRefreshExecutor) ValidateUnauthorizedRequestRefresh(_ context.Context, _ string, _ *Auth, refreshed *Auth) (*Auth, error) {
	return refreshed, nil
}

type chatGPTWebRefreshPersistenceStore struct {
	mu          sync.Mutex
	saved       *Auth
	saveStarted chan struct{}
	releaseSave chan struct{}
	startOnce   sync.Once
}

func (*chatGPTWebRefreshPersistenceStore) RefreshPersistenceConcurrency() int {
	return 1
}

type chatGPTWebDoneObservedContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (ctx *chatGPTWebDoneObservedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}

func (*chatGPTWebRefreshPersistenceStore) List(context.Context) ([]*Auth, error) {
	return nil, nil
}

func TestChatGPTWebRequestRefreshNilManagerReturnsError(t *testing.T) {
	var manager *Manager
	if _, err := manager.refreshProviderForRequest(t.Context(), "auth-id", "token", "chatgpt-web", &Auth{}); err == nil {
		t.Fatal("nil manager refresh returned no error")
	}
}

func (store *chatGPTWebRefreshPersistenceStore) Save(ctx context.Context, auth *Auth) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if store.saveStarted != nil {
		store.startOnce.Do(func() { close(store.saveStarted) })
	}
	if store.releaseSave != nil {
		select {
		case <-store.releaseSave:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	store.mu.Lock()
	store.saved = auth.Clone()
	store.mu.Unlock()
	return "", nil
}

func (*chatGPTWebRefreshPersistenceStore) Delete(context.Context, string) error {
	return nil
}

func (store *chatGPTWebRefreshPersistenceStore) snapshot() *Auth {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.saved.Clone()
}

func (err chatGPTWebRequestRefreshError) Error() string { return "chatgpt web refresh failed" }
func (chatGPTWebRequestRefreshError) StatusCode() int   { return http.StatusServiceUnavailable }
func (chatGPTWebRequestRefreshError) SkipAuthResult() bool {
	return true
}
func (chatGPTWebRequestRefreshError) RetryOtherAuth() bool {
	return true
}
func (chatGPTWebRequestRefreshError) ChatGPTWebCredentialUnavailable() bool {
	return true
}
func (err chatGPTWebRequestRefreshError) PersistAuthUpdateOnError() bool {
	return err.persist
}
func (err chatGPTWebRequestRefreshError) ChatGPTWebRequestRefreshOutcome() string {
	return err.outcome
}

func (*chatGPTWebUnauthorizedRefreshExecutor) Identifier() string { return "chatgpt-web" }

func (executor *chatGPTWebUnauthorizedRefreshExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	executor.executeCalls = append(executor.executeCalls, auth.ID)
	if authAccessToken(auth) == "stale" {
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"}
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID + ":" + authAccessToken(auth))}, nil
}

func (executor *chatGPTWebUnauthorizedRefreshExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	executor.streamCalls = append(executor.streamCalls, auth.ID)
	if authAccessToken(auth) == "stale" {
		return nil, &Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"}
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(auth.ID + ":" + authAccessToken(auth))}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (executor *chatGPTWebUnauthorizedChunkExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	executor.streamCalls = append(executor.streamCalls, auth.ID)
	chunks := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		chunks <- cliproxyexecutor.BootstrapCommitStreamChunk()
		if executor.releaseUnauthorized != nil {
			<-executor.releaseUnauthorized
		}
		chunks <- cliproxyexecutor.StreamChunk{Err: &Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"}}
	}()
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (executor *chatGPTWebUnauthorizedRefreshExecutor) CountTokens(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	executor.countCalls = append(executor.countCalls, auth.ID)
	if authAccessToken(auth) == "stale" {
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"}
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID + ":" + authAccessToken(auth))}, nil
}

func (executor *chatGPTWebUnauthorizedRefreshExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	executor.refreshCalls++
	if executor.afterRefresh != nil {
		defer executor.afterRefresh()
	}
	if executor.beforeRefresh != nil {
		executor.beforeRefresh()
	}
	if executor.refreshContextHook != nil {
		if err := executor.refreshContextHook(ctx); err != nil {
			return nil, err
		}
	}
	if executor.refreshErr != nil || executor.persistError {
		if !executor.persistError {
			return nil, executor.refreshErr
		}
		updated := auth.Clone()
		updated.Metadata["lifecycle_state"] = LifecycleStateReloginPending
		updated.Metadata["lifecycle_reason"] = "token_invalid"
		return updated, chatGPTWebRequestRefreshError{persist: true}
	}
	updated := auth.Clone()
	updated.Metadata["access_token"] = "fresh"
	if executor.refreshHook != nil {
		executor.refreshHook(auth, updated)
	}
	return updated, nil
}

func (executor *chatGPTWebUnauthorizedRefreshExecutor) ValidateUnauthorizedRequestRefresh(
	_ context.Context,
	failedAccessToken string,
	previous *Auth,
	refreshed *Auth,
) (*Auth, error) {
	executor.validateCalls.Add(1)
	if executor.validateHook != nil {
		return executor.validateHook(failedAccessToken, previous, refreshed)
	}
	return refreshed, nil
}

func (executor *chatGPTWebUnauthorizedRefreshExecutor) ShouldPrepareRequestAuth(*Auth) bool {
	return executor.prepareErr != nil
}

func (executor *chatGPTWebUnauthorizedRefreshExecutor) PrepareRequestAuth(_ context.Context, auth *Auth) (*Auth, error) {
	executor.prepareCalls++
	if executor.prepareErr != nil {
		return auth, executor.prepareErr
	}
	return auth, nil
}

func (*chatGPTWebUnauthorizedRefreshExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func newChatGPTWebUnauthorizedRefreshFixture(t *testing.T) (*Manager, *chatGPTWebUnauthorizedRefreshExecutor, *Auth, *Auth, string) {
	t.Helper()
	const model = "chatgpt-web-refresh-model"
	executor := &chatGPTWebUnauthorizedRefreshExecutor{}
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(executor)
	auths := []*Auth{
		{
			ID:       "aa-chatgpt-web-primary",
			Provider: "chatgpt-web",
			Status:   StatusActive,
			Metadata: map[string]any{
				"access_token":    "stale",
				"refresh_token":   "refresh",
				"lifecycle_state": LifecycleStateActive,
			},
		},
		{
			ID:       "bb-chatgpt-web-backup",
			Provider: "chatgpt-web",
			Status:   StatusActive,
			Metadata: map[string]any{
				"access_token":    "backup",
				"refresh_token":   "backup-refresh",
				"lifecycle_state": LifecycleStateActive,
			},
		},
	}
	for _, auth := range auths {
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register(%s) error: %v", auth.ID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	}
	t.Cleanup(func() {
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("CloseExecutors() error: %v", errClose)
		}
	})
	return manager, executor, auths[0], auths[1], model
}

func chatGPTWebRequestRefreshBlockCount(manager *Manager, authID string) int {
	if manager == nil || manager.scheduler == nil {
		return 0
	}
	manager.scheduler.mu.RLock()
	blocks := manager.scheduler.requestRefreshBlocks[authID]
	manager.scheduler.mu.RUnlock()
	return blocks
}

func TestChatGPTWebUnauthorizedReturnsWithoutWaitingForBackgroundRefresh(t *testing.T) {
	testCases := []struct {
		name   string
		invoke func(context.Context, *Manager, string) error
		calls  func(*chatGPTWebUnauthorizedRefreshExecutor) []string
	}{
		{
			name: "execute",
			invoke: func(ctx context.Context, manager *Manager, model string) error {
				_, err := manager.Execute(ctx, []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return err
			},
			calls: func(executor *chatGPTWebUnauthorizedRefreshExecutor) []string { return executor.executeCalls },
		},
		{
			name: "count_tokens",
			invoke: func(ctx context.Context, manager *Manager, model string) error {
				_, err := manager.ExecuteCount(ctx, []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return err
			},
			calls: func(executor *chatGPTWebUnauthorizedRefreshExecutor) []string { return executor.countCalls },
		},
		{
			name: "stream",
			invoke: func(ctx context.Context, manager *Manager, model string) error {
				_, err := manager.ExecuteStream(ctx, []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return err
			},
			calls: func(executor *chatGPTWebUnauthorizedRefreshExecutor) []string { return executor.streamCalls },
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			manager, executor, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
			refreshStarted := make(chan struct{})
			releaseRefresh := make(chan struct{})
			var startOnce sync.Once
			executor.beforeRefresh = func() {
				startOnce.Do(func() { close(refreshStarted) })
				<-releaseRefresh
			}
			requestDone := make(chan error, 1)
			go func() {
				requestDone <- testCase.invoke(t.Context(), manager, model)
			}()

			select {
			case <-refreshStarted:
			case <-time.After(5 * time.Second):
				close(releaseRefresh)
				t.Fatal("background refresh did not start")
			}
			select {
			case errRequest := <-requestDone:
				if statusCodeFromError(errRequest) != http.StatusUnauthorized {
					close(releaseRefresh)
					t.Fatalf("request error = %v, want original 401", errRequest)
				}
				directStatus, ok := errRequest.(interface{ StatusCode() int })
				if !ok || directStatus.StatusCode() != http.StatusUnauthorized {
					close(releaseRefresh)
					t.Fatalf("direct request status = %T %v, want 401", errRequest, errRequest)
				}
				if errRequest.Error() != "invalid access token" {
					close(releaseRefresh)
					t.Fatalf("request error text = %q, want original upstream message", errRequest.Error())
				}
			case <-time.After(time.Second):
				close(releaseRefresh)
				t.Fatal("request waited for the background refresh")
			}
			calls := testCase.calls(executor)
			if len(calls) != 1 || calls[0] != primary.ID {
				close(releaseRefresh)
				t.Fatalf("upstream calls = %v, want primary once", calls)
			}
			for _, calls := range [][]string{executor.executeCalls, executor.countCalls, executor.streamCalls} {
				for _, authID := range calls {
					if authID == backup.ID {
						close(releaseRefresh)
						t.Fatalf("401 request fell back to backup: execute=%v count=%v stream=%v", executor.executeCalls, executor.countCalls, executor.streamCalls)
					}
				}
			}
			current, ok := manager.GetByID(primary.ID)
			if !ok || current == nil || current.Unavailable || current.CooldownScope != "" {
				close(releaseRefresh)
				t.Fatalf("401 request synchronously mutated cooldown before refresh result: %#v", current)
			}
			if blocks := chatGPTWebRequestRefreshBlockCount(manager, primary.ID); blocks != 1 {
				close(releaseRefresh)
				t.Fatalf("request refresh blocks = %d, want exactly one while refresh is active", blocks)
			}

			close(releaseRefresh)
			deadline := time.Now().Add(5 * time.Second)
			for {
				current, ok = manager.GetByID(primary.ID)
				if ok && authAccessToken(current) == "fresh" && !current.Unavailable {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("background refresh did not restore credential: %#v", current)
				}
				time.Sleep(time.Millisecond)
			}
			if executor.refreshCalls != 1 {
				t.Fatalf("refresh calls = %d, want 1", executor.refreshCalls)
			}
			if blocks := chatGPTWebRequestRefreshBlockCount(manager, primary.ID); blocks != 0 {
				t.Fatalf("request refresh blocks after completion = %d, want 0", blocks)
			}
		})
	}
}

func TestChatGPTWebUnauthorizedRecoveryStartsAfterRequestCancellation(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, attempted, errRequest := manager.tryRefreshAfterUnauthorized(
		ctx,
		executor,
		primary,
		&Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"},
		false,
	)
	if !attempted || statusCodeFromError(errRequest) != http.StatusUnauthorized {
		t.Fatalf("tryRefreshAfterUnauthorized() = attempted %t, error %v", attempted, errRequest)
	}
	triggerChatGPTWebUnauthorizedRequestRefresh(errRequest)
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok := manager.GetByID(primary.ID)
		snapshot := manager.ChatGPTWebRequestRefreshSnapshot()
		if ok && current != nil && authAccessToken(current) == "fresh" && snapshot.Succeeded == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("detached unauthorized recovery did not finish: auth=%#v metrics=%#v", current, snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	if executor.refreshCalls != 1 || executor.validateCalls.Load() != 1 {
		t.Fatalf("refresh/validate calls = %d/%d, want 1/1", executor.refreshCalls, executor.validateCalls.Load())
	}
}

func TestChatGPTWebUnauthorizedWithoutRefreshFlightPersistsCooldownAndDoesNotFallback(t *testing.T) {
	manager, executor, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil {
		t.Fatal("primary credential was not registered")
	}
	updated := current.Clone()
	updated.Attributes = map[string]string{"compat_name": "compat-without-refresh"}
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), updated); errUpdate != nil {
		t.Fatal(errUpdate)
	}

	_, errRequest := manager.Execute(
		t.Context(),
		[]string{"chatgpt-web"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	if statusCodeFromError(errRequest) != http.StatusUnauthorized || errRequest.Error() != "invalid access token" {
		t.Fatalf("request error = %v, want original upstream 401", errRequest)
	}
	if len(executor.executeCalls) != 1 || executor.executeCalls[0] != primary.ID {
		t.Fatalf("execute calls = %v, want primary once without fallback to %q", executor.executeCalls, backup.ID)
	}
	if executor.refreshCalls != 0 {
		t.Fatalf("refresh calls = %d, want zero", executor.refreshCalls)
	}
	current, ok = manager.GetByID(primary.ID)
	if !ok || current == nil {
		t.Fatal("primary credential disappeared after 401")
	}
	if current.LastError == nil || current.LastError.StatusCode() != http.StatusUnauthorized ||
		!current.Unavailable || current.CooldownScope != cooldownScopeAuth || !current.NextRetryAfter.After(time.Now()) {
		t.Fatalf("401 without refresh flight was not persisted as cooldown: %#v", current)
	}
	snapshot := manager.ChatGPTWebRequestRefreshSnapshot()
	if snapshot.Received != 1 || snapshot.NoStart != 1 || snapshot.Queued != 0 || snapshot.Running != 0 {
		t.Fatalf("no-start snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebUnauthorizedRefreshDeadlineIsNotPersistenceBackpressure(t *testing.T) {
	manager, executor, primary, _, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	executor.refreshContextHook = func(context.Context) error { return context.DeadlineExceeded }

	_, errRequest := manager.Execute(
		t.Context(),
		[]string{"chatgpt-web"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	if statusCodeFromError(errRequest) != http.StatusUnauthorized {
		t.Fatalf("request error = %v, want original upstream 401", errRequest)
	}
	waitForResultPersistenceCondition(t, 5*time.Second, "refresh deadline did not settle", func() bool {
		snapshot := manager.ChatGPTWebRequestRefreshSnapshot()
		return snapshot.Failed == 1 && snapshot.SchedulerBlocked == 0
	})
	snapshot := manager.ChatGPTWebRequestRefreshSnapshot()
	if snapshot.Backpressured != 0 {
		t.Fatalf("request refresh backpressure = %d, want zero for provider timeout", snapshot.Backpressured)
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil || current.LastError == nil || current.LastError.Code == "refresh_persist_backpressure" {
		t.Fatalf("provider timeout was persisted as backpressure: %#v", current)
	}
}

func TestChatGPTWebUnauthorizedLifecycleDeadIsPersistentlyIsolatedWithoutRefresh(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	errDead := &chatgptwebauth.AuthError{
		Code:           "account_deactivated",
		State:          chatgptwebauth.LifecycleDead,
		LifecycleState: chatgptwebauth.LifecycleDead,
		Status:         http.StatusForbidden,
		StatusCode:     http.StatusForbidden,
		Terminal:       true,
		Message:        "account is unavailable",
	}

	errRequest := manager.wrapChatGPTWebUnauthorizedRequestError(
		t.Context(),
		primary,
		chatGPTWebDeadRequestError{lifecycle: errDead},
	)
	if statusCodeFromError(errRequest) != http.StatusForbidden {
		t.Fatalf("wrapped dead status = %v, want 403", errRequest)
	}
	triggerChatGPTWebUnauthorizedRequestRefresh(errRequest)

	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil || current.LifecycleState() != LifecycleStateDead ||
		current.LifecycleSelectable() || current.LifecycleRefreshable() ||
		current.LastError == nil || current.LastError.Code != "account_deactivated" {
		t.Fatalf("dead credential state = %#v", current)
	}
	if executor.refreshCalls != 0 || chatGPTWebRequestRefreshBlockCount(manager, primary.ID) != 0 {
		t.Fatalf("dead credential refresh calls/blocks = %d/%d, want 0/0", executor.refreshCalls, chatGPTWebRequestRefreshBlockCount(manager, primary.ID))
	}
	snapshot := manager.ChatGPTWebRequestRefreshSnapshot()
	if snapshot.DeadConfirmed != 1 || snapshot.NoStart != 1 || snapshot.Running != 0 || snapshot.Queued != 0 {
		t.Fatalf("dead recovery snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebUnauthorizedStreamChunkStartsBackgroundRefreshWithoutCooldown(t *testing.T) {
	manager, baseExecutor, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	releaseUnauthorized := make(chan struct{})
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var refreshStartOnce sync.Once
	baseExecutor.beforeRefresh = func() {
		refreshStartOnce.Do(func() { close(refreshStarted) })
		<-releaseRefresh
	}
	executor := &chatGPTWebUnauthorizedChunkExecutor{
		chatGPTWebUnauthorizedRefreshExecutor: baseExecutor,
		releaseUnauthorized:                   releaseUnauthorized,
	}
	manager.RegisterExecutor(executor)

	stream, errStream := manager.ExecuteStream(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errStream != nil || stream == nil {
		t.Fatalf("ExecuteStream() = %#v, %v", stream, errStream)
	}
	close(releaseUnauthorized)
	var unauthorized error
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			unauthorized = chunk.Err
		}
	}
	if statusCodeFromError(unauthorized) != http.StatusUnauthorized || unauthorized.Error() != "invalid access token" {
		close(releaseRefresh)
		t.Fatalf("stream error = %T %v, want original 401", unauthorized, unauthorized)
	}
	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		close(releaseRefresh)
		t.Fatal("stream 401 did not start background refresh")
	}
	if len(baseExecutor.streamCalls) != 1 || baseExecutor.streamCalls[0] != primary.ID {
		close(releaseRefresh)
		t.Fatalf("stream calls = %v, want primary once", baseExecutor.streamCalls)
	}
	for _, authID := range baseExecutor.streamCalls {
		if authID == backup.ID {
			close(releaseRefresh)
			t.Fatalf("stream 401 fell back to backup: %v", baseExecutor.streamCalls)
		}
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil || current.Unavailable || current.CooldownScope != "" || current.LastError != nil {
		close(releaseRefresh)
		t.Fatalf("stream 401 synchronously mutated credential state: %#v", current)
	}
	if blocks := chatGPTWebRequestRefreshBlockCount(manager, primary.ID); blocks != 1 {
		close(releaseRefresh)
		t.Fatalf("stream 401 request refresh blocks = %d, want 1", blocks)
	}
	close(releaseRefresh)
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok = manager.GetByID(primary.ID)
		if ok && authAccessToken(current) == "fresh" && chatGPTWebRequestRefreshBlockCount(manager, primary.ID) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream 401 background refresh did not finish: %#v", current)
		}
		time.Sleep(time.Millisecond)
	}
	if baseExecutor.refreshCalls != 1 {
		t.Fatalf("refresh calls = %d, want 1", baseExecutor.refreshCalls)
	}
}

func TestChatGPTWebUnauthorizedCredentialIsExcludedDuringBackgroundRefreshWhenCooldownDisabled(t *testing.T) {
	manager, executor, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	manager.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusUnauthorized}})
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	refreshFinished := make(chan struct{})
	var startOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRefresh) }) }
	t.Cleanup(release)
	executor.beforeRefresh = func() {
		startOnce.Do(func() { close(refreshStarted) })
		<-releaseRefresh
	}
	executor.afterRefresh = func() { close(refreshFinished) }

	firstDone := make(chan error, 1)
	go func() {
		_, err := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		firstDone <- err
	}()
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("background refresh did not start")
	}
	select {
	case errRequest := <-firstDone:
		if statusCodeFromError(errRequest) != http.StatusUnauthorized {
			t.Fatalf("first request error = %v, want original 401", errRequest)
		}
	case <-time.After(time.Second):
		t.Fatal("first request waited for the background refresh")
	}

	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil || current.Unavailable {
		t.Fatalf("401 cooldown override was not applied: %#v", current)
	}

	response, errSecond := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errSecond != nil {
		t.Fatalf("second request error: %v", errSecond)
	}
	if got := string(response.Payload); got != backup.ID+":backup" {
		t.Fatalf("second request payload = %q, want backup credential", got)
	}
	if len(executor.executeCalls) != 2 || executor.executeCalls[0] != primary.ID || executor.executeCalls[1] != backup.ID {
		t.Fatalf("execute calls = %v, want primary then backup", executor.executeCalls)
	}

	release()
	select {
	case <-refreshFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("background refresh did not finish")
	}
}

func TestChatGPTWebUnauthorizedRefreshBackpressureTemporarilyExcludesOnlyFailedCredential(t *testing.T) {
	const model = "chatgpt-web-refresh-backpressure-model"
	primaryAuth := resultPersistenceTestAuth("aa-chatgpt-web-refresh-backpressure", 10)
	primaryAuth.Metadata["access_token"] = "stale"
	backupAuth := resultPersistenceTestAuth("bb-chatgpt-web-refresh-backpressure", 10)
	backupAuth.Metadata["access_token"] = "backup"
	store := newResultPersistenceTestStore(1, 1, primaryAuth, backupAuth)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	executor := &chatGPTWebUnauthorizedRefreshExecutor{}
	manager.RegisterExecutor(executor)
	primary := registerResultPersistenceTestAuth(t, manager, store, primaryAuth.ID)
	backup := registerResultPersistenceTestAuth(t, manager, store, backupAuth.ID)
	for _, auth := range []*Auth{primary, backup} {
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
		defer registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	}

	coordinator := manager.refreshPersistence.Load()
	coordinator.queueLimit = 0
	active, errActive := coordinator.acquireContext(t.Context(), RefreshPersistencePrioritySession, "capacity-blocker")
	if errActive != nil {
		t.Fatal(errActive)
	}
	defer func() {
		active.release()
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("CloseExecutors() error: %v", errClose)
		}
	}()

	_, errFirst := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if statusCodeFromError(errFirst) != http.StatusUnauthorized || errFirst.Error() != "invalid access token" {
		t.Fatalf("first request error = %v, want original upstream 401", errFirst)
	}
	waitForResultPersistenceCondition(t, time.Second, "backpressured 401 credential was not excluded", func() bool {
		current, ok := manager.GetByID(primary.ID)
		return ok && current != nil && current.Unavailable && current.CooldownScope == cooldownScopeAuth &&
			current.NextRetryAfter.After(time.Now()) && current.NextRefreshAfter.After(time.Now()) &&
			current.LastError != nil && current.LastError.Code == "refresh_persist_backpressure" &&
			chatGPTWebRequestRefreshBlockCount(manager, primary.ID) == 0
	})
	if executor.refreshCalls != 0 {
		t.Fatalf("refresh calls = %d, want zero before persistence admission", executor.refreshCalls)
	}

	response, errSecond := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errSecond != nil {
		t.Fatalf("second request error: %v", errSecond)
	}
	if got := string(response.Payload); got != backup.ID+":backup" {
		t.Fatalf("second request payload = %q, want ready backup credential", got)
	}
	if len(executor.executeCalls) != 2 || executor.executeCalls[0] != primary.ID || executor.executeCalls[1] != backup.ID {
		t.Fatalf("execute calls = %v, want failed credential once then backup", executor.executeCalls)
	}
	currentBackup, ok := manager.GetByID(backup.ID)
	if !ok || currentBackup == nil || currentBackup.Unavailable {
		t.Fatalf("backpressure affected unrelated credential: %#v", currentBackup)
	}
}

func TestChatGPTWebRequestRefreshBackpressureClassification(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "queue full", err: refreshPersistenceBackpressureError(), want: true},
		{name: "admission deadline", err: context.DeadlineExceeded, want: true},
		{name: "shutdown cancellation", err: context.Canceled, want: false},
		{name: "coordinator closed", err: refreshPersistenceClosedError(), want: false},
		{name: "unrelated error", err: errors.New("refresh failed"), want: false},
		{name: "success", err: nil, want: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := chatGPTWebRequestRefreshBackpressured(testCase.err); got != testCase.want {
				t.Fatalf("chatGPTWebRequestRefreshBackpressured(%v) = %t, want %t", testCase.err, got, testCase.want)
			}
		})
	}
}

func TestChatGPTWebUnauthorizedBackgroundRefreshIsSingleflight(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	refreshFinished := make(chan struct{})
	var startOnce sync.Once
	executor.beforeRefresh = func() {
		startOnce.Do(func() { close(refreshStarted) })
		<-releaseRefresh
	}
	executor.afterRefresh = func() { close(refreshFinished) }

	const callers = 32
	results := make(chan error, callers)
	for range callers {
		go func() {
			_, _, err := manager.tryRefreshAfterUnauthorized(
				t.Context(),
				executor,
				primary,
				&Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"},
				false,
			)
			triggerChatGPTWebUnauthorizedRequestRefresh(err)
			results <- err
		}()
	}
	for range callers {
		select {
		case errRequest := <-results:
			if statusCodeFromError(errRequest) != http.StatusUnauthorized || !isChatGPTWebUnauthorizedRequestError(errRequest) {
				close(releaseRefresh)
				t.Fatalf("trigger error = %v, want immediate wrapped 401", errRequest)
			}
		case <-time.After(time.Second):
			close(releaseRefresh)
			t.Fatal("401 caller waited for the shared refresh")
		}
	}
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		close(releaseRefresh)
		t.Fatal("shared background refresh did not start")
	}
	if executor.refreshCalls != 1 {
		close(releaseRefresh)
		t.Fatalf("refresh calls = %d, want one shared attempt", executor.refreshCalls)
	}
	if blocks := chatGPTWebRequestRefreshBlockCount(manager, primary.ID); blocks != 1 {
		close(releaseRefresh)
		t.Fatalf("request refresh blocks = %d, want one shared block", blocks)
	}
	manager.chatGPTWebRefreshMu.Lock()
	activeFlights := len(manager.chatGPTWebRefreshes)
	manager.chatGPTWebRefreshMu.Unlock()
	if activeFlights != 1 {
		close(releaseRefresh)
		t.Fatalf("active ChatGPT Web refresh flights = %d, want 1", activeFlights)
	}
	close(releaseRefresh)
	select {
	case <-refreshFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("shared background refresh did not finish")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok := manager.GetByID(primary.ID)
		if ok && authAccessToken(current) == "fresh" && chatGPTWebRequestRefreshBlockCount(manager, primary.ID) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shared background refresh was not installed: %#v", current)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestChatGPTWebUnauthorizedManyDistinctCredentialsRemainIndependentlyIsolated(t *testing.T) {
	const credentialCount = 64
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	executor := &chatGPTWebDistinctUnauthorizedRefreshExecutor{release: release}
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(executor)
	t.Cleanup(func() {
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("CloseExecutors() error: %v", errClose)
		}
	})

	auths := make([]*Auth, 0, credentialCount)
	for index := range credentialCount {
		auth := &Auth{
			ID:       fmt.Sprintf("chatgpt-web-distinct-401-%03d", index),
			Provider: "chatgpt-web",
			Status:   StatusActive,
			Metadata: map[string]any{
				"access_token":    "stale",
				"refresh_token":   "refresh",
				"lifecycle_state": LifecycleStateActive,
			},
		}
		installed, errRegister := manager.Register(t.Context(), auth)
		if errRegister != nil {
			t.Fatal(errRegister)
		}
		auths = append(auths, installed)
	}

	for _, auth := range auths {
		_, attempted, errRequest := manager.tryRefreshAfterUnauthorized(
			t.Context(),
			executor,
			auth,
			&Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"},
			false,
		)
		triggerChatGPTWebUnauthorizedRequestRefresh(errRequest)
		if !attempted || statusCodeFromError(errRequest) != http.StatusUnauthorized {
			t.Fatalf("401 trigger for %q = attempted %t, error %v", auth.ID, attempted, errRequest)
		}
	}
	waitForResultPersistenceCondition(t, time.Second, "distinct 401 credentials were not isolated", func() bool {
		snapshot := manager.ChatGPTWebRequestRefreshSnapshot()
		return snapshot.Received == credentialCount && snapshot.SchedulerBlocked == credentialCount
	})
	if got := executor.refreshCalls.Load(); got > credentialCount {
		t.Fatalf("refresh calls before release = %d, want at most %d", got, credentialCount)
	}

	releaseOnce.Do(func() { close(release) })
	waitForResultPersistenceCondition(t, 5*time.Second, "distinct 401 refreshes did not finish", func() bool {
		snapshot := manager.ChatGPTWebRequestRefreshSnapshot()
		return snapshot.SchedulerBlocked == 0 && snapshot.Succeeded == credentialCount
	})
	if got := executor.refreshCalls.Load(); got != credentialCount {
		t.Fatalf("refresh calls = %d, want %d", got, credentialCount)
	}
	for _, auth := range auths {
		current, ok := manager.GetByID(auth.ID)
		if !ok || authAccessToken(current) != "fresh-"+auth.ID || !current.LifecycleSelectable() {
			t.Fatalf("recovered credential %q = %#v", auth.ID, current)
		}
	}
}

func TestChatGPTWebUnauthorizedSameTokenNeverReturnsToRouting(t *testing.T) {
	manager, executor, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	executor.refreshHook = func(_ *Auth, updated *Auth) {
		updated.Metadata["access_token"] = "stale"
	}
	executor.validateHook = func(failedAccessToken string, _ *Auth, refreshed *Auth) (*Auth, error) {
		if got := authAccessToken(refreshed); got != failedAccessToken {
			return nil, fmt.Errorf("refreshed token = %q, want failed token %q", got, failedAccessToken)
		}
		updated := refreshed.Clone()
		updated.Metadata["lifecycle_state"] = LifecycleStateReloginPending
		updated.Metadata["lifecycle_reason"] = "session_expired"
		return updated, chatGPTWebRequestRefreshError{
			persist: true,
			outcome: ChatGPTWebRequestRefreshOutcomeSameToken,
		}
	}

	_, errRequest := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if statusCodeFromError(errRequest) != http.StatusUnauthorized || errRequest.Error() != "invalid access token" {
		t.Fatalf("request error = %v, want original upstream 401", errRequest)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok := manager.GetByID(primary.ID)
		if ok && current != nil && current.LifecycleState() == LifecycleStateReloginPending &&
			chatGPTWebRequestRefreshBlockCount(manager, primary.ID) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("same-token refresh did not remain isolated: %#v", current)
		}
		time.Sleep(time.Millisecond)
	}
	response, errSecond := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errSecond != nil || string(response.Payload) != backup.ID+":backup" {
		t.Fatalf("second request = %q, %v; want backup credential", response.Payload, errSecond)
	}
	if executor.refreshCalls != 1 || executor.validateCalls.Load() != 1 {
		t.Fatalf("refresh/validate calls = %d/%d, want 1/1", executor.refreshCalls, executor.validateCalls.Load())
	}
	snapshot := manager.ChatGPTWebRequestRefreshSnapshot()
	if snapshot.Received != 1 || snapshot.SameToken != 1 || snapshot.Succeeded != 0 || snapshot.Failed != 1 || snapshot.SchedulerBlocked != 0 {
		t.Fatalf("request refresh snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebUnauthorizedRejectsReplacementWithoutAuthenticatedValidator(t *testing.T) {
	base := &chatGPTWebUnauthorizedRefreshExecutor{}
	executor := &chatGPTWebRefreshWithoutValidation{ProviderExecutor: base}
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(executor)
	t.Cleanup(func() {
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("CloseExecutors() error: %v", errClose)
		}
	})
	installed, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
		ID:       "chatgpt-web-unvalidated-refresh",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "stale",
			"refresh_token":   "refresh",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	refreshed, errRefresh := manager.refreshProviderForRequest(
		t.Context(),
		installed.ID,
		"stale",
		"chatgpt-web",
		installed,
	)
	if errRefresh == nil || refreshed != nil || !strings.Contains(errRefresh.Error(), "cannot validate") {
		t.Fatalf("refresh without validator = (%#v, %v)", refreshed, errRefresh)
	}
	current, ok := manager.GetByID(installed.ID)
	if !ok || current == nil || authAccessToken(current) != "stale" {
		t.Fatalf("unverified replacement was installed: %#v", current)
	}
}

func TestChatGPTWebUnauthorizedTransientProbePersistsRotatedCredentialAsUnavailable(t *testing.T) {
	manager, executor, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	executor.validateHook = func(_ string, _ *Auth, refreshed *Auth) (*Auth, error) {
		updated := refreshed.Clone()
		updated.Status = StatusError
		updated.Unavailable = true
		updated.CooldownScope = cooldownScopeAuth
		updated.NextRetryAfter = time.Now().Add(time.Minute)
		updated.NextRefreshAfter = time.Now().Add(time.Minute)
		updated.LastError = &Error{
			Code:       "unauthorized_refresh_probe_transient",
			HTTPStatus: http.StatusServiceUnavailable,
			Retryable:  true,
		}
		return updated, chatGPTWebRequestRefreshError{
			persist: true,
			outcome: ChatGPTWebRequestRefreshOutcomeProbeTransient,
		}
	}

	_, errRequest := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if statusCodeFromError(errRequest) != http.StatusUnauthorized || errRequest.Error() != "invalid access token" {
		t.Fatalf("request error = %v, want original upstream 401", errRequest)
	}
	waitForResultPersistenceCondition(t, 5*time.Second, "transient probe result was not persisted", func() bool {
		current, ok := manager.GetByID(primary.ID)
		return ok && current != nil && authAccessToken(current) == "fresh" && current.Unavailable &&
			current.CooldownScope == cooldownScopeAuth && current.LastError != nil &&
			current.LastError.Code == "unauthorized_refresh_probe_transient" && current.NextRetryAfter.After(time.Now()) &&
			current.NextRefreshAfter.After(time.Now()) &&
			chatGPTWebRequestRefreshBlockCount(manager, primary.ID) == 0
	})
	response, errSecond := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errSecond != nil || string(response.Payload) != backup.ID+":backup" {
		t.Fatalf("second request = %q, %v; want backup credential", response.Payload, errSecond)
	}
	snapshot := manager.ChatGPTWebRequestRefreshSnapshot()
	if snapshot.ProbeTransient != 1 || snapshot.Failed != 1 || snapshot.Succeeded != 0 {
		t.Fatalf("request refresh snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebUnauthorizedProbeDeadIsCountedAndNeverReturnsToRouting(t *testing.T) {
	manager, executor, primary, _, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	executor.validateHook = func(_ string, _ *Auth, refreshed *Auth) (*Auth, error) {
		updated := refreshed.Clone()
		updated.Metadata["lifecycle_state"] = LifecycleStateDead
		updated.Metadata["lifecycle_reason"] = "account_deactivated"
		return updated, chatGPTWebRequestRefreshError{
			persist: true,
			outcome: ChatGPTWebRequestRefreshOutcomeProbeUnauthorized,
		}
	}

	_, errRequest := manager.Execute(
		t.Context(),
		[]string{"chatgpt-web"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	if statusCodeFromError(errRequest) != http.StatusUnauthorized || errRequest.Error() != "invalid access token" {
		t.Fatalf("request error = %v, want original upstream 401", errRequest)
	}
	waitForResultPersistenceCondition(t, 5*time.Second, "dead probe result was not installed", func() bool {
		current, ok := manager.GetByID(primary.ID)
		snapshot := manager.ChatGPTWebRequestRefreshSnapshot()
		return ok && current != nil && current.LifecycleState() == LifecycleStateDead &&
			!current.LifecycleSelectable() && snapshot.DeadConfirmed == 1 && snapshot.SchedulerBlocked == 0
	})
	if executor.refreshCalls != 1 || executor.validateCalls.Load() != 1 {
		t.Fatalf("refresh/validate calls = %d/%d, want 1/1", executor.refreshCalls, executor.validateCalls.Load())
	}
}

func TestChatGPTWebUnauthorizedPreparationReturnsWithoutWaitingForRefresh(t *testing.T) {
	manager, executor, primary, _, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	executor.prepareErr = &Error{HTTPStatus: http.StatusUnauthorized, Message: "expired access token"}
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	refreshFinished := make(chan struct{})
	var startOnce sync.Once
	executor.beforeRefresh = func() {
		startOnce.Do(func() { close(refreshStarted) })
		<-releaseRefresh
	}
	executor.afterRefresh = func() { close(refreshFinished) }
	requestDone := make(chan error, 1)
	go func() {
		_, err := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		requestDone <- err
	}()

	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		close(releaseRefresh)
		t.Fatal("background refresh did not start after preparation 401")
	}
	select {
	case errRequest := <-requestDone:
		if statusCodeFromError(errRequest) != http.StatusUnauthorized {
			close(releaseRefresh)
			t.Fatalf("request error = %v, want original preparation 401", errRequest)
		}
		if errRequest.Error() != "expired access token" {
			close(releaseRefresh)
			t.Fatalf("request error text = %q, want original preparation message", errRequest.Error())
		}
	case <-time.After(time.Second):
		close(releaseRefresh)
		t.Fatal("preparation 401 waited for the background refresh")
	}
	if executor.prepareCalls != 1 || len(executor.executeCalls) != 0 {
		close(releaseRefresh)
		t.Fatalf("prepare calls = %d, execute calls = %v", executor.prepareCalls, executor.executeCalls)
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil || current.Unavailable || current.CooldownScope != "" {
		close(releaseRefresh)
		t.Fatalf("preparation 401 synchronously mutated cooldown before refresh result: %#v", current)
	}
	if blocks := chatGPTWebRequestRefreshBlockCount(manager, primary.ID); blocks != 1 {
		close(releaseRefresh)
		t.Fatalf("preparation request refresh blocks = %d, want exactly one", blocks)
	}
	close(releaseRefresh)
	select {
	case <-refreshFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("preparation background refresh did not finish")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok = manager.GetByID(primary.ID)
		if ok && authAccessToken(current) == "fresh" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("preparation background refresh was not installed: %#v", current)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestChatGPTWebUnauthorizedRefreshUsesBackgroundResultInstalledBeforeLock(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	updated := primary.Clone()
	updated.Metadata["access_token"] = "background-fresh"
	saved, err := manager.applyRefreshedAuth(t.Context(), primary, primary.Clone(), updated, time.Time{})
	if err != nil {
		t.Fatalf("applyRefreshedAuth() error: %v", err)
	}
	if saved == nil || !chatGPTWebRefreshLineageMatches(primary, saved) {
		t.Fatalf("background refresh lineage = %#v", saved)
	}
	if _, exists := manager.requestRefreshLocks.Load(primary.ID); exists {
		t.Fatal("request refresh lock existed before the request-time refresh")
	}

	refreshed, err := manager.refreshProviderForRequest(
		t.Context(),
		primary.ID,
		authAccessToken(primary),
		"chatgpt-web",
		primary,
	)
	if err != nil {
		t.Fatalf("refreshProviderForRequest() error: %v", err)
	}
	if refreshed == nil || authAccessToken(refreshed) != "background-fresh" {
		t.Fatalf("refreshed auth = %#v", refreshed)
	}
	if executor.refreshCalls != 0 {
		t.Fatalf("request-time refresh calls = %d, want 0", executor.refreshCalls)
	}
	if executor.validateCalls.Load() != 1 {
		t.Fatalf("authenticated validation calls = %d, want 1", executor.validateCalls.Load())
	}
}

func TestChatGPTWebUnauthorizedRefreshUsesTransitiveBackgroundResult(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	secondInput := primary.Clone()
	secondInput.Metadata["access_token"] = "background-second"
	secondInput.Metadata["refresh_token"] = "refresh-second"
	second, err := manager.applyRefreshedAuth(t.Context(), primary, primary.Clone(), secondInput, time.Time{})
	if err != nil {
		t.Fatalf("first applyRefreshedAuth() error: %v", err)
	}
	thirdInput := second.Clone()
	thirdInput.Metadata["access_token"] = "background-third"
	thirdInput.Metadata["refresh_token"] = "refresh-third"
	third, err := manager.applyRefreshedAuth(t.Context(), second, second.Clone(), thirdInput, time.Time{})
	if err != nil {
		t.Fatalf("second applyRefreshedAuth() error: %v", err)
	}
	if !chatGPTWebRefreshLineageMatches(primary, third) {
		t.Fatalf("transitive refresh lineage = primary:%q third:%q", primary.requestRefreshFamilyID, third.requestRefreshFamilyID)
	}
	if _, exists := manager.requestRefreshLocks.Load(primary.ID); exists {
		t.Fatal("request refresh lock existed before the request-time refresh")
	}

	refreshed, err := manager.refreshProviderForRequest(
		t.Context(),
		primary.ID,
		authAccessToken(primary),
		"chatgpt-web",
		primary,
	)
	if err != nil {
		t.Fatalf("refreshProviderForRequest() error: %v", err)
	}
	if refreshed == nil || authAccessToken(refreshed) != "background-third" {
		t.Fatalf("refreshed auth = %#v", refreshed)
	}
	if executor.refreshCalls != 0 {
		t.Fatalf("request-time refresh calls = %d, want 0", executor.refreshCalls)
	}
	if executor.validateCalls.Load() != 1 {
		t.Fatalf("authenticated validation calls = %d, want 1", executor.validateCalls.Load())
	}
}

func TestChatGPTWebUnauthorizedRefreshValidatesConcurrentReplacementBeforeRouting(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	replacement := primary.Clone()
	replacement.Metadata["access_token"] = "background-invalid"
	saved, errApply := manager.applyRefreshedAuth(t.Context(), primary, primary.Clone(), replacement, time.Time{})
	if errApply != nil || saved == nil {
		t.Fatalf("applyRefreshedAuth() = (%#v, %v)", saved, errApply)
	}
	executor.validateHook = func(failedAccessToken string, previous, refreshed *Auth) (*Auth, error) {
		if failedAccessToken != "stale" || authAccessToken(previous) != "stale" || authAccessToken(refreshed) != "background-invalid" {
			t.Fatalf("validation inputs = failed:%q previous:%q refreshed:%q", failedAccessToken, authAccessToken(previous), authAccessToken(refreshed))
		}
		updated := refreshed.Clone()
		updated.Metadata["lifecycle_state"] = LifecycleStateReloginPending
		updated.Metadata["lifecycle_reason"] = "session_expired"
		return updated, chatGPTWebRequestRefreshError{
			persist: true,
			outcome: ChatGPTWebRequestRefreshOutcomeProbeUnauthorized,
		}
	}

	refreshed, errRefresh := manager.refreshProviderForRequest(
		t.Context(),
		primary.ID,
		authAccessToken(primary),
		"chatgpt-web",
		primary,
	)
	if errRefresh == nil || refreshed != nil {
		t.Fatalf("refreshProviderForRequest() = (%#v, %v), want validation failure", refreshed, errRefresh)
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil || current.LifecycleState() != LifecycleStateReloginPending || authAccessToken(current) != "background-invalid" {
		t.Fatalf("invalid concurrent replacement returned to routing: %#v", current)
	}
	if executor.refreshCalls != 0 || executor.validateCalls.Load() != 1 {
		t.Fatalf("refresh/validate calls = %d/%d, want 0/1", executor.refreshCalls, executor.validateCalls.Load())
	}
}

func TestChatGPTWebUnauthorizedRefreshRejectsConcurrentSameTokenReinstall(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	reinstallDone := make(chan error, 1)
	executor.refreshHook = func(_ *Auth, updated *Auth) {
		replacement := updated.Clone()
		replacement.Label = "externally-reinstalled"
		go func() {
			_, err := manager.Update(
				WithForceRuntimeReplacement(WithSkipPersist(t.Context())),
				replacement,
			)
			reinstallDone <- err
		}()
		deadline := time.Now().Add(5 * time.Second)
		for {
			current, ok := manager.GetByID(primary.ID)
			if ok && current.installationID != primary.installationID && authAccessToken(current) == "fresh" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("same-token replacement was not installed")
			}
			time.Sleep(time.Millisecond)
		}
	}

	_, err := manager.refreshProviderForRequest(
		t.Context(),
		primary.ID,
		authAccessToken(primary),
		"chatgpt-web",
		primary,
	)
	var authErr *Error
	if !errors.As(err, &authErr) || authErr.Code != "auth_instance_retired" {
		t.Fatalf("refresh error = %#v, want auth_instance_retired", err)
	}
	select {
	case err = <-reinstallDone:
		if err != nil {
			t.Fatalf("concurrent Update() error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent replacement did not finish")
	}
}

func TestChatGPTWebUnauthorizedRefreshWaitersReuseSingleflightResult(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var startOnce sync.Once
	executor.refreshHook = func(_ *Auth, updated *Auth) {
		updated.Metadata["refresh_token"] = "rotated-refresh"
		startOnce.Do(func() { close(refreshStarted) })
		<-releaseRefresh
	}

	type refreshResult struct {
		caller int
		auth   *Auth
		err    error
	}
	results := make(chan refreshResult, 2)
	refresh := func(caller int) {
		auth, err := manager.refreshProviderForRequest(
			t.Context(),
			primary.ID,
			authAccessToken(primary),
			"chatgpt-web",
			primary,
		)
		results <- refreshResult{caller: caller, auth: auth, err: err}
	}
	go refresh(1)
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first request-time refresh did not start")
	}
	go refresh(2)
	close(releaseRefresh)

	for range 2 {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("request-time refresh caller %d error: %v", result.caller, result.err)
			}
			if result.auth == nil || authAccessToken(result.auth) != "fresh" {
				t.Fatalf("request-time refresh caller %d auth = %#v", result.caller, result.auth)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("request-time refresh waiter did not finish")
		}
	}
	if executor.refreshCalls != 1 {
		t.Fatalf("refresh calls = %d, want 1", executor.refreshCalls)
	}
	if _, exists := manager.requestRefreshLocks.Load(primary.ID); exists {
		t.Fatal("inactive request refresh lock was retained")
	}
}

func TestChatGPTWebUnauthorizedRefreshSurvivesLeaderCancellationForWaiter(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var startOnce sync.Once
	executor.refreshContextHook = func(ctx context.Context) error {
		startOnce.Do(func() { close(refreshStarted) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-releaseRefresh:
			return ctx.Err()
		}
	}
	executor.refreshHook = func(_ *Auth, updated *Auth) {
		updated.Metadata["refresh_token"] = "rotated-refresh"
	}

	leaderCtx, cancelLeader := context.WithCancel(t.Context())
	defer cancelLeader()
	type refreshResult struct {
		auth *Auth
		err  error
	}
	leaderResult := make(chan refreshResult, 1)
	go func() {
		refreshed, err := manager.refreshProviderForRequest(
			leaderCtx,
			primary.ID,
			authAccessToken(primary),
			"chatgpt-web",
			primary,
		)
		leaderResult <- refreshResult{auth: refreshed, err: err}
	}()
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("leader request-time refresh did not start")
	}

	waiterResult := make(chan refreshResult, 1)
	waiterCtx := &chatGPTWebDoneObservedContext{Context: t.Context(), observed: make(chan struct{})}
	go func() {
		refreshed, err := manager.refreshProviderForRequest(
			waiterCtx,
			primary.ID,
			authAccessToken(primary),
			"chatgpt-web",
			primary,
		)
		waiterResult <- refreshResult{auth: refreshed, err: err}
	}()
	select {
	case <-waiterCtx.observed:
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not join the shared refresh")
	}
	lockValue, ok := manager.requestRefreshLocks.Load(primary.ID)
	lock, _ := lockValue.(*authRequestRefreshLock)
	if !ok || lock == nil {
		t.Fatal("shared refresh lock is missing")
	}
	manager.requestRefreshLocksMu.Lock()
	active := lock.active
	manager.requestRefreshLocksMu.Unlock()
	if active != 1 {
		t.Fatalf("active refresh workers = %d, want one shared worker", active)
	}

	cancelLeader()
	select {
	case result := <-leaderResult:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("leader refresh error = %v, want context canceled", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled leader refresh did not return")
	}
	close(releaseRefresh)
	select {
	case result := <-waiterResult:
		if result.err != nil {
			t.Fatalf("waiter refresh error: %v", result.err)
		}
		if result.auth == nil || authAccessToken(result.auth) != "fresh" {
			t.Fatalf("waiter refresh auth = %#v", result.auth)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter refresh did not finish")
	}
	if executor.refreshCalls != 1 {
		t.Fatalf("refresh calls = %d, want one shared attempt", executor.refreshCalls)
	}
}

func TestChatGPTWebUnauthorizedRefreshInstallsResultAfterOnlyCallerCancels(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	executor.refreshContextHook = func(ctx context.Context) error {
		close(refreshStarted)
		<-releaseRefresh
		return ctx.Err()
	}
	executor.refreshHook = func(_ *Auth, updated *Auth) {
		updated.Metadata["refresh_token"] = "rotated-refresh"
	}

	requestCtx, cancelRequest := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := manager.refreshProviderForRequest(
			requestCtx,
			primary.ID,
			authAccessToken(primary),
			"chatgpt-web",
			primary,
		)
		result <- err
	}()
	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		t.Fatal("request-time refresh did not start")
	}
	cancelRequest()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled request refresh error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled request did not return")
	}

	close(releaseRefresh)
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok := manager.GetByID(primary.ID)
		if ok && authAccessToken(current) == "fresh" && requestPrepareString(current.Metadata["refresh_token"]) == "rotated-refresh" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("detached refresh result was not installed: %#v", current)
		}
		time.Sleep(time.Millisecond)
	}
	if executor.refreshCalls != 1 {
		t.Fatalf("refresh calls = %d, want one detached attempt", executor.refreshCalls)
	}
}

func TestChatGPTWebRequestRefreshResultsRetainMultipleSourceGenerations(t *testing.T) {
	lock := &authRequestRefreshLock{semaphore: make(chan struct{}, 1)}
	sourceOne := &Auth{installationID: "source-installation-one", instanceID: "source-runtime-one", requestRefreshFamilyID: "refresh-family"}
	sourceTwo := &Auth{installationID: "source-installation-two", instanceID: "source-runtime-two", requestRefreshFamilyID: "refresh-family"}
	resultOne := &Auth{installationID: "result-installation-one", instanceID: "result-runtime-one", requestRefreshFamilyID: "refresh-family"}
	resultTwo := &Auth{installationID: "result-installation-two", instanceID: "result-runtime-two", requestRefreshFamilyID: "refresh-family"}

	lock.remember("chatgpt-web", sourceOne, resultOne)
	lock.remember("chatgpt-web", sourceTwo, resultTwo)

	if !chatGPTWebRequestRefreshResultReusable(lock, "chatgpt-web", sourceOne, resultOne) {
		t.Fatal("second refresh result overwrote the first source generation")
	}
	if !chatGPTWebRequestRefreshResultReusable(lock, "chatgpt-web", sourceTwo, resultTwo) {
		t.Fatal("latest source generation result was not retained")
	}
	if chatGPTWebRequestRefreshResultReusable(lock, "chatgpt-web", sourceOne, resultTwo) {
		t.Fatal("source generation reused another refresh result")
	}
}

func TestChatGPTWebUnauthorizedRefreshWaitersShareTransientFailure(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	executor.refreshErr = chatGPTWebRequestRefreshError{}
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var startOnce sync.Once
	executor.beforeRefresh = func() {
		startOnce.Do(func() { close(refreshStarted) })
		<-releaseRefresh
	}

	results := make(chan error, 2)
	refresh := func(ctx context.Context) {
		_, err := manager.refreshProviderForRequest(
			ctx,
			primary.ID,
			authAccessToken(primary),
			"chatgpt-web",
			primary,
		)
		results <- err
	}
	go refresh(t.Context())
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first request-time refresh did not start")
	}
	waiterCtx := &chatGPTWebDoneObservedContext{Context: t.Context(), observed: make(chan struct{})}
	go refresh(waiterCtx)
	select {
	case <-waiterCtx.observed:
	case <-time.After(5 * time.Second):
		t.Fatal("second request-time refresh did not join the shared attempt")
	}
	close(releaseRefresh)

	for range 2 {
		select {
		case err := <-results:
			if err == nil {
				t.Fatal("request-time refresh returned no transient error")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("request-time refresh waiter did not finish")
		}
	}
	if executor.refreshCalls != 1 {
		t.Fatalf("refresh calls = %d, want one shared attempt", executor.refreshCalls)
	}
	if _, exists := manager.requestRefreshLocks.Load(primary.ID); exists {
		t.Fatal("inactive failed request refresh lock was retained")
	}

	executor.beforeRefresh = nil
	_, err := manager.refreshProviderForRequest(
		t.Context(),
		primary.ID,
		authAccessToken(primary),
		"chatgpt-web",
		primary,
	)
	if err == nil {
		t.Fatal("later request-time refresh returned no transient error")
	}
	if executor.refreshCalls != 2 {
		t.Fatalf("refresh calls = %d, want a new attempt after prior waiters finished", executor.refreshCalls)
	}
}

func TestChatGPTWebUnauthorizedRefreshWaitersReusePersistedTerminalState(t *testing.T) {
	manager, executor, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	primary, _ = manager.GetByID(primary.ID)
	executor.persistError = true
	failedToken := authAccessToken(primary)

	_, firstErr := manager.refreshProviderForRequest(
		t.Context(),
		primary.ID,
		failedToken,
		"chatgpt-web",
		primary,
	)
	if firstErr == nil {
		t.Fatal("first terminal refresh returned no error")
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current.LifecycleState() != LifecycleStateReloginPending {
		t.Fatalf("persisted lifecycle = %#v", current)
	}

	_, secondErr := manager.refreshProviderForRequest(
		t.Context(),
		primary.ID,
		failedToken,
		"chatgpt-web",
		primary,
	)
	if secondErr == nil {
		t.Fatal("terminal refresh waiter returned no error")
	}
	var persisted interface{ PersistAuthUpdateOnError() bool }
	if !errors.As(secondErr, &persisted) || !persisted.PersistAuthUpdateOnError() {
		t.Fatalf("terminal waiter error = %#v, want persisted unavailable state", secondErr)
	}
	if executor.refreshCalls != 1 {
		t.Fatalf("refresh calls = %d, want 1", executor.refreshCalls)
	}
}

func TestChatGPTWebTerminalRefreshPersistenceSurvivesRequestCancellation(t *testing.T) {
	store := &chatGPTWebRefreshPersistenceStore{}
	manager := NewManager(store, &FillFirstSelector{}, nil)
	executor := &chatGPTWebUnauthorizedRefreshExecutor{persistError: true}
	manager.RegisterExecutor(executor)
	registered, err := manager.Register(WithSkipPersist(t.Context()), &Auth{
		ID:       "chatgpt-web-terminal-cancel",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "stale",
			"refresh_token":   "refresh",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}
	requestCtx, cancel := context.WithCancel(t.Context())
	releaseRefresh := make(chan struct{})
	executor.beforeRefresh = func() {
		cancel()
		<-releaseRefresh
	}

	_, err = manager.refreshProviderForRequest(
		requestCtx,
		registered.ID,
		authAccessToken(registered),
		"chatgpt-web",
		registered,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("terminal refresh error = %v, want context canceled", err)
	}
	close(releaseRefresh)
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok := manager.GetByID(registered.ID)
		persisted := store.snapshot()
		if ok && current.LifecycleState() == LifecycleStateReloginPending &&
			persisted != nil && persisted.LifecycleState() == LifecycleStateReloginPending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("terminal refresh was not persisted: runtime=%#v persisted=%#v", current, persisted)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestChatGPTWebSuccessfulRefreshPersistenceSurvivesRequestCancellation(t *testing.T) {
	store := &chatGPTWebRefreshPersistenceStore{}
	manager := NewManager(store, &FillFirstSelector{}, nil)
	executor := &chatGPTWebUnauthorizedRefreshExecutor{}
	manager.RegisterExecutor(executor)
	registered, err := manager.Register(WithSkipPersist(t.Context()), &Auth{
		ID:       "chatgpt-web-success-cancel",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "stale",
			"refresh_token":   "refresh",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}
	requestCtx, cancel := context.WithCancel(t.Context())
	releaseRefresh := make(chan struct{})
	executor.beforeRefresh = func() {
		cancel()
		<-releaseRefresh
	}

	_, err = manager.refreshProviderForRequest(
		requestCtx,
		registered.ID,
		authAccessToken(registered),
		"chatgpt-web",
		registered,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("refreshProviderForRequest() error = %v, want context canceled", err)
	}
	close(releaseRefresh)
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok := manager.GetByID(registered.ID)
		persisted := store.snapshot()
		if ok && authAccessToken(current) == "fresh" &&
			persisted != nil && authAccessToken(persisted) == "fresh" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("successful refresh was not persisted: runtime=%#v persisted=%#v", current, persisted)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCloseExecutorsWaitsForDetachedChatGPTWebRefreshPersistence(t *testing.T) {
	saveStarted := make(chan struct{})
	releaseSave := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseSave)
		}
	}()
	store := &chatGPTWebRefreshPersistenceStore{
		saveStarted: saveStarted,
		releaseSave: releaseSave,
	}
	manager := NewManager(store, &FillFirstSelector{}, nil)
	executor := &chatGPTWebUnauthorizedRefreshExecutor{}
	manager.RegisterExecutor(executor)
	registered, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
		ID:       "chatgpt-web-close-waits-for-refresh",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "stale",
			"refresh_token":   "refresh",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatalf("Register() error: %v", errRegister)
	}

	type refreshResult struct {
		auth *Auth
		err  error
	}
	refreshDone := make(chan refreshResult, 1)
	go func() {
		refreshed, errRefresh := manager.refreshProviderForRequest(
			t.Context(),
			registered.ID,
			authAccessToken(registered),
			"chatgpt-web",
			registered,
		)
		refreshDone <- refreshResult{auth: refreshed, err: errRefresh}
	}()
	select {
	case <-saveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("request refresh did not reach persistence")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- manager.CloseExecutors()
	}()
	select {
	case errClose := <-closeDone:
		t.Fatalf("CloseExecutors() returned before refresh persistence completed: %v", errClose)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseSave)
	released = true
	select {
	case result := <-refreshDone:
		if result.err != nil {
			t.Fatalf("refreshProviderForRequest() error: %v", result.err)
		}
		if result.auth == nil || authAccessToken(result.auth) != "fresh" {
			t.Fatalf("refreshed auth = %#v", result.auth)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request refresh did not finish after persistence was released")
	}
	select {
	case errClose := <-closeDone:
		if errClose != nil {
			t.Fatalf("CloseExecutors() error: %v", errClose)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CloseExecutors() did not wait for and finish the request refresh")
	}
	persisted := store.snapshot()
	if persisted == nil || authAccessToken(persisted) != "fresh" {
		t.Fatalf("persisted auth = %#v", persisted)
	}
	if _, errRefresh := manager.refreshProviderForRequest(
		t.Context(),
		registered.ID,
		authAccessToken(registered),
		"chatgpt-web",
		registered,
	); errRefresh == nil || !strings.Contains(errRefresh.Error(), "executors are closed") {
		t.Fatalf("post-close refresh error = %v, want closed executor error", errRefresh)
	}
}

func TestChatGPTWebUnauthorizedRefreshRejectsReplacedRuntime(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	executor := &chatGPTWebUnauthorizedRefreshExecutor{}
	manager.RegisterExecutor(executor)

	original := chatGPTWebIdentityTestAuth("chatgpt-web-replaced-refresh", "account-a", "user-a")
	original.Metadata["refresh_token"] = "refresh-a"
	registered, err := manager.Register(WithSkipPersist(t.Context()), original)
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}
	failedToken := authAccessToken(registered)

	replacement := chatGPTWebIdentityTestAuth(registered.ID, "account-b", "user-b")
	replacement.Metadata["refresh_token"] = "refresh-b"
	installed, err := manager.Update(WithSkipPersist(t.Context()), replacement)
	if err != nil {
		t.Fatalf("Update() error: %v", err)
	}
	if installed.RuntimeInstanceID() == registered.RuntimeInstanceID() {
		t.Fatal("different account reused the old runtime instance")
	}

	_, err = manager.refreshProviderForRequest(t.Context(), registered.ID, failedToken, "chatgpt-web", registered)
	var authErr *Error
	if !errors.As(err, &authErr) || authErr.Code != "auth_instance_retired" {
		t.Fatalf("refresh error = %#v, want auth_instance_retired", err)
	}
	if executor.refreshCalls != 0 {
		t.Fatalf("replacement credential was refreshed %d times", executor.refreshCalls)
	}
}

func TestChatGPTWebUnauthorizedRefreshRejectsStaleSameAccountInstallation(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	executor := &chatGPTWebUnauthorizedRefreshExecutor{}
	manager.RegisterExecutor(executor)

	original := chatGPTWebIdentityTestAuth("chatgpt-web-stale-install-refresh", "account-a", "user-a")
	original.Metadata["refresh_token"] = "refresh-a"
	registered, err := manager.Register(WithSkipPersist(t.Context()), original)
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}
	failedToken := authAccessToken(registered)

	replacement := registered.Clone()
	replacement.Label = "same-account-reinstalled"
	replacement.Metadata["access_token"] = "externally-fresh"
	installed, err := manager.Update(WithSkipPersist(t.Context()), replacement)
	if err != nil {
		t.Fatalf("Update() error: %v", err)
	}
	if installed.installationID == registered.installationID {
		t.Fatal("same-account update reused the old installation")
	}

	_, err = manager.refreshProviderForRequest(t.Context(), registered.ID, failedToken, "chatgpt-web", registered)
	var authErr *Error
	if !errors.As(err, &authErr) || authErr.Code != "auth_instance_retired" {
		t.Fatalf("refresh error = %#v, want auth_instance_retired", err)
	}
	if executor.refreshCalls != 0 {
		t.Fatalf("replacement credential was refreshed %d times", executor.refreshCalls)
	}
}

func TestChatGPTWebTransientRefreshFailureCoolsWholeCredential(t *testing.T) {
	manager, executor, primary, _, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	executor.refreshErr = chatGPTWebRequestRefreshError{}
	refreshFinished := make(chan struct{})
	executor.afterRefresh = func() { close(refreshFinished) }
	_, err := manager.Execute(context.Background(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if statusCodeFromError(err) != http.StatusUnauthorized {
		t.Fatalf("Execute() error = %v, want original 401", err)
	}
	select {
	case <-refreshFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("background refresh failure did not finish")
	}
	deadline := time.Now().Add(5 * time.Second)
	var current *Auth
	for {
		var ok bool
		current, ok = manager.GetByID(primary.ID)
		if !ok || current == nil {
			t.Fatal("primary auth missing")
		}
		if current.Unavailable && current.CooldownScope == cooldownScopeAuth && !current.NextRetryAfter.IsZero() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("primary cooldown = unavailable:%t scope:%q until:%s", current.Unavailable, current.CooldownScope, current.NextRetryAfter)
		}
		time.Sleep(time.Millisecond)
	}
	if len(current.ModelStates) != 0 {
		t.Fatalf("401 created model-only state: %#v", current.ModelStates)
	}
	if len(executor.executeCalls) != 1 || executor.executeCalls[0] != primary.ID {
		t.Fatalf("execute calls = %v, want primary once", executor.executeCalls)
	}
}

func TestChatGPTWebTerminalRefreshFailurePersistsLifecycleInBackground(t *testing.T) {
	manager, executor, primary, _, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	executor.persistError = true
	_, err := manager.Execute(context.Background(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if statusCodeFromError(err) != http.StatusUnauthorized {
		t.Fatalf("Execute() error = %v, want original 401", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var current *Auth
	var ok bool
	for {
		current, ok = manager.GetByID(primary.ID)
		if ok && current != nil && current.LifecycleState() == LifecycleStateReloginPending && current.Status == StatusPending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("primary lifecycle = %#v, want relogin_pending/pending", current)
		}
		time.Sleep(time.Millisecond)
	}
	if current.Unavailable || current.CooldownScope != "" {
		t.Fatalf("terminal refresh failure created cooldown: %#v", current)
	}
	if len(executor.executeCalls) != 1 || executor.executeCalls[0] != primary.ID {
		t.Fatalf("execute calls = %v, want primary once", executor.executeCalls)
	}
}
