package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type chatGPTWebUnauthorizedRefreshExecutor struct {
	executeCalls       []string
	refreshCalls       int
	refreshErr         error
	persistError       bool
	beforeRefresh      func()
	refreshContextHook func(context.Context) error
	refreshHook        func(*Auth, *Auth)
}

type chatGPTWebRequestRefreshError struct {
	persist bool
}

type chatGPTWebRefreshPersistenceStore struct {
	mu    sync.Mutex
	saved *Auth
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

func (*chatGPTWebUnauthorizedRefreshExecutor) Identifier() string { return "chatgpt-web" }

func (executor *chatGPTWebUnauthorizedRefreshExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	executor.executeCalls = append(executor.executeCalls, auth.ID)
	if authAccessToken(auth) == "stale" {
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusUnauthorized, Message: "invalid access token"}
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID + ":" + authAccessToken(auth))}, nil
}

func (executor *chatGPTWebUnauthorizedRefreshExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (executor *chatGPTWebUnauthorizedRefreshExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (executor *chatGPTWebUnauthorizedRefreshExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	executor.refreshCalls++
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
	return manager, executor, auths[0], auths[1], model
}

func TestChatGPTWebUnauthorizedRefreshesSameCredentialBeforeFallback(t *testing.T) {
	manager, executor, primary, _, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	response, err := manager.Execute(context.Background(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if got := string(response.Payload); got != primary.ID+":fresh" {
		t.Fatalf("payload = %q, want refreshed primary", got)
	}
	if executor.refreshCalls != 1 || len(executor.executeCalls) != 2 ||
		executor.executeCalls[0] != primary.ID || executor.executeCalls[1] != primary.ID {
		t.Fatalf("refresh calls = %d, execute calls = %v", executor.refreshCalls, executor.executeCalls)
	}
}

func TestChatGPTWebUnauthorizedRefreshUsesConcurrentBackgroundResult(t *testing.T) {
	manager, executor, primary, _, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	backgroundDone := make(chan error, 1)
	executor.refreshHook = func(_ *Auth, updated *Auth) {
		manager.mu.RLock()
		expected := manager.auths[primary.ID]
		baseline := expected.Clone()
		manager.mu.RUnlock()
		go func() {
			_, err := manager.applyRefreshedAuth(t.Context(), expected, baseline, updated.Clone(), time.Time{})
			backgroundDone <- err
		}()
		deadline := time.Now().Add(5 * time.Second)
		for {
			current, ok := manager.GetByID(primary.ID)
			if ok && authAccessToken(current) == "fresh" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("concurrent background refresh did not install the fresh credential")
			}
			time.Sleep(time.Millisecond)
		}
	}

	response, err := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if got := string(response.Payload); got != primary.ID+":fresh" {
		t.Fatalf("payload = %q, want concurrently refreshed primary", got)
	}
	select {
	case err = <-backgroundDone:
		if err != nil {
			t.Fatalf("background applyRefreshedAuth() error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("background refresh installation did not finish")
	}
	if len(executor.executeCalls) != 2 || executor.executeCalls[0] != primary.ID || executor.executeCalls[1] != primary.ID {
		t.Fatalf("execute calls = %v, want refreshed primary twice", executor.executeCalls)
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
	manager, executor, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	executor.refreshErr = chatGPTWebRequestRefreshError{}
	response, err := manager.Execute(context.Background(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if got := string(response.Payload); got != backup.ID+":backup" {
		t.Fatalf("payload = %q, want backup", got)
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil {
		t.Fatal("primary auth missing")
	}
	if !current.Unavailable || current.CooldownScope != cooldownScopeAuth || current.NextRetryAfter.IsZero() {
		t.Fatalf("primary cooldown = unavailable:%t scope:%q until:%s", current.Unavailable, current.CooldownScope, current.NextRetryAfter)
	}
	if len(current.ModelStates) != 0 {
		t.Fatalf("401 created model-only state: %#v", current.ModelStates)
	}
}

func TestChatGPTWebTerminalRefreshFailurePersistsLifecycleBeforeFallback(t *testing.T) {
	manager, executor, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
	executor.persistError = true
	response, err := manager.Execute(context.Background(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if got := string(response.Payload); got != backup.ID+":backup" {
		t.Fatalf("payload = %q, want backup", got)
	}
	current, ok := manager.GetByID(primary.ID)
	if !ok || current == nil {
		t.Fatal("primary auth missing")
	}
	if current.LifecycleState() != LifecycleStateReloginPending || current.Status != StatusPending {
		t.Fatalf("primary lifecycle = %q/%q, want relogin_pending/pending", current.LifecycleState(), current.Status)
	}
	if current.Unavailable || current.CooldownScope != "" {
		t.Fatalf("terminal refresh failure created cooldown: %#v", current)
	}
}
