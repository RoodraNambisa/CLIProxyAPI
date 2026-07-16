package auth

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestManagerProxyResolutionFailureFallsBackWithoutCoolingAuth(t *testing.T) {
	manager, executor, resolver, model := newProxyResolverExecutionManager(t)
	response, errExecute := manager.Execute(
		context.Background(),
		[]string{"codex"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if got := string(response.Payload); got != "low" {
		t.Fatalf("Execute() payload = %q, want low", got)
	}
	if got := executor.executedAuthIDs(); len(got) != 1 || got[0] != "low" {
		t.Fatalf("executor auths = %v, want [low]", got)
	}
	if got := resolver.resolvedAuthIDs(); len(got) != 2 || got[0] != "high" || got[1] != "low" {
		t.Fatalf("resolver auths = %v, want [high low]", got)
	}
	assertProxyResolutionDidNotCoolAuth(t, manager, "high")
}

func TestManagerStreamProxyResolutionFailureFallsBackWithoutCoolingAuth(t *testing.T) {
	manager, executor, _, model := newProxyResolverExecutionManager(t)
	stream, errExecute := manager.ExecuteStream(
		context.Background(),
		[]string{"codex"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{Stream: true},
	)
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	if stream == nil || stream.Chunks == nil {
		t.Fatal("ExecuteStream() returned nil stream")
	}
	var payload string
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
		payload += string(chunk.Payload)
	}
	if payload != "low" {
		t.Fatalf("stream payload = %q, want low", payload)
	}
	if got := executor.streamedAuthIDs(); len(got) != 1 || got[0] != "low" {
		t.Fatalf("stream executor auths = %v, want [low]", got)
	}
	assertProxyResolutionDidNotCoolAuth(t, manager, "high")
}

func TestManagerProxyRequestFailureFallsBackWithoutCoolingAuth(t *testing.T) {
	manager, executor, resolver, model := newProxyResolverExecutionManager(t)
	resolver.failAuthID = ""
	resolver.proxyFailureAuthID = "high"
	executor.failAuthID = "high"

	response, errExecute := manager.Execute(
		context.Background(),
		[]string{"codex"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if got := string(response.Payload); got != "low" {
		t.Fatalf("Execute() payload = %q, want low", got)
	}
	if got := executor.executedAuthIDs(); len(got) != 2 || got[0] != "high" || got[1] != "low" {
		t.Fatalf("executor auths = %v, want [high low]", got)
	}
	assertProxyResolutionDidNotCoolAuth(t, manager, "high")
}

func TestManagerAllProxyResolutionFailuresReturnProxyUnavailableWithoutCooling(t *testing.T) {
	manager, executor, resolver, model := newProxyResolverExecutionManager(t)
	manager.SetRetryConfig(3, 0, 0)
	resolver.failAuthID = "*"

	_, errExecute := manager.Execute(
		context.Background(),
		[]string{"codex"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	if errExecute == nil {
		t.Fatal("Execute() error = nil, want proxy_unavailable")
	}
	var statusErr interface{ StatusCode() int }
	if !errors.As(errExecute, &statusErr) || statusErr.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("Execute() error = %v, want status 503", errExecute)
	}
	if !strings.Contains(errExecute.Error(), "proxy_unavailable") {
		t.Fatalf("Execute() error = %v, want proxy_unavailable", errExecute)
	}
	if got := executor.executedAuthIDs(); len(got) != 0 {
		t.Fatalf("executor auths = %v, want none", got)
	}
	if got := resolver.resolvedAuthIDs(); len(got) != 2 {
		t.Fatalf("resolver auths = %v, want one pass over two credentials", got)
	}
	assertProxyResolutionDidNotCoolAuth(t, manager, "high")
	assertProxyResolutionDidNotCoolAuth(t, manager, "low")
}

func TestManagerRequestAuthPreparerReceivesRuntimeProxyWithoutPersistingIt(t *testing.T) {
	manager, executor, resolver, model := newProxyResolverExecutionManager(t)
	resolver.failAuthID = ""
	executor.prepare = true
	store := &requestPrepareStore{}
	manager.SetStore(store)

	response, errExecute := manager.Execute(
		context.Background(),
		[]string{"codex"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if got := string(response.Payload); got != "high" {
		t.Fatalf("Execute() payload = %q, want high", got)
	}
	if got := executor.preparedProxyBindings(); len(got) != 1 || got[0] != "binding-high" {
		t.Fatalf("prepared proxy bindings = %v, want [binding-high]", got)
	}
	current, ok := manager.GetByID("high")
	if !ok || current == nil {
		t.Fatal("prepared auth is unavailable")
	}
	if current.RuntimeProxyURL != "" || current.RuntimeProxyBindingID != "" {
		t.Fatalf("stored auth retained runtime proxy: %+v", current)
	}
	persisted := store.lastAuth()
	if persisted == nil {
		t.Fatal("request preparation was not persisted")
	}
	if persisted.RuntimeProxyURL != "" || persisted.RuntimeProxyBindingID != "" {
		t.Fatalf("persisted auth retained runtime proxy: %+v", persisted)
	}
}

func TestManagerUnauthorizedPreparationRefreshUsesLatestRuntimeProxy(t *testing.T) {
	manager := NewManager(&requestPrepareStore{}, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &proxyResolverExecutionExecutor{provider: "antigravity", prepare: true}
	executor.prepareUnauthorizedOnce.Store(true)
	resolver := &proxyResolverExecutionResolver{bindingIDs: []string{"initial", "refresh", "latest"}}
	manager.RegisterExecutor(executor)
	manager.SetProxyResolver(resolver)
	auth := &Auth{
		ID:       "antigravity-a",
		Provider: "antigravity",
		Metadata: map[string]any{"access_token": "stale", "refresh_token": "refresh"},
	}
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	model := "proxy-resolver-refresh-model"
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

	if _, errExecute := manager.Execute(t.Context(), []string{auth.Provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if got := executor.preparedProxyBindings(); len(got) != 2 || got[0] != "initial" || got[1] != "latest" {
		t.Fatalf("prepared proxy bindings = %v, want [initial latest]", got)
	}
	if got := executor.executedProxyBindings(); len(got) != 1 || got[0] != "latest" {
		t.Fatalf("executed proxy bindings = %v, want [latest]", got)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("refreshed auth is unavailable")
	}
	if current.RuntimeProxyURL != "" || current.RuntimeProxyBindingID != "" {
		t.Fatalf("manager auth retained runtime proxy: %+v", current)
	}
}

func TestCarryRuntimeProxyPreservesLatestExplicitNoProxyDecision(t *testing.T) {
	previous := &Auth{
		RuntimeProxyURL:       "http://old-proxy.example:8080",
		RuntimeProxyBindingID: "old-binding",
		runtimeProxyResolved:  true,
	}
	latest := &Auth{runtimeProxyResolved: true}
	carryRuntimeProxy(previous, latest)
	if latest.RuntimeProxyURL != "" || latest.RuntimeProxyBindingID != "" {
		t.Fatalf("latest no-proxy decision was overwritten: %+v", latest)
	}
}

func TestUpdateDoesNotInstallRequestScopedProxyState(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	registered, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: "auth-a", Provider: "codex"})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	update := registered.Clone()
	update.RuntimeProxyURL = "http://proxy.example:8080"
	update.RuntimeProxyBindingID = "binding-a"
	update.runtimeProxyResolved = true
	installed, errUpdate := manager.Update(WithSkipPersist(t.Context()), update)
	if errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if installed.RuntimeProxyURL != "" || installed.RuntimeProxyBindingID != "" || installed.runtimeProxyResolved {
		t.Fatalf("installed runtime proxy state = %#v", installed)
	}
	stored, ok := manager.GetByID("auth-a")
	if !ok || stored == nil {
		t.Fatal("updated auth not found")
	}
	if stored.RuntimeProxyURL != "" || stored.RuntimeProxyBindingID != "" || stored.runtimeProxyResolved {
		t.Fatalf("stored runtime proxy state = %#v", stored)
	}
	if update.RuntimeProxyURL == "" || update.RuntimeProxyBindingID == "" || !update.runtimeProxyResolved {
		t.Fatal("Update() mutated the caller's execution clone")
	}
}

func TestManagerProxyFailureReportingUsesActiveRuntimeContext(t *testing.T) {
	t.Run("http_request", func(t *testing.T) {
		manager, executor, resolver, _ := newProxyResolverExecutionManager(t)
		resolver.failAuthID = ""
		resolver.proxyFailureAuthID = "high"
		executor.httpFailAuthID = "high"
		auth, ok := manager.GetByID("high")
		if !ok || auth == nil {
			t.Fatal("high auth is unavailable")
		}
		request := httptest.NewRequest(http.MethodGet, "https://upstream.example", nil)
		_, errRequest := manager.HttpRequest(t.Context(), auth, request)
		assertProxyUnavailableError(t, errRequest)
		assertProxyReportContextsActive(t, resolver, 1)
	})

	t.Run("background_refresh", func(t *testing.T) {
		manager, executor, resolver, _ := newProxyResolverExecutionManager(t)
		resolver.failAuthID = ""
		resolver.proxyFailureAuthID = "high"
		executor.refreshFailAuthID = "high"
		manager.mu.RLock()
		expected := manager.auths["high"]
		manager.mu.RUnlock()
		manager.refreshAuthExpected(t.Context(), "high", expected, time.Time{})
		assertProxyReportContextsActive(t, resolver, 1)
		assertProxyResolutionDidNotCoolAuth(t, manager, "high")
	})

	t.Run("antigravity_request_refresh", func(t *testing.T) {
		manager := NewManager(nil, &FillFirstSelector{}, nil)
		executor := &proxyResolverExecutionExecutor{provider: "antigravity", refreshFailAuthID: "antigravity-a"}
		resolver := &proxyResolverExecutionResolver{proxyFailureAuthID: "antigravity-a"}
		manager.RegisterExecutor(executor)
		manager.SetProxyResolver(resolver)
		if _, errRegister := manager.Register(t.Context(), &Auth{
			ID:       "antigravity-a",
			Provider: "antigravity",
			Metadata: map[string]any{"access_token": "stale"},
		}); errRegister != nil {
			t.Fatalf("Register() error = %v", errRegister)
		}
		_, errRefresh := manager.refreshAntigravityForRequest(t.Context(), "antigravity-a", "stale")
		assertProxyUnavailableError(t, errRefresh)
		assertProxyReportContextsActive(t, resolver, 1)
		assertProxyResolutionDidNotCoolAuth(t, manager, "antigravity-a")
	})
}

func newProxyResolverExecutionManager(t *testing.T) (*Manager, *proxyResolverExecutionExecutor, *proxyResolverExecutionResolver, string) {
	t.Helper()
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &proxyResolverExecutionExecutor{}
	resolver := &proxyResolverExecutionResolver{failAuthID: "high"}
	manager.RegisterExecutor(executor)
	manager.SetProxyResolver(resolver)
	for _, auth := range []*Auth{
		{ID: "high", Provider: "codex", Attributes: map[string]string{"priority": "10"}, Metadata: map[string]any{}},
		{ID: "low", Provider: "codex", Attributes: map[string]string{"priority": "0"}, Metadata: map[string]any{}},
	} {
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, errRegister)
		}
	}
	model := "proxy-resolver-execution-model"
	registry.GetGlobalRegistry().RegisterClient("high", "codex", []*registry.ModelInfo{{ID: model}})
	registry.GetGlobalRegistry().RegisterClient("low", "codex", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient("high")
		registry.GetGlobalRegistry().UnregisterClient("low")
	})
	return manager, executor, resolver, model
}

func assertProxyResolutionDidNotCoolAuth(t *testing.T, manager *Manager, authID string) {
	t.Helper()
	auth, ok := manager.GetByID(authID)
	if !ok || auth == nil {
		t.Fatalf("GetByID(%q) did not return auth", authID)
	}
	if auth.Unavailable || auth.LastError != nil || !auth.NextRetryAfter.IsZero() {
		t.Fatalf("auth after proxy resolution failure = %+v", auth)
	}
}

type proxyResolverExecutionResolver struct {
	mu                 sync.Mutex
	failAuthID         string
	proxyFailureAuthID string
	resolved           []string
	reportContextErrs  []error
	bindingIDs         []string
	resolveCalls       int
}

func (r *proxyResolverExecutionResolver) Resolve(_ context.Context, auth *Auth) (ResolvedProxy, error) {
	r.mu.Lock()
	r.resolved = append(r.resolved, auth.ID)
	bindingID := "binding-" + auth.ID
	if r.resolveCalls < len(r.bindingIDs) {
		bindingID = r.bindingIDs[r.resolveCalls]
	}
	r.resolveCalls++
	r.mu.Unlock()
	if r.failAuthID == "*" || auth.ID == r.failAuthID {
		return ResolvedProxy{}, proxyResolutionUnavailableError{}
	}
	return ResolvedProxy{URL: "http://proxy.example:8080", Source: "pool", BindingID: bindingID}, nil
}

func (r *proxyResolverExecutionResolver) ReportFailure(ctx context.Context, auth *Auth, err error) error {
	r.mu.Lock()
	r.reportContextErrs = append(r.reportContextErrs, ctx.Err())
	proxyFailureAuthID := r.proxyFailureAuthID
	r.mu.Unlock()
	if ctx.Err() == nil && auth != nil && auth.ID == proxyFailureAuthID {
		return proxyResolutionUnavailableError{}
	}
	return err
}

func (r *proxyResolverExecutionResolver) resolvedAuthIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.resolved...)
}

func (r *proxyResolverExecutionResolver) reportContexts() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.reportContextErrs...)
}

type proxyResolutionUnavailableError struct{}

func (proxyResolutionUnavailableError) Error() string        { return "proxy_unavailable" }
func (proxyResolutionUnavailableError) StatusCode() int      { return http.StatusServiceUnavailable }
func (proxyResolutionUnavailableError) SkipAuthResult() bool { return true }
func (proxyResolutionUnavailableError) RetryOtherAuth() bool { return true }

type proxyResolverExecutionExecutor struct {
	mu                      sync.Mutex
	provider                string
	failAuthID              string
	httpFailAuthID          string
	refreshFailAuthID       string
	prepare                 bool
	prepareUnauthorizedOnce atomic.Bool
	executed                []string
	executedBindingIDs      []string
	streamed                []string
	preparedBindingIDs      []string
}

func (e *proxyResolverExecutionExecutor) Identifier() string {
	if strings.TrimSpace(e.provider) != "" {
		return e.provider
	}
	return "codex"
}

func (e *proxyResolverExecutionExecutor) ShouldPrepareRequestAuth(*Auth) bool { return e.prepare }

func (e *proxyResolverExecutionExecutor) PrepareRequestAuth(_ context.Context, auth *Auth) (*Auth, error) {
	e.mu.Lock()
	e.preparedBindingIDs = append(e.preparedBindingIDs, auth.EffectiveProxyBindingID())
	e.mu.Unlock()
	if e.prepareUnauthorizedOnce.CompareAndSwap(true, false) {
		return nil, &Error{HTTPStatus: http.StatusUnauthorized, Message: "expired access token"}
	}
	return auth.Clone(), nil
}

func (e *proxyResolverExecutionExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if auth.RuntimeProxyBindingID == "" || auth.RuntimeProxyURL == "" {
		return cliproxyexecutor.Response{}, errors.New("runtime proxy missing")
	}
	e.mu.Lock()
	e.executed = append(e.executed, auth.ID)
	e.executedBindingIDs = append(e.executedBindingIDs, auth.EffectiveProxyBindingID())
	e.mu.Unlock()
	if auth.ID == e.failAuthID {
		return cliproxyexecutor.Response{}, &net.DNSError{Err: "proxy lookup failed", Name: "proxy.example"}
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID)}, nil
}

func (e *proxyResolverExecutionExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if auth.RuntimeProxyBindingID == "" || auth.RuntimeProxyURL == "" {
		return nil, errors.New("runtime proxy missing")
	}
	e.mu.Lock()
	e.streamed = append(e.streamed, auth.ID)
	e.mu.Unlock()
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(auth.ID)}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (*proxyResolverExecutionExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *proxyResolverExecutionExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	if auth == nil || auth.RuntimeProxyBindingID == "" || auth.RuntimeProxyURL == "" {
		return nil, errors.New("runtime proxy missing")
	}
	if auth.ID == e.refreshFailAuthID {
		return nil, &net.DNSError{Err: "proxy lookup failed", Name: "proxy.example"}
	}
	return auth.Clone(), nil
}

func (e *proxyResolverExecutionExecutor) HttpRequest(_ context.Context, auth *Auth, _ *http.Request) (*http.Response, error) {
	if auth == nil || auth.RuntimeProxyBindingID == "" || auth.RuntimeProxyURL == "" {
		return nil, errors.New("runtime proxy missing")
	}
	if auth.ID == e.httpFailAuthID {
		return nil, &net.DNSError{Err: "proxy lookup failed", Name: "proxy.example"}
	}
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok"))}, nil
}

func (e *proxyResolverExecutionExecutor) executedAuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.executed...)
}

func (e *proxyResolverExecutionExecutor) executedProxyBindings() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.executedBindingIDs...)
}

func (e *proxyResolverExecutionExecutor) streamedAuthIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.streamed...)
}

func (e *proxyResolverExecutionExecutor) preparedProxyBindings() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.preparedBindingIDs...)
}

func assertProxyUnavailableError(t *testing.T, err error) {
	t.Helper()
	var statusErr interface{ StatusCode() int }
	if !errors.As(err, &statusErr) || statusErr.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("error = %v, want proxy unavailable status 503", err)
	}
}

func assertProxyReportContextsActive(t *testing.T, resolver *proxyResolverExecutionResolver, want int) {
	t.Helper()
	contexts := resolver.reportContexts()
	if len(contexts) != want {
		t.Fatalf("reported contexts = %v, want %d", contexts, want)
	}
	for _, errContext := range contexts {
		if errContext != nil {
			t.Fatalf("proxy failure was reported with canceled context: %v", errContext)
		}
	}
}
