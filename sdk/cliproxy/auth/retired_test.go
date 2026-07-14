package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type retiredAuthTestStore struct {
	mu              sync.Mutex
	records         map[string]*Auth
	blockNextSave   bool
	nextSaveStarted chan struct{}
	nextSaveRelease chan struct{}
}

type retiredBlockingRefreshExecutor struct {
	schedulerProviderTestExecutor
	started chan struct{}
	release chan struct{}
}

type retiredReturningRefreshExecutor struct {
	schedulerProviderTestExecutor
}

type retiredHTTPDirectExecutor struct {
	schedulerProviderTestExecutor
	prepareCalls int
	httpCalls    int
}

type retiredBlockingHTTPExecutor struct {
	schedulerProviderTestExecutor
	once    sync.Once
	started chan struct{}
}

type retiredBlockingRequestPreparer struct {
	schedulerProviderTestExecutor
	started chan struct{}
	once    sync.Once
}

type retiredResultContextHook struct {
	NoopHook
	ctxErr chan error
}

type retiredBlockingExecuteExecutor struct {
	schedulerProviderTestExecutor
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type retiredBlockingFailureExecutor struct {
	schedulerProviderTestExecutor
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type retiredReentrantBindSelector struct {
	manager *Manager
	done    chan error
	once    sync.Once
}

type retiredBlockingTransactionalSelector struct {
	*SessionAffinitySelector
	started     chan struct{}
	release     chan struct{}
	blockBefore bool
	once        sync.Once
}

type retiredReentrantInvalidatorSelector struct {
	manager *Manager
	done    chan error
	once    sync.Once
}

type retiredRefreshingCloserExecutor struct {
	schedulerProviderTestExecutor
	closed chan string
}

type retiredReentrantCloseExecutor struct {
	schedulerProviderTestExecutor
	manager *Manager
	done    chan error
	release chan struct{}
	once    sync.Once
}

type retiredDeleteOnUpdateHook struct {
	NoopHook
	manager *Manager
	done    chan error
	once    sync.Once
}

type retiredPanickingUpdateHook struct {
	NoopHook
}

type retiredPanickingInvalidatorSelector struct {
	RoundRobinSelector
}

type retiredPanickingCloseExecutor struct {
	schedulerProviderTestExecutor
}

type retiredExecutionSpy struct {
	schedulerProviderTestExecutor
	calls int
}

type retiredInstanceCaptureExecutor struct {
	schedulerProviderTestExecutor
	instanceID chan string
}

type retiredCleanupOrder struct {
	mu     sync.Mutex
	events []string
}

type retiredOrderedCloserExecutor struct {
	schedulerProviderTestExecutor
	order *retiredCleanupOrder
}

type retiredOrderedUpdateHook struct {
	NoopHook
	order *retiredCleanupOrder
}

type retiredObservedDoneContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (c *retiredObservedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func (o *retiredCleanupOrder) record(event string) {
	o.mu.Lock()
	o.events = append(o.events, event)
	o.mu.Unlock()
}

func (e *retiredOrderedCloserExecutor) CloseAuthInstanceExecutionSessions(string, string, string) {
	e.order.record("close")
}

func (h *retiredOrderedUpdateHook) OnAuthUpdated(context.Context, *Auth) {
	h.order.record("hook")
}

func (e *retiredExecutionSpy) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.calls++
	return cliproxyexecutor.Response{}, nil
}

func (e *retiredExecutionSpy) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.calls++
	return cliproxyexecutor.Response{}, nil
}

func (e *retiredExecutionSpy) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.calls++
	chunks := make(chan cliproxyexecutor.StreamChunk)
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *retiredInstanceCaptureExecutor) ExecuteStream(_ context.Context, _ *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	instanceID, _ := opts.Metadata[cliproxyexecutor.SelectedAuthInstanceMetadataKey].(string)
	e.instanceID <- instanceID
	chunks := make(chan cliproxyexecutor.StreamChunk)
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (h *retiredDeleteOnUpdateHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	h.once.Do(func() {
		h.done <- h.manager.Delete(WithSkipPersist(ctx), auth.ID)
	})
}

func (retiredPanickingUpdateHook) OnAuthUpdated(context.Context, *Auth) {
	panic("update hook panic")
}

func (*retiredPanickingInvalidatorSelector) InvalidateAuth(string) {
	panic("session invalidator panic")
}

func (*retiredPanickingCloseExecutor) CloseAuthInstanceExecutionSessions(string, string, string) {
	panic("execution session closer panic")
}

func (e *retiredReentrantCloseExecutor) CloseAuthInstanceExecutionSessions(id string, _ string, _ string) {
	e.once.Do(func() {
		_, errRegister := e.manager.Register(context.Background(), &Auth{ID: id, Provider: "codex", Metadata: map[string]any{"type": "codex"}})
		e.done <- errRegister
		if e.release != nil {
			<-e.release
		}
	})
}

func (e *retiredBlockingRefreshExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	close(e.started)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.release:
		return auth, nil
	}
}

func (e *retiredReturningRefreshExecutor) Refresh(context.Context, *Auth) (*Auth, error) {
	return &Auth{Provider: "gemini-cli"}, nil
}

func (e *retiredRefreshingCloserExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	updated := auth.Clone()
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	updated.Metadata["access_token"] = "refreshed-token"
	return updated, nil
}

func (e *retiredRefreshingCloserExecutor) CloseAuthInstanceExecutionSessions(authID string, _ string, reason string) {
	e.closed <- authID + ":" + reason
}

func (e *retiredHTTPDirectExecutor) PrepareRequest(*http.Request, *Auth) error {
	e.prepareCalls++
	return nil
}

func (e *retiredBlockingRequestPreparer) PrepareRequest(req *http.Request, _ *Auth) error {
	req.Header.Set("Authorization", "Bearer stale-token")
	e.once.Do(func() { close(e.started) })
	<-req.Context().Done()
	return req.Context().Err()
}

func (h *retiredResultContextHook) OnResult(ctx context.Context, _ Result) {
	h.ctxErr <- ctx.Err()
}

func (e *retiredHTTPDirectExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	e.httpCalls++
	return &http.Response{StatusCode: http.StatusOK}, nil
}

func (e *retiredBlockingHTTPExecutor) HttpRequest(ctx context.Context, _ *Auth, _ *http.Request) (*http.Response, error) {
	e.once.Do(func() { close(e.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (e *retiredBlockingExecuteExecutor) Execute(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.once.Do(func() { close(e.started) })
	select {
	case <-ctx.Done():
		return cliproxyexecutor.Response{}, ctx.Err()
	case <-e.release:
		return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
	}
}

func (e *retiredBlockingFailureExecutor) Execute(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.once.Do(func() { close(e.started) })
	select {
	case <-ctx.Done():
		return cliproxyexecutor.Response{}, ctx.Err()
	case <-e.release:
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"}
	}
}

func (s *retiredReentrantBindSelector) Pick(_ context.Context, _ string, _ string, _ cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	if len(auths) == 0 {
		return nil, fmt.Errorf("no auth available")
	}
	return auths[0], nil
}

func (s *retiredReentrantBindSelector) BindSession(ctx context.Context, _ string, _ string, _ cliproxyexecutor.Options, authID string) {
	s.once.Do(func() {
		auth, ok := s.manager.GetByID(authID)
		if !ok || auth == nil {
			s.done <- fmt.Errorf("auth %s not found", authID)
			return
		}
		if auth.Metadata == nil {
			auth.Metadata = make(map[string]any)
		}
		auth.Metadata["bound"] = true
		_, errUpdate := s.manager.Update(ctx, auth)
		s.done <- errUpdate
	})
}

func (s *retiredBlockingTransactionalSelector) BindSessionWithRollback(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, authID string) func() {
	if s.blockBefore {
		s.once.Do(func() {
			close(s.started)
			<-s.release
		})
	}
	rollback := s.SessionAffinitySelector.BindSessionWithRollback(ctx, provider, model, opts, authID)
	if !s.blockBefore {
		s.once.Do(func() {
			close(s.started)
			<-s.release
		})
	}
	return rollback
}

func (s *retiredReentrantInvalidatorSelector) Pick(_ context.Context, _ string, _ string, _ cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	if len(auths) == 0 {
		return nil, fmt.Errorf("no auth available")
	}
	return auths[0], nil
}

func (s *retiredReentrantInvalidatorSelector) InvalidateAuth(authID string) {
	s.once.Do(func() {
		_, errRegister := s.manager.Register(WithSkipPersist(context.Background()), &Auth{
			ID:       authID,
			Provider: "codex",
			Status:   StatusActive,
			Metadata: map[string]any{"type": "codex"},
		})
		s.done <- errRegister
	})
}

func (s *retiredAuthTestStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]*Auth, 0, len(s.records))
	for _, auth := range s.records {
		items = append(items, auth.Clone())
	}
	return items, nil
}

func (s *retiredAuthTestStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.mu.Lock()
	if s.blockNextSave {
		s.blockNextSave = false
		started := s.nextSaveStarted
		release := s.nextSaveRelease
		s.nextSaveStarted = nil
		s.nextSaveRelease = nil
		if started != nil {
			close(started)
		}
		s.mu.Unlock()
		if release != nil {
			<-release
		}
		s.mu.Lock()
	}
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]*Auth)
	}
	if auth != nil {
		s.records[auth.ID] = auth.Clone()
	}
	return "", nil
}

func (s *retiredAuthTestStore) blockOneSave() (<-chan struct{}, chan<- struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	started := make(chan struct{})
	release := make(chan struct{})
	s.blockNextSave = true
	s.nextSaveStarted = started
	s.nextSaveRelease = release
	return started, release
}

func (s *retiredAuthTestStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	delete(s.records, id)
	s.mu.Unlock()
	return nil
}

func assertRetiredProviderNotSupported(t *testing.T, err error) {
	t.Helper()
	authErr, ok := err.(*Error)
	if !ok || authErr.Code != "provider_not_supported" || authErr.HTTPStatus != http.StatusGone {
		t.Fatalf("error = %#v, want provider_not_supported 410", err)
	}
}

func TestIsRetiredGeminiCLIAuth_DoesNotMatchGeminiAPIKey(t *testing.T) {
	tests := []struct {
		name string
		auth *Auth
		want bool
	}{
		{name: "legacy gemini file", auth: &Auth{Metadata: map[string]any{"type": "gemini"}}, want: true},
		{name: "legacy explicit provider", auth: &Auth{Metadata: map[string]any{"type": "gemini-cli"}}, want: true},
		{name: "legacy provider without metadata", auth: &Auth{Provider: "gemini-cli"}, want: true},
		{name: "legacy gemini oauth marker without metadata", auth: &Auth{Provider: "gemini", Attributes: map[string]string{"auth_kind": "oauth"}}, want: true},
		{name: "legacy v6 gemini provider without type", auth: &Auth{Provider: "gemini", Metadata: map[string]any{"email": "user@example.com", "project_id": "project"}}, want: true},
		{name: "legacy provider ignores api key attribute", auth: &Auth{Provider: "gemini-cli", Attributes: map[string]string{"api_key": "key"}}, want: true},
		{name: "legacy token metadata overrides api key", auth: &Auth{Provider: "gemini", Attributes: map[string]string{"api_key": "key"}, Metadata: map[string]any{"type": "gemini", "access_token": "token"}}, want: true},
		{name: "legacy refresh token overrides api key", auth: &Auth{Provider: "gemini", Attributes: map[string]string{"api_key": "key"}, Metadata: map[string]any{"type": "gemini", "refresh_token": "token"}}, want: true},
		{name: "legacy token string overrides api key", auth: &Auth{Provider: "gemini", Attributes: map[string]string{"api_key": "key"}, Metadata: map[string]any{"type": "gemini", "token": "token"}}, want: true},
		{name: "legacy oauth kind overrides api key", auth: &Auth{Provider: "gemini", Attributes: map[string]string{"api_key": "key"}, Metadata: map[string]any{"type": "gemini", "auth_kind": "oauth"}}, want: true},
		{name: "gemini api key", auth: &Auth{Provider: "gemini", Attributes: map[string]string{"api_key": "key"}, Metadata: map[string]any{"type": "gemini"}}, want: false},
		{name: "file-backed gemini api key with labels", auth: &Auth{Provider: "gemini", Metadata: map[string]any{"type": "gemini", "api_key": "key", "email": "key@example.com", "project_id": "project"}}, want: false},
		{name: "legacy token map overrides api key", auth: &Auth{Provider: "gemini", Attributes: map[string]string{"api_key": "key"}, Metadata: map[string]any{"type": "gemini", "token": map[string]any{"access_token": "token"}}}, want: true},
		{name: "config gemini api key", auth: &Auth{Provider: "gemini", Attributes: map[string]string{"api_key": "key"}}, want: false},
		{name: "antigravity", auth: &Auth{Provider: "antigravity", Metadata: map[string]any{"type": "antigravity"}}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetiredGeminiCLIAuth(tt.auth); got != tt.want {
				t.Fatalf("IsRetiredGeminiCLIAuth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyFileBackedGeminiAPIKey(t *testing.T) {
	auth := &Auth{Provider: "gemini", Metadata: map[string]any{"type": "gemini", "api_key": " key "}}
	ApplyFileBackedGeminiAPIKey(auth)
	if auth.Attributes["api_key"] != "key" || auth.Attributes["auth_kind"] != "apikey" {
		t.Fatalf("Gemini API key attributes = %#v", auth.Attributes)
	}
	retired := &Auth{Provider: "gemini-cli", Metadata: map[string]any{"type": "gemini-cli", "api_key": "key"}}
	ApplyFileBackedGeminiAPIKey(retired)
	if len(retired.Attributes) != 0 {
		t.Fatalf("retired Gemini CLI attributes = %#v", retired.Attributes)
	}
	for _, test := range []struct {
		name     string
		metadata map[string]any
	}{
		{name: "access token", metadata: map[string]any{"access_token": "legacy"}},
		{name: "refresh token", metadata: map[string]any{"refresh_token": "legacy"}},
		{name: "token string", metadata: map[string]any{"token": "legacy"}},
		{name: "token object", metadata: map[string]any{"token": map[string]any{"access_token": "legacy"}}},
		{name: "oauth auth kind", metadata: map[string]any{"auth_kind": "oauth"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.metadata["type"] = "gemini"
			test.metadata["api_key"] = "key"
			mixed := &Auth{Provider: "gemini", Metadata: test.metadata}
			ApplyFileBackedGeminiAPIKey(mixed)
			if len(mixed.Attributes) != 0 {
				t.Fatalf("mixed Gemini CLI attributes = %#v", mixed.Attributes)
			}
		})
	}
}

func TestIsRetiredGeminiCLIAuthFileData_DoesNotMatchGeminiAPIKey(t *testing.T) {
	tests := []struct {
		name string
		data string
		want bool
	}{
		{name: "legacy gemini", data: `{"type":"gemini","access_token":"token"}`, want: true},
		{name: "explicit gemini cli", data: `{"type":"gemini-cli","api_key":"key"}`, want: true},
		{name: "legacy token overrides api key", data: `{"type":"gemini","api_key":"key","access_token":"token"}`, want: true},
		{name: "legacy refresh token overrides api key", data: `{"type":"gemini","api_key":"key","refresh_token":"token"}`, want: true},
		{name: "legacy token string overrides api key", data: `{"type":"gemini","api_key":"key","token":"token"}`, want: true},
		{name: "legacy oauth kind overrides api key", data: `{"type":"gemini","api_key":"key","auth_kind":"oauth"}`, want: true},
		{name: "gemini api key with labels", data: `{"type":"gemini","api_key":"key","email":"key@example.com","project_id":"project"}`, want: false},
		{name: "legacy token map overrides api key", data: `{"type":"gemini","api_key":"key","token":{"access_token":"token"}}`, want: true},
		{name: "other provider", data: `{"type":"codex"}`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetiredGeminiCLIAuthFileData([]byte(tt.data)); got != tt.want {
				t.Fatalf("IsRetiredGeminiCLIAuthFileData() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestManagerRegister_RetiredGeminiCLIDoesNotCreateStoredOrRuntimeAuth(t *testing.T) {
	store := &retiredAuthTestStore{}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	retired := &Auth{
		ID:       "legacy.json",
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini", "access_token": "secret"},
	}
	_, errRegister := manager.Register(t.Context(), retired)
	assertRetiredProviderNotSupported(t, errRegister)
	if _, ok := manager.GetByID(retired.ID); ok {
		t.Fatal("retired Gemini CLI auth entered runtime manager")
	}
	items, errList := store.List(t.Context())
	if errList != nil || len(items) != 0 {
		t.Fatalf("persisted items = %#v, error = %v; want none", items, errList)
	}
}

func TestManagerUpdate_RetiredGeminiCLIDoesNotCreateStoredOrRuntimeAuth(t *testing.T) {
	store := &retiredAuthTestStore{}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	retired := &Auth{
		ID:       "legacy.json",
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini", "access_token": "secret"},
	}
	_, errUpdate := manager.Update(t.Context(), retired)
	assertRetiredProviderNotSupported(t, errUpdate)
	if _, ok := manager.GetByID(retired.ID); ok {
		t.Fatal("retired Gemini CLI auth entered runtime manager")
	}
	items, errList := store.List(t.Context())
	if errList != nil || len(items) != 0 {
		t.Fatalf("persisted items = %#v, error = %v; want none", items, errList)
	}
}

func TestManagerRegister_RetiredGeminiCLIDoesNotEvictRuntimeOnlyIDCollision(t *testing.T) {
	store := &retiredAuthTestStore{}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	const authID = "shared-id"
	runtimeAuth := &Auth{
		ID:         authID,
		Provider:   "codex",
		Attributes: map[string]string{"runtime_only": "true"},
	}
	if _, errRegister := manager.Register(t.Context(), runtimeAuth); errRegister != nil {
		t.Fatalf("register runtime-only auth: %v", errRegister)
	}
	retired := &Auth{ID: authID, Provider: "gemini", Metadata: map[string]any{"type": "gemini"}}
	_, errRetired := manager.Register(t.Context(), retired)
	assertRetiredProviderNotSupported(t, errRetired)
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || current.Provider != "codex" || !authIsRuntimeOnly(current) {
		t.Fatalf("current auth = %#v, want runtime-only codex auth", current)
	}
	items, errList := store.List(t.Context())
	if errList != nil || len(items) != 0 {
		t.Fatalf("persisted items = %#v, error = %v; want none", items, errList)
	}
}

func TestManagerLoad_FiltersRetiredGeminiCLIOnly(t *testing.T) {
	store := &retiredAuthTestStore{records: map[string]*Auth{
		"legacy.json": {
			ID:       "legacy.json",
			Provider: "gemini",
			Metadata: map[string]any{"type": "gemini"},
		},
		"gemini-key": {
			ID:         "gemini-key",
			Provider:   "gemini",
			Attributes: map[string]string{"api_key": "key"},
			Metadata:   map[string]any{"type": "gemini"},
		},
		"codex.json": {
			ID:       "codex.json",
			Provider: "codex",
			Metadata: map[string]any{"type": "codex"},
		},
	}}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("Load() error = %v", errLoad)
	}
	if _, ok := manager.GetByID("legacy.json"); ok {
		t.Fatal("retired Gemini CLI auth entered runtime manager during Load")
	}
	if _, ok := manager.GetByID("gemini-key"); !ok {
		t.Fatal("Gemini API key was filtered from runtime manager")
	}
	if _, ok := manager.GetByID("codex.json"); !ok {
		t.Fatal("supported OAuth auth was filtered from runtime manager")
	}
}

func TestManagerDirectHTTPMethodsRejectRetiredGeminiCLIAuth(t *testing.T) {
	executor := &retiredHTTPDirectExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "gemini"},
	}
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(executor)
	retired := &Auth{
		ID:       "legacy.json",
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini", "access_token": "legacy-token"},
	}

	request, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("create request: %v", errRequest)
	}
	assertRetiredError := func(name string, err error) {
		t.Helper()
		authErr, ok := err.(*Error)
		if !ok || authErr.Code != "provider_not_supported" || authErr.HTTPStatus != http.StatusGone {
			t.Fatalf("%s error = %#v, want provider_not_supported 410", name, err)
		}
	}
	assertRetiredError("PrepareHttpRequest", manager.PrepareHttpRequest(t.Context(), retired, request.Clone(t.Context())))
	_, errNew := manager.NewHttpRequest(t.Context(), retired, http.MethodGet, "https://example.com", nil, nil)
	assertRetiredError("NewHttpRequest", errNew)
	_, errHTTP := manager.HttpRequest(t.Context(), retired, request.Clone(t.Context()))
	assertRetiredError("HttpRequest", errHTTP)
	if executor.prepareCalls != 0 || executor.httpCalls != 0 {
		t.Fatalf("retired direct HTTP calls reached executor: prepare=%d http=%d", executor.prepareCalls, executor.httpCalls)
	}
}

func TestManagerExecutionEntryPointsRejectRetiredGeminiCLIFormats(t *testing.T) {
	executor := &retiredExecutionSpy{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"}}
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(executor)
	assertGone := func(name string, err error) {
		t.Helper()
		authErr, ok := err.(*Error)
		if !ok || authErr.Code != "provider_not_supported" || authErr.HTTPStatus != http.StatusGone {
			t.Fatalf("%s error = %#v, want provider_not_supported 410", name, err)
		}
	}
	_, errExecute := manager.Execute(t.Context(), []string{"codex"}, cliproxyexecutor.Request{Format: " Gemini-CLI "}, cliproxyexecutor.Options{})
	assertGone("Execute", errExecute)
	_, errCount := manager.ExecuteCount(t.Context(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{SourceFormat: "GEMINI-CLI"})
	assertGone("ExecuteCount", errCount)
	_, errStream := manager.ExecuteStream(t.Context(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{ResponseFormat: "gemini-cli"})
	assertGone("ExecuteStream", errStream)
	_, errExecute = manager.Execute(t.Context(), []string{"gemini-cli"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	assertGone("Execute provider", errExecute)
	_, errCount = manager.ExecuteCount(t.Context(), []string{"gemini-cli"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	assertGone("ExecuteCount provider", errCount)
	_, errStream = manager.ExecuteStream(t.Context(), []string{"gemini-cli"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	assertGone("ExecuteStream provider", errStream)
	if executor.calls != 0 {
		t.Fatalf("retired formats reached executor %d times", executor.calls)
	}
}

func TestManagerExecuteStreamPublishesSelectedAuthInstance(t *testing.T) {
	const (
		authID = "instance-auth"
		model  = "instance-model"
	)
	executor := &retiredInstanceCaptureExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "xai"},
		instanceID:                    make(chan string, 1),
	}
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(executor)
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "xai", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	wantInstanceID := managerAuthInstanceID(t, manager, authID)
	result, errStream := manager.ExecuteStream(t.Context(), []string{"xai"}, cliproxyexecutor.Request{Model: model, Payload: []byte(`{}`)}, cliproxyexecutor.Options{OriginalRequest: []byte(`{}`)})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	if result != nil {
		for range result.Chunks {
		}
	}
	select {
	case gotInstanceID := <-executor.instanceID:
		if gotInstanceID != wantInstanceID {
			t.Fatalf("selected instance = %q, want %q", gotInstanceID, wantInstanceID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executor did not receive selected auth instance")
	}
}

func TestManagerReplacementRetiresSelectedAuthInstance(t *testing.T) {
	const authID = "retired-selected-instance"
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if selected == nil {
		t.Fatal("selected auth is missing")
	}
	opts := withSelectedAuthInstanceMetadata(cliproxyexecutor.Options{}, selected)
	retirement, ok := opts.Metadata[cliproxyexecutor.SelectedAuthInstanceRetirementMetadataKey].(cliproxyexecutor.AuthInstanceRetirement)
	if !ok || retirement == nil || retirement.Retired() {
		t.Fatalf("initial retirement state = %#v", retirement)
	}
	replacement, errUpdate := manager.Update(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}})
	if errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if !retirement.Retired() {
		t.Fatal("replacement did not retire the selected auth instance")
	}
	if replacement == nil || replacement.RuntimeInstanceRetired() {
		t.Fatalf("replacement instance state = %#v", replacement)
	}
	if _, errUpdate = manager.Update(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}}); errUpdate != nil {
		t.Fatalf("second Update() error = %v", errUpdate)
	}
	if !replacement.RuntimeInstanceRetired() {
		t.Fatal("second update did not retire the Update return value")
	}
}

func TestRuntimeExecutionLeaseCancelsWithoutBlockingReplacement(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "guarded-runtime-instance"
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	runtimeCtx, releaseExecution, active := selected.BeginRuntimeExecution(t.Context())
	if !active {
		t.Fatal("new runtime instance rejected execution lease")
	}
	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}})
		updateDone <- errUpdate
	}()
	select {
	case errUpdate := <-updateDone:
		if errUpdate != nil {
			t.Fatalf("Update() error = %v", errUpdate)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Update() blocked on active runtime execution")
	}
	select {
	case <-runtimeCtx.Done():
		if !errors.Is(context.Cause(runtimeCtx), errRuntimeAuthInstanceRetired) {
			t.Fatalf("runtime execution cause = %v, want retired", context.Cause(runtimeCtx))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replacement did not cancel runtime execution")
	}
	if !releaseExecution() {
		t.Fatal("lease completion did not observe concurrent retirement")
	}
	_, _, active = selected.BeginRuntimeExecution(t.Context())
	if active {
		t.Fatal("retired runtime instance accepted a new execution lease")
	}
}

func TestDeleteWithOperationSerializesConcurrentUpdate(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "delete-operation-serialized"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Label: "old", Metadata: map[string]any{"type": "xai"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	operationStarted := make(chan struct{})
	releaseOperation := make(chan struct{})
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- manager.DeleteWithOperation(t.Context(), authID, func(context.Context) error {
			close(operationStarted)
			<-releaseOperation
			return nil
		})
	}()
	select {
	case <-operationStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("delete operation did not start")
	}
	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(t.Context(), &Auth{ID: authID, Provider: "xai", Label: "new", Metadata: map[string]any{"type": "xai"}})
		updateDone <- errUpdate
	}()
	select {
	case errUpdate := <-updateDone:
		t.Fatalf("concurrent Update() completed before delete operation: %v", errUpdate)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseOperation)
	if errDelete := <-deleteDone; errDelete != nil {
		t.Fatalf("DeleteWithOperation() error = %v", errDelete)
	}
	select {
	case errUpdate := <-updateDone:
		if errUpdate != nil {
			t.Fatalf("Update() error = %v", errUpdate)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Update() remained blocked")
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || current.Label != "new" || current.RuntimeInstanceRetired() {
		t.Fatalf("current auth after serialized update = %#v", current)
	}
}

func TestDeleteWithOperationRestoresRuntimeAfterFailure(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "delete-operation-rollback"
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	wantErr := errors.New("backing store delete failed")
	errDelete := manager.DeleteWithOperation(t.Context(), authID, func(context.Context) error {
		return NewDeleteOutcomeError(DeleteOutcomeRolledBack, wantErr)
	})
	if !errors.Is(errDelete, wantErr) {
		t.Fatalf("DeleteWithOperation() error = %v, want %v", errDelete, wantErr)
	}
	if !selected.RuntimeInstanceRetired() {
		t.Fatal("failed deletion did not retire the old runtime instance")
	}
	restored, ok := manager.GetByID(authID)
	if !ok || restored == nil || restored.RuntimeInstanceRetired() {
		t.Fatalf("restored auth = %#v", restored)
	}
	_, releaseExecution, active := restored.BeginRuntimeExecution(t.Context())
	if !active {
		t.Fatal("restored auth rejected a runtime execution")
	}
	releaseExecution()
}

func TestDeleteWithOperationFailClosedDoesNotRestoreRuntimeAfterRollback(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "fail-closed-delete"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex"}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	wantErr := errors.New("durable delete rolled back")
	errDelete := manager.DeleteWithOperationFailClosed(t.Context(), authID, func(context.Context) error {
		return NewDeleteOutcomeError(DeleteOutcomeRolledBack, wantErr)
	})
	if !errors.Is(errDelete, wantErr) {
		t.Fatalf("DeleteWithOperationFailClosed() error = %v, want %v", errDelete, wantErr)
	}
	if auth, ok := manager.GetByID(authID); ok || auth != nil {
		t.Fatalf("rolled-back fail-closed auth was restored: %#v", auth)
	}
}

func TestDeleteWithOperationFailsClosedWhenOutcomeIsUncertain(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "delete-operation-uncertain"
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	wantErr := errors.New("delete result could not be verified")
	errDelete := manager.DeleteWithOperation(t.Context(), authID, func(context.Context) error {
		return NewDeleteOutcomeError(DeleteOutcomeUncertain, wantErr)
	})
	if !errors.Is(errDelete, wantErr) {
		t.Fatalf("DeleteWithOperation() error = %v, want %v", errDelete, wantErr)
	}
	if !selected.RuntimeInstanceRetired() {
		t.Fatal("uncertain deletion did not retire the runtime instance")
	}
	if current, ok := manager.GetByID(authID); ok || current != nil {
		t.Fatalf("uncertain deletion left executable auth = %#v", current)
	}
}

func TestDeleteWithOperationTreatsVerifiedCommitAsSuccess(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "delete-operation-committed"
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	transportErr := errors.New("delete response was lost")
	errDelete := manager.DeleteWithOperation(t.Context(), authID, func(context.Context) error {
		return NewDeleteOutcomeError(DeleteOutcomeCommitted, transportErr)
	})
	if errDelete != nil {
		t.Fatalf("DeleteWithOperation() error = %v", errDelete)
	}
	if !selected.RuntimeInstanceRetired() {
		t.Fatal("verified committed deletion did not retire the runtime instance")
	}
	if current, ok := manager.GetByID(authID); ok || current != nil {
		t.Fatalf("verified committed deletion left auth = %#v", current)
	}
}

func TestManagerHttpRequestUsesRequestContextWhenContextIsNil(t *testing.T) {
	executor := &retiredBlockingHTTPExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "xai"},
		started:                       make(chan struct{}),
	}
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: "http-request-context", Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	requestCtx, cancelRequest := context.WithCancel(t.Context())
	request, errRequest := http.NewRequestWithContext(requestCtx, http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("create request: %v", errRequest)
	}
	done := make(chan error, 1)
	go func() {
		_, errHTTP := manager.HttpRequest(nil, selected, request)
		done <- errHTTP
	}()
	select {
	case <-executor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not start")
	}
	cancelRequest()
	select {
	case errHTTP := <-done:
		if !errors.Is(errHTTP, context.Canceled) {
			t.Fatalf("HttpRequest() error = %v, want context canceled", errHTTP)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not observe request context cancellation")
	}
}

func TestManagerHttpRequestReportsRuntimeRetirement(t *testing.T) {
	executor := &retiredBlockingHTTPExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "xai"},
		started:                       make(chan struct{}),
	}
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: "http-request-retired", Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	request, errRequest := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("create request: %v", errRequest)
	}
	done := make(chan error, 1)
	go func() {
		_, errHTTP := manager.HttpRequest(t.Context(), selected, request)
		done <- errHTTP
	}()
	select {
	case <-executor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not start")
	}
	if _, errUpdate := manager.Update(t.Context(), &Auth{ID: selected.ID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	select {
	case errHTTP := <-done:
		var authErr *Error
		if !errors.As(errHTTP, &authErr) || authErr.Code != "auth_instance_retired" || !authErr.Retryable {
			t.Fatalf("HttpRequest() error = %#v, want retryable auth_instance_retired", errHTTP)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not finish after auth retirement")
	}
}

func TestManagerClosesRetiredSessionsBeforeUpdateHook(t *testing.T) {
	order := &retiredCleanupOrder{}
	manager := NewManager(nil, nil, &retiredOrderedUpdateHook{order: order})
	manager.RegisterExecutor(&retiredOrderedCloserExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "xai"},
		order:                         order,
	})
	const authID = "ordered-session-cleanup"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errUpdate := manager.Update(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	order.mu.Lock()
	events := append([]string(nil), order.events...)
	order.mu.Unlock()
	if len(events) != 2 || events[0] != "close" || events[1] != "hook" {
		t.Fatalf("cleanup order = %#v, want [close hook]", events)
	}
}

func TestWrapStreamResultReportsRuntimeRetirement(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "retired-active-stream"
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	runtimeCtx, releaseExecution, active := selected.BeginRuntimeExecution(t.Context())
	if !active {
		t.Fatal("new runtime instance rejected stream lease")
	}
	remaining := make(chan cliproxyexecutor.StreamChunk)
	defer close(remaining)
	result := manager.wrapStreamResult(runtimeCtx, t.Context(), selected, []string{"xai"}, "xai", "model", "model", cliproxyexecutor.Options{}, nil, []cliproxyexecutor.StreamChunk{{Payload: []byte("first")}}, remaining, OAuthModelAliasResult{}, releaseExecution)
	first := <-result.Chunks
	if string(first.Payload) != "first" || first.Err != nil {
		t.Fatalf("first stream chunk = %#v", first)
	}
	if _, errUpdate := manager.Update(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	select {
	case terminal, ok := <-result.Chunks:
		if !ok || terminal.Err == nil {
			t.Fatalf("retired stream terminal = (%#v, %t), want explicit error", terminal, ok)
		}
		var authErr *Error
		if !errors.As(terminal.Err, &authErr) || authErr.Code != "auth_instance_retired" {
			t.Fatalf("retired stream error = %#v", terminal.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retired stream did not terminate")
	}
}

func TestWrapStreamResultNormalizesProducerCancellationAfterRetirement(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "retired-cancelled-stream"
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	runtimeCtx, releaseExecution, active := selected.BeginRuntimeExecution(t.Context())
	if !active {
		t.Fatal("new runtime instance rejected stream lease")
	}
	remaining := make(chan cliproxyexecutor.StreamChunk, 1)
	result := manager.wrapStreamResult(runtimeCtx, t.Context(), selected, []string{"xai"}, "xai", "model", "model", cliproxyexecutor.Options{}, nil, nil, remaining, OAuthModelAliasResult{}, releaseExecution)
	if _, errUpdate := manager.Update(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	remaining <- cliproxyexecutor.StreamChunk{Err: context.Canceled}
	close(remaining)
	terminal, ok := <-result.Chunks
	if !ok || terminal.Err == nil {
		t.Fatalf("retired stream terminal = (%#v, %t)", terminal, ok)
	}
	var authErr *Error
	if !errors.As(terminal.Err, &authErr) || authErr.Code != "auth_instance_retired" {
		t.Fatalf("retired stream error = %#v", terminal.Err)
	}
}

func TestManagerLoadSkipsDurablyQuarantinedAuthFile(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "quarantined.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	authfileguard.MarkQuarantined(path)
	t.Cleanup(func() { authfileguard.ClearQuarantined(path) })
	store := &retiredAuthTestStore{records: map[string]*Auth{
		"quarantined": {
			ID:       "quarantined",
			Provider: "codex",
			FileName: filepath.Base(path),
			Attributes: map[string]string{
				"path": path,
			},
			Metadata: map[string]any{"type": "codex"},
		},
	}}
	manager := NewManager(store, nil, nil)
	manager.SetConfig(&internalconfig.Config{AuthDir: authDir})
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("Load() error = %v", errLoad)
	}
	if auth, ok := manager.GetByID("quarantined"); ok || auth != nil {
		t.Fatalf("quarantined auth was admitted: %#v", auth)
	}
}

func TestWrapStreamResultDoesNotAppendRetirementAfterSuccessfulTerminal(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	const authID = "terminal-before-retirement"
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	runtimeCtx, releaseExecution, active := selected.BeginRuntimeExecution(t.Context())
	if !active {
		t.Fatal("new runtime instance rejected stream lease")
	}
	remaining := make(chan cliproxyexecutor.StreamChunk)
	result := manager.wrapStreamResult(runtimeCtx, t.Context(), selected, nil, "xai", "model", "model", cliproxyexecutor.Options{}, nil, []cliproxyexecutor.StreamChunk{{Payload: []byte("completed")}, cliproxyexecutor.SuccessfulStreamTerminalChunk()}, remaining, OAuthModelAliasResult{}, releaseExecution)
	terminal := <-result.Chunks
	if string(terminal.Payload) != "completed" || terminal.Err != nil {
		t.Fatalf("successful terminal chunk = %#v", terminal)
	}
	if _, errUpdate := manager.Update(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	close(remaining)
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("unexpected error after successful terminal: %v", chunk.Err)
		}
	}
}

func TestWrapStreamResultStopsAfterFirstTerminal(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: "terminal-first", Provider: "xai"}
	remaining := make(chan cliproxyexecutor.StreamChunk, 3)
	remaining <- cliproxyexecutor.StreamChunk{Payload: []byte("completed")}
	remaining <- cliproxyexecutor.SuccessfulStreamTerminalChunk()
	remaining <- cliproxyexecutor.StreamChunk{Payload: []byte("late")}
	close(remaining)

	result := manager.wrapStreamResult(t.Context(), t.Context(), auth, nil, "xai", "model", "model", cliproxyexecutor.Options{}, nil, nil, remaining, OAuthModelAliasResult{}, nil)
	chunks := make([]cliproxyexecutor.StreamChunk, 0, 1)
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 1 || string(chunks[0].Payload) != "completed" || chunks[0].Err != nil {
		t.Fatalf("terminal stream chunks = %#v", chunks)
	}
}

func TestWrapStreamResultStopsAfterFirstError(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: "error-first", Provider: "xai"}
	wantErr := errors.New("terminal failure")
	remaining := make(chan cliproxyexecutor.StreamChunk, 2)
	remaining <- cliproxyexecutor.StreamChunk{Err: wantErr}
	remaining <- cliproxyexecutor.StreamChunk{Payload: []byte("late")}
	close(remaining)

	result := manager.wrapStreamResult(t.Context(), t.Context(), auth, nil, "xai", "model", "model", cliproxyexecutor.Options{}, nil, nil, remaining, OAuthModelAliasResult{}, nil)
	chunks := make([]cliproxyexecutor.StreamChunk, 0, 1)
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 1 || !errors.Is(chunks[0].Err, wantErr) {
		t.Fatalf("error stream chunks = %#v", chunks)
	}
}

func TestReadStreamBootstrapReturnsOnEmptyTerminal(t *testing.T) {
	remaining := make(chan cliproxyexecutor.StreamChunk, 2)
	remaining <- cliproxyexecutor.SuccessfulStreamTerminalChunk()
	remaining <- cliproxyexecutor.StreamChunk{Payload: []byte("late")}
	close(remaining)

	buffered, closed, err := readStreamBootstrap(t.Context(), remaining)
	if err != nil {
		t.Fatalf("readStreamBootstrap() error = %v", err)
	}
	if !closed || len(buffered) != 1 || !cliproxyexecutor.IsSuccessfulStreamTerminalChunk(buffered[0]) {
		t.Fatalf("bootstrap = %#v, closed=%t", buffered, closed)
	}
}

func TestEnqueueTerminalStreamChunkPreservesBufferedPayloadOrder(t *testing.T) {
	out := make(chan cliproxyexecutor.StreamChunk, cliproxyexecutor.StreamBufferSize)
	for i := 0; i < cap(out); i++ {
		out <- cliproxyexecutor.StreamChunk{Payload: []byte(fmt.Sprintf("payload-%d", i))}
	}
	ctx := &retiredObservedDoneContext{Context: t.Context(), observed: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		enqueueTerminalStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: runtimeAuthInstanceRetiredError()})
		close(done)
	}()
	<-ctx.observed
	select {
	case <-done:
		t.Fatal("terminal stream enqueue completed by discarding buffered payload")
	default:
	}

	for i := 0; i < cap(out); i++ {
		chunk := <-out
		if got, want := string(chunk.Payload), fmt.Sprintf("payload-%d", i); got != want {
			t.Fatalf("buffered payload %d = %q, want %q", i, got, want)
		}
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal stream enqueue did not finish after space became available")
	}
	terminal := <-out
	var authErr *Error
	if !errors.As(terminal.Err, &authErr) || authErr.Code != "auth_instance_retired" {
		t.Fatalf("terminal chunk = %#v", terminal)
	}
}

func TestEnqueueTerminalStreamChunkStopsWhenRequestIsCanceled(t *testing.T) {
	out := make(chan cliproxyexecutor.StreamChunk, 1)
	out <- cliproxyexecutor.StreamChunk{Payload: []byte("queued")}
	baseCtx, cancel := context.WithCancel(t.Context())
	ctx := &retiredObservedDoneContext{Context: baseCtx, observed: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		enqueueTerminalStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: runtimeAuthInstanceRetiredError()})
		close(done)
	}()
	<-ctx.observed
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal stream enqueue ignored request cancellation")
	}
	if got := string((<-out).Payload); got != "queued" {
		t.Fatalf("queued payload = %q, want %q", got, "queued")
	}
	select {
	case chunk := <-out:
		t.Fatalf("canceled enqueue wrote terminal chunk: %#v", chunk)
	default:
	}
}

func TestWrapStreamResultRecordsSuccessWithRequestContext(t *testing.T) {
	hook := &retiredResultContextHook{ctxErr: make(chan error, 1)}
	manager := NewManager(nil, nil, hook)
	selected, errRegister := manager.Register(t.Context(), &Auth{ID: "stream-result-context", Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	runtimeCtx, releaseExecution, active := selected.BeginRuntimeExecution(t.Context())
	if !active {
		t.Fatal("new runtime instance rejected stream lease")
	}
	remaining := make(chan cliproxyexecutor.StreamChunk)
	close(remaining)
	result := manager.wrapStreamResult(runtimeCtx, t.Context(), selected, nil, "xai", "model", "model", cliproxyexecutor.Options{}, nil, []cliproxyexecutor.StreamChunk{{Payload: []byte("ok")}}, remaining, OAuthModelAliasResult{}, releaseExecution)
	for range result.Chunks {
	}
	select {
	case errCtx := <-hook.ctxErr:
		if errCtx != nil {
			t.Fatalf("result hook context error = %v, want active request context", errCtx)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("success result hook was not called")
	}
}

func TestManagerRequestPreparationFailsWhenAuthIsReplaced(t *testing.T) {
	tests := []struct {
		name string
		call func(*Manager, *Auth, *http.Request) error
	}{
		{
			name: "prepare http request",
			call: func(manager *Manager, auth *Auth, req *http.Request) error {
				return manager.PrepareHttpRequest(req.Context(), auth, req)
			},
		},
		{
			name: "inject credentials",
			call: func(manager *Manager, auth *Auth, req *http.Request) error {
				return manager.InjectCredentials(req, auth.ID)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := &retiredBlockingRequestPreparer{
				schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "xai"},
				started:                       make(chan struct{}),
			}
			manager := NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			selected, errRegister := manager.Register(t.Context(), &Auth{ID: "preparer-auth", Provider: "xai", Metadata: map[string]any{"type": "xai"}})
			if errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}
			request, errRequest := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com", nil)
			if errRequest != nil {
				t.Fatalf("create request: %v", errRequest)
			}
			result := make(chan error, 1)
			go func() { result <- test.call(manager, selected, request) }()
			select {
			case <-executor.started:
			case <-time.After(5 * time.Second):
				t.Fatal("request preparer did not start")
			}
			if _, errUpdate := manager.Update(t.Context(), &Auth{ID: selected.ID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
				t.Fatalf("replace auth: %v", errUpdate)
			}
			select {
			case errPrepare := <-result:
				var authErr *Error
				if !errors.As(errPrepare, &authErr) || authErr.Code != "auth_instance_retired" {
					t.Fatalf("preparation error = %#v, want auth_instance_retired", errPrepare)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("request preparation was not canceled by auth replacement")
			}
			if errCtx := request.Context().Err(); errCtx != nil {
				t.Fatalf("returned request retained canceled lease context: %v", errCtx)
			}
		})
	}
}

func TestManagerInjectCredentialsFailsClosedForRemovedAuth(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	request, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("create request: %v", errRequest)
	}
	errInject := manager.InjectCredentials(request, "legacy.json")
	authErr, ok := errInject.(*Error)
	if !ok || authErr.Code != "auth_not_found" {
		t.Fatalf("InjectCredentials() error = %#v, want auth_not_found", errInject)
	}
}

func TestManagerExecute_CancelsDeletedAuthWithoutRebinding(t *testing.T) {
	const (
		authID    = "auth-1"
		model     = "test-model"
		sessionID = "stale-session"
	)
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	t.Cleanup(selector.Stop)
	executor := &retiredBlockingExecuteExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
		started:                       make(chan struct{}),
		release:                       make(chan struct{}),
	}
	manager := NewManager(&retiredAuthTestStore{}, selector, nil)
	manager.RegisterExecutor(executor)
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	headers := make(http.Header)
	headers.Set("X-Session-ID", sessionID)
	opts := cliproxyexecutor.Options{Headers: headers, OriginalRequest: []byte(`{}`)}
	executeDone := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(t.Context(), []string{"codex"}, cliproxyexecutor.Request{Model: model, Payload: []byte(`{}`)}, opts)
		executeDone <- errExecute
	}()
	select {
	case <-executor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("execute did not reach executor")
	}
	if errDelete := manager.Delete(t.Context(), authID); errDelete != nil {
		t.Fatalf("delete auth: %v", errDelete)
	}
	select {
	case errExecute := <-executeDone:
		if errExecute == nil {
			t.Fatal("execute succeeded after selected auth was deleted")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("execute was not canceled after auth deletion")
	}
	close(executor.release)

	cacheKey := "codex::" + sessionID + "::" + selectionArgForSelector(selector, model)
	if boundID, ok := selector.cache.Get(cacheKey); ok {
		t.Fatalf("deleted auth was rebound after success: %s", boundID)
	}
}

func TestManagerUpdate_ToRetiredGeminiCLILeavesExistingRuntimeAuthUnchanged(t *testing.T) {
	store := &retiredAuthTestStore{}
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	t.Cleanup(selector.Stop)
	manager := NewManager(store, selector, nil)
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: "auth-1", Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	selector.cache.Set("session-1", "auth-1")
	_, errUpdate := manager.Update(t.Context(), &Auth{ID: "auth-1", Provider: "gemini-cli"})
	assertRetiredProviderNotSupported(t, errUpdate)
	current, ok := manager.GetByID("auth-1")
	if !ok || current == nil || current.Provider != "codex" {
		t.Fatalf("current auth = %#v, want original codex auth", current)
	}
	if boundID, ok := selector.cache.Get("session-1"); !ok || boundID != "auth-1" {
		t.Fatalf("session binding = (%q, %t), want original auth", boundID, ok)
	}
	items, errList := store.List(t.Context())
	if errList != nil || len(items) != 1 || items[0].Provider != "codex" {
		t.Fatalf("persisted records = %#v, error = %v; want original codex auth", items, errList)
	}
}

func TestManagerRefresh_RetiredResultIsRemovedFromRuntime(t *testing.T) {
	store := &retiredAuthTestStore{}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	auth := &Auth{ID: "auth-1", Provider: "codex", Metadata: map[string]any{"type": "codex"}}
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.RegisterExecutor(&retiredReturningRefreshExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
	})

	manager.refreshAuth(t.Context(), auth.ID)

	if _, ok := manager.GetByID(auth.ID); ok {
		t.Fatal("retired refresh result remained in runtime manager")
	}
	items, errList := store.List(t.Context())
	if errList != nil || len(items) != 1 || items[0].Provider != "codex" {
		t.Fatalf("persisted records = %#v, error = %v; want original codex auth unchanged", items, errList)
	}
	manager.scheduler.mu.RLock()
	_, scheduled := manager.scheduler.authProviders[auth.ID]
	manager.scheduler.mu.RUnlock()
	if scheduled {
		t.Fatal("retired refresh result remained in scheduler")
	}
}

func TestManagerHandleRetiredAuthKeepsReplacementRuntime(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	expected := &Auth{ID: "auth-1", Provider: "codex", Metadata: map[string]any{"type": "codex"}}
	if _, errRegister := manager.Register(t.Context(), expected); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.mu.Lock()
	expected = manager.auths[expected.ID]
	replacement := expected.Clone()
	replacement.Provider = "claude"
	replacement.Metadata = map[string]any{"type": "claude"}
	manager.auths[expected.ID] = replacement
	manager.mu.Unlock()

	result, errRetired := manager.handleRetiredAuth(t.Context(), &Auth{ID: expected.ID, Provider: "gemini-cli"}, expected)
	if errRetired != nil {
		t.Fatalf("handleRetiredAuth() error = %v", errRetired)
	}
	if result != nil {
		t.Fatalf("handleRetiredAuth() result = %#v, want stale result discarded", result)
	}
	current, ok := manager.GetByID(expected.ID)
	if !ok || current == nil || current.Provider != "claude" {
		t.Fatalf("replacement auth = %#v, want claude auth retained", current)
	}
}

func TestManagerLoad_RetiredGeminiCLICleansRuntimeSideEffects(t *testing.T) {
	const authID = "legacy.json"
	store := &retiredAuthTestStore{records: map[string]*Auth{
		authID: {
			ID:       authID,
			Provider: "codex",
			Metadata: map[string]any{"type": "codex"},
		},
	}}
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	t.Cleanup(selector.Stop)
	manager := NewManager(store, selector, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("initial Load() error = %v", errLoad)
	}

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: "retired-test-model"}})
	t.Cleanup(func() { reg.UnregisterClient(authID) })
	selector.cache.Set("session-1", authID)
	loop := newAuthAutoRefreshLoop(manager, time.Hour, 1)
	loop.upsert(authID, time.Now().Add(time.Hour))
	manager.mu.Lock()
	manager.refreshLoop = loop
	manager.mu.Unlock()

	store.mu.Lock()
	store.records[authID] = &Auth{
		ID:       authID,
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
	}
	store.mu.Unlock()
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("retired Load() error = %v", errLoad)
	}
	loop.applyDirty(time.Now())

	if _, ok := manager.GetByID(authID); ok {
		t.Fatal("retired auth remained in runtime manager after Load")
	}
	if models := reg.GetModelsForClient(authID); len(models) != 0 {
		t.Fatalf("retired auth registry models = %d, want 0", len(models))
	}
	if _, ok := selector.cache.Get("session-1"); ok {
		t.Fatal("retired Load left a session-affinity binding")
	}
	manager.scheduler.mu.RLock()
	_, scheduled := manager.scheduler.authProviders[authID]
	manager.scheduler.mu.RUnlock()
	if scheduled {
		t.Fatal("retired auth remained in scheduler after Load")
	}
	loop.mu.Lock()
	_, refreshScheduled := loop.index[authID]
	loop.mu.Unlock()
	if refreshScheduled {
		t.Fatal("retired auth remained in refresh scheduler after Load")
	}
}

func TestManagerRefresh_DoesNotResurrectRemovedAuth(t *testing.T) {
	store := &retiredAuthTestStore{}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	auth := &Auth{ID: "auth-1", Provider: "codex", Metadata: map[string]any{"type": "codex"}}
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	executor := &retiredBlockingRefreshExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
		started:                       make(chan struct{}),
		release:                       make(chan struct{}),
	}
	manager.RegisterExecutor(executor)
	done := make(chan struct{})
	go func() {
		manager.refreshAuth(t.Context(), auth.ID)
		close(done)
	}()
	select {
	case <-executor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not start")
	}

	if errDelete := manager.Delete(WithSkipPersist(t.Context()), auth.ID); errDelete != nil {
		t.Fatalf("remove auth during refresh: %v", errDelete)
	}
	close(executor.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not finish")
	}

	if _, ok := manager.GetByID(auth.ID); ok {
		t.Fatal("completed refresh resurrected retired runtime auth")
	}
	store.mu.Lock()
	persistedRecord := store.records[auth.ID]
	var persisted *Auth
	if persistedRecord != nil {
		persisted = persistedRecord.Clone()
	}
	store.mu.Unlock()
	if persisted == nil || persisted.Provider != "codex" {
		t.Fatalf("persisted auth = %#v, want original codex record unchanged", persisted)
	}
}

func TestManagerRefreshRotatesInstanceAndClosesOldSessions(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	const authID = "refresh-session-auth"
	if _, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "codex", "access_token": "old-token"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	instanceID := managerAuthInstanceID(t, manager, authID)
	executor := &retiredRefreshingCloserExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
		closed:                        make(chan string, 1),
	}
	manager.RegisterExecutor(executor)
	manager.refreshAuth(t.Context(), authID)
	if current := managerAuthInstanceID(t, manager, authID); current == instanceID {
		t.Fatalf("refresh retained old instance %q", current)
	}
	select {
	case got := <-executor.closed:
		if got != authID+":auth_refreshed" {
			t.Fatalf("session close = %q", got)
		}
	default:
		t.Fatal("refresh did not close old execution sessions")
	}
}

func TestManagerRefreshPreservesConcurrentRateLimitState(t *testing.T) {
	store := &retiredAuthTestStore{}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	const (
		authID = "refresh-concurrent-rate-limit"
		model  = "gpt-test"
	)
	if _, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "codex", "access_token": "old-token"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	executor := &retiredBlockingRefreshExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
		started:                       make(chan struct{}),
		release:                       make(chan struct{}),
	}
	manager.RegisterExecutor(executor)
	refreshDone := make(chan struct{})
	go func() {
		manager.refreshAuth(t.Context(), authID)
		close(refreshDone)
	}()
	select {
	case <-executor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not start")
	}
	retryAfter := 10 * time.Minute
	manager.MarkResult(t.Context(), Result{
		AuthID:     authID,
		Provider:   "codex",
		Model:      model,
		Error:      &Error{HTTPStatus: http.StatusTooManyRequests, Message: "rate limited"},
		RetryAfter: &retryAfter,
	})
	close(executor.release)
	select {
	case <-refreshDone:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not finish")
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("refreshed auth disappeared")
	}
	state := current.ModelStates[model]
	if current.Status != StatusError || current.LastError == nil || state == nil || !state.Quota.Exceeded || state.NextRetryAfter.IsZero() {
		t.Fatalf("concurrent rate-limit state was lost after refresh: %#v", current)
	}
	reloaded := NewManager(store, &RoundRobinSelector{}, nil)
	if errLoad := reloaded.Load(t.Context()); errLoad != nil {
		t.Fatalf("reload persisted auth: %v", errLoad)
	}
	persisted, ok := reloaded.GetByID(authID)
	if !ok || persisted == nil {
		t.Fatal("persisted refreshed auth is missing")
	}
	persistedState := persisted.ModelStates[model]
	if persisted.Status != StatusError || persisted.LastError == nil || persistedState == nil || !persistedState.Quota.Exceeded || persistedState.NextRetryAfter.IsZero() {
		t.Fatalf("persisted concurrent rate-limit state was lost: %#v", persisted)
	}
}

func TestManagerReplacementCleanupRecoversExternalCallbackPanics(t *testing.T) {
	testCases := []struct {
		name     string
		selector Selector
		hook     Hook
		executor ProviderExecutor
	}{
		{
			name:     "hook",
			selector: &RoundRobinSelector{},
			hook:     retiredPanickingUpdateHook{},
		},
		{
			name:     "session invalidator",
			selector: &retiredPanickingInvalidatorSelector{},
		},
		{
			name:     "execution session closer",
			selector: &RoundRobinSelector{},
			executor: &retiredPanickingCloseExecutor{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"}},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewManager(nil, testCase.selector, nil)
			const authID = "panic-cleanup-auth"
			if _, errRegister := manager.Register(t.Context(), &Auth{
				ID:       authID,
				Provider: "codex",
				Status:   StatusActive,
				Attributes: map[string]string{
					SourceHashAttributeKey: "source-a",
				},
				Metadata: map[string]any{"type": "codex"},
			}); errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}
			if testCase.hook != nil {
				manager.SetHook(testCase.hook)
			}
			if testCase.executor != nil {
				manager.RegisterExecutor(testCase.executor)
			} else {
				manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "codex"})
			}
			if _, errUpdate := manager.Update(t.Context(), &Auth{
				ID:       authID,
				Provider: "codex",
				Status:   StatusActive,
				Attributes: map[string]string{
					SourceHashAttributeKey: "source-b",
				},
				Metadata: map[string]any{"type": "codex"},
			}); errUpdate != nil {
				t.Fatalf("Update() error = %v", errUpdate)
			}
			selected, _, errPick := manager.pickNext(t.Context(), "codex", "", cliproxyexecutor.Options{}, nil)
			if errPick != nil || selected == nil || selected.ID != authID {
				t.Fatalf("pick after callback panic = (%#v, %v), want %s", selected, errPick, authID)
			}
		})
	}
}

func TestManagerExecuteAcceptsResultAfterSameSourceUpdate(t *testing.T) {
	hook := &countingHook{}
	manager := NewManager(nil, &RoundRobinSelector{}, hook)
	manager.SetRetryConfig(0, 0, 1)
	const authID = "same-source-auth"
	if _, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: "codex",
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "same-source",
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	executor := &retiredBlockingFailureExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
		started:                       make(chan struct{}),
		release:                       make(chan struct{}),
	}
	manager.RegisterExecutor(executor)

	executeDone := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(t.Context(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
		executeDone <- errExecute
	}()
	select {
	case <-executor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute() did not start")
	}

	updated, ok := manager.GetByID(authID)
	if !ok || updated == nil {
		t.Fatal("GetByID() did not return auth")
	}
	updated.Metadata["label"] = "updated while executing"
	if _, errUpdate := manager.Update(t.Context(), updated); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	close(executor.release)
	select {
	case errExecute := <-executeDone:
		if errExecute == nil {
			t.Fatal("Execute() error = nil, want unauthorized")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Execute() did not finish")
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil || current.LastError == nil || current.LastError.HTTPStatus != http.StatusUnauthorized {
		t.Fatalf("same-source execution result was dropped: %#v", current)
	}
	if hook.results != 1 {
		t.Fatalf("result hook count = %d, want 1", hook.results)
	}
}

func TestManagerLoadPreservesOnlySameSourceInstance(t *testing.T) {
	const authID = "load-generation-auth"
	store := &retiredAuthTestStore{records: map[string]*Auth{
		authID: {
			ID:       authID,
			Provider: "codex",
			Status:   StatusActive,
			Attributes: map[string]string{
				SourceHashAttributeKey: "source-a",
			},
			Metadata: map[string]any{"type": "codex"},
		},
	}}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	closer := &authScopedCloserExecutor{}
	manager.RegisterExecutor(closer)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("initial Load() error = %v", errLoad)
	}
	instanceID := managerAuthInstanceID(t, manager, authID)
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: "replacement-model"}})
	t.Cleanup(func() { reg.UnregisterClient(authID) })

	store.mu.Lock()
	store.records[authID].Label = "same source"
	store.mu.Unlock()
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("same-source Load() error = %v", errLoad)
	}
	if current := managerAuthInstanceID(t, manager, authID); current != instanceID {
		t.Fatalf("same-source instance changed from %q to %q", instanceID, current)
	}
	closer.mu.Lock()
	closedAfterSameSource := len(closer.closedAuthIDs)
	closer.mu.Unlock()
	if closedAfterSameSource != 0 {
		t.Fatalf("same-source Load() closed %d auth sessions", closedAfterSameSource)
	}

	store.mu.Lock()
	store.records[authID].Attributes[SourceHashAttributeKey] = "source-b"
	store.mu.Unlock()
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("replacement Load() error = %v", errLoad)
	}
	if current := managerAuthInstanceID(t, manager, authID); current == instanceID {
		t.Fatalf("replacement source retained instance %q", current)
	}
	closer.mu.Lock()
	closedAfterReplacement := append([]string(nil), closer.closedAuthIDs...)
	closer.mu.Unlock()
	if len(closedAfterReplacement) != 1 || closedAfterReplacement[0] != authID {
		t.Fatalf("replacement session cleanup = %#v, want [%s]", closedAfterReplacement, authID)
	}
	if !reg.ClientSupportsModel(authID, "replacement-model") {
		t.Fatal("same-ID replacement lost its model registration")
	}
}

func TestManagerBindSessionAllowsReentrantUpdate(t *testing.T) {
	selector := &retiredReentrantBindSelector{done: make(chan error, 1)}
	manager := NewManager(nil, selector, nil)
	selector.manager = manager
	const authID = "reentrant-bind-auth"
	if _, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: "codex",
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "same-source",
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "codex"})

	executeDone := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(t.Context(), []string{"codex"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
		executeDone <- errExecute
	}()
	select {
	case errBind := <-selector.done:
		if errBind != nil {
			t.Fatalf("reentrant Update() error = %v", errBind)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("BindSession() deadlocked during reentrant Update()")
	}
	select {
	case errExecute := <-executeDone:
		if errExecute != nil {
			t.Fatalf("Execute() error = %v", errExecute)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Execute() did not return after reentrant BindSession()")
	}
}

func TestManagerStaleBindRollbackKeepsReplacementBinding(t *testing.T) {
	baseSelector := NewSessionAffinitySelector(&RoundRobinSelector{})
	selector := &retiredBlockingTransactionalSelector{
		SessionAffinitySelector: baseSelector,
		started:                 make(chan struct{}),
		release:                 make(chan struct{}),
		blockBefore:             true,
	}
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	const (
		authID = "stale-bind-replacement"
		model  = "test-model"
	)
	if _, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: "codex",
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "source-a",
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	oldAuth, ok := manager.GetByID(authID)
	if !ok || oldAuth == nil {
		t.Fatal("original auth not found")
	}
	headers := make(http.Header)
	headers.Set("X-Session-ID", "replacement-session")
	opts := cliproxyexecutor.Options{Headers: headers}
	bindDone := make(chan struct{})
	go func() {
		manager.bindSessionAffinity(t.Context(), []string{"codex"}, model, opts, oldAuth)
		close(bindDone)
	}()
	select {
	case <-selector.started:
	case <-time.After(5 * time.Second):
		t.Fatal("stale bind did not start")
	}
	if _, errUpdate := manager.Update(t.Context(), &Auth{
		ID:       authID,
		Provider: "codex",
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "source-b",
		},
		Metadata: map[string]any{"type": "codex"},
	}); errUpdate != nil {
		t.Fatalf("replacement Update() error = %v", errUpdate)
	}
	baseSelector.bindSessionWithRollback(t.Context(), "codex", model, opts, authID)
	close(selector.release)
	select {
	case <-bindDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stale bind did not finish")
	}
	primaryID, _ := extractSessionIDs(opts.Headers, opts.OriginalRequest, opts.Metadata)
	cacheKey := "codex::" + primaryID + "::" + model
	if got, found := baseSelector.cache.Get(cacheKey); !found || got != authID {
		t.Fatalf("replacement binding = (%q, %t), want %q", got, found, authID)
	}
}

func TestManagerDeleteAllowsReentrantSessionInvalidator(t *testing.T) {
	selector := &retiredReentrantInvalidatorSelector{done: make(chan error, 1)}
	manager := NewManager(&retiredAuthTestStore{}, selector, nil)
	selector.manager = manager
	const authID = "reentrant-invalidator-delete"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Status: StatusActive, Metadata: map[string]any{"type": "codex"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- manager.Delete(t.Context(), authID) }()
	select {
	case errRegister := <-selector.done:
		if errRegister != nil {
			t.Fatalf("reentrant Register() error = %v", errRegister)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("InvalidateAuth() deadlocked during reentrant Register()")
	}
	select {
	case errDelete := <-deleteDone:
		if errDelete != nil {
			t.Fatalf("Delete() error = %v", errDelete)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Delete() did not finish")
	}
	if current, ok := manager.GetByID(authID); !ok || current == nil {
		t.Fatal("reentrantly registered auth was lost")
	}
}

func TestManagerLoadAllowsReentrantSessionInvalidator(t *testing.T) {
	const authID = "reentrant-invalidator-load"
	store := &retiredAuthTestStore{records: map[string]*Auth{
		authID: {ID: authID, Provider: "codex", Status: StatusActive, Metadata: map[string]any{"type": "codex"}},
	}}
	selector := &retiredReentrantInvalidatorSelector{done: make(chan error, 1)}
	manager := NewManager(store, selector, nil)
	selector.manager = manager
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("initial Load() error = %v", errLoad)
	}
	store.mu.Lock()
	delete(store.records, authID)
	store.mu.Unlock()
	loadDone := make(chan error, 1)
	go func() { loadDone <- manager.Load(t.Context()) }()
	select {
	case errRegister := <-selector.done:
		if errRegister != nil {
			t.Fatalf("reentrant Register() error = %v", errRegister)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("InvalidateAuth() deadlocked behind Load persistence barrier")
	}
	select {
	case errLoad := <-loadDone:
		if errLoad != nil {
			t.Fatalf("Load() error = %v", errLoad)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Load() did not finish")
	}
	if current, ok := manager.GetByID(authID); !ok || current == nil {
		t.Fatal("reentrantly registered auth was lost")
	}
}

func managerAuthInstanceID(t *testing.T, manager *Manager, authID string) string {
	t.Helper()
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	auth := manager.auths[authID]
	if auth == nil || auth.instanceID == "" {
		t.Fatalf("auth %q has no runtime instance", authID)
	}
	return auth.instanceID
}

func TestManagerLoad_ReleasesPersistBarrierBeforeExternalCleanup(t *testing.T) {
	const authID = "auth-1"
	store := &retiredAuthTestStore{records: map[string]*Auth{
		authID: {ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}},
	}}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("initial Load() error = %v", errLoad)
	}
	executor := &retiredReentrantCloseExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
		manager:                       manager,
		done:                          make(chan error, 1),
		release:                       make(chan struct{}),
	}
	manager.RegisterExecutor(executor)
	store.mu.Lock()
	store.records[authID] = &Auth{ID: authID, Provider: "gemini", Metadata: map[string]any{"type": "gemini"}}
	store.mu.Unlock()

	loadDone := make(chan error, 1)
	go func() { loadDone <- manager.Load(t.Context()) }()
	select {
	case errRegister := <-executor.done:
		if errRegister != nil {
			t.Fatalf("reentrant Register() error = %v", errRegister)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("external cleanup deadlocked during reentrant Register")
	}
	if selected, _, errPick := manager.pickNext(t.Context(), "codex", "", cliproxyexecutor.Options{}, nil); errPick == nil || selected != nil {
		t.Fatalf("pick during session cleanup = (%#v, %v), want unavailable", selected, errPick)
	}
	close(executor.release)
	select {
	case errLoad := <-loadDone:
		if errLoad != nil {
			t.Fatalf("retired Load() error = %v", errLoad)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Load did not return after reentrant external cleanup")
	}
	if current, ok := manager.GetByID(authID); !ok || current.Provider != "codex" {
		t.Fatalf("current auth = %#v, want newly registered codex auth", current)
	}
	if selected, _, errPick := manager.pickNext(t.Context(), "codex", "", cliproxyexecutor.Options{}, nil); errPick != nil || selected == nil || selected.ID != authID {
		t.Fatalf("pick after session cleanup = (%#v, %v), want %s", selected, errPick, authID)
	}
}

func TestManagerDelete_ReleasesPersistLockBeforeExternalCleanup(t *testing.T) {
	hook := &countingHook{}
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, hook)
	const authID = "auth-1"
	_, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	original, ok := manager.GetByID(authID)
	if !ok || original == nil {
		t.Fatal("GetByID() did not return the original auth")
	}
	staleResult := resultForAuth(original, "codex", "", false)
	staleResult.Error = &Error{HTTPStatus: 401, Message: "old credential unauthorized"}
	executor := &retiredReentrantCloseExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
		manager:                       manager,
		done:                          make(chan error, 1),
		release:                       make(chan struct{}),
	}
	manager.RegisterExecutor(executor)
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- manager.Delete(t.Context(), authID) }()
	select {
	case errRegister := <-executor.done:
		if errRegister != nil {
			t.Fatalf("reentrant Register() error = %v", errRegister)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("external cleanup deadlocked during reentrant Register")
	}
	manager.markExecutionResult(t.Context(), staleResult)
	if current, ok := manager.GetByID(authID); !ok || current == nil || current.Status == StatusError || current.LastError != nil || !current.NextRetryAfter.IsZero() {
		t.Fatalf("replacement auth was changed by stale result: %#v", current)
	}
	if hook.results != 0 {
		t.Fatalf("stale result hook count = %d, want 0", hook.results)
	}
	if selected, _, errPick := manager.pickNext(t.Context(), "codex", "", cliproxyexecutor.Options{}, nil); errPick == nil || selected != nil {
		t.Fatalf("pick during session cleanup = (%#v, %v), want unavailable", selected, errPick)
	}
	close(executor.release)
	select {
	case errDelete := <-deleteDone:
		if errDelete != nil {
			t.Fatalf("Delete() error = %v", errDelete)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Delete did not return after reentrant external cleanup")
	}
	if current, ok := manager.GetByID(authID); !ok || current.Provider != "codex" {
		t.Fatalf("current auth = %#v, want reentrantly registered codex auth", current)
	}
	if selected, _, errPick := manager.pickNext(t.Context(), "codex", "", cliproxyexecutor.Options{}, nil); errPick != nil || selected == nil || selected.ID != authID {
		t.Fatalf("pick after session cleanup = (%#v, %v), want %s", selected, errPick, authID)
	}
}

func TestManagerEndSessionCleanupKeepsSchedulerRestoreAtomic(t *testing.T) {
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	const authID = "auth-1"
	manager.beginSessionCleanup(authID)

	manager.scheduler.mu.Lock()
	cleanupDone := make(chan struct{})
	go func() {
		manager.endSessionCleanup(authID)
		close(cleanupDone)
	}()

	deadline := time.Now().Add(2 * time.Second)
	managerLocked := false
	for time.Now().Before(deadline) {
		if manager.mu.TryLock() {
			manager.mu.Unlock()
			time.Sleep(time.Millisecond)
			continue
		}
		managerLocked = true
		break
	}
	if !managerLocked {
		manager.scheduler.mu.Unlock()
		<-cleanupDone
		t.Fatal("endSessionCleanup did not acquire the manager lock")
	}
	time.Sleep(20 * time.Millisecond)
	if manager.mu.TryLock() {
		manager.mu.Unlock()
		manager.scheduler.mu.Unlock()
		<-cleanupDone
		t.Fatal("manager state became writable before scheduler quarantine was removed")
	}

	manager.scheduler.mu.Unlock()
	select {
	case <-cleanupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("endSessionCleanup did not finish after scheduler lock was released")
	}
}

func TestManagerBeginSessionCleanupKeepsSchedulerQuarantineAtomic(t *testing.T) {
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	const authID = "auth-1"

	manager.scheduler.mu.Lock()
	cleanupStarted := make(chan struct{})
	go func() {
		manager.beginSessionCleanup(authID)
		close(cleanupStarted)
	}()

	deadline := time.Now().Add(2 * time.Second)
	managerLocked := false
	for time.Now().Before(deadline) {
		if manager.mu.TryLock() {
			manager.mu.Unlock()
			time.Sleep(time.Millisecond)
			continue
		}
		managerLocked = true
		break
	}
	if !managerLocked {
		manager.scheduler.mu.Unlock()
		<-cleanupStarted
		t.Fatal("beginSessionCleanup did not acquire the manager lock")
	}
	time.Sleep(20 * time.Millisecond)
	if manager.mu.TryLock() {
		manager.mu.Unlock()
		manager.scheduler.mu.Unlock()
		<-cleanupStarted
		t.Fatal("manager state became writable before scheduler quarantine was installed")
	}

	manager.scheduler.mu.Unlock()
	select {
	case <-cleanupStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("beginSessionCleanup did not finish after scheduler lock was released")
	}
	manager.endSessionCleanup(authID)
}

func TestManagerDeleteInstallsSchedulerQuarantineBeforeReleasingManagerState(t *testing.T) {
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	const authID = "auth-1"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	manager.scheduler.mu.Lock()
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- manager.Delete(t.Context(), authID) }()

	deadline := time.Now().Add(2 * time.Second)
	managerLocked := false
	for time.Now().Before(deadline) {
		if manager.mu.TryLock() {
			manager.mu.Unlock()
			time.Sleep(time.Millisecond)
			continue
		}
		managerLocked = true
		break
	}
	if !managerLocked {
		manager.scheduler.mu.Unlock()
		<-deleteDone
		t.Fatal("Delete() did not acquire the manager lock")
	}
	time.Sleep(20 * time.Millisecond)
	if manager.mu.TryLock() {
		manager.mu.Unlock()
		manager.scheduler.mu.Unlock()
		<-deleteDone
		t.Fatal("Delete() released manager state before scheduler quarantine was installed")
	}

	manager.scheduler.mu.Unlock()
	select {
	case errDelete := <-deleteDone:
		if errDelete != nil {
			t.Fatalf("Delete() error = %v", errDelete)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Delete() did not finish after scheduler lock was released")
	}
}

func TestManagerRefreshJobClearsOnlyItsPendingBackoffDuringCleanup(t *testing.T) {
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	const authID = "auth-1"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "codex"})
	job, marked := manager.markRefreshPending(authID, time.Now())
	if !marked {
		t.Fatal("markRefreshPending() = false, want true")
	}

	manager.beginSessionCleanup(authID)
	currentLoop := newAuthAutoRefreshLoop(manager, time.Second, 1)
	manager.mu.Lock()
	manager.refreshLoop = currentLoop
	manager.mu.Unlock()
	manager.refreshAuthJob(t.Context(), job)
	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("auth disappeared during session cleanup")
	}
	if !current.NextRefreshAfter.IsZero() {
		t.Fatalf("NextRefreshAfter = %v, want cleared pending marker", current.NextRefreshAfter)
	}
	if !current.LastRefreshedAt.IsZero() {
		t.Fatalf("LastRefreshedAt = %v, want no refresh during cleanup", current.LastRefreshedAt)
	}
	if _, marked = manager.markRefreshPending(authID, time.Now()); marked {
		t.Fatal("markRefreshPending() accepted auth during session cleanup")
	}
	currentLoop.mu.Lock()
	_, rescheduled := currentLoop.dirty[authID]
	currentLoop.mu.Unlock()
	if !rescheduled {
		t.Fatal("cleanup pending clear did not reschedule the current refresh loop")
	}
	manager.endSessionCleanup(authID)
}

func TestManagerRefreshJobDoesNotDelayReplacementAuth(t *testing.T) {
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	const authID = "auth-1"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "codex"})
	job, marked := manager.markRefreshPending(authID, time.Now())
	if !marked {
		t.Fatal("markRefreshPending() = false, want true")
	}
	replacement, ok := manager.GetByID(authID)
	if !ok || replacement == nil {
		t.Fatal("GetByID() did not return marked auth")
	}
	if _, errUpdate := manager.Update(t.Context(), replacement); errUpdate != nil {
		t.Fatalf("Update() replacement error = %v", errUpdate)
	}
	currentLoop := newAuthAutoRefreshLoop(manager, time.Second, 1)
	manager.mu.Lock()
	manager.refreshLoop = currentLoop
	manager.mu.Unlock()
	manager.refreshAuthJob(t.Context(), job)
	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("replacement auth disappeared")
	}
	if !current.NextRefreshAfter.IsZero() {
		t.Fatalf("replacement NextRefreshAfter = %v, want stale pending marker cleared", current.NextRefreshAfter)
	}
	if !current.LastRefreshedAt.IsZero() {
		t.Fatalf("replacement LastRefreshedAt = %v, want stale job skipped", current.LastRefreshedAt)
	}
	currentLoop.mu.Lock()
	_, rescheduled := currentLoop.dirty[authID]
	currentLoop.mu.Unlock()
	if !rescheduled {
		t.Fatal("replacement pending clear did not reschedule the current refresh loop")
	}
}

func TestManagerRefreshJobWithoutExecutorReschedulesCurrentLoop(t *testing.T) {
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	const authID = "auth-1"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	job, marked := manager.markRefreshPending(authID, time.Now())
	if !marked {
		t.Fatal("markRefreshPending() = false, want true")
	}
	currentLoop := newAuthAutoRefreshLoop(manager, time.Second, 1)
	manager.mu.Lock()
	manager.refreshLoop = currentLoop
	manager.mu.Unlock()

	manager.refreshAuthJob(t.Context(), job)
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || !current.NextRefreshAfter.IsZero() {
		t.Fatalf("auth after missing executor = %#v, want zero NextRefreshAfter", current)
	}
	currentLoop.mu.Lock()
	_, rescheduled := currentLoop.dirty[authID]
	currentLoop.mu.Unlock()
	if !rescheduled {
		t.Fatal("missing executor pending clear did not reschedule the current refresh loop")
	}
}

func TestManagerClearRefreshPendingJobReschedulesCurrentLoop(t *testing.T) {
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	const authID = "auth-1"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	job, marked := manager.markRefreshPending(authID, time.Now())
	if !marked {
		t.Fatal("markRefreshPending() = false, want true")
	}
	currentLoop := newAuthAutoRefreshLoop(manager, time.Second, 1)
	manager.mu.Lock()
	manager.refreshLoop = currentLoop
	manager.mu.Unlock()

	manager.clearRefreshPendingJob(job)
	current, ok := manager.GetByID(authID)
	if !ok || current == nil || !current.NextRefreshAfter.IsZero() {
		t.Fatalf("auth after pending clear = %#v, want zero NextRefreshAfter", current)
	}
	currentLoop.mu.Lock()
	_, rescheduled := currentLoop.dirty[authID]
	currentLoop.mu.Unlock()
	if !rescheduled {
		t.Fatal("pending marker clear did not reschedule the current refresh loop")
	}
}

func TestManagerRefreshJobDoesNotDelayReplacementInstalledDuringRefresh(t *testing.T) {
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	const authID = "auth-1"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	executor := &retiredBlockingRefreshExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"},
		started:                       make(chan struct{}),
		release:                       make(chan struct{}),
	}
	manager.RegisterExecutor(executor)
	job, marked := manager.markRefreshPending(authID, time.Now())
	if !marked {
		t.Fatal("markRefreshPending() = false, want true")
	}

	refreshDone := make(chan struct{})
	go func() {
		manager.refreshAuthJob(t.Context(), job)
		close(refreshDone)
	}()
	select {
	case <-executor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not start")
	}
	replacement, ok := manager.GetByID(authID)
	if !ok || replacement == nil {
		t.Fatal("GetByID() did not return marked auth")
	}
	if _, errUpdate := manager.Update(t.Context(), replacement); errUpdate != nil {
		t.Fatalf("Update() replacement error = %v", errUpdate)
	}
	close(executor.release)
	select {
	case <-refreshDone:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not finish")
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("replacement auth disappeared")
	}
	if !current.NextRefreshAfter.IsZero() {
		t.Fatalf("replacement NextRefreshAfter = %v, want stale pending marker cleared", current.NextRefreshAfter)
	}
	if !current.LastRefreshedAt.IsZero() {
		t.Fatalf("replacement LastRefreshedAt = %v, want in-flight stale result skipped", current.LastRefreshedAt)
	}
}

func TestManagerRefreshJobClearsPendingBackoffWhenExecutorDisappears(t *testing.T) {
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	const authID = "auth-1"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "codex"})
	job, marked := manager.markRefreshPending(authID, time.Now())
	if !marked {
		t.Fatal("markRefreshPending() = false, want true")
	}
	manager.mu.Lock()
	delete(manager.executors, "codex")
	manager.mu.Unlock()

	manager.refreshAuthJob(t.Context(), job)
	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("auth disappeared")
	}
	if !current.NextRefreshAfter.IsZero() {
		t.Fatalf("NextRefreshAfter = %v, want pending marker cleared", current.NextRefreshAfter)
	}
}

func TestManagerRefresh_ReleasesPersistLockBeforeUpdateHook(t *testing.T) {
	manager := NewManager(&retiredAuthTestStore{}, &RoundRobinSelector{}, nil)
	auth := &Auth{ID: "auth-1", Provider: "codex", Metadata: map[string]any{"type": "codex"}}
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "codex"})
	hook := &retiredDeleteOnUpdateHook{manager: manager, done: make(chan error, 1)}
	manager.SetHook(hook)
	refreshDone := make(chan struct{})
	go func() {
		manager.refreshAuth(t.Context(), auth.ID)
		close(refreshDone)
	}()
	select {
	case errDelete := <-hook.done:
		if errDelete != nil {
			t.Fatalf("hook Delete() error = %v", errDelete)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("update hook deadlocked while deleting refreshed auth")
	}
	select {
	case <-refreshDone:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not return after reentrant hook")
	}
}
