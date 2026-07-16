package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type requestPrepareStore struct {
	saveCount atomic.Int32
	mu        sync.Mutex
	last      *Auth
	items     []*Auth
	saveErr   error
	saveStart chan struct{}
	saveGate  chan struct{}
	saveOnce  sync.Once
}

func (s *requestPrepareStore) List(context.Context) ([]*Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]*Auth, 0, len(s.items))
	for _, auth := range s.items {
		items = append(items, auth.Clone())
	}
	return items, nil
}

func (s *requestPrepareStore) Save(ctx context.Context, auth *Auth) (string, error) {
	s.saveCount.Add(1)
	if s.saveStart != nil {
		s.saveOnce.Do(func() { close(s.saveStart) })
	}
	if s.saveGate != nil {
		select {
		case <-s.saveGate:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if s.saveErr != nil {
		return "", s.saveErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = auth.Clone()
	return "", nil
}

func (s *requestPrepareStore) Delete(context.Context, string) error { return nil }

func (s *requestPrepareStore) lastAuth() *Auth {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		return nil
	}
	return s.last.Clone()
}

func (s *requestPrepareStore) setItems(items ...*Auth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make([]*Auth, 0, len(items))
	for _, auth := range items {
		s.items = append(s.items, auth.Clone())
	}
}

type requestPrepareExecutor struct {
	prepareCalls    atomic.Int32
	refreshCalls    atomic.Int32
	unauthorizedOne atomic.Bool
	prepareStart    chan struct{}
	prepareGate     chan struct{}
	prepareErr      error
	updateOnError   bool
}

type requestPreparePersistError struct {
	err error
}

func (e requestPreparePersistError) Error() string { return e.err.Error() }
func (e requestPreparePersistError) Unwrap() error { return e.err }
func (requestPreparePersistError) PersistAuthUpdateOnError() bool {
	return true
}

func (*requestPrepareExecutor) Identifier() string { return "antigravity" }

func (*requestPrepareExecutor) ShouldPrepareRequestAuth(auth *Auth) bool {
	return auth == nil || requestPrepareString(auth.Metadata["project_id"]) == ""
}

func (e *requestPrepareExecutor) PrepareRequestAuth(ctx context.Context, auth *Auth) (*Auth, error) {
	e.prepareCalls.Add(1)
	if e.prepareStart != nil {
		select {
		case e.prepareStart <- struct{}{}:
		default:
		}
	}
	if e.prepareGate != nil {
		select {
		case <-e.prepareGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if e.unauthorizedOne.CompareAndSwap(true, false) {
		return nil, &Error{HTTPStatus: http.StatusUnauthorized, Message: "expired access token"}
	}
	if e.prepareErr != nil {
		if e.updateOnError {
			updated := auth.Clone()
			updated.Metadata["lifecycle_state"] = LifecycleStateReauthRequired
			updated.Metadata["lifecycle_reason"] = "refresh_token_invalid"
			return updated, e.prepareErr
		}
		return nil, e.prepareErr
	}
	updated := auth.Clone()
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	updated.Metadata["project_id"] = "prepared-project"
	return updated, nil
}

func (*requestPrepareExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if requestPrepareString(auth.Metadata["project_id"]) != "prepared-project" {
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusBadRequest, Message: "missing prepared project"}
	}
	return cliproxyexecutor.Response{Payload: []byte("execute")}, nil
}

func (*requestPrepareExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if requestPrepareString(auth.Metadata["project_id"]) != "prepared-project" {
		return nil, &Error{HTTPStatus: http.StatusBadRequest, Message: "missing prepared project"}
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("stream")}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (*requestPrepareExecutor) CountTokens(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if requestPrepareString(auth.Metadata["project_id"]) != "prepared-project" {
		return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusBadRequest, Message: "missing prepared project"}
	}
	return cliproxyexecutor.Response{Payload: []byte("count")}, nil
}

func (e *requestPrepareExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	e.refreshCalls.Add(1)
	updated := auth.Clone()
	updated.Metadata["access_token"] = "refreshed-token"
	return updated, nil
}

func (*requestPrepareExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, &Error{HTTPStatus: http.StatusNotImplemented, Message: "http not implemented"}
}

func TestManagerPreparesAndPersistsMissingRequestAuthMetadata(t *testing.T) {
	testCases := []struct {
		name   string
		invoke func(context.Context, *Manager, string) error
	}{
		{
			name: "execute",
			invoke: func(ctx context.Context, manager *Manager, model string) error {
				_, errExecute := manager.Execute(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return errExecute
			},
		},
		{
			name: "count_tokens",
			invoke: func(ctx context.Context, manager *Manager, model string) error {
				_, errCount := manager.ExecuteCount(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return errCount
			},
		},
		{
			name: "stream",
			invoke: func(ctx context.Context, manager *Manager, model string) error {
				result, errStream := manager.ExecuteStream(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				if errStream != nil {
					return errStream
				}
				for range result.Chunks {
				}
				return nil
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			manager, store, executor, authID, model := newRequestPrepareManager(t, testCase.name, nil, nil)
			originalInstance := managerAuthInstanceID(t, manager, authID)
			if errInvoke := testCase.invoke(t.Context(), manager, model); errInvoke != nil {
				t.Fatalf("first request error: %v", errInvoke)
			}
			assertPreparedRequestAuth(t, manager, store, executor, authID, 1)
			if currentInstance := managerAuthInstanceID(t, manager, authID); currentInstance != originalInstance {
				t.Fatalf("request preparation replaced runtime instance: got %q, want %q", currentInstance, originalInstance)
			}

			if errInvoke := testCase.invoke(t.Context(), manager, model); errInvoke != nil {
				t.Fatalf("second request error: %v", errInvoke)
			}
			assertPreparedRequestAuth(t, manager, store, executor, authID, 1)
		})
	}
}

func TestManagerRefreshesOnceWhenRequestAuthPreparationReturnsUnauthorized(t *testing.T) {
	manager, store, executor, authID, model := newRequestPrepareManager(t, "unauthorized", nil, nil)
	executor.unauthorizedOne.Store(true)

	if _, errExecute := manager.Execute(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); errExecute != nil {
		t.Fatalf("Execute() error: %v", errExecute)
	}
	assertPreparedRequestAuth(t, manager, store, executor, authID, 2)
	if got := executor.refreshCalls.Load(); got != 1 {
		t.Fatalf("Refresh() calls = %d, want 1", got)
	}
}

func TestManagerRequestAuthPreparationCancellationDoesNotMarkCredentialFailure(t *testing.T) {
	prepareStart := make(chan struct{}, 1)
	prepareGate := make(chan struct{})
	requestPrepareGateRelease(t, prepareGate)
	manager, _, _, authID, model := newRequestPrepareManager(t, "cancel", prepareStart, prepareGate)
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		errCh <- errExecute
	}()
	waitRequestPrepareStarted(t, prepareStart)
	cancel()

	select {
	case errExecute := <-errCh:
		if !errors.Is(errExecute, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context.Canceled", errExecute)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not return after cancellation")
	}
	current, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("auth disappeared after request cancellation")
	}
	if current.LastError != nil || current.Unavailable {
		t.Fatalf("canceled preparation polluted auth state: %#v", current)
	}
}

func TestManagerRequestAuthPreparationLockWaitIsCancellable(t *testing.T) {
	prepareStart := make(chan struct{}, 1)
	prepareGate := make(chan struct{})
	manager, _, _, _, model := newRequestPrepareManager(t, "cancel-wait", prepareStart, prepareGate)
	releasePreparation := requestPrepareGateRelease(t, prepareGate)

	firstErr := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		firstErr <- errExecute
	}()
	waitRequestPrepareStarted(t, prepareStart)

	ctx, cancel := context.WithCancel(t.Context())
	secondErr := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		secondErr <- errExecute
	}()
	cancel()
	select {
	case errExecute := <-secondErr:
		if !errors.Is(errExecute, context.Canceled) {
			t.Fatalf("second Execute() error = %v, want context.Canceled", errExecute)
		}
	case <-time.After(time.Second):
		t.Fatal("second Execute() did not stop after cancellation")
	}

	releasePreparation()
	select {
	case errExecute := <-firstErr:
		if errExecute != nil {
			t.Fatalf("first Execute() error: %v", errExecute)
		}
	case <-time.After(time.Second):
		t.Fatal("first Execute() did not finish")
	}
}

func TestManagerRequestAuthPersistenceLockWaitIsCancellable(t *testing.T) {
	prepareStart := make(chan struct{}, 1)
	manager, store, _, authID, model := newRequestPrepareManager(t, "cancel-persist-wait", prepareStart, nil)
	store.saveStart = make(chan struct{})
	store.saveGate = make(chan struct{})
	releaseSave := requestPrepareGateRelease(t, store.saveGate)

	replacement, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("auth missing before persistence lock test")
	}
	updateErr := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(t.Context(), replacement)
		updateErr <- errUpdate
	}()
	select {
	case <-store.saveStart:
	case <-time.After(time.Second):
		t.Fatal("blocking store save did not start")
	}

	ctx, cancel := context.WithCancel(t.Context())
	executeErr := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		executeErr <- errExecute
	}()
	waitRequestPrepareStarted(t, prepareStart)
	cancel()
	select {
	case errExecute := <-executeErr:
		if !errors.Is(errExecute, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context.Canceled", errExecute)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not stop while waiting for persistence lock")
	}

	releaseSave()
	select {
	case errUpdate := <-updateErr:
		if errUpdate != nil {
			t.Fatalf("Update() error: %v", errUpdate)
		}
	case <-time.After(time.Second):
		t.Fatal("Update() did not finish")
	}
}

func TestManagerRequestAuthPreparationDoesNotResurrectDeletedCredential(t *testing.T) {
	prepareStart := make(chan struct{}, 1)
	prepareGate := make(chan struct{})
	requestPrepareGateRelease(t, prepareGate)
	manager, _, _, authID, model := newRequestPrepareManager(t, "delete", prepareStart, prepareGate)
	executeErr := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		executeErr <- errExecute
	}()
	waitRequestPrepareStarted(t, prepareStart)
	deleteErr := make(chan error, 1)
	go func() { deleteErr <- manager.Delete(t.Context(), authID) }()

	select {
	case errDelete := <-deleteErr:
		if errDelete != nil {
			t.Fatalf("Delete() error: %v", errDelete)
		}
	case <-time.After(time.Second):
		t.Fatal("Delete() did not finish")
	}
	select {
	case errExecute := <-executeErr:
		if errExecute == nil {
			t.Fatal("Execute() unexpectedly succeeded after auth deletion")
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not finish after auth deletion")
	}
	if _, ok := manager.GetByID(authID); ok {
		t.Fatal("request preparation resurrected deleted auth")
	}
}

func TestManagerRequestAuthPreparationDoesNotOverwriteConcurrentUpdate(t *testing.T) {
	prepareStart := make(chan struct{}, 1)
	prepareGate := make(chan struct{})
	releasePreparation := requestPrepareGateRelease(t, prepareGate)
	manager, _, _, authID, model := newRequestPrepareManager(t, "update", prepareStart, prepareGate)
	executeErr := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		executeErr <- errExecute
	}()
	waitRequestPrepareStarted(t, prepareStart)
	replacement, _ := manager.GetByID(authID)
	replacement.Metadata["project_id"] = "prepared-project"
	replacement.Metadata["marker"] = "concurrent-update"
	updateErr := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(WithSkipPersist(t.Context()), replacement)
		updateErr <- errUpdate
	}()

	select {
	case errUpdate := <-updateErr:
		if errUpdate != nil {
			t.Fatalf("Update() error: %v", errUpdate)
		}
	case <-time.After(time.Second):
		t.Fatal("Update() did not finish")
	}
	releasePreparation()
	select {
	case errExecute := <-executeErr:
		if errExecute != nil {
			t.Fatalf("Execute() error after replacement: %v", errExecute)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not finish after replacement")
	}
	current, ok := manager.GetByID(authID)
	if !ok || requestPrepareString(current.Metadata["marker"]) != "concurrent-update" {
		t.Fatalf("request preparation overwrote replacement auth: %#v", current)
	}
}

func TestManagerRequestAuthPreparationDoesNotOverwriteSameSourceReload(t *testing.T) {
	prepareStart := make(chan struct{}, 1)
	prepareGate := make(chan struct{})
	manager, store, _, authID, model := newRequestPrepareManager(t, "reload", prepareStart, prepareGate)
	releasePreparation := requestPrepareGateRelease(t, prepareGate)

	executeErr := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		executeErr <- errExecute
	}()
	waitRequestPrepareStarted(t, prepareStart)

	replacement, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("auth missing before reload")
	}
	replacement.Metadata["marker"] = "same-source-reload"
	store.setItems(replacement)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatalf("Load() error: %v", errLoad)
	}
	releasePreparation()

	select {
	case errExecute := <-executeErr:
		if errExecute != nil {
			t.Fatalf("Execute() error after reload: %v", errExecute)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not finish after reload")
	}
	current, ok := manager.GetByID(authID)
	if !ok || requestPrepareString(current.Metadata["marker"]) != "same-source-reload" {
		t.Fatalf("request preparation overwrote same-source reload: %#v", current)
	}
}

func TestManagerRequestAuthPersistenceFailureDoesNotMarkCredential(t *testing.T) {
	manager, store, _, authID, model := newRequestPrepareManager(t, "persist-error", nil, nil)
	store.saveErr = errors.New("store unavailable")

	_, errExecute := manager.Execute(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errExecute == nil || !strings.Contains(errExecute.Error(), "store unavailable") {
		t.Fatalf("Execute() error = %v, want persistence error", errExecute)
	}
	current, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("auth disappeared after persistence failure")
	}
	if current.LastError != nil || current.Unavailable || current.Status == StatusError {
		t.Fatalf("persistence failure polluted auth state: %#v", current)
	}
}

func TestManagerPersistsRequestAuthLifecycleTransitionReturnedWithError(t *testing.T) {
	manager, store, executor, authID, _ := newRequestPrepareManager(t, "lifecycle-error", nil, nil)
	executor.updateOnError = true
	executor.prepareErr = requestPreparePersistError{err: &Error{HTTPStatus: http.StatusUnauthorized, Message: "refresh token is no longer valid"}}

	auth, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("auth missing before preparation")
	}
	_, errPrepare := manager.prepareRequestAuth(t.Context(), executor, auth)
	if errPrepare == nil {
		t.Fatal("prepareRequestAuth() error = nil, want terminal refresh error")
	}
	current, ok := manager.GetByID(authID)
	if !ok || current.LifecycleState() != LifecycleStateReauthRequired {
		t.Fatalf("manager lifecycle state = %#v, want reauth_required", current)
	}
	if current.Status != StatusError || current.StatusMessage != "refresh_token_invalid" {
		t.Fatalf("manager runtime status = %q/%q, want lifecycle error", current.Status, current.StatusMessage)
	}
	stored := store.lastAuth()
	if stored == nil || stored.LifecycleState() != LifecycleStateReauthRequired {
		t.Fatalf("persisted lifecycle state = %#v, want reauth_required", stored)
	}
}

func TestManagerDoesNotPersistUnmarkedRequestAuthUpdateReturnedWithError(t *testing.T) {
	manager, store, executor, authID, _ := newRequestPrepareManager(t, "unmarked-lifecycle-error", nil, nil)
	executor.updateOnError = true
	executor.prepareErr = &Error{HTTPStatus: http.StatusUnauthorized, Message: "refresh failed"}

	auth, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("auth missing before preparation")
	}
	_, errPrepare := manager.prepareRequestAuth(t.Context(), executor, auth)
	if errPrepare == nil {
		t.Fatal("prepareRequestAuth() error = nil, want refresh error")
	}
	current, ok := manager.GetByID(authID)
	if !ok || current.LifecycleState() != "" {
		t.Fatalf("manager lifecycle state = %#v, want unchanged auth", current)
	}
	if stored := store.lastAuth(); stored != nil && stored.LifecycleState() != "" {
		t.Fatalf("persisted lifecycle state = %#v, want unchanged auth", stored)
	}
}

func TestAntigravityCreditsFallbackReturnsRequestAuthPreparationError(t *testing.T) {
	manager, _, executor, authID, _ := newRequestPrepareManager(t, "credits-error", nil, nil)
	model := "claude-credits-request-prepare"
	registry.GetGlobalRegistry().RegisterClient(authID, "antigravity", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	executor.prepareErr = &Error{HTTPStatus: http.StatusTooManyRequests, Message: "project discovery rate limited"}

	_, ok, errCredits := manager.tryAntigravityCreditsExecute(t.Context(), cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if ok {
		t.Fatal("credits fallback unexpectedly succeeded")
	}
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(errCredits, &statusErr) || statusErr.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("credits fallback error = %v, want 429", errCredits)
	}
	current, exists := manager.GetByID(authID)
	if !exists || current.LastError == nil || current.LastError.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("credits preparation failure was not recorded: %#v", current)
	}
}

func TestManagerSerializesConcurrentRequestAuthPreparation(t *testing.T) {
	prepareStart := make(chan struct{}, 1)
	prepareGate := make(chan struct{})
	releasePreparation := requestPrepareGateRelease(t, prepareGate)
	manager, store, executor, authID, model := newRequestPrepareManager(t, "concurrent", prepareStart, prepareGate)

	errs := make(chan error, 2)
	invoke := func() {
		_, errExecute := manager.Execute(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		errs <- errExecute
	}
	go invoke()
	waitRequestPrepareStarted(t, prepareStart)
	go invoke()
	releasePreparation()

	for range 2 {
		if errExecute := <-errs; errExecute != nil {
			t.Fatalf("concurrent Execute error: %v", errExecute)
		}
	}
	assertPreparedRequestAuth(t, manager, store, executor, authID, 1)
}

func newRequestPrepareManager(t *testing.T, suffix string, prepareStart, prepareGate chan struct{}) (*Manager, *requestPrepareStore, *requestPrepareExecutor, string, string) {
	t.Helper()
	authID := "auth-request-prepare-" + suffix
	model := "request-prepare-model-" + suffix
	store := &requestPrepareStore{}
	executor := &requestPrepareExecutor{prepareStart: prepareStart, prepareGate: prepareGate}
	manager := NewManager(store, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &Auth{
		ID:         authID,
		Provider:   "antigravity",
		FileName:   authID + ".json",
		Attributes: map[string]string{SourceHashAttributeKey: "request-prepare-source-" + suffix},
		Metadata:   map[string]any{"access_token": "token", "refresh_token": "refresh-token"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("Register() error: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "antigravity", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	return manager, store, executor, authID, model
}

func assertPreparedRequestAuth(t *testing.T, manager *Manager, store *requestPrepareStore, executor *requestPrepareExecutor, authID string, wantPrepareCalls int32) {
	t.Helper()
	if got := executor.prepareCalls.Load(); got != wantPrepareCalls {
		t.Fatalf("prepare calls = %d, want %d", got, wantPrepareCalls)
	}
	if got := store.saveCount.Load(); got < 1 {
		t.Fatalf("save count = %d, want at least 1", got)
	}
	stored := store.lastAuth()
	if stored == nil || requestPrepareString(stored.Metadata["project_id"]) != "prepared-project" {
		t.Fatalf("persisted project_id = %v, want prepared-project", stored)
	}
	current, ok := manager.GetByID(authID)
	if !ok || requestPrepareString(current.Metadata["project_id"]) != "prepared-project" {
		t.Fatalf("manager project_id = %v, want prepared-project", current)
	}
}

func TestUpdateIfCurrentRejectsReplacedAuth(t *testing.T) {
	manager, _, _, authID, _ := newRequestPrepareManager(t, "conditional-update", nil, nil)
	expected, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("expected registered auth")
	}

	replacement := expected.Clone()
	replacement.Attributes[SourceHashAttributeKey] = "replacement-source"
	replacement.Metadata["access_token"] = "replacement-token"
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatalf("Update() error: %v", errUpdate)
	}

	staleResult := expected.Clone()
	staleResult.Metadata["access_token"] = "stale-background-token"
	installed, current, errConditional := manager.UpdateIfCurrent(t.Context(), expected, staleResult)
	if errConditional != nil {
		t.Fatalf("UpdateIfCurrent() error: %v", errConditional)
	}
	if current || installed != nil {
		t.Fatalf("UpdateIfCurrent() = (%v, %v), want stale rejection", installed, current)
	}

	got, ok := manager.GetByID(authID)
	if !ok || requestPrepareString(got.Metadata["access_token"]) != "replacement-token" {
		t.Fatalf("current access token = %v, want replacement-token", got)
	}
}

func TestUpdateIfCurrentPersistsMatchingAuth(t *testing.T) {
	manager, store, _, authID, _ := newRequestPrepareManager(t, "conditional-success", nil, nil)
	expected, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("expected registered auth")
	}

	updated := expected.Clone()
	updated.Metadata["access_token"] = "background-token"
	installed, current, errConditional := manager.UpdateIfCurrent(t.Context(), expected, updated)
	if errConditional != nil {
		t.Fatalf("UpdateIfCurrent() error: %v", errConditional)
	}
	if !current || installed == nil {
		t.Fatalf("UpdateIfCurrent() = (%v, %v), want installed auth", installed, current)
	}
	if got := requestPrepareString(installed.Metadata["access_token"]); got != "background-token" {
		t.Fatalf("installed access token = %q, want background-token", got)
	}
	if stored := store.lastAuth(); stored == nil || requestPrepareString(stored.Metadata["access_token"]) != "background-token" {
		t.Fatalf("persisted auth = %v, want background-token", stored)
	}
}

func requestPrepareString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case []byte:
		return strings.TrimSpace(string(typed))
	default:
		return ""
	}
}

func requestPrepareGateRelease(t *testing.T, gate chan struct{}) func() {
	t.Helper()
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return release
}

func waitRequestPrepareStarted(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request auth preparation did not start")
	}
}
