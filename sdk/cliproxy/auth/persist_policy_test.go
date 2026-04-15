package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type countingStore struct {
	saveCount   atomic.Int32
	deleteCount atomic.Int32
	saveErr     error
	deleteErr   error
}

type blockingDeleteStore struct {
	deleteStarted chan struct{}
	releaseDelete chan struct{}
	mu            sync.Mutex
	records       map[string]*Auth
}

type contextAwareBlockingDeleteStore struct {
	deleteStarted chan struct{}
	releaseDelete chan struct{}
	mu            sync.Mutex
	records       map[string]*Auth
}

type authScopedCloserExecutor struct {
	closedAuthIDs []string
	reasons       []string
	mu            sync.Mutex
}

type hashingFileStore struct {
	baseDir string
}

func (s *hashingFileStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *hashingFileStore) Save(_ context.Context, auth *Auth) (string, error) {
	if auth == nil {
		return "", nil
	}
	path := strings.TrimSpace(auth.FileName)
	if path == "" && auth.Attributes != nil {
		path = strings.TrimSpace(auth.Attributes["path"])
	}
	if path == "" {
		path = filepath.Join(s.baseDir, auth.ID)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	raw, err := CanonicalMetadataBytes(auth)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return "", err
	}
	SetSourceHashAttribute(auth, raw)
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["path"] = path
	return path, nil
}

func (s *hashingFileStore) Delete(context.Context, string) error { return nil }

func (s *countingStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *countingStore) Save(context.Context, *Auth) (string, error) {
	s.saveCount.Add(1)
	if s.saveErr != nil {
		return "", s.saveErr
	}
	return "", nil
}

func (s *countingStore) Delete(context.Context, string) error {
	s.deleteCount.Add(1)
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return nil
}

func (s *blockingDeleteStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *blockingDeleteStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]*Auth)
	}
	if auth != nil {
		s.records[auth.ID] = auth.Clone()
	}
	return "", nil
}

func (s *blockingDeleteStore) Delete(_ context.Context, id string) error {
	if s.deleteStarted != nil {
		close(s.deleteStarted)
	}
	if s.releaseDelete != nil {
		<-s.releaseDelete
	}
	s.mu.Lock()
	delete(s.records, id)
	s.mu.Unlock()
	return nil
}

func (s *blockingDeleteStore) Has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.records[id]
	return ok
}

func (s *contextAwareBlockingDeleteStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *contextAwareBlockingDeleteStore) Save(ctx context.Context, auth *Auth) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]*Auth)
	}
	if auth != nil {
		s.records[auth.ID] = auth.Clone()
	}
	return "", nil
}

func (s *contextAwareBlockingDeleteStore) Delete(_ context.Context, id string) error {
	if s.deleteStarted != nil {
		close(s.deleteStarted)
	}
	if s.releaseDelete != nil {
		<-s.releaseDelete
	}
	s.mu.Lock()
	delete(s.records, id)
	s.mu.Unlock()
	return nil
}

func (s *contextAwareBlockingDeleteStore) Has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.records[id]
	return ok
}

func (e *authScopedCloserExecutor) Identifier() string { return "codex" }

func (e *authScopedCloserExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *authScopedCloserExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}

func (e *authScopedCloserExecutor) Refresh(context.Context, *Auth) (*Auth, error) {
	return nil, nil
}

func (e *authScopedCloserExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *authScopedCloserExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (e *authScopedCloserExecutor) CloseAuthExecutionSessions(authID string, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closedAuthIDs = append(e.closedAuthIDs, authID)
	e.reasons = append(e.reasons, reason)
}

func TestWithSkipPersist_DisablesUpdatePersistence(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "antigravity",
		Metadata: map[string]any{"type": "antigravity"},
	}

	if _, err := mgr.Update(context.Background(), auth); err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 1 {
		t.Fatalf("expected 1 Save call, got %d", got)
	}

	ctxSkip := WithSkipPersist(context.Background())
	if _, err := mgr.Update(ctxSkip, auth); err != nil {
		t.Fatalf("Update(skipPersist) returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 1 {
		t.Fatalf("expected Save call count to remain 1, got %d", got)
	}
}

func TestWithSkipPersist_DisablesRegisterPersistence(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "antigravity",
		Metadata: map[string]any{"type": "antigravity"},
	}

	if _, err := mgr.Register(WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatalf("Register(skipPersist) returned error: %v", err)
	}
	if got := store.saveCount.Load(); got != 0 {
		t.Fatalf("expected 0 Save calls, got %d", got)
	}
}

func TestWithSkipPersist_DisablesDeletePersistence(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "antigravity",
		Metadata: map[string]any{"type": "antigravity"},
	}

	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := mgr.Delete(WithSkipPersist(context.Background()), auth.ID); err != nil {
		t.Fatalf("Delete(skipPersist) returned error: %v", err)
	}
	if got := store.deleteCount.Load(); got != 0 {
		t.Fatalf("expected 0 Delete calls, got %d", got)
	}
	if _, ok := mgr.GetByID(auth.ID); ok {
		t.Fatal("expected auth to be removed from runtime state")
	}
}

func TestRegister_FailedPersistReturnsErrorAndKeepsRuntimeStateUnchanged(t *testing.T) {
	store := &countingStore{saveErr: errors.New("save failed")}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-register-failure",
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
	}

	if _, err := mgr.Register(context.Background(), auth); err == nil {
		t.Fatal("Register() error = nil, want save failure")
	}
	if _, ok := mgr.GetByID(auth.ID); ok {
		t.Fatal("expected auth to remain absent from runtime state after register failure")
	}
}

func TestUpdate_FailedPersistReturnsErrorAndKeepsPreviousRuntimeState(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-update-failure",
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
		Attributes: map[string]string{
			"api_key": "old-key",
		},
	}

	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	store.saveErr = errors.New("save failed")

	replacement := auth.Clone()
	replacement.Attributes["api_key"] = "new-key"
	if _, err := mgr.Update(context.Background(), replacement); err == nil {
		t.Fatal("Update() error = nil, want save failure")
	}

	current, ok := mgr.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain in runtime state after update failure")
	}
	if got := current.Attributes["api_key"]; got != "old-key" {
		t.Fatalf("runtime auth api_key = %q, want %q", got, "old-key")
	}
}

func TestDelete_PersistsWithoutMetadata(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:         "auth-no-metadata",
		Provider:   "gemini",
		Attributes: map[string]string{"api_key": "test-key"},
	}

	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := mgr.Delete(context.Background(), auth.ID); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if got := store.deleteCount.Load(); got != 1 {
		t.Fatalf("expected 1 Delete call, got %d", got)
	}
	if _, ok := mgr.GetByID(auth.ID); ok {
		t.Fatal("expected auth to be removed from runtime state")
	}
}

func TestDelete_UnregistersClientModels(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-with-models",
		Provider: "claude",
		Metadata: map[string]any{"type": "claude"},
	}

	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })

	if err := mgr.Delete(context.Background(), auth.ID); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if models := reg.GetModelsForClient(auth.ID); len(models) != 0 {
		t.Fatalf("GetModelsForClient() len = %d, want 0", len(models))
	}
}

func TestDelete_FailedPersistKeepsRuntimeState(t *testing.T) {
	store := &countingStore{deleteErr: errors.New("delete failed")}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-delete-failure",
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
	}

	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := mgr.Delete(context.Background(), auth.ID); err == nil {
		t.Fatal("Delete() error = nil, want delete failure")
	}
	if _, ok := mgr.GetByID(auth.ID); !ok {
		t.Fatal("expected auth to remain in runtime state after delete failure")
	}
}

func TestDelete_ConcurrentReplacementKeepsNewRuntimeAuth(t *testing.T) {
	store := &blockingDeleteStore{
		deleteStarted: make(chan struct{}),
		releaseDelete: make(chan struct{}),
	}
	mgr := NewManager(store, nil, nil)
	original := &Auth{
		ID:       "auth-replaced",
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
	}
	if _, err := mgr.Register(context.Background(), original); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- mgr.Delete(context.Background(), original.ID)
	}()

	<-store.deleteStarted
	replacement := original.Clone()
	replacement.Attributes = map[string]string{"api_key": "new-key"}
	replacement.UpdatedAt = time.Now().UTC().Add(time.Second)
	updateDone := make(chan error, 1)
	go func() {
		_, err := mgr.Update(context.Background(), replacement)
		updateDone <- err
	}()

	select {
	case err := <-updateDone:
		t.Fatalf("expected update to wait for in-flight delete, got err=%v", err)
	default:
	}
	close(store.releaseDelete)

	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if err := <-updateDone; err != nil {
		t.Fatalf("Update returned error: %v", err)
	}

	current, ok := mgr.GetByID(original.ID)
	if !ok || current == nil {
		t.Fatal("expected replacement auth to remain in runtime state")
	}
	if got := current.Attributes["api_key"]; got != "new-key" {
		t.Fatalf("replacement auth api_key = %q, want %q", got, "new-key")
	}
	if !store.Has(original.ID) {
		t.Fatalf("expected replacement auth %q to remain persisted after delete race", original.ID)
	}
}

func TestDelete_ConcurrentReplacementPersistsWithCanceledDeleteContext(t *testing.T) {
	store := &contextAwareBlockingDeleteStore{
		deleteStarted: make(chan struct{}),
		releaseDelete: make(chan struct{}),
	}
	mgr := NewManager(store, nil, nil)
	original := &Auth{
		ID:       "auth-replaced-canceled-context",
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
	}
	if _, err := mgr.Register(context.Background(), original); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	deleteCtx, cancelDelete := context.WithCancel(context.Background())
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- mgr.Delete(deleteCtx, original.ID)
	}()

	<-store.deleteStarted
	replacement := original.Clone()
	replacement.Attributes = map[string]string{"api_key": "new-key"}
	replacement.UpdatedAt = time.Now().UTC().Add(time.Second)
	updateDone := make(chan error, 1)
	go func() {
		_, err := mgr.Update(context.Background(), replacement)
		updateDone <- err
	}()

	select {
	case err := <-updateDone:
		t.Fatalf("expected update to wait for in-flight delete, got err=%v", err)
	default:
	}
	cancelDelete()
	close(store.releaseDelete)

	if err := <-deleteDone; err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if err := <-updateDone; err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
	if !store.Has(original.ID) {
		t.Fatalf("expected replacement auth %q to remain persisted after canceled delete context", original.ID)
	}
}

func TestDelete_ClosesAuthScopedSessionsForCodex(t *testing.T) {
	store := &countingStore{}
	mgr := NewManager(store, nil, nil)
	closer := &authScopedCloserExecutor{}
	mgr.RegisterExecutor(closer)
	auth := &Auth{
		ID:       "codex-auth",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex"},
	}

	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := mgr.Delete(context.Background(), auth.ID); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	closer.mu.Lock()
	defer closer.mu.Unlock()
	if len(closer.closedAuthIDs) != 1 {
		t.Fatalf("closed auth count = %d, want 1", len(closer.closedAuthIDs))
	}
	if closer.closedAuthIDs[0] != auth.ID {
		t.Fatalf("closed auth ID = %q, want %q", closer.closedAuthIDs[0], auth.ID)
	}
	if closer.reasons[0] != "auth_removed" {
		t.Fatalf("close reason = %q, want %q", closer.reasons[0], "auth_removed")
	}
}

func TestMarkResult_ConcurrentDeleteDoesNotDeadlock(t *testing.T) {
	store := &blockingDeleteStore{
		deleteStarted: make(chan struct{}),
		releaseDelete: make(chan struct{}),
	}
	mgr := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-markresult-delete-race",
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
	}
	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- mgr.Delete(context.Background(), auth.ID)
	}()

	<-store.deleteStarted
	markDone := make(chan struct{})
	go func() {
		mgr.MarkResult(context.Background(), Result{
			AuthID:   auth.ID,
			Provider: auth.Provider,
			Model:    "test-model",
			Success:  false,
			Error:    &Error{HTTPStatus: 429, Message: "quota exhausted"},
		})
		close(markDone)
	}()

	close(store.releaseDelete)

	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("Delete returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Delete deadlocked with concurrent MarkResult")
	}

	select {
	case <-markDone:
	case <-time.After(2 * time.Second):
		t.Fatal("MarkResult deadlocked with concurrent Delete")
	}
}

func TestUpdate_ClearsTransientFailureStateForSameIDReplacement(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	auth := &Auth{
		ID:       "auth-markresult-update-state",
		Provider: "gemini",
		Attributes: map[string]string{
			SourceHashAttributeKey: "old-hash",
		},
		Metadata: map[string]any{"type": "gemini"},
	}
	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	mgr.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: auth.Provider,
		Model:    "test-model",
		Success:  false,
		Error:    &Error{HTTPStatus: 429, Message: "quota exhausted"},
	})

	reloaded := &Auth{
		ID:       auth.ID,
		Provider: auth.Provider,
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "new-hash",
		},
		Metadata: map[string]any{"type": "gemini"},
	}
	if _, err := mgr.Update(WithSkipPersist(context.Background()), reloaded); err != nil {
		t.Fatalf("Update returned error: %v", err)
	}

	current, ok := mgr.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if current.Unavailable {
		t.Fatal("expected same-ID replacement to clear unavailable flag")
	}
	if current.Status != StatusActive {
		t.Fatalf("status = %q, want %q", current.Status, StatusActive)
	}
	if current.LastError != nil {
		t.Fatalf("last error = %#v, want nil", current.LastError)
	}
	if current.StatusMessage != "" {
		t.Fatalf("status message = %q, want empty", current.StatusMessage)
	}
	if !current.NextRetryAfter.IsZero() {
		t.Fatalf("next retry after = %v, want zero", current.NextRetryAfter)
	}
	if current.Quota.Exceeded {
		t.Fatalf("quota = %#v, want zero state", current.Quota)
	}
	if len(current.ModelStates) != 0 {
		t.Fatalf("expected replacement auth to clear stale model states, got %#v", current.ModelStates)
	}
}

func TestUpdate_DoesNotCarryTransientFailureStateWithoutSourceHash(t *testing.T) {
	mgr := NewManager(nil, nil, nil)
	auth := &Auth{
		ID:       "auth-hashless-replacement",
		Provider: "gemini",
		Metadata: map[string]any{"type": "gemini"},
	}
	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	mgr.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: auth.Provider,
		Model:    "test-model",
		Success:  false,
		Error:    &Error{HTTPStatus: 429, Message: "quota exhausted"},
	})

	reloaded := &Auth{
		ID:       auth.ID,
		Provider: auth.Provider,
		Status:   StatusActive,
		Metadata: map[string]any{"type": "gemini", "label": "replacement"},
	}
	if _, err := mgr.Update(WithSkipPersist(context.Background()), reloaded); err != nil {
		t.Fatalf("Update returned error: %v", err)
	}

	current, ok := mgr.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if current.Unavailable {
		t.Fatal("expected hashless same-ID replacement to clear unavailable flag")
	}
	if current.Status != StatusActive {
		t.Fatalf("status = %q, want %q", current.Status, StatusActive)
	}
	if current.LastError != nil {
		t.Fatalf("last error = %#v, want nil", current.LastError)
	}
	if current.StatusMessage != "" {
		t.Fatalf("status message = %q, want empty", current.StatusMessage)
	}
	if !current.NextRetryAfter.IsZero() {
		t.Fatalf("next retry after = %v, want zero", current.NextRetryAfter)
	}
	if current.Quota.Exceeded {
		t.Fatalf("quota = %#v, want zero state", current.Quota)
	}
	if len(current.ModelStates) != 0 {
		t.Fatalf("expected hashless same-ID replacement to clear stale model states, got %#v", current.ModelStates)
	}
}

func TestUpdate_PreservesModelStatesAfterInProcessFileRewrite(t *testing.T) {
	dir := t.TempDir()
	store := &hashingFileStore{baseDir: dir}
	mgr := NewManager(store, nil, nil)

	path := filepath.Join(dir, "auth.json")
	initialMetadata := map[string]any{"type": "gemini", "disabled": false, "label": "initial"}
	initialRaw, err := json.Marshal(initialMetadata)
	if err != nil {
		t.Fatalf("marshal initial metadata: %v", err)
	}
	auth := &Auth{
		ID:       "auth.json",
		Provider: "gemini",
		FileName: path,
		Status:   StatusActive,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: initialMetadata,
		ModelStates: map[string]*ModelState{
			"test-model": {
				Unavailable:    true,
				NextRetryAfter: time.Now().Add(5 * time.Minute),
			},
		},
	}
	SetSourceHashAttribute(auth, initialRaw)
	if _, err := mgr.Register(context.Background(), auth); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	mgr.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: auth.Provider,
		Model:    "test-model",
		Success:  false,
		Error:    &Error{HTTPStatus: 429, Message: "quota exhausted"},
	})

	current, ok := mgr.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to be registered")
	}
	current.Metadata = map[string]any{"type": "gemini", "disabled": false, "label": "updated"}
	if _, err := mgr.Update(context.Background(), current); err != nil {
		t.Fatalf("Update returned error: %v", err)
	}

	reloadedRaw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted auth file: %v", err)
	}
	reloaded := &Auth{
		ID:       auth.ID,
		Provider: auth.Provider,
		FileName: path,
		Status:   StatusActive,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "gemini", "disabled": false, "label": "updated"},
	}
	SetSourceHashAttribute(reloaded, reloadedRaw)
	if _, err := mgr.Update(WithSkipPersist(context.Background()), reloaded); err != nil {
		t.Fatalf("reload Update returned error: %v", err)
	}

	current, ok = mgr.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if len(current.ModelStates) == 0 {
		t.Fatal("expected in-process rewrite to preserve model states on watcher reload")
	}
	if !current.Unavailable {
		t.Fatal("expected in-process rewrite to preserve auth unavailable state on watcher reload")
	}
	if current.LastError == nil || current.LastError.HTTPStatus != 429 {
		t.Fatalf("last error = %#v, want preserved 429 error", current.LastError)
	}
	if current.Status != StatusError {
		t.Fatalf("status = %q, want %q", current.Status, StatusError)
	}
	if current.StatusMessage == "" {
		t.Fatal("expected in-process rewrite to preserve auth status message")
	}
	if current.NextRetryAfter.IsZero() {
		t.Fatal("expected in-process rewrite to preserve auth cooldown")
	}
	if !current.Quota.Exceeded {
		t.Fatalf("quota = %#v, want preserved exceeded state", current.Quota)
	}
}
