package cliproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type serviceFailingDeleteStore struct{}

type serviceCountingDeleteStore struct {
	deleteCount atomic.Int32
}

type serviceToggleSaveStore struct {
	saveCount atomic.Int32
	failSave  atomic.Bool
}

type serviceDeleteSideEffectStore struct {
	deleteCount atomic.Int32
	onDelete    func(id string)
}

type serviceRecordingStore struct {
	mu    sync.Mutex
	saved *coreauth.Auth
}

type serviceChatGPTWebReplacementHook struct {
	coreauth.NoopHook
	replacements atomic.Int32
	updated      chan struct{}
}

func waitForModelSyncTaskRunning(t *testing.T, service *Service, authID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		service.modelSyncMu.Lock()
		state, ok := service.modelSyncPending[authID]
		running := ok && state.running
		service.modelSyncMu.Unlock()
		if running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("model sync task %q did not enter the running state", authID)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForAuthModelTransitionWaiter(t *testing.T, service *Service, authID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		service.authModelTransitionMu.Lock()
		entry := service.authModelTransitionLocks[authID]
		waiting := entry != nil && entry.references >= 2
		service.authModelTransitionMu.Unlock()
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("auth model transition waiter for %q was not registered", authID)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForChatGPTWebModelFetchWaiter(t *testing.T, service *Service, authID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		service.chatGPTWebModelFetchMu.Lock()
		entry := service.chatGPTWebModelFetchLocks[authID]
		waiting := entry != nil && entry.references >= 2
		service.chatGPTWebModelFetchMu.Unlock()
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ChatGPT Web model fetch waiter for %q was not registered", authID)
		}
		time.Sleep(time.Millisecond)
	}
}

func (hook *serviceChatGPTWebReplacementHook) OnAuthUpdated(ctx context.Context, _ *coreauth.Auth) {
	if !coreauth.ChatGPTWebCredentialReplaced(ctx) {
		return
	}
	hook.replacements.Add(1)
	select {
	case hook.updated <- struct{}{}:
	default:
	}
}

func (s *serviceFailingDeleteStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (s *serviceFailingDeleteStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	if auth == nil {
		return "", nil
	}
	return "", nil
}

func (s *serviceFailingDeleteStore) Delete(context.Context, string) error {
	return errors.New("delete failed")
}

func (s *serviceFailingDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, id, _ string) error {
	return s.Delete(ctx, id)
}

func (s *serviceCountingDeleteStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (s *serviceCountingDeleteStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	if auth == nil {
		return "", nil
	}
	return "", nil
}

func (s *serviceCountingDeleteStore) Delete(context.Context, string) error {
	s.deleteCount.Add(1)
	return nil
}

func (s *serviceCountingDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, id, _ string) error {
	return s.Delete(ctx, id)
}

func (s *serviceToggleSaveStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (s *serviceToggleSaveStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	s.saveCount.Add(1)
	if auth == nil {
		return "", nil
	}
	if s.failSave.Load() {
		return "", errors.New("save failed")
	}
	return "", nil
}

func (s *serviceToggleSaveStore) Delete(context.Context, string) error { return nil }

func (s *serviceToggleSaveStore) DeleteIfSourceHashMatches(ctx context.Context, id, _ string) error {
	return s.Delete(ctx, id)
}

func (s *serviceDeleteSideEffectStore) List(context.Context) ([]*coreauth.Auth, error) {
	return nil, nil
}

func (s *serviceDeleteSideEffectStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	if auth == nil {
		return "", nil
	}
	return "", nil
}

func (s *serviceDeleteSideEffectStore) Delete(_ context.Context, id string) error {
	s.deleteCount.Add(1)
	if s.onDelete != nil {
		s.onDelete(id)
	}
	return nil
}

func (s *serviceDeleteSideEffectStore) DeleteIfSourceHashMatches(ctx context.Context, id, _ string) error {
	return s.Delete(ctx, id)
}

func (s *serviceRecordingStore) List(context.Context) ([]*coreauth.Auth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saved == nil {
		return nil, nil
	}
	return []*coreauth.Auth{s.saved.Clone()}, nil
}

func (s *serviceRecordingStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if auth == nil {
		return "", nil
	}
	s.saved = auth.Clone()
	return auth.ID, nil
}

func (s *serviceRecordingStore) SaveIfSourceHashMatches(ctx context.Context, auth *coreauth.Auth, _ string) (string, error) {
	return s.Save(ctx, auth)
}

func (s *serviceRecordingStore) Delete(context.Context, string) error {
	s.mu.Lock()
	s.saved = nil
	s.mu.Unlock()
	return nil
}

func (s *serviceRecordingStore) DeleteIfSourceHashMatches(ctx context.Context, id, _ string) error {
	return s.Delete(ctx, id)
}

func (s *serviceRecordingStore) snapshot() *coreauth.Auth {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saved == nil {
		return nil
	}
	return s.saved.Clone()
}

func TestServiceApplyCoreAuthAddOrUpdate_ModelSyncWorkerEventuallyRegistersModels(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	service.startModelSyncLoop(context.Background())
	defer service.stopModelSyncLoop()

	authID := "service-async-model-sync-auth"
	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected async model sync to register models for %q", authID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestModelSyncLoopDrainsTaskAcceptedBeforeStartup(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	authID := "service-model-sync-before-start"
	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})
	auth := &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	}
	if _, errRegister := service.coreManager.Register(coreauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	if !service.enqueueModelSyncTask(authID, true) {
		t.Fatal("model sync task was not retained before worker startup")
	}

	service.startModelSyncLoop(t.Context())
	defer service.stopModelSyncLoop()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("startup did not drain the retained model sync task")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestModelSyncStopPreservesTasksFromNewGeneration(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	close(done)
	service := &Service{
		modelSyncCancel:     cancel,
		modelSyncDone:       done,
		modelSyncQueue:      make(chan string, 1),
		modelSyncPending:    map[string]modelSyncTaskState{"same": {epoch: 1, running: true}},
		modelSyncGeneration: 1,
		modelSyncNextEpoch:  1,
	}

	service.stopModelSyncLoop()
	service.cancelModelSyncTask("same")
	if !service.enqueueModelSyncTask("same", true) {
		t.Fatal("new task was not retained while the pool was stopped")
	}
	if service.completeModelSyncTask("same", 1, 1, true) {
		t.Fatal("stopped worker generation requested another sync")
	}

	service.modelSyncMu.Lock()
	state, pending := service.modelSyncPending["same"]
	service.modelSyncMu.Unlock()
	if !pending {
		t.Fatal("stopped worker generation removed a newly retained task")
	}
	if state.epoch == 1 {
		t.Fatal("recreated task reused the stopped worker epoch")
	}
}

func TestModelSyncStopCancelsTransitionWait(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
	authID := "service-model-sync-cancel-transition"
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	}); errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	unlockTransition := service.lockAuthModelTransition(authID)
	service.startModelSyncLoop(t.Context())
	if !service.enqueueModelSync(authID) {
		unlockTransition()
		t.Fatal("model sync task was not accepted")
	}
	waitForAuthModelTransitionWaiter(t, service, authID)

	stopped := make(chan struct{})
	go func() {
		service.stopModelSyncLoop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		unlockTransition()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("model sync stop remained blocked after the transition lock was released")
		}
		t.Fatal("model sync stop did not cancel the transition lock wait")
	}
	unlockTransition()
}

func TestServiceSyncAuthModelsUsesMutationBeforeTransition(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
	mutationLocked := make(chan struct{}, 1)
	service.modelSyncMutationLockedObserved = func(*coreauth.Auth) {
		mutationLocked <- struct{}{}
	}
	authID := "service-model-sync-lock-order"
	_, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
		ModelStates: map[string]*coreauth.ModelState{
			"removed-model": {
				Quota: coreauth.QuotaState{BackoffLevel: 1},
			},
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	unlockTransition := service.lockAuthModelTransition(authID)
	var transitionOnce sync.Once
	releaseTransition := func() {
		transitionOnce.Do(unlockTransition)
	}

	syncCtx, cancelSync := context.WithCancel(t.Context())
	syncDone := make(chan struct{})
	go func() {
		service.syncAuthModels(syncCtx, authID)
		close(syncDone)
	}()
	waitSync := func() bool {
		select {
		case <-syncDone:
			return true
		case <-time.After(time.Second):
			return false
		}
	}
	t.Cleanup(func() {
		cancelSync()
		releaseTransition()
		if !waitSync() {
			t.Errorf("model sync did not stop during test cleanup")
		}
	})
	select {
	case <-mutationLocked:
	case <-time.After(time.Second):
		cancelSync()
		releaseTransition()
		if !waitSync() {
			t.Fatal("model sync did not stop after lock-order observation failed")
		}
		t.Fatal("model synchronization did not acquire the auth mutation lock")
	}

	releaseTransition()
	if !waitSync() {
		cancelSync()
		if !waitSync() {
			t.Fatal("model sync did not stop after transition lock release timed out")
		}
		t.Fatal("model sync did not finish after transition lock release")
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("auth disappeared during model synchronization")
	}
	if _, stale := current.ModelStates["removed-model"]; stale {
		t.Fatal("model synchronization did not persist stale model-state cleanup")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) == 0 {
		t.Fatal("model synchronization did not update the registry")
	}
}

func TestServiceFieldUpdateHookReusesOuterMutationLockDuringModelStateCleanup(t *testing.T) {
	store := &serviceRecordingStore{}
	manager := coreauth.NewManager(store, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}

	authID := "service-field-update-reentrant-model-state"
	auth := &coreauth.Auth{
		ID:       authID,
		FileName: authID,
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"priority": "9",
		},
		Metadata: map[string]any{
			"type":     "codex",
			"priority": 9,
		},
		ModelStates: map[string]*coreauth.ModelState{
			"removed-model": {
				Unavailable:    true,
				NextRetryAfter: time.Now().Add(time.Hour),
			},
		},
	}
	if errSync := coreauth.SyncPersistedMetadataAndSourceHash(auth, []byte(`{"type":"codex","priority":9}`)); errSync != nil {
		t.Fatal(errSync)
	}
	registered, errRegister := manager.Register(t.Context(), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })
	manager.AddHook(authMaintenanceHook{service: service})

	lockedCtx, unlockMutation, errLock := manager.LockAuthMutation(t.Context(), registered)
	if errLock != nil {
		t.Fatal(errLock)
	}
	mutationLocked := true
	defer func() {
		if mutationLocked {
			unlockMutation()
		}
	}()

	candidate := registered.Clone()
	candidate.Attributes["priority"] = "0"
	candidate.Metadata["priority"] = 0
	type updateResult struct {
		auth    *coreauth.Auth
		current bool
		err     error
	}
	updateDone := make(chan updateResult, 1)
	go func() {
		updated, current, errUpdate := manager.UpdateIfCurrentSourceHash(lockedCtx, registered, candidate)
		updateDone <- updateResult{auth: updated, current: current, err: errUpdate}
	}()

	var result updateResult
	select {
	case result = <-updateDone:
		unlockMutation()
		mutationLocked = false
	case <-time.After(500 * time.Millisecond):
		unlockMutation()
		mutationLocked = false
		select {
		case <-updateDone:
		case <-time.After(time.Second):
		}
		t.Fatal("field update deadlocked while the model-state hook reused the outer mutation lock")
	}
	if result.err != nil || !result.current || result.auth == nil {
		t.Fatalf("field update result = (%#v, %v, %v)", result.auth, result.current, result.err)
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("updated auth is missing")
	}
	if current.Attributes["priority"] != "0" {
		t.Fatalf("runtime priority = %q, want 0", current.Attributes["priority"])
	}
	if len(current.ModelStates) != 0 {
		t.Fatalf("runtime model states = %#v, want none", current.ModelStates)
	}
	persisted := store.snapshot()
	if persisted == nil || persisted.Attributes["priority"] != "0" || len(persisted.ModelStates) != 0 {
		t.Fatalf("persisted auth = %#v, want priority 0 without stale model states", persisted)
	}
	if _, pending := service.modelSyncPending[authID]; pending {
		t.Fatal("non-Antigravity field update unexpectedly queued model synchronization")
	}
}

func TestAuthHookRetainsModelSyncWhenTransitionWaitIsCanceled(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
	authID := "service-hook-canceled-transition"
	installed, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })
	service.syncAuthModels(t.Context(), authID)
	if len(registry.GetGlobalRegistry().GetModelsForClient(authID)) == 0 {
		t.Fatal("initial models were not registered")
	}
	manager.SetHook(authMaintenanceHook{service: service})
	service.startModelSyncLoop(t.Context())
	defer service.stopModelSyncLoop()

	unlockTransition := service.lockAuthModelTransition(authID)
	transitionLocked := true
	defer func() {
		if transitionLocked {
			unlockTransition()
		}
	}()

	updated := installed.Clone()
	updated.Disabled = true
	updated.Status = coreauth.StatusDisabled
	updateCtx, cancelUpdate := context.WithCancel(t.Context())
	defer cancelUpdate()
	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(coreauth.WithSkipPersist(updateCtx), updated)
		updateDone <- errUpdate
	}()
	waitForAuthModelTransitionWaiter(t, service, authID)
	cancelUpdate()
	select {
	case errUpdate := <-updateDone:
		if errUpdate != nil {
			t.Fatalf("Update() error: %v", errUpdate)
		}
	case <-time.After(time.Second):
		unlockTransition()
		transitionLocked = false
		select {
		case <-updateDone:
		case <-time.After(time.Second):
		}
		t.Fatal("canceled auth update did not leave the transition wait")
	}
	if updateCtx.Err() == nil {
		t.Fatal("update context was not canceled")
	}

	unlockTransition()
	transitionLocked = false
	deadline := time.Now().Add(time.Second)
	for len(registry.GetGlobalRegistry().GetModelsForClient(authID)) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("canceled hook lost the committed model synchronization")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAuthHookCancellationStillResumesChatGPTWebRelogin(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
	authID := "service-hook-canceled-chatgpt-web"
	installed, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusPending,
		Metadata: map[string]any{
			"access_token":    "token",
			"account_id":      "account",
			"lifecycle_state": coreauth.LifecycleStateReloginPending,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	reloginObserved := make(chan string, 1)
	service.chatGPTWebReloginObserved = func(auth *coreauth.Auth) {
		reloginObserved <- auth.ID
	}
	unlockTransition := service.lockAuthModelTransition(authID)
	transitionLocked := true
	defer func() {
		if transitionLocked {
			unlockTransition()
		}
	}()

	hookCtx, cancelHook := context.WithCancel(t.Context())
	defer cancelHook()
	hookDone := make(chan struct{})
	go func() {
		authMaintenanceHook{service: service}.OnAuthUpdated(hookCtx, installed)
		close(hookDone)
	}()
	waitForAuthModelTransitionWaiter(t, service, authID)
	cancelHook()
	select {
	case <-hookDone:
	case <-time.After(time.Second):
		unlockTransition()
		transitionLocked = false
		select {
		case <-hookDone:
		case <-time.After(time.Second):
		}
		t.Fatal("canceled ChatGPT Web hook did not leave the transition wait")
	}

	select {
	case observedID := <-reloginObserved:
		if observedID != authID {
			t.Fatalf("re-login auth ID = %q, want %q", observedID, authID)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled model transition lost ChatGPT Web re-login scheduling")
	}
	service.modelSyncMu.Lock()
	state, queued := service.modelSyncPending[authID]
	service.modelSyncMu.Unlock()
	if !queued || (!state.queued && !state.running) {
		t.Fatalf("canceled model transition did not retain model sync: %#v", state)
	}

	unlockTransition()
	transitionLocked = false
}

func TestStopAuthUpdateQueueCancelsAuthFileLockWait(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "auth-file-lock-wait.json")
	contents := []byte(`{"type":"claude"}`)
	if errWrite := os.WriteFile(path, contents, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	auth := &coreauth.Auth{
		ID:         "service-auth-file-lock-wait",
		Provider:   "claude",
		Status:     coreauth.StatusActive,
		FileName:   path,
		Attributes: map[string]string{"path": path},
	}
	if errSync := coreauth.SyncPersistedMetadataAndSourceHash(auth, contents); errSync != nil {
		t.Fatal(errSync)
	}
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	mutationLocked := make(chan struct{}, 1)
	service.authUpdateMutationLockedObserved = func(*coreauth.Auth) {
		mutationLocked <- struct{}{}
	}
	unlockPath := authfileguard.Lock(path)
	pathLocked := true
	defer func() {
		if pathLocked {
			unlockPath()
		}
	}()

	service.ensureAuthUpdateQueue(t.Context())
	service.authUpdates <- watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionAdd,
		Auth:   auth,
	}
	select {
	case <-mutationLocked:
	case <-time.After(time.Second):
		unlockPath()
		pathLocked = false
		t.Fatal("auth update did not acquire the auth mutation lock")
	}

	stopped := make(chan struct{})
	go func() {
		if errStop := service.stopAuthUpdateQueue(t.Context()); errStop != nil {
			t.Errorf("stop auth update queue: %v", errStop)
		}
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		unlockPath()
		pathLocked = false
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("auth update queue stop remained blocked after the auth file lock was released")
		}
		t.Fatal("auth update queue stop did not cancel the auth file lock wait")
	}
	unlockPath()
	pathLocked = false
}

func TestStopAuthUpdateQueueTimeoutDoesNotStartConcurrentConsumer(t *testing.T) {
	done := make(chan struct{})
	service := &Service{
		authQueueStop: func() {},
		authQueueDone: done,
	}

	stopCtx, cancelStop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancelStop()
	if errStop := service.stopAuthUpdateQueue(stopCtx); !errors.Is(errStop, context.DeadlineExceeded) {
		t.Fatalf("stop auth update queue error = %v, want deadline exceeded", errStop)
	}

	service.ensureAuthUpdateQueue(t.Context())
	service.authQueueMu.Lock()
	activeDone := service.authQueueDone
	service.authQueueMu.Unlock()
	if activeDone != done {
		t.Fatal("timed-out consumer was replaced before it exited")
	}

	close(done)
	service.ensureAuthUpdateQueue(t.Context())
	service.authQueueMu.Lock()
	replacementDone := service.authQueueDone
	service.authQueueMu.Unlock()
	if replacementDone == nil || replacementDone == done {
		t.Fatal("exited consumer was not replaced")
	}
	if errStop := service.stopAuthUpdateQueue(t.Context()); errStop != nil {
		t.Fatalf("stop replacement auth update queue: %v", errStop)
	}
}

func TestModelSyncStopKeepsStoppingGenerationPublishedUntilWorkersExit(t *testing.T) {
	canceled := make(chan struct{})
	var cancelOnce sync.Once
	oldDone := make(chan struct{})
	var oldDoneOnce sync.Once
	releaseOldGeneration := func() {
		oldDoneOnce.Do(func() { close(oldDone) })
	}
	oldQueue := make(chan string, 1)
	service := &Service{
		modelSyncCancel: func() {
			cancelOnce.Do(func() { close(canceled) })
		},
		modelSyncDone:  oldDone,
		modelSyncQueue: oldQueue,
	}

	stopped := make(chan struct{})
	go func() {
		service.stopModelSyncLoop()
		close(stopped)
	}()
	t.Cleanup(func() {
		releaseOldGeneration()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Errorf("model sync stop did not finish during test cleanup")
		}
		service.stopModelSyncLoop()
	})
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("model sync stop did not cancel the active generation")
	}

	service.startModelSyncLoop(t.Context())
	service.modelSyncMu.Lock()
	activeDone := service.modelSyncDone
	activeQueue := service.modelSyncQueue
	service.modelSyncMu.Unlock()
	if activeDone != oldDone || activeQueue != oldQueue {
		releaseOldGeneration()
		select {
		case <-stopped:
		case <-time.After(time.Second):
		}
		t.Fatal("a replacement model sync generation started before the old workers exited")
	}

	releaseOldGeneration()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("model sync stop did not finish after the old workers exited")
	}

	service.startModelSyncLoop(t.Context())
	service.modelSyncMu.Lock()
	replacementDone := service.modelSyncDone
	replacementQueue := service.modelSyncQueue
	service.modelSyncMu.Unlock()
	if replacementDone == nil || replacementDone == oldDone || replacementQueue == nil || replacementQueue == oldQueue {
		t.Fatal("model sync generation did not restart after the old workers exited")
	}
	service.stopModelSyncLoop()
}

func TestModelSyncStopCancelsChatGPTWebFetchLockWait(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
	authID := "service-model-sync-cancel-web-fetch"
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	}); errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	unlockFetch := service.lockChatGPTWebModelFetch(authID)
	service.startModelSyncLoop(t.Context())
	if !service.enqueueModelSync(authID) {
		unlockFetch()
		t.Fatal("model sync task was not accepted")
	}
	waitForChatGPTWebModelFetchWaiter(t, service, authID)

	stopped := make(chan struct{})
	go func() {
		service.stopModelSyncLoop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		unlockFetch()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("model sync stop remained blocked after the model fetch lock was released")
		}
		t.Fatal("model sync stop did not cancel the model fetch lock wait")
	}
	unlockFetch()
}

func TestChatGPTWebDeleteCleanupHonorsCanceledTransitionWait(t *testing.T) {
	service := &Service{}
	authID := "service-delete-cleanup-canceled-transition"
	unlockTransition := service.lockAuthModelTransition(authID)
	ctx, cancel := context.WithCancel(t.Context())
	cleanupDone := make(chan struct{})
	go func() {
		service.cleanupChatGPTWebModelResourcesAfterDelete(ctx, authID, "")
		close(cleanupDone)
	}()
	waitForAuthModelTransitionWaiter(t, service, authID)
	cancel()
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		unlockTransition()
		select {
		case <-cleanupDone:
		case <-time.After(time.Second):
			t.Fatal("delete cleanup remained blocked after the transition lock was released")
		}
		t.Fatal("delete cleanup did not honor transition wait cancellation")
	}
	unlockTransition()
}

func TestDeleteCoreAuthCancelsPendingModelSync(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		coreManager: manager,
		modelSyncPending: map[string]modelSyncTaskState{
			"deleted": {epoch: 1},
			"kept":    {epoch: 2},
		},
		modelSyncOverflow: []string{"deleted", "kept", "deleted"},
	}
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID:       "deleted",
		Provider: "claude",
		Status:   coreauth.StatusActive,
	}); errRegister != nil {
		t.Fatal(errRegister)
	}

	if errDelete := service.deleteCoreAuth(coreauth.WithSkipPersist(t.Context()), "deleted"); errDelete != nil {
		t.Fatal(errDelete)
	}

	service.modelSyncMu.Lock()
	defer service.modelSyncMu.Unlock()
	if _, pending := service.modelSyncPending["deleted"]; pending {
		t.Fatal("deleted auth remained in pending model sync tasks")
	}
	for _, authID := range service.modelSyncOverflow {
		if authID == "deleted" {
			t.Fatal("deleted auth remained in model sync overflow")
		}
	}
	if _, pending := service.modelSyncPending["kept"]; !pending {
		t.Fatal("unrelated pending model sync task was removed")
	}
}

func TestModelSyncWorkersAutomaticallyDrainOverflow(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	blockerUnlocks := make([]func(), 0, defaultModelSyncWorkers)
	for index := range defaultModelSyncWorkers {
		blockerUnlocks = append(blockerUnlocks, service.lockAuthModelTransition(fmt.Sprintf("blocked-%d", index)))
	}
	blockersReleased := false
	service.startModelSyncLoop(t.Context())
	defer service.stopModelSyncLoop()
	defer func() {
		if blockersReleased {
			return
		}
		for _, unlock := range blockerUnlocks {
			unlock()
		}
	}()
	for index := range defaultModelSyncWorkers {
		if !service.enqueueModelSync(fmt.Sprintf("blocked-%d", index)) {
			t.Fatal("blocking model sync task was not accepted")
		}
	}
	deadline := time.Now().Add(time.Second)
	for len(service.modelSyncQueue) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("workers did not take the blocking tasks")
		}
		time.Sleep(time.Millisecond)
	}
	for index := range defaultModelSyncQueueSize {
		if !service.enqueueModelSync(fmt.Sprintf("queued-%d", index)) {
			t.Fatal("queued model sync task was not accepted")
		}
	}

	authID := "service-model-sync-overflow-target"
	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})
	if _, errRegister := service.coreManager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	}); errRegister != nil {
		t.Fatal(errRegister)
	}
	if !service.enqueueModelSync(authID) {
		t.Fatal("overflow model sync task was not accepted")
	}
	service.modelSyncMu.Lock()
	overflowed := len(service.modelSyncOverflow) > 0
	service.modelSyncMu.Unlock()
	if !overflowed {
		t.Fatal("target model sync task did not enter overflow")
	}
	for _, unlock := range blockerUnlocks {
		unlock()
	}
	blockersReleased = true

	deadline = time.Now().Add(3 * time.Second)
	for {
		service.modelSyncMu.Lock()
		pending := len(service.modelSyncPending)
		overflow := len(service.modelSyncOverflow)
		service.modelSyncMu.Unlock()
		models := registry.GetGlobalRegistry().GetModelsForClient(authID)
		if len(models) > 0 && pending == 0 && overflow == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("overflow did not drain: models=%d pending=%d overflow=%d", len(models), pending, overflow)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServiceApplyCoreAuthAddOrUpdate_QueuesOverflowWhenQueueIsFull(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 1)
	service.modelSyncQueue <- "busy"
	service.modelSyncPending = make(map[string]modelSyncTaskState)

	authID := "service-inline-model-sync-auth"
	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	service.applyCoreAuthAddOrUpdate(ctx, &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	})

	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) != 0 {
		t.Fatalf("queue-full update unexpectedly synchronized models inline for %q", authID)
	}
	if _, exists := service.modelSyncPending[authID]; !exists {
		t.Fatalf("queue-full update did not retain pending state for %q", authID)
	}
	if len(service.modelSyncOverflow) != 1 || service.modelSyncOverflow[0] != authID {
		t.Fatalf("model sync overflow = %v, want [%q]", service.modelSyncOverflow, authID)
	}
}

func TestEnqueueModelSyncPromotesOlderOverflowBeforeNewTask(t *testing.T) {
	service := &Service{}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 1)
	service.modelSyncQueue <- "busy"
	service.modelSyncPending = make(map[string]modelSyncTaskState)

	if !service.enqueueModelSync("older") {
		t.Fatal("older task was not accepted")
	}
	if queued := <-service.modelSyncQueue; queued != "busy" {
		t.Fatalf("queued task = %q, want busy", queued)
	}
	if !service.enqueueModelSync("newer") {
		t.Fatal("newer task was not accepted")
	}
	if queued := <-service.modelSyncQueue; queued != "older" {
		t.Fatalf("promoted task = %q, want older", queued)
	}
	if len(service.modelSyncOverflow) != 1 || service.modelSyncOverflow[0] != "newer" {
		t.Fatalf("overflow = %v, want [newer]", service.modelSyncOverflow)
	}
}

func TestAuthMaintenanceHookQueuesAntigravityModelSyncAfterAuthUpdate(t *testing.T) {
	service := &Service{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 1)
	service.modelSyncPending = make(map[string]modelSyncTaskState)
	hook := authMaintenanceHook{service: service}

	hook.OnAuthUpdated(ctx, &coreauth.Auth{
		ID:       "service-antigravity-refresh-resync",
		Provider: "antigravity",
		Status:   coreauth.StatusActive,
	})

	select {
	case authID := <-service.modelSyncQueue:
		if authID != "service-antigravity-refresh-resync" {
			t.Fatalf("queued auth ID = %q", authID)
		}
	default:
		t.Fatal("expected refreshed Antigravity auth to queue model capability sync")
	}
}

func TestAuthMaintenanceHookQueuesOverflowWhenModelSyncQueueIsFull(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 1)
	service.modelSyncQueue <- "busy"
	service.modelSyncPending = make(map[string]modelSyncTaskState)
	auth := &coreauth.Auth{ID: "service-antigravity-full-sync-queue", Provider: "antigravity", Status: coreauth.StatusActive}
	if _, errRegister := service.coreManager.Register(ctx, auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	authMaintenanceHook{service: service}.OnAuthUpdated(ctx, auth)

	if _, pending := service.modelSyncPending[auth.ID]; !pending {
		t.Fatal("queue-full hook did not retain pending task")
	}
	if len(service.modelSyncOverflow) != 1 || service.modelSyncOverflow[0] != auth.ID {
		t.Fatalf("model sync overflow = %v, want [%q]", service.modelSyncOverflow, auth.ID)
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); len(models) != 0 {
		t.Fatal("queue-full hook unexpectedly ran model sync inline")
	}
}

func TestAuthMaintenanceHookSkipsSuppressedModelSync(t *testing.T) {
	service := &Service{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 1)
	service.modelSyncPending = make(map[string]modelSyncTaskState)
	hook := authMaintenanceHook{service: service}
	refreshCtx := context.WithValue(ctx, modelSyncHookSuppressedContextKey{}, true)

	hook.OnAuthUpdated(refreshCtx, &coreauth.Auth{
		ID:       "service-antigravity-capability-refresh",
		Provider: "antigravity",
		Status:   coreauth.StatusActive,
	})

	select {
	case authID := <-service.modelSyncQueue:
		t.Fatalf("capability refresh unexpectedly queued a second sync for %q", authID)
	default:
	}
}

func TestAuthMaintenanceHookSuppressedUpdateDoesNotReenterAuthModelTransitionLock(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
	manager.AddHook(authMaintenanceHook{service: service})
	ctx := context.WithValue(context.Background(), modelSyncHookSuppressedContextKey{}, true)
	auth := &coreauth.Auth{
		ID:       "service-suppressed-chatgpt-web-hook",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "token",
			"account_id":      "account",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	}

	unlockTransition := service.lockAuthModelTransition(auth.ID)
	done := make(chan error, 1)
	go func() {
		_, err := manager.Register(ctx, auth)
		done <- err
	}()

	select {
	case err := <-done:
		unlockTransition()
		if err != nil {
			t.Fatalf("register auth: %v", err)
		}
	case <-time.After(time.Second):
		unlockTransition()
		err := <-done
		t.Fatalf("suppressed hook re-entered the auth model transition lock: %v", err)
	}
}

func TestAuthMaintenanceHookDifferentAuthDoesNotWaitForModelTransitionLock(t *testing.T) {
	service := &Service{cfg: &config.Config{}}
	auth := &coreauth.Auth{
		ID:       "service-non-chatgpt-model-hook",
		Provider: "claude",
		Status:   coreauth.StatusActive,
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	unlockTransition := service.lockAuthModelTransition("service-other-model-hook")
	done := make(chan struct{})
	go func() {
		authMaintenanceHook{service: service}.OnAuthUpdated(context.Background(), auth)
		close(done)
	}()

	select {
	case <-done:
		unlockTransition()
	case <-time.After(time.Second):
		unlockTransition()
		<-done
		t.Fatal("auth update waited for another auth's model transition lock")
	}
}

func TestAuthMaintenanceHookFreshSameProviderUpdateResetsRegistryQuota(t *testing.T) {
	service := &Service{cfg: &config.Config{}}
	auth := &coreauth.Auth{
		ID:       "service-fresh-same-provider-hook",
		Provider: "claude",
		Status:   coreauth.StatusActive,
	}
	models := registry.GetClaudeModels()
	if len(models) == 0 {
		t.Fatal("Claude model catalog is empty")
	}
	modelID := models[0].ID
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	GlobalModelRegistry().RegisterClient(auth.ID, auth.Provider, models)
	beforeQuota := registry.GetGlobalRegistry().GetModelCount(modelID)
	GlobalModelRegistry().SetModelQuotaExceeded(auth.ID, modelID)
	if got := registry.GetGlobalRegistry().GetModelCount(modelID); got >= beforeQuota {
		t.Fatalf("registry quota did not suppress model: before=%d after=%d", beforeQuota, got)
	}

	authMaintenanceHook{service: service}.OnAuthUpdated(context.Background(), auth)
	if got := registry.GetGlobalRegistry().GetModelCount(modelID); got != beforeQuota {
		t.Fatalf("fresh same-provider update retained registry quota: got=%d want=%d", got, beforeQuota)
	}
}

func TestAuthMaintenanceHookRejectsStaleInstallationAfterModelLockWait(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
	authID := "service-stale-hook-installation"
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	oldAuth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "old-token",
			"account_id":      "old-account",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register old auth: %v", err)
	}

	unlockTransition := service.lockAuthModelTransition(authID)
	hookStarted := make(chan struct{})
	hookDone := make(chan struct{})
	go func() {
		close(hookStarted)
		authMaintenanceHook{service: service}.OnAuthUpdated(context.Background(), oldAuth)
		close(hookDone)
	}()
	<-hookStarted
	waitForAuthModelTransitionWaiter(t, service, authID)

	replacement := oldAuth.Clone()
	replacement.Provider = "claude"
	replacement.Metadata = nil
	installed, err := manager.Update(context.Background(), replacement)
	if err != nil {
		unlockTransition()
		t.Fatalf("install replacement auth: %v", err)
	}
	GlobalModelRegistry().RegisterClient(installed.ID, installed.Provider, []*registry.ModelInfo{{ID: "claude-current-model"}})
	unlockTransition()

	select {
	case <-hookDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stale hook did not finish")
	}
	if provider := registry.GetGlobalRegistry().GetProviderForClient(authID); provider != "claude" {
		t.Fatalf("stale hook replaced provider with %q", provider)
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(authID)
	if len(models) != 1 || models[0].ID != "claude-current-model" {
		t.Fatalf("stale hook replaced current models: %v", models)
	}
}

func TestAuthMaintenanceHookRejectsStaleNonChatGPTInstallationAfterTransitionWait(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
	authID := "service-stale-non-chatgpt-hook-installation"
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	oldAuth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	})
	if err != nil {
		t.Fatalf("register old auth: %v", err)
	}

	unlockTransition := service.lockAuthModelTransition(authID)
	hookStarted := make(chan struct{})
	hookDone := make(chan struct{})
	go func() {
		close(hookStarted)
		authMaintenanceHook{service: service}.OnAuthUpdated(context.Background(), oldAuth)
		close(hookDone)
	}()
	<-hookStarted
	waitForAuthModelTransitionWaiter(t, service, authID)

	replacement := oldAuth.Clone()
	replacement.Provider = "chatgpt-web"
	replacement.Metadata = map[string]any{
		"access_token":    "token",
		"account_id":      "account",
		"lifecycle_state": coreauth.LifecycleStateActive,
	}
	installed, err := manager.Update(context.Background(), replacement)
	if err != nil {
		unlockTransition()
		t.Fatalf("install replacement auth: %v", err)
	}
	GlobalModelRegistry().RegisterClient(installed.ID, installed.Provider, []*registry.ModelInfo{{ID: "chatgpt-current-model"}})
	unlockTransition()

	select {
	case <-hookDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stale non-ChatGPT hook did not finish")
	}
	if provider := registry.GetGlobalRegistry().GetProviderForClient(authID); provider != "chatgpt-web" {
		t.Fatalf("stale hook replaced provider with %q", provider)
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(authID)
	if len(models) != 1 || models[0].ID != "chatgpt-current-model" {
		t.Fatalf("stale hook replaced current models: %v", models)
	}
}

func TestServiceSyncAuthModelsRequeuesReplacementAfterMutationWait(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
	loadedOldAuth := make(chan *coreauth.Auth, 1)
	service.modelSyncAuthLoadedObserved = func(auth *coreauth.Auth) {
		select {
		case loadedOldAuth <- auth.Clone():
		default:
		}
	}
	authID := "service-model-sync-provider-transition"
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	oldAuth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	})
	if err != nil {
		t.Fatalf("register old auth: %v", err)
	}

	lockedCtx, unlockMutation, errLock := manager.LockAuthMutation(t.Context(), oldAuth)
	if errLock != nil {
		t.Fatal(errLock)
	}
	mutationLocked := true
	defer func() {
		if mutationLocked {
			unlockMutation()
		}
	}()
	service.startModelSyncLoop(t.Context())
	defer service.stopModelSyncLoop()
	if !service.enqueueModelSync(authID) {
		t.Fatal("model sync task was not accepted")
	}
	select {
	case loaded := <-loadedOldAuth:
		if loaded.RuntimeInstallationID() != oldAuth.RuntimeInstallationID() {
			t.Fatalf("model sync loaded installation %q, want old installation %q", loaded.RuntimeInstallationID(), oldAuth.RuntimeInstallationID())
		}
	case <-time.After(time.Second):
		t.Fatal("model sync did not load the old auth before waiting for mutation")
	}

	replacement := oldAuth.Clone()
	replacement.Provider = "xai"
	installed, err := manager.Update(lockedCtx, replacement)
	if err != nil {
		t.Fatalf("install replacement auth: %v", err)
	}
	unlockMutation()
	mutationLocked = false

	deadline := time.Now().Add(5 * time.Second)
	for {
		provider := registry.GetGlobalRegistry().GetProviderForClient(authID)
		models := registry.GetGlobalRegistry().GetModelsForClient(authID)
		if provider == installed.Provider && len(models) > 0 && containsRegisteredModel(models, registry.GetXAIModels()[0].ID) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replacement model sync did not converge: provider=%q models=%v", provider, registeredModelIDs(models))
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAuthMaintenanceHookClearsChatGPTWebRegistryBeforeAntigravitySync(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := &Service{
		cfg:              &config.Config{},
		coreManager:      manager,
		modelSyncCancel:  cancel,
		modelSyncQueue:   make(chan string, 1),
		modelSyncPending: make(map[string]modelSyncTaskState),
	}
	authID := "service-chatgpt-web-to-antigravity"
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "token",
			"account_id":      "account",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register ChatGPT Web auth: %v", err)
	}
	GlobalModelRegistry().RegisterClient(authID, "chatgpt-web", []*registry.ModelInfo{{ID: "web-only-model"}})
	manager.RefreshSchedulerEntry(authID)
	manager.AddHook(authMaintenanceHook{service: service})
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	replacement := auth.Clone()
	replacement.Provider = "antigravity"
	replacement.Metadata = map[string]any{"access_token": "antigravity-token"}
	if _, err = manager.Update(context.Background(), replacement); err != nil {
		t.Fatalf("switch auth provider: %v", err)
	}

	if provider := registry.GetGlobalRegistry().GetProviderForClient(authID); provider != "" {
		t.Fatalf("stale registry provider = %q", provider)
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) != 0 {
		t.Fatalf("stale ChatGPT Web models remained: %v", models)
	}
	select {
	case queuedID := <-service.modelSyncQueue:
		if queuedID != authID {
			t.Fatalf("queued auth ID = %q, want %q", queuedID, authID)
		}
	default:
		t.Fatal("Antigravity model sync was not queued")
	}
}

func TestServiceConcurrentSameAccountUpdatesReplaceChatGPTWebRuntimeOnce(t *testing.T) {
	hook := &serviceChatGPTWebReplacementHook{updated: make(chan struct{}, 2)}
	manager := coreauth.NewManager(nil, nil, hook)
	service := &Service{
		cfg:              &config.Config{},
		coreManager:      manager,
		modelSyncQueue:   make(chan string, 4),
		modelSyncPending: make(map[string]modelSyncTaskState),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	authID := "service-concurrent-same-chatgpt-web-account"
	oldAuth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "old-token",
			"account_id":      "old-account",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register old auth: %v", err)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	replacement := oldAuth.Clone()
	replacement.Metadata["access_token"] = "new-token"
	replacement.Metadata["account_id"] = "new-account"
	update := watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionModify,
		Auth:   replacement,
	}

	unlockTransition := service.lockAuthModelTransition(authID)
	started := make(chan struct{}, 2)
	done := make(chan struct{}, 2)
	for range 2 {
		go func() {
			started <- struct{}{}
			service.handleAuthUpdate(ctx, update)
			done <- struct{}{}
		}()
	}
	<-started
	<-started
	time.Sleep(25 * time.Millisecond)
	unlockTransition()
	<-done
	<-done

	select {
	case <-hook.updated:
	case <-time.After(5 * time.Second):
		t.Fatal("account replacement hook did not run")
	}
	time.Sleep(25 * time.Millisecond)
	if got := hook.replacements.Load(); got != 1 {
		t.Fatalf("runtime replacement count = %d, want 1", got)
	}
}

func TestServiceApplyCoreAuthAddOrUpdateQueuesAntigravitySyncOnce(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 2)
	service.modelSyncPending = make(map[string]modelSyncTaskState)
	service.coreManager.AddHook(authMaintenanceHook{service: service})
	authID := "service-antigravity-single-sync"
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	service.applyCoreAuthAddOrUpdate(ctx, &coreauth.Auth{
		ID:       authID,
		Provider: "antigravity",
		Status:   coreauth.StatusActive,
	})

	select {
	case queuedID := <-service.modelSyncQueue:
		if queuedID != authID {
			t.Fatalf("queued auth ID = %q, want %q", queuedID, authID)
		}
	default:
		t.Fatal("expected Antigravity model sync to be queued")
	}
	select {
	case queuedID := <-service.modelSyncQueue:
		t.Fatalf("duplicate model sync queued for %q", queuedID)
	default:
	}
	state, ok := service.modelSyncPending[authID]
	if !ok {
		t.Fatal("expected queued Antigravity sync to remain pending")
	}
	if state.dirty {
		t.Fatal("single Antigravity update incorrectly marked model sync dirty")
	}
}

func TestModelSyncQueuedUpdatesCoalesceWithoutDirty(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	const authID = "queued-model-sync"
	service := &Service{
		modelSyncCancel:     cancel,
		modelSyncQueue:      make(chan string, 2),
		modelSyncPending:    make(map[string]modelSyncTaskState),
		modelSyncGeneration: 1,
	}

	if !service.enqueueModelSync(authID) || !service.enqueueModelSync(authID) {
		t.Fatal("queued model sync update was not accepted")
	}
	if queuedID := <-service.modelSyncQueue; queuedID != authID {
		t.Fatalf("queued auth ID = %q, want %q", queuedID, authID)
	}
	select {
	case duplicate := <-service.modelSyncQueue:
		t.Fatalf("queued update added duplicate task %q", duplicate)
	default:
	}
	state := service.modelSyncPending[authID]
	if !state.queued || state.running || state.dirty {
		t.Fatalf("queued task state = %#v", state)
	}
}

func TestModelSyncRunningUpdatesRetryOnceThenRequeue(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	const authID = "running-model-sync"
	service := &Service{
		modelSyncCancel:     cancel,
		modelSyncQueue:      make(chan string, 2),
		modelSyncPending:    make(map[string]modelSyncTaskState),
		modelSyncGeneration: 1,
	}
	if !service.enqueueModelSync(authID) {
		t.Fatal("model sync task was not accepted")
	}
	<-service.modelSyncQueue
	epoch, ok := service.beginModelSyncTask(authID, 1)
	if !ok {
		t.Fatal("queued task did not begin")
	}

	service.enqueueModelSync(authID)
	if !service.completeModelSyncTask(authID, epoch, 1, true) {
		t.Fatal("running update did not receive one immediate retry")
	}
	service.enqueueModelSync(authID)
	if service.completeModelSyncTask(authID, epoch, 1, false) {
		t.Fatal("second running update incorrectly retained the worker")
	}

	if queuedID := <-service.modelSyncQueue; queuedID != authID {
		t.Fatalf("requeued auth ID = %q, want %q", queuedID, authID)
	}
	state := service.modelSyncPending[authID]
	if state.epoch != epoch || !state.queued || state.running || state.dirty {
		t.Fatalf("requeued task state = %#v", state)
	}
}

func TestModelSyncTaskEpochRejectsDeletedAuthCompletion(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	const authID = "recreated-model-sync"
	service := &Service{
		modelSyncCancel:     cancel,
		modelSyncQueue:      make(chan string, 2),
		modelSyncPending:    make(map[string]modelSyncTaskState),
		modelSyncGeneration: 1,
	}
	service.enqueueModelSync(authID)
	<-service.modelSyncQueue
	oldEpoch, ok := service.beginModelSyncTask(authID, 1)
	if !ok {
		t.Fatal("old task did not begin")
	}
	service.cancelModelSyncTask(authID)
	service.enqueueModelSync(authID)
	newState := service.modelSyncPending[authID]
	if newState.epoch == oldEpoch {
		t.Fatal("recreated auth reused the old task epoch")
	}

	if service.completeModelSyncTask(authID, oldEpoch, 1, true) {
		t.Fatal("old task completion was accepted for recreated auth")
	}
	current := service.modelSyncPending[authID]
	if current.epoch != newState.epoch || !current.queued {
		t.Fatalf("old completion changed recreated task: %#v", current)
	}
}

func TestModelSyncConditionalCancelPreservesRecreatedTask(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	const authID = "conditionally-canceled-model-sync"
	service := &Service{
		modelSyncCancel:     cancel,
		modelSyncQueue:      make(chan string, 2),
		modelSyncPending:    make(map[string]modelSyncTaskState),
		modelSyncGeneration: 1,
	}
	service.enqueueModelSync(authID)
	oldEpoch := service.modelSyncTaskEpoch(authID)
	service.cancelModelSyncTaskIfEpoch(authID, oldEpoch)
	service.enqueueModelSync(authID)
	newEpoch := service.modelSyncTaskEpoch(authID)
	if newEpoch == oldEpoch {
		t.Fatal("recreated task reused old epoch")
	}

	service.cancelModelSyncTaskIfEpoch(authID, oldEpoch)
	if currentEpoch := service.modelSyncTaskEpoch(authID); currentEpoch != newEpoch {
		t.Fatalf("old conditional cancel removed epoch %d, current = %d", newEpoch, currentEpoch)
	}
}

func TestModelSyncRecreatedInstallationSupersedesQueuedOrRunningTask(t *testing.T) {
	for _, running := range []bool{false, true} {
		name := "queued"
		if running {
			name = "running"
		}
		t.Run(name, func(t *testing.T) {
			_, cancel := context.WithCancel(context.Background())
			defer cancel()
			const authID = "recreated-installation-model-sync"
			service := &Service{
				modelSyncCancel:     cancel,
				modelSyncQueue:      make(chan string, 2),
				modelSyncPending:    make(map[string]modelSyncTaskState),
				modelSyncGeneration: 1,
			}
			if !service.enqueueModelSyncTaskForInstallation(authID, "old-installation", false) {
				t.Fatal("old installation task was not accepted")
			}
			oldEpoch := service.modelSyncTaskEpoch(authID)
			if running {
				queuedID := <-service.modelSyncQueue
				if queuedID != authID {
					t.Fatalf("queued auth ID = %q, want %q", queuedID, authID)
				}
				if _, ok := service.beginModelSyncTask(authID, 1); !ok {
					t.Fatal("old installation task did not begin")
				}
			}

			if !service.enqueueModelSyncTaskForInstallation(authID, "new-installation", false) {
				t.Fatal("new installation task was not accepted")
			}
			newState := service.modelSyncPending[authID]
			newEpoch := service.modelSyncTaskEpoch(authID)
			if newEpoch == oldEpoch {
				t.Fatal("new installation reused the old task epoch")
			}
			if running {
				if newState.nextInstallationID != "new-installation" ||
					newState.nextEpoch != newEpoch ||
					!newState.running ||
					newState.queued {
					t.Fatalf("running installation successor state = %#v", newState)
				}
				select {
				case duplicate := <-service.modelSyncQueue:
					t.Fatalf("new installation was dispatched while old task was running: %q", duplicate)
				default:
				}
			} else if newState.installationID != "new-installation" || !newState.queued {
				t.Fatalf("queued installation state = %#v", newState)
			}

			service.cancelModelSyncTaskIfEpoch(authID, oldEpoch)
			current := service.modelSyncPending[authID]
			if service.modelSyncTaskEpoch(authID) != newEpoch {
				t.Fatalf("old conditional cancel changed new installation task: %#v", current)
			}
			if running {
				if service.completeModelSyncTask(authID, oldEpoch, 1, true) {
					t.Fatal("superseded running task requested an inline retry")
				}
				queuedID := <-service.modelSyncQueue
				if queuedID != authID {
					t.Fatalf("successor queued auth ID = %q, want %q", queuedID, authID)
				}
				current = service.modelSyncPending[authID]
				if current.epoch != newEpoch ||
					current.installationID != "new-installation" ||
					!current.queued ||
					current.running {
					t.Fatalf("successor state after old completion = %#v", current)
				}
			} else if current.epoch != newEpoch || current.installationID != "new-installation" {
				t.Fatalf("old conditional cancel changed queued installation task: %#v", current)
			}
		})
	}
}

func TestModelSyncCanceledQueuedTaskPromotesOverflow(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := &Service{
		modelSyncCancel:     cancel,
		modelSyncQueue:      make(chan string, 1),
		modelSyncPending:    make(map[string]modelSyncTaskState),
		modelSyncGeneration: 1,
	}
	if !service.enqueueModelSync("canceled") || !service.enqueueModelSync("waiting") {
		t.Fatal("model sync tasks were not accepted")
	}
	service.cancelModelSyncTask("canceled")
	if queuedID := <-service.modelSyncQueue; queuedID != "canceled" {
		t.Fatalf("queued auth ID = %q, want canceled", queuedID)
	}
	if _, ok := service.beginModelSyncTask("canceled", 1); ok {
		t.Fatal("canceled channel entry unexpectedly began")
	}
	if queuedID := <-service.modelSyncQueue; queuedID != "waiting" {
		t.Fatalf("promoted auth ID = %q, want waiting", queuedID)
	}
}

func TestServiceApplyCoreAuthAddOrUpdateQueuesChatGPTWebSyncBeforeRelogin(t *testing.T) {
	authID := "service-chatgpt-web-sync-before-relogin"
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 1)
	service.modelSyncPending = make(map[string]modelSyncTaskState)
	reloginObserved := make(chan bool, 1)
	service.chatGPTWebReloginObserved = func(auth *coreauth.Auth) {
		service.modelSyncMu.Lock()
		_, queued := service.modelSyncPending[auth.ID]
		service.modelSyncMu.Unlock()
		reloginObserved <- queued
	}
	service.coreManager.AddHook(authMaintenanceHook{service: service})
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	service.applyCoreAuthAddOrUpdate(ctx, &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusPending,
		Metadata: map[string]any{
			"access_token":         "token",
			"account_id":           "account",
			"lifecycle_state":      coreauth.LifecycleStateReloginPending,
			"lifecycle_reason":     "refresh failed",
			"lifecycle_updated_at": time.Now().UTC().Format(time.RFC3339Nano),
		},
	})

	select {
	case queued := <-reloginObserved:
		if !queued {
			t.Fatal("re-login was scheduled before the model sync task was queued")
		}
	case <-time.After(time.Second):
		t.Fatal("re-login scheduling was not observed")
	}
	select {
	case queuedID := <-service.modelSyncQueue:
		if queuedID != authID {
			t.Fatalf("queued auth ID = %q, want %q", queuedID, authID)
		}
	default:
		t.Fatal("expected ChatGPT Web model sync to be queued")
	}
}

func TestAuthMaintenanceHookClearsStaleAntigravityCapabilityCache(t *testing.T) {
	service := &Service{}
	hook := authMaintenanceHook{service: service}
	for _, auth := range []*coreauth.Auth{
		{ID: "service-antigravity-disabled-cache", Provider: "antigravity", Disabled: true, Status: coreauth.StatusDisabled},
		{ID: "service-antigravity-provider-change-cache", Provider: "claude", Status: coreauth.StatusActive},
	} {
		service.antigravityModelCapabilities.Store(auth.ID, &antigravityModelCapabilityCacheEntry{
			RuntimeInstanceID: auth.RuntimeInstanceID(),
			Hints: antigravityModelCapabilityHints{
				WebSearchModelIDs: map[string]struct{}{"gemini-3.1-flash-lite": {}},
			},
		})
		hook.OnAuthUpdated(context.Background(), auth)
		if _, exists := service.antigravityModelCapabilities.Load(auth.ID); exists {
			t.Fatalf("stale capability cache remained for %q", auth.ID)
		}
	}
}

func TestServiceSyncAuthModelsRejectsStaleAntigravityDiscoveryAfterProviderChange(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseResponse := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseResponse
		_, _ = w.Write([]byte(`{"webSearchModelIds":["gemini-3.1-flash-lite"]}`))
	}))
	defer server.Close()

	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	authID := "service-stale-antigravity-discovery"
	oldAuth := &coreauth.Auth{
		ID:         authID,
		Provider:   "antigravity",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"base_url": server.URL},
		Metadata:   map[string]any{"access_token": "token"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), oldAuth); errRegister != nil {
		t.Fatalf("register old auth: %v", errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.syncAuthModels(context.Background(), authID)
	}()
	<-requestStarted

	newAuth := &coreauth.Auth{ID: authID, Provider: "claude", Status: coreauth.StatusActive}
	installed, errUpdate := service.coreManager.Update(context.Background(), newAuth)
	if errUpdate != nil {
		close(releaseResponse)
		<-done
		t.Fatalf("replace auth provider: %v", errUpdate)
	}
	service.registerModelsForAuth(installed)
	close(releaseResponse)
	<-done

	if _, exists := service.antigravityModelCapabilities.Load(authID); exists {
		t.Fatal("stale Antigravity discovery populated cache after provider change")
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(authID)
	if len(models) == 0 || !containsRegisteredModel(models, registry.GetClaudeModels()[0].ID) {
		t.Fatalf("stale discovery replaced the current Claude registration: %v", registeredModelIDs(models))
	}
}

func TestServiceHandleManagementAuthStatusChange_ReRegistersModelsForEnabledAuth(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	auth := &coreauth.Auth{
		ID:       "service-management-enable-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	GlobalModelRegistry().UnregisterClient(auth.ID)
	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(auth.ID)
	})

	service.handleManagementAuthStatusChange(context.Background(), auth)

	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); len(models) == 0 {
		t.Fatalf("expected management status change hook to re-register models for %q", auth.ID)
	}
}

func TestServiceHandleManagementAuthStatusChangeReusesQueuedAntigravitySync(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 2)
	service.modelSyncPending = make(map[string]modelSyncTaskState)
	service.coreManager.AddHook(authMaintenanceHook{service: service})
	auth := &coreauth.Auth{
		ID:       "service-management-antigravity-single-sync",
		Provider: "antigravity",
		Status:   coreauth.StatusActive,
	}
	installed, errRegister := service.coreManager.Register(ctx, auth)
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	service.handleManagementAuthStatusChange(ctx, installed)

	select {
	case queuedID := <-service.modelSyncQueue:
		if queuedID != auth.ID {
			t.Fatalf("queued auth ID = %q, want %q", queuedID, auth.ID)
		}
	default:
		t.Fatal("expected manager hook to queue Antigravity model sync")
	}
	select {
	case queuedID := <-service.modelSyncQueue:
		t.Fatalf("management status callback queued duplicate sync for %q", queuedID)
	default:
	}
	state, ok := service.modelSyncPending[auth.ID]
	if !ok {
		t.Fatal("expected queued Antigravity sync to remain pending")
	}
	if state.dirty {
		t.Fatal("management status callback incorrectly marked Antigravity sync dirty")
	}
}

func TestServiceRefreshModelRegistrationForAuth_UpdatesCodexImageModelAfterConfigChange(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-2"},
			},
		},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	auth := &coreauth.Auth{
		ID:       "service-codex-image-refresh-auth",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "plus",
		},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.UnregisterClient(auth.ID)
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})

	service.registerModelsForAuth(auth)
	if !containsRegisteredModel(reg.GetModelsForClient(auth.ID), "gpt-image-2") {
		t.Fatalf("expected initial image model registration")
	}

	service.cfg = &config.Config{
		SDKConfig: config.SDKConfig{
			Images: config.ImagesConfig{ImageModel: "gpt-image-custom"},
		},
	}
	if !service.refreshModelRegistrationForAuth(auth) {
		t.Fatal("expected refreshModelRegistrationForAuth to succeed")
	}

	models := reg.GetModelsForClient(auth.ID)
	if containsRegisteredModel(models, "gpt-image-2") {
		t.Fatalf("expected old image model to be removed")
	}
	if !containsRegisteredModel(models, "gpt-image-custom") {
		t.Fatalf("expected new image model to be registered")
	}
}

func TestShouldRefreshCodexRegistrations(t *testing.T) {
	testCases := []struct {
		name     string
		previous *config.Config
		next     *config.Config
		want     bool
	}{
		{
			name: "image model unchanged and free toggle unchanged",
			previous: &config.Config{SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-2", EnableFreePlanImageModel: false},
			}},
			next: &config.Config{SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-2", EnableFreePlanImageModel: false},
			}},
			want: false,
		},
		{
			name: "image model changed",
			previous: &config.Config{SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-2", EnableFreePlanImageModel: false},
			}},
			next: &config.Config{SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-custom", EnableFreePlanImageModel: false},
			}},
			want: true,
		},
		{
			name: "free toggle changed",
			previous: &config.Config{SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-2", EnableFreePlanImageModel: false},
			}},
			next: &config.Config{SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{ImageModel: "gpt-image-2", EnableFreePlanImageModel: true},
			}},
			want: true,
		},
		{
			name: "native image models changed",
			previous: &config.Config{SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{
					ImageModel: "gpt-image-2",
					Native: config.NativeImagesConfig{
						Generations: config.NativeImageEndpointConfig{
							Enabled: true,
							Models:  []string{"gpt-image-2"},
						},
					},
				},
			}},
			next: &config.Config{SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{
					ImageModel: "gpt-image-2",
					Native: config.NativeImagesConfig{
						Generations: config.NativeImageEndpointConfig{
							Enabled: true,
							Models:  []string{"gpt-image-2", "gpt-image-1.5"},
						},
					},
				},
			}},
			want: true,
		},
		{
			name: "native image enabled changed",
			previous: &config.Config{SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{
					ImageModel: "gpt-image-2",
					Native: config.NativeImagesConfig{
						Generations: config.NativeImageEndpointConfig{
							Models: []string{"gpt-image-1.5"},
						},
					},
				},
			}},
			next: &config.Config{SDKConfig: config.SDKConfig{
				Images: config.ImagesConfig{
					ImageModel: "gpt-image-2",
					Native: config.NativeImagesConfig{
						Generations: config.NativeImageEndpointConfig{
							Enabled: true,
							Models:  []string{"gpt-image-1.5"},
						},
					},
				},
			}},
			want: true,
		},
		{
			name: "custom models changed",
			previous: &config.Config{
				CodexCustomModels: []config.CodexCustomModel{
					{ID: "gpt-5.5-codex", DisplayName: "GPT 5.5 Codex", Groups: []string{"plus"}},
				},
			},
			next: &config.Config{
				CodexCustomModels: []config.CodexCustomModel{
					{ID: "gpt-5.5-codex", DisplayName: "GPT 5.5 Codex", Groups: []string{"plus", "pro"}},
				},
			},
			want: true,
		},
		{
			name: "custom models unchanged",
			previous: &config.Config{
				CodexCustomModels: []config.CodexCustomModel{
					{ID: "gpt-5.5-codex", DisplayName: "GPT 5.5 Codex", Groups: []string{"plus", "pro"}},
				},
			},
			next: &config.Config{
				CodexCustomModels: []config.CodexCustomModel{
					{ID: "gpt-5.5-codex", DisplayName: "GPT 5.5 Codex", Groups: []string{"plus", "pro"}},
				},
			},
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRefreshCodexRegistrations(tc.previous, tc.next); got != tc.want {
				t.Fatalf("shouldRefreshCodexRegistrations() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAuthModelExclusionsSignature(t *testing.T) {
	previous := &config.Config{
		AuthModelExclusions: []config.AuthModelExclusionRule{
			{Models: []string{"gpt-image-2"}, Priorities: []int{-1}},
		},
	}
	nextSame := &config.Config{
		AuthModelExclusions: []config.AuthModelExclusionRule{
			{Models: []string{"gpt-image-2"}, Priorities: []int{-1}},
		},
	}
	nextChanged := &config.Config{
		AuthModelExclusions: []config.AuthModelExclusionRule{
			{Models: []string{"gpt-image-2"}, KeywordContains: []string{"free"}},
		},
	}

	if authModelExclusionsSignature(previous) != authModelExclusionsSignature(nextSame) {
		t.Fatal("expected unchanged auth model exclusions to have the same signature")
	}
	if authModelExclusionsSignature(previous) == authModelExclusionsSignature(nextChanged) {
		t.Fatal("expected changed auth model exclusions to have a different signature")
	}
}

func TestServiceDeleteCoreAuth_DeleteFailureKeepsRuntimeAndModels(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(&serviceFailingDeleteStore{}, nil, nil),
	}

	auth := &coreauth.Auth{
		ID:       "service-delete-failure-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
	service.antigravityModelCapabilities.Store(auth.ID, &antigravityModelCapabilityCacheEntry{
		RuntimeInstanceID: auth.RuntimeInstanceID(),
		Hints: antigravityModelCapabilityHints{
			WebSearchModelIDs: map[string]struct{}{"gemini-3.1-flash-lite": {}},
		},
	})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	if err := service.deleteCoreAuth(context.Background(), auth.ID); err == nil {
		t.Fatal("expected deleteCoreAuth to report delete failure")
	}

	if _, ok := service.coreManager.GetByID(auth.ID); !ok {
		t.Fatal("expected auth to remain registered after delete failure")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); len(models) == 0 {
		t.Fatalf("expected models to remain registered after delete failure for %q", auth.ID)
	}
	if _, exists := service.antigravityModelCapabilities.Load(auth.ID); !exists {
		t.Fatal("expected capability cache to remain after delete persistence failure")
	}
}

func TestServiceDeleteCoreAuth_ClearsAntigravityCapabilityCache(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{ID: "service-delete-antigravity-cache", Provider: "antigravity", Status: coreauth.StatusActive}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	service.antigravityModelCapabilities.Store(auth.ID, &antigravityModelCapabilityCacheEntry{
		RuntimeInstanceID: auth.RuntimeInstanceID(),
		Hints: antigravityModelCapabilityHints{
			WebSearchModelIDs: map[string]struct{}{"gemini-3.1-flash-lite": {}},
		},
	})

	if errDelete := service.deleteCoreAuth(context.Background(), auth.ID); errDelete != nil {
		t.Fatalf("delete auth: %v", errDelete)
	}
	if _, exists := service.antigravityModelCapabilities.Load(auth.ID); exists {
		t.Fatal("capability cache remained after successful auth deletion")
	}
}

func containsRegisteredModel(models []*registry.ModelInfo, modelID string) bool {
	for _, model := range models {
		if model != nil && strings.EqualFold(strings.TrimSpace(model.ID), modelID) {
			return true
		}
	}
	return false
}

func readServiceTestAuthMetadata(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read auth file: %v", err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatalf("unmarshal auth file: %v", err)
	}
	return metadata
}

func TestServiceDeleteAuthMaintenanceCandidate_PersistsDelete(t *testing.T) {
	authDir := t.TempDir()
	store := &serviceCountingDeleteStore{}
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	path := filepath.Join(authDir, "service-maintenance-persist-delete-auth.json")
	raw := []byte(`{"type":"claude","email":"persist@example.com"}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	auth := &coreauth.Auth{
		ID:       "service-maintenance-persist-delete-auth.json",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "claude", "email": "persist@example.com"},
	}
	if _, err := service.coreManager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(auth, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected auth maintenance candidate")
	}

	deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), candidate)
	if err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate returned error: %v", err)
	}
	if !deleted {
		t.Fatal("expected maintenance delete to complete")
	}
	if got := store.deleteCount.Load(); got != 1 {
		t.Fatalf("delete count = %d, want 1", got)
	}
	if _, ok := service.coreManager.GetByID(auth.ID); ok {
		t.Fatal("expected auth to be removed from runtime state")
	}
}

func TestServiceHandleAuthMaintenanceResult_DisablesStatusCodeWithoutDeletingFile(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	service := &Service{
		cfg: &config.Config{
			AuthDir: authDir,
			AuthMaintenance: config.AuthMaintenanceConfig{
				Enable:             true,
				DisableStatusCodes: []int{http.StatusUnauthorized},
			},
		},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	path := filepath.Join(authDir, "service-maintenance-disable-401-auth.json")
	if err := os.WriteFile(path, []byte(`{"type":"claude","email":"disabled@example.com"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	auth := &coreauth.Auth{
		ID:       "service-maintenance-disable-401-auth.json",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "claude", "email": "disabled@example.com"},
	}
	if _, err := service.coreManager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	service.handleAuthMaintenanceResult(context.Background(), coreauth.Result{
		AuthID:   auth.ID,
		Provider: auth.Provider,
		Success:  false,
		Error:    &coreauth.Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
	})

	current, ok := service.coreManager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if !current.Disabled || current.Status != coreauth.StatusDisabled {
		t.Fatalf("disabled=%v status=%q, want disabled status", current.Disabled, current.Status)
	}
	if got, _ := current.Metadata[authMaintenanceActionMetadataKey].(string); got != authMaintenanceDisableAction {
		t.Fatalf("maintenance action = %q, want %q", got, authMaintenanceDisableAction)
	}
	if got, _ := current.Metadata[authMaintenanceReasonMetadataKey].(string); got != "http_401" {
		t.Fatalf("maintenance reason = %q, want http_401", got)
	}
	metadata := readServiceTestAuthMetadata(t, path)
	if disabled, _ := metadata["disabled"].(bool); !disabled {
		t.Fatalf("persisted disabled = %#v, want true", metadata["disabled"])
	}
	service.maintenanceMu.Lock()
	queueLen := len(service.maintenanceQueue)
	service.maintenanceMu.Unlock()
	if queueLen != 0 {
		t.Fatalf("maintenance queue length = %d, want 0", queueLen)
	}
}

func TestServiceScanAuthMaintenance_DisablesStatusCodeWithoutQueueingDelete(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	path := filepath.Join(authDir, "service-maintenance-scan-disable-401-auth.json")
	if err := os.WriteFile(path, []byte(`{"type":"claude","email":"scan@example.com"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	auth := &coreauth.Auth{
		ID:            "service-maintenance-scan-disable-401-auth.json",
		Provider:      "claude",
		Status:        coreauth.StatusError,
		StatusMessage: "unauthorized",
		LastError:     &coreauth.Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
		FileName:      path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "claude", "email": "scan@example.com"},
	}
	if _, err := service.coreManager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	cfg := config.AuthMaintenanceConfig{
		Enable:             true,
		DisableStatusCodes: []int{http.StatusUnauthorized},
	}
	if candidates := service.scanAuthMaintenanceCandidates(cfg, authDir); len(candidates) != 0 {
		t.Fatalf("delete candidates = %d, want 0", len(candidates))
	}
	candidates := service.scanAuthMaintenanceDisableCandidates(cfg, authDir)
	if len(candidates) != 1 {
		t.Fatalf("disable candidates = %d, want 1", len(candidates))
	}
	if got := strings.TrimSpace(candidates[0].Reason); got != "http_401" {
		t.Fatalf("candidate reason = %q, want http_401", got)
	}
	if !service.disableAuthMaintenanceCandidate(context.Background(), candidates[0], false) {
		t.Fatal("expected disable maintenance candidate to persist")
	}
	current, ok := service.coreManager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if !current.Disabled || current.Status != coreauth.StatusDisabled {
		t.Fatalf("disabled=%v status=%q, want disabled status", current.Disabled, current.Status)
	}
	if candidates := service.scanAuthMaintenanceDisableCandidates(cfg, authDir); len(candidates) != 0 {
		t.Fatalf("disable candidates after disabled auth = %d, want 0", len(candidates))
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected auth file to remain after disable, stat err=%v", err)
	}
}

func TestServiceAuthMaintenance_DeleteStatusCodeTakesPrecedenceOverDisable(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	service := &Service{
		cfg: &config.Config{
			AuthDir: authDir,
			AuthMaintenance: config.AuthMaintenanceConfig{
				Enable:             true,
				DeleteStatusCodes:  []int{http.StatusUnauthorized},
				DisableStatusCodes: []int{http.StatusUnauthorized},
			},
		},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	path := filepath.Join(authDir, "service-maintenance-delete-priority-401-auth.json")
	if err := os.WriteFile(path, []byte(`{"type":"claude","email":"delete-priority@example.com"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	auth := &coreauth.Auth{
		ID:       "service-maintenance-delete-priority-401-auth.json",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "claude", "email": "delete-priority@example.com"},
	}
	if _, err := service.coreManager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	service.handleAuthMaintenanceResult(context.Background(), coreauth.Result{
		AuthID:   auth.ID,
		Provider: auth.Provider,
		Success:  false,
		Error:    &coreauth.Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
	})

	current, ok := service.coreManager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered before queued delete runs")
	}
	if !authMaintenancePendingDelete(current) {
		t.Fatal("expected auth to be marked pending delete")
	}
	if got, _ := current.Metadata[authMaintenanceActionMetadataKey].(string); got != authMaintenanceDeleteAction {
		t.Fatalf("maintenance action = %q, want %q", got, authMaintenanceDeleteAction)
	}
	service.maintenanceMu.Lock()
	queueLen := len(service.maintenanceQueue)
	service.maintenanceMu.Unlock()
	if queueLen != 1 {
		t.Fatalf("maintenance queue length = %d, want 1", queueLen)
	}
}

func TestServiceStartAuthMaintenance_QueuesDeleteOnlyAfterPendingDeletePersists(t *testing.T) {
	authDir := t.TempDir()
	store := &serviceToggleSaveStore{}
	service := &Service{
		cfg: &config.Config{
			AuthDir: authDir,
			AuthMaintenance: config.AuthMaintenanceConfig{
				Enable:                true,
				ScanIntervalSeconds:   1,
				DeleteIntervalSeconds: 1,
				DeleteQuotaExceeded:   true,
				QuotaStrikeThreshold:  1,
			},
		},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	path := filepath.Join(authDir, "service-maintenance-pending-delete-save-failure.json")
	raw := []byte(`{"type":"claude","email":"persist@example.com"}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	auth := &coreauth.Auth{
		ID:       "service-maintenance-pending-delete-save-failure.json",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "claude", "email": "persist@example.com"},
		Quota: coreauth.QuotaState{
			Exceeded:    true,
			StrikeCount: 1,
		},
	}
	if _, err := service.coreManager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	store.failSave.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.startAuthMaintenance(ctx)
	defer service.stopAuthMaintenance()
	service.wakeAuthMaintenance()

	deadline := time.Now().Add(2 * time.Second)
	for store.saveCount.Load() <= 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := store.saveCount.Load(); got <= 1 {
		t.Fatalf("expected maintenance disable to attempt persistence, save count = %d", got)
	}

	service.maintenanceMu.Lock()
	queueLen := len(service.maintenanceQueue)
	_, pending := service.maintenancePending[path]
	service.maintenanceMu.Unlock()
	if queueLen != 0 {
		t.Fatalf("expected failed pending-delete save not to queue maintenance delete, got %d items", queueLen)
	}
	if pending {
		t.Fatal("expected failed pending-delete save not to leave a pending maintenance entry")
	}

	current, ok := service.coreManager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered after failed pending-delete update")
	}
	if authMaintenancePendingDelete(current) {
		t.Fatal("expected auth to remain unmarked when pending-delete persistence fails")
	}
	if got := strings.TrimSpace(current.StatusMessage); got == "disabled" || strings.Contains(got, "auth maintenance") {
		t.Fatalf("status message = %q, want active auth state after failed pending-delete update", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected auth file to remain after failed pending-delete update, stat err=%v", err)
	}
}

func TestServiceHandleAuthMaintenanceResult_QueuesDeleteOnlyAfterPendingDeletePersists(t *testing.T) {
	authDir := t.TempDir()
	store := &serviceToggleSaveStore{}
	service := &Service{
		cfg: &config.Config{
			AuthDir: authDir,
			AuthMaintenance: config.AuthMaintenanceConfig{
				Enable:                true,
				DeleteQuotaExceeded:   true,
				QuotaStrikeThreshold:  1,
				DeleteIntervalSeconds: 1,
			},
		},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	path := filepath.Join(authDir, "service-result-pending-delete-save-failure.json")
	raw := []byte(`{"type":"claude","email":"result@example.com"}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	auth := &coreauth.Auth{
		ID:       "service-result-pending-delete-save-failure.json",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "claude", "email": "result@example.com"},
		Quota: coreauth.QuotaState{
			Exceeded:    true,
			StrikeCount: 1,
		},
	}
	if _, err := service.coreManager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	store.failSave.Store(true)
	service.handleAuthMaintenanceResult(context.Background(), coreauth.Result{
		AuthID:   auth.ID,
		Provider: auth.Provider,
		Success:  false,
		Error:    &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"},
	})

	if got := store.saveCount.Load(); got <= 1 {
		t.Fatalf("expected result-driven pending-delete path to attempt persistence, save count = %d", got)
	}

	service.maintenanceMu.Lock()
	queueLen := len(service.maintenanceQueue)
	_, pending := service.maintenancePending[path]
	service.maintenanceMu.Unlock()
	if queueLen != 0 {
		t.Fatalf("expected failed pending-delete save not to queue result-driven maintenance delete, got %d items", queueLen)
	}
	if pending {
		t.Fatal("expected failed pending-delete save not to leave a pending maintenance entry")
	}

	current, ok := service.coreManager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered after failed result-driven pending-delete update")
	}
	if authMaintenancePendingDelete(current) {
		t.Fatal("expected auth to remain unmarked when result-driven pending-delete persistence fails")
	}
	if got := strings.TrimSpace(current.StatusMessage); got == "disabled" || strings.Contains(got, "auth maintenance") {
		t.Fatalf("status message = %q, want active auth state after failed result-driven pending-delete update", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected auth file to remain after failed result-driven pending-delete update, stat err=%v", err)
	}
}

func TestServiceDeleteAuthMaintenanceCandidate_DeleteFailureRestoresFile(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(&serviceFailingDeleteStore{}, nil, nil),
	}

	path := filepath.Join(authDir, "service-maintenance-delete-failure-auth.json")
	raw := []byte(`{"type":"claude","email":"persist@example.com"}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	auth := &coreauth.Auth{
		ID:       "service-maintenance-delete-failure-auth.json",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "claude", "email": "persist@example.com"},
	}
	if _, err := service.coreManager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(auth, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected auth maintenance candidate")
	}

	deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), candidate)
	if err == nil {
		t.Fatal("expected maintenance delete to report delete failure")
	}
	if deleted {
		t.Fatal("expected failed maintenance delete not to report success")
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("expected auth file to be restored after delete failure, stat err=%v", statErr)
	}
	if _, ok := service.coreManager.GetByID(auth.ID); !ok {
		t.Fatal("expected auth to remain registered after delete failure")
	}
}

func TestServiceDeleteAuthMaintenanceCandidate_PersistsSharedFileOnce(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "service-maintenance-recheck-between-deletes.json")
	raw := []byte(`{"type":"claude","email":"persist@example.com"}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	var recreated atomic.Bool
	store := &serviceDeleteSideEffectStore{
		onDelete: func(id string) {
			if recreated.Swap(true) {
				return
			}
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Errorf("recreate auth file: %v", err)
			}
		},
	}
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	authA := &coreauth.Auth{
		ID:       "auth-a",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "claude", "email": "a@example.com"},
	}
	authB := &coreauth.Auth{
		ID:       "auth-b",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "claude", "email": "b@example.com"},
	}
	if _, err := service.coreManager.Register(context.Background(), authA); err != nil {
		t.Fatalf("register authA: %v", err)
	}
	if _, err := service.coreManager.Register(context.Background(), authB); err != nil {
		t.Fatalf("register authB: %v", err)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(authA, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected auth maintenance candidate")
	}

	deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), candidate)
	if err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate returned error: %v", err)
	}
	if !deleted {
		t.Fatal("expected shared-path maintenance delete to complete")
	}
	if got := store.deleteCount.Load(); got != 1 {
		t.Fatalf("delete count = %d, want 1", got)
	}
	if _, ok := service.coreManager.GetByID(authA.ID); ok {
		t.Fatal("expected first shared runtime auth to be removed")
	}
	if _, ok := service.coreManager.GetByID(authB.ID); ok {
		t.Fatal("expected second shared runtime auth to be removed without another store delete")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected recreated auth file to remain, stat err=%v", err)
	}
}

func TestServiceHandleManagementAuthStatusChange_CancelsMaintenanceDelete(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	auth := &coreauth.Auth{
		ID:       "service-maintenance-cancel-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: filepath.Join(authDir, "service-maintenance-cancel-auth.json"),
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(auth, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected auth maintenance candidate")
	}
	if !service.enqueueAuthMaintenanceCandidate(candidate) {
		t.Fatal("expected candidate to be enqueued")
	}
	dequeued, ok := service.dequeueAuthMaintenanceCandidate()
	if !ok {
		t.Fatal("expected candidate to be dequeued")
	}

	service.handleManagementAuthStatusChange(context.Background(), auth)

	if !service.authMaintenanceCandidateCanceled(dequeued) {
		t.Fatal("expected dequeued maintenance candidate to be canceled after manual re-enable")
	}

	queuedCandidate, ok := service.authMaintenanceCandidateForAuth(auth, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected queued auth maintenance candidate")
	}
	if !service.enqueueAuthMaintenanceCandidate(queuedCandidate) {
		t.Fatal("expected queued candidate to be enqueued")
	}
	service.handleManagementAuthStatusChange(context.Background(), auth)

	service.maintenanceMu.Lock()
	defer service.maintenanceMu.Unlock()
	if len(service.maintenanceQueue) != 0 {
		t.Fatalf("expected maintenance queue to be empty, got %d items", len(service.maintenanceQueue))
	}
	if _, exists := service.maintenancePending[candidate.Key]; exists {
		t.Fatal("expected pending maintenance entry to be removed")
	}
	if service.maintenanceGeneration[candidate.Key] == 0 {
		t.Fatal("expected maintenance generation to advance after cancellation")
	}
}

func TestServiceHandleAuthUpdate_AddCancelsMaintenanceDelete(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	auth := &coreauth.Auth{
		ID:       "service-auth-update-cancel-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: filepath.Join(authDir, "service-auth-update-cancel-auth.json"),
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(auth, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected auth maintenance candidate")
	}
	if !service.enqueueAuthMaintenanceCandidate(candidate) {
		t.Fatal("expected candidate to be enqueued")
	}
	dequeued, ok := service.dequeueAuthMaintenanceCandidate()
	if !ok {
		t.Fatal("expected candidate to be dequeued")
	}

	reloaded := auth.Clone()
	reloaded.Metadata = map[string]any{"type": "claude", "note": "reloaded"}
	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionModify,
		ID:     reloaded.ID,
		Auth:   reloaded,
	})

	if !service.authMaintenanceCandidateCanceled(dequeued) {
		t.Fatal("expected dequeued maintenance candidate to be canceled after auth reload")
	}

	if !service.enqueueAuthMaintenanceCandidate(candidate) {
		t.Fatal("expected candidate to be enqueued again")
	}
	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionAdd,
		ID:     reloaded.ID,
		Auth:   reloaded,
	})

	service.maintenanceMu.Lock()
	defer service.maintenanceMu.Unlock()
	if len(service.maintenanceQueue) != 0 {
		t.Fatalf("expected maintenance queue to be empty, got %d items", len(service.maintenanceQueue))
	}
	if _, exists := service.maintenancePending[candidate.Key]; exists {
		t.Fatal("expected pending maintenance entry to be removed")
	}
	if service.maintenanceGeneration[candidate.Key] == 0 {
		t.Fatal("expected maintenance generation to advance after auth reload cancellation")
	}
}

func TestServiceHandleAuthUpdate_MaintenanceRewriteKeepsDeleteQueued(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	auth := &coreauth.Auth{
		ID:       "service-auth-update-pending-delete-auth",
		Provider: "claude",
		Status:   coreauth.StatusDisabled,
		FileName: filepath.Join(authDir, "service-auth-update-pending-delete-auth.json"),
		Metadata: map[string]any{
			"type":                                  "claude",
			authMaintenancePendingDeleteMetadataKey: true,
		},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(auth, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected auth maintenance candidate")
	}
	if !service.enqueueAuthMaintenanceCandidate(candidate) {
		t.Fatal("expected candidate to be enqueued")
	}

	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionModify,
		ID:     auth.ID,
		Auth:   auth.Clone(),
	})

	service.maintenanceMu.Lock()
	defer service.maintenanceMu.Unlock()
	if len(service.maintenanceQueue) != 1 {
		t.Fatalf("expected maintenance queue to keep pending delete candidate, got %d items", len(service.maintenanceQueue))
	}
	if _, exists := service.maintenancePending[candidate.Key]; !exists {
		t.Fatal("expected pending maintenance entry to remain after maintenance rewrite")
	}
}

func TestServiceDeleteAuthMaintenanceCandidate_MissingPathDoesNotEmitDelete(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
		authUpdates: make(chan watcher.AuthUpdate, 1),
	}

	auth := &coreauth.Auth{
		ID:       "service-missing-maintenance-path-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: filepath.Join(authDir, "service-missing-maintenance-path-auth.json"),
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	candidate := authMaintenanceCandidate{
		Key:    auth.FileName,
		Path:   auth.FileName,
		IDs:    []string{auth.ID},
		Reason: "quota_delete_6",
	}
	deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), candidate)
	if err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate returned error: %v", err)
	}
	if deleted {
		t.Fatal("expected missing maintenance path to be treated as stale, not deleted")
	}

	select {
	case update := <-service.authUpdates:
		t.Fatalf("expected no auth delete update for missing path, got action=%v id=%s", update.Action, update.ID)
	default:
	}
	if _, ok := service.coreManager.GetByID(auth.ID); !ok {
		t.Fatal("expected auth to remain registered when maintenance path is already missing")
	}
}

func TestServiceDeleteAuthMaintenanceCandidate_CanceledCandidateDoesNotDelete(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
		authUpdates: make(chan watcher.AuthUpdate, 1),
	}

	path := filepath.Join(authDir, "service-canceled-maintenance-auth.json")
	if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	auth := &coreauth.Auth{
		ID:       "service-canceled-maintenance-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(auth, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected auth maintenance candidate")
	}
	if !service.enqueueAuthMaintenanceCandidate(candidate) {
		t.Fatal("expected candidate to be enqueued")
	}
	dequeued, ok := service.dequeueAuthMaintenanceCandidate()
	if !ok {
		t.Fatal("expected candidate to be dequeued")
	}
	service.cancelAuthMaintenanceCandidate(candidate)

	deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), dequeued)
	if err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate returned error: %v", err)
	}
	if deleted {
		t.Fatal("expected canceled maintenance candidate to skip deletion")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected auth file to remain after canceled maintenance delete, stat err=%v", err)
	}
	select {
	case update := <-service.authUpdates:
		t.Fatalf("expected no auth delete update for canceled candidate, got action=%v id=%s", update.Action, update.ID)
	default:
	}
}

func TestServiceDeleteAuthMaintenanceCandidate_CancelAfterStartRestoresFile(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
		authUpdates: make(chan watcher.AuthUpdate, 1),
	}

	path := filepath.Join(authDir, "service-cancel-after-start-auth.json")
	if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	auth := &coreauth.Auth{
		ID:       "service-cancel-after-start-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(auth, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected auth maintenance candidate")
	}
	originalRead := readAuthMaintenanceFile
	t.Cleanup(func() { readAuthMaintenanceFile = originalRead })
	started := make(chan struct{})
	releaseRead := make(chan struct{})
	var blocked atomic.Bool
	var reads atomic.Int32
	readAuthMaintenanceFile = func(targetPath string) ([]byte, error) {
		if targetPath == path && reads.Add(1) == 2 && blocked.CompareAndSwap(false, true) {
			close(started)
			<-releaseRead
		}
		return originalRead(targetPath)
	}

	type deleteResult struct {
		deleted bool
		err     error
	}
	done := make(chan deleteResult, 1)
	go func() {
		deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), candidate)
		done <- deleteResult{deleted: deleted, err: err}
	}()

	<-started
	service.cancelAuthMaintenanceCandidate(candidate)
	if !service.authMaintenanceCandidateCanceled(candidate) {
		t.Fatal("expected cancel to advance maintenance generation after delete started")
	}
	close(releaseRead)

	result := <-done
	if result.err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate returned error: %v", result.err)
	}
	if result.deleted {
		t.Fatal("expected canceled maintenance delete to be treated as skipped")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected auth file to be restored after cancellation, stat err=%v", err)
	}
	select {
	case update := <-service.authUpdates:
		t.Fatalf("expected no auth delete update after canceled in-flight delete, got action=%v id=%s", update.Action, update.ID)
	default:
	}
}

func TestServiceDeleteAuthMaintenanceCandidate_CancelAfterStartRestoresCurrentRuntimeAuth(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(store, nil, nil),
		authUpdates: make(chan watcher.AuthUpdate, 1),
	}

	path := filepath.Join(authDir, "service-cancel-after-start-runtime-restore-auth.json")
	originalContents := []byte(`{"type":"claude","broken":true}`)
	repairedContents := []byte(`{"type":"claude","broken":false}`)
	if err := os.WriteFile(path, originalContents, 0o644); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	auth := &coreauth.Auth{
		ID:       "service-cancel-after-start-runtime-restore-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{"type": "claude", "broken": true},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(auth, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected auth maintenance candidate")
	}

	originalRead := readAuthMaintenanceFile
	t.Cleanup(func() { readAuthMaintenanceFile = originalRead })
	started := make(chan struct{})
	releaseRead := make(chan struct{})
	var blocked atomic.Bool
	var reads atomic.Int32
	readAuthMaintenanceFile = func(targetPath string) ([]byte, error) {
		if targetPath == path && reads.Add(1) == 2 && blocked.CompareAndSwap(false, true) {
			close(started)
			<-releaseRead
		}
		return originalRead(targetPath)
	}

	type deleteResult struct {
		deleted bool
		err     error
	}
	done := make(chan deleteResult, 1)
	go func() {
		deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), candidate)
		done <- deleteResult{deleted: deleted, err: err}
	}()

	<-started
	repaired := auth.Clone()
	repaired.Metadata = map[string]any{"type": "claude", "broken": false}
	coreauth.SetSourceHashAttribute(repaired, repairedContents)
	if _, errUpdate := service.coreManager.Update(coreauth.WithSkipPersist(context.Background()), repaired); errUpdate != nil {
		t.Fatalf("update runtime auth: %v", errUpdate)
	}
	if errWrite := os.WriteFile(path, repairedContents, 0o644); errWrite != nil {
		t.Fatalf("write repaired auth file: %v", errWrite)
	}
	service.cancelAuthMaintenanceCandidate(candidate)
	close(releaseRead)

	result := <-done
	if result.err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate returned error: %v", result.err)
	}
	if result.deleted {
		t.Fatal("expected canceled maintenance delete to be treated as skipped")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored auth file: %v", err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatalf("unmarshal restored auth file: %v", err)
	}
	if broken, _ := metadata["broken"].(bool); broken {
		t.Fatalf("restored auth file should keep repaired state, got %s", data)
	}
	if got, _ := metadata["type"].(string); got != "claude" {
		t.Fatalf("type = %q, want %q", got, "claude")
	}
}

func TestServiceDeleteAuthMaintenanceCandidate_RepairBeforeDeleteKeepsNewContents(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
		authUpdates: make(chan watcher.AuthUpdate, 1),
	}

	path := filepath.Join(authDir, "service-repair-before-delete-auth.json")
	originalContents := []byte(`{"broken":true}`)
	repairedContents := []byte(`{"broken":false}`)
	if err := os.WriteFile(path, originalContents, 0o644); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	auth := &coreauth.Auth{
		ID:       "service-repair-before-delete-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(auth, authDir, "quota_delete_6")
	if !ok {
		t.Fatal("expected auth maintenance candidate")
	}

	originalRead := readAuthMaintenanceFile
	t.Cleanup(func() { readAuthMaintenanceFile = originalRead })

	var reads atomic.Int32
	readAuthMaintenanceFile = func(targetPath string) ([]byte, error) {
		if targetPath == path && reads.Add(1) == 2 {
			if err := os.WriteFile(path, repairedContents, 0o644); err != nil {
				return nil, err
			}
		}
		return originalRead(targetPath)
	}

	deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), candidate)
	if err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate returned error: %v", err)
	}
	if deleted {
		t.Fatal("expected repaired auth file to skip maintenance delete")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read repaired auth file: %v", err)
	}
	if string(data) != string(repairedContents) {
		t.Fatalf("auth file contents = %s, want %s", data, repairedContents)
	}
	select {
	case update := <-service.authUpdates:
		t.Fatalf("expected no auth delete update for repaired file, got action=%v id=%s", update.Action, update.ID)
	default:
	}
}

func TestServiceApplyCoreAuthRemovalWithReason_PendingDeleteKeepsDeleteAction(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{
		ID:       "service-pending-delete-action-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"type": "claude"},
	}
	if _, err := service.coreManager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	service.applyCoreAuthRemovalWithReason(context.Background(), auth.ID, "quota_delete_6", true)

	current, ok := service.coreManager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if got, _ := current.Metadata[authMaintenanceActionMetadataKey].(string); got != authMaintenanceDeleteAction {
		t.Fatalf("maintenance action = %q, want %q", got, authMaintenanceDeleteAction)
	}
}

func TestServiceHandleAuthUpdate_MaintenanceDeleteSkipsRescuedAuthAtSamePath(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	path := filepath.Join(authDir, "service-maintenance-delete-same-path-auth.json")
	current := &coreauth.Auth{
		ID:       "service-maintenance-delete-same-path-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: path,
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), current); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionDelete,
		ID:     current.ID,
		Auth: &coreauth.Auth{
			ID:       current.ID,
			FileName: path,
			Attributes: map[string]string{
				"path": path,
			},
			Metadata: map[string]any{
				authMaintenanceActionMetadataKey:        authMaintenanceDeleteAction,
				authMaintenancePendingDeleteMetadataKey: true,
			},
		},
	})

	remaining, ok := service.coreManager.GetByID(current.ID)
	if !ok || remaining == nil {
		t.Fatal("expected rescued auth to remain after stale maintenance delete update")
	}
	if got := resolveAuthFilePath(remaining, authDir); got != path {
		t.Fatalf("remaining auth path = %q, want %q", got, path)
	}
}

func TestServiceHandleAuthUpdate_DeleteWithStalePathKeepsReplacementAuth(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	currentPath := filepath.Join(authDir, "replacement-auth.json")
	current := &coreauth.Auth{
		ID:       "service-stale-delete-replacement-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: currentPath,
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), current); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	stalePath := filepath.Join(authDir, "old-auth.json")
	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionDelete,
		ID:     current.ID,
		Auth: &coreauth.Auth{
			ID:       current.ID,
			FileName: stalePath,
			Attributes: map[string]string{
				"path": stalePath,
			},
		},
	})

	remaining, ok := service.coreManager.GetByID(current.ID)
	if !ok || remaining == nil {
		t.Fatal("expected replacement auth to remain registered after stale path delete")
	}
	if got := resolveAuthFilePath(remaining, authDir); got != currentPath {
		t.Fatalf("remaining auth path = %q, want %q", got, currentPath)
	}
}

func TestServiceHandleAuthUpdate_DeleteMatchesSymlinkedAuthPath(t *testing.T) {
	realDir := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "auths")
	if errLink := os.Symlink(realDir, linkDir); errLink != nil {
		t.Skipf("symlink is unavailable: %v", errLink)
	}
	service := &Service{
		cfg:         &config.Config{AuthDir: linkDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	current := &coreauth.Auth{
		ID:       "service-symlink-delete-auth",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		FileName: filepath.Join(realDir, "auth.json"),
		Metadata: map[string]any{"type": "claude"},
	}
	if _, errRegister := service.coreManager.Register(context.Background(), current); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionDelete,
		ID:     current.ID,
		Auth: &coreauth.Auth{
			ID:       current.ID,
			FileName: filepath.Join(linkDir, "auth.json"),
		},
	})
	if _, exists := service.coreManager.GetByID(current.ID); exists {
		t.Fatal("equivalent symlinked delete path left auth registered")
	}
}

func TestServiceHandleAuthUpdateDeleteClearsChatGPTWebCatalogState(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "chatgpt-web-delete.json")
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{
		ID:         "chatgpt-web-direct-delete",
		Provider:   "chatgpt-web",
		Status:     coreauth.StatusActive,
		FileName:   path,
		Attributes: map[string]string{"path": path},
		Metadata: map[string]any{
			"access_token":    "token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	}
	installed, err := service.coreManager.Register(context.Background(), auth)
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	service.chatGPTWebModelCatalog.Store(installed.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID: installed.RuntimeInstanceID(),
		Models:            []*registry.ModelInfo{chatGPTWebTextModelInfo("remote-model", "", 0, "")},
	})
	service.chatGPTWebModelFetchLocks = map[string]*chatGPTWebModelFetchLockEntry{
		installed.ID: {},
	}

	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionDelete,
		ID:     installed.ID,
		Auth: &coreauth.Auth{
			ID:         installed.ID,
			FileName:   path,
			Attributes: map[string]string{"path": path},
		},
	})

	if _, exists := service.coreManager.GetByID(installed.ID); exists {
		t.Fatal("deleted ChatGPT Web auth remained registered")
	}
	if _, exists := service.chatGPTWebModelCatalog.Load(installed.ID); exists {
		t.Fatal("deleted ChatGPT Web auth retained its model catalog")
	}
	if _, exists := service.chatGPTWebModelFetchLocks[installed.ID]; exists {
		t.Fatal("deleted ChatGPT Web auth retained its model fetch lock")
	}
}

func TestServiceHandleAuthUpdate_DeleteWithStaleGenerationKeepsSamePathReplacement(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "replacement.json")
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	current := &coreauth.Auth{
		ID:         "same-path-replacement",
		Provider:   "claude",
		Status:     coreauth.StatusActive,
		FileName:   path,
		Attributes: map[string]string{"path": path},
	}
	if errSync := coreauth.SyncPersistedMetadataAndSourceHash(current, []byte(`{"type":"claude","generation":"new"}`)); errSync != nil {
		t.Fatalf("set current source hash: %v", errSync)
	}
	if _, errRegister := service.coreManager.Register(context.Background(), current); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	deleted := &coreauth.Auth{
		ID:         current.ID,
		Provider:   current.Provider,
		FileName:   path,
		Attributes: map[string]string{"path": path},
	}
	if errSync := coreauth.SyncPersistedMetadataAndSourceHash(deleted, []byte(`{"type":"claude","generation":"old"}`)); errSync != nil {
		t.Fatalf("set deleted source hash: %v", errSync)
	}

	service.handleAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionDelete,
		ID:     current.ID,
		Auth:   deleted,
	})
	if _, exists := service.coreManager.GetByID(current.ID); !exists {
		t.Fatal("stale same-path delete removed replacement generation")
	}
}

func TestServiceAuthMaintenanceCandidateForAuth_ExcludesRuntimeOnlyChildrenFromFileGroup(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	path := filepath.Join(authDir, "oauth-multi.json")
	primary := &coreauth.Auth{
		ID:       "oauth-multi.json",
		Provider: "claude",
		Status:   coreauth.StatusDisabled,
		Disabled: true,
		FileName: path,
		Attributes: map[string]string{
			"path": path,
		},
		Metadata: map[string]any{
			authMaintenanceActionMetadataKey:        authMaintenanceDeleteAction,
			authMaintenancePendingDeleteMetadataKey: true,
			authMaintenanceReasonMetadataKey:        "pending_delete",
			"type":                                  "claude",
		},
	}
	virtualA := &coreauth.Auth{
		ID:       "oauth-multi.json#child-a",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path":         path,
			"runtime_only": "true",
		},
		Metadata: map[string]any{
			authMaintenanceActionMetadataKey:        authMaintenanceDeleteAction,
			authMaintenancePendingDeleteMetadataKey: true,
			authMaintenanceReasonMetadataKey:        "pending_delete",
			"type":                                  "claude",
		},
	}
	virtualB := &coreauth.Auth{
		ID:       "oauth-multi.json#child-b",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path":         path,
			"runtime_only": "true",
		},
		Metadata: map[string]any{
			authMaintenanceActionMetadataKey:        authMaintenanceDeleteAction,
			authMaintenancePendingDeleteMetadataKey: true,
			authMaintenanceReasonMetadataKey:        "pending_delete",
			"type":                                  "claude",
		},
	}

	for _, auth := range []*coreauth.Auth{primary, virtualA, virtualB} {
		if _, err := service.coreManager.Register(context.Background(), auth); err != nil {
			t.Fatalf("register auth %s: %v", auth.ID, err)
		}
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(primary, authDir, "pending_delete")
	if !ok {
		t.Fatal("expected auth maintenance candidate for primary auth")
	}
	if got := strings.TrimSpace(candidate.Path); got != path {
		t.Fatalf("candidate path = %q, want %q", got, path)
	}
	if len(candidate.IDs) != 1 || candidate.IDs[0] != primary.ID {
		t.Fatalf("candidate IDs = %v, want only %q", candidate.IDs, primary.ID)
	}

	scanned := service.scanAuthMaintenanceCandidates(config.AuthMaintenanceConfig{Enable: true}, authDir)
	if len(scanned) != 1 {
		t.Fatalf("scanAuthMaintenanceCandidates() returned %d candidates, want 1", len(scanned))
	}
	if got := strings.TrimSpace(scanned[0].Path); got != path {
		t.Fatalf("scanned candidate path = %q, want %q", got, path)
	}
	if len(scanned[0].IDs) != 1 || scanned[0].IDs[0] != primary.ID {
		t.Fatalf("scanned candidate IDs = %v, want only %q", scanned[0].IDs, primary.ID)
	}
}
