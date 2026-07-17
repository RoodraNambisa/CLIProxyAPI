package cliproxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

type serviceChatGPTWebReplacementHook struct {
	coreauth.NoopHook
	replacements atomic.Int32
	updated      chan struct{}
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

func TestServiceApplyCoreAuthAddOrUpdate_FallsBackToInlineSyncWhenQueueIsFull(t *testing.T) {
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

	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) == 0 {
		t.Fatalf("expected inline model sync to register models for %q", authID)
	}
	if _, exists := service.modelSyncPending[authID]; exists {
		t.Fatalf("expected inline fallback to clear pending state for %q", authID)
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

func TestAuthMaintenanceHookFallsBackInlineWhenModelSyncQueueIsFull(t *testing.T) {
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

	if _, pending := service.modelSyncPending[auth.ID]; pending {
		t.Fatal("queue-full inline fallback left an unowned pending task")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); len(models) == 0 {
		t.Fatal("queue-full hook did not run the inline model sync fallback")
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

func TestServiceSyncAuthModelsReReadsProviderAfterTransitionWait(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
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

	unlockTransition := service.lockAuthModelTransition(authID)
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		service.syncAuthModels(context.Background(), authID)
		close(done)
	}()
	<-started

	select {
	case <-done:
		unlockTransition()
		t.Fatal("model sync bypassed the provider transition lock")
	case <-time.After(25 * time.Millisecond):
	}

	replacement := oldAuth.Clone()
	replacement.Provider = "xai"
	installed, err := manager.Update(context.Background(), replacement)
	if err != nil {
		unlockTransition()
		<-done
		t.Fatalf("install replacement auth: %v", err)
	}
	unlockTransition()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("model sync did not finish after provider transition")
	}
	if provider := registry.GetGlobalRegistry().GetProviderForClient(authID); provider != installed.Provider {
		t.Fatalf("registered provider = %q, want %q", provider, installed.Provider)
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(authID)
	if len(models) == 0 || !containsRegisteredModel(models, registry.GetXAIModels()[0].ID) {
		t.Fatalf("provider transition registered stale models: %v", registeredModelIDs(models))
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

func TestServiceSyncAuthModelsInlineDrainsDirtyTaskWithFullQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const authID = "dirty-model-sync"
	service := &Service{
		modelSyncCancel: cancel,
		modelSyncQueue:  make(chan string, 1),
		modelSyncPending: map[string]modelSyncTaskState{
			authID: {dirty: true},
		},
	}
	service.modelSyncQueue <- "occupied"

	done := make(chan struct{})
	go func() {
		service.syncAuthModelsInline(ctx, authID)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dirty model sync blocked on a full worker queue")
	}
	service.modelSyncMu.Lock()
	_, pending := service.modelSyncPending[authID]
	service.modelSyncMu.Unlock()
	if pending {
		t.Fatal("dirty model sync remained pending after inline drain")
	}
	if queuedID := <-service.modelSyncQueue; queuedID != "occupied" {
		t.Fatalf("worker queue item = %q, want occupied", queuedID)
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

func TestServiceDeleteAuthMaintenanceCandidate_RechecksBetweenAuthDeletes(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "service-maintenance-recheck-between-deletes.json")
	raw := []byte(`{"type":"claude","email":"persist@example.com"}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	var recreated atomic.Bool
	firstDeletedID := ""
	store := &serviceDeleteSideEffectStore{
		onDelete: func(id string) {
			if recreated.Swap(true) {
				return
			}
			firstDeletedID = id
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
	if deleted {
		t.Fatal("expected recreated auth file to stop stale maintenance delete")
	}
	if got := store.deleteCount.Load(); got != 1 {
		t.Fatalf("delete count = %d, want 1", got)
	}
	if firstDeletedID == "" {
		t.Fatal("expected one auth delete before file recreation")
	}
	if firstDeletedID == authA.ID {
		if _, ok := service.coreManager.GetByID(authA.ID); ok {
			t.Fatal("expected first deleted auth to be removed before file recreation")
		}
		if _, ok := service.coreManager.GetByID(authB.ID); !ok {
			t.Fatal("expected remaining auth to stay after file recreation")
		}
	} else if firstDeletedID == authB.ID {
		if _, ok := service.coreManager.GetByID(authB.ID); ok {
			t.Fatal("expected first deleted auth to be removed before file recreation")
		}
		if _, ok := service.coreManager.GetByID(authA.ID); !ok {
			t.Fatal("expected remaining auth to stay after file recreation")
		}
	} else {
		t.Fatalf("unexpected deleted auth id %q", firstDeletedID)
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

	originalRemove := removeAuthMaintenanceFile
	t.Cleanup(func() {
		removeAuthMaintenanceFile = originalRemove
	})

	started := make(chan struct{})
	releaseRemove := make(chan struct{})
	var blocked atomic.Bool
	removeAuthMaintenanceFile = func(targetPath string) error {
		err := originalRemove(targetPath)
		if err == nil && blocked.CompareAndSwap(false, true) {
			close(started)
			<-releaseRemove
		}
		return err
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
	close(releaseRemove)

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

	originalRemove := removeAuthMaintenanceFile
	t.Cleanup(func() {
		removeAuthMaintenanceFile = originalRemove
	})

	started := make(chan struct{})
	releaseRemove := make(chan struct{})
	var blocked atomic.Bool
	removeAuthMaintenanceFile = func(targetPath string) error {
		err := originalRemove(targetPath)
		if err == nil && blocked.CompareAndSwap(false, true) {
			close(started)
			<-releaseRemove
		}
		return err
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
	service.cancelAuthMaintenanceCandidate(candidate)
	close(releaseRemove)

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
	originalRemoveIfMatches := removeAuthMaintenanceFileIfSnapshotMatches
	t.Cleanup(func() {
		readAuthMaintenanceFile = originalRead
		removeAuthMaintenanceFileIfSnapshotMatches = originalRemoveIfMatches
	})

	var reads atomic.Int32
	readAuthMaintenanceFile = func(targetPath string) ([]byte, error) {
		if targetPath == path && reads.Add(1) == 3 {
			if err := os.WriteFile(path, repairedContents, 0o644); err != nil {
				return nil, err
			}
		}
		return originalRead(targetPath)
	}
	removeAuthMaintenanceFileIfSnapshotMatches = originalRemoveIfMatches

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
