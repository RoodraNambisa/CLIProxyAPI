package cliproxy

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func newServiceStaleStateTestService() *Service {
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&chatGPTWebCatalogTestExecutor{})
	return &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
}

func TestServiceApplyCoreAuthAddOrUpdate_DeleteReAddDoesNotInheritStaleRuntimeState(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	authID := "service-stale-state-auth"
	modelID := "stale-model"
	lastRefreshedAt := time.Date(2026, time.March, 1, 8, 0, 0, 0, time.UTC)
	nextRefreshAfter := lastRefreshedAt.Add(30 * time.Minute)

	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:               authID,
		Provider:         "claude",
		Status:           coreauth.StatusActive,
		LastRefreshedAt:  lastRefreshedAt,
		NextRefreshAfter: nextRefreshAfter,
		ModelStates: map[string]*coreauth.ModelState{
			modelID: {
				Quota: coreauth.QuotaState{BackoffLevel: 7},
			},
		},
	})

	service.applyCoreAuthRemoval(context.Background(), authID)

	disabled, ok := service.coreManager.GetByID(authID)
	if !ok || disabled == nil {
		t.Fatalf("expected disabled auth after removal")
	}
	if !disabled.Disabled || disabled.Status != coreauth.StatusDisabled {
		t.Fatalf("expected disabled auth after removal, got disabled=%v status=%v", disabled.Disabled, disabled.Status)
	}
	if disabled.LastRefreshedAt.IsZero() {
		t.Fatalf("expected disabled auth to still carry prior LastRefreshedAt for regression setup")
	}
	if disabled.NextRefreshAfter.IsZero() {
		t.Fatalf("expected disabled auth to still carry prior NextRefreshAfter for regression setup")
	}

	// Reconcile prunes unsupported model state during registration, so seed the
	// disabled snapshot explicitly before exercising delete -> re-add behavior.
	disabled.ModelStates = map[string]*coreauth.ModelState{
		modelID: {
			Quota: coreauth.QuotaState{BackoffLevel: 7},
		},
	}
	if _, err := service.coreManager.Update(context.Background(), disabled); err != nil {
		t.Fatalf("seed disabled auth stale ModelStates: %v", err)
	}

	disabled, ok = service.coreManager.GetByID(authID)
	if !ok || disabled == nil {
		t.Fatalf("expected disabled auth after stale state seeding")
	}
	if len(disabled.ModelStates) == 0 {
		t.Fatalf("expected disabled auth to carry seeded ModelStates for regression setup")
	}

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	})

	updated, ok := service.coreManager.GetByID(authID)
	if !ok || updated == nil {
		t.Fatalf("expected re-added auth to be present")
	}
	if updated.Disabled {
		t.Fatalf("expected re-added auth to be active")
	}
	if !updated.LastRefreshedAt.IsZero() {
		t.Fatalf("expected LastRefreshedAt to reset on delete -> re-add, got %v", updated.LastRefreshedAt)
	}
	if !updated.NextRefreshAfter.IsZero() {
		t.Fatalf("expected NextRefreshAfter to reset on delete -> re-add, got %v", updated.NextRefreshAfter)
	}
	if len(updated.ModelStates) != 0 {
		t.Fatalf("expected ModelStates to reset on delete -> re-add, got %d entries", len(updated.ModelStates))
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) == 0 {
		t.Fatalf("expected re-added auth to re-register models in global registry")
	}
}

func TestServiceApplyCoreAuthAddOrUpdate_DifferentChatGPTWebAccountDoesNotInheritModelState(t *testing.T) {
	service := newServiceStaleStateTestService()
	authID := "chatgpt-web-account-replacement"
	modelID := "gpt-image-2"
	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "same-file-snapshot",
		},
		Metadata: map[string]any{
			"access_token":    "old-token",
			"email":           "old@example.com",
			"account_id":      "account-a",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	current, ok := service.coreManager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("old ChatGPT Web auth was not installed")
	}
	oldRuntimeInstanceID := current.RuntimeInstanceID()
	cooldownUntil := time.Now().Add(time.Hour).UTC()
	current.Status = coreauth.StatusError
	current.Unavailable = true
	current.NextRetryAfter = cooldownUntil
	current.CooldownScope = "auth"
	current.ModelStates = map[string]*coreauth.ModelState{
		modelID: {
			Status:         coreauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: cooldownUntil,
		},
	}
	if _, err := service.coreManager.Update(context.Background(), current); err != nil {
		t.Fatalf("seed old account model state: %v", err)
	}
	registryBeforeQuota := registry.GetGlobalRegistry().GetModelCount(modelID)
	GlobalModelRegistry().SetModelQuotaExceeded(authID, modelID)
	if got := registry.GetGlobalRegistry().GetModelCount(modelID); got >= registryBeforeQuota {
		t.Fatalf("registry quota did not suppress old account: before=%d after=%d", registryBeforeQuota, got)
	}

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "same-file-snapshot",
		},
		Metadata: map[string]any{
			"access_token":    "replacement-token",
			"email":           "replacement@example.com",
			"account_id":      "account-b",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})

	replacement, ok := service.coreManager.GetByID(authID)
	if !ok || replacement == nil {
		t.Fatal("replacement ChatGPT Web auth was not installed")
	}
	if replacement.RuntimeInstanceID() == oldRuntimeInstanceID {
		t.Fatal("different ChatGPT Web account reused the old runtime instance")
	}
	if len(replacement.ModelStates) != 0 {
		t.Fatalf("replacement inherited old account model state: %#v", replacement.ModelStates)
	}
	if replacement.Unavailable || replacement.Status != coreauth.StatusActive ||
		!replacement.NextRetryAfter.IsZero() || replacement.CooldownScope != "" {
		t.Fatalf("replacement inherited old auth state: %#v", replacement)
	}
	if got := registry.GetGlobalRegistry().GetModelCount(modelID); got != registryBeforeQuota {
		t.Fatalf("replacement retained registry quota: got=%d want=%d", got, registryBeforeQuota)
	}
}

func TestAuthMaintenanceHookResetsRegistryStateAfterChatGPTWebAccountReplacementWithoutCatalog(t *testing.T) {
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
	authID := "chatgpt-web-hook-account-replacement"
	modelID := "gpt-image-2"
	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	registered, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "old-token",
			"account_id":      "account-a",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register old account: %v", err)
	}
	service.registerModelsForAuth(registered)
	beforeQuota := registry.GetGlobalRegistry().GetModelCount(modelID)
	GlobalModelRegistry().SetModelQuotaExceeded(authID, modelID)
	if got := registry.GetGlobalRegistry().GetModelCount(modelID); got >= beforeQuota {
		t.Fatalf("registry quota did not suppress old account: before=%d after=%d", beforeQuota, got)
	}

	manager.AddHook(authMaintenanceHook{service: service})
	replacement := registered.Clone()
	replacement.Metadata["access_token"] = "replacement-token"
	replacement.Metadata["account_id"] = "account-b"
	installed, current, err := manager.UpdateIfCurrent(context.Background(), registered, replacement)
	if err != nil {
		t.Fatalf("replace account: %v", err)
	}
	if !current || installed == nil {
		t.Fatalf("replace account = (%v, %v), want current install", installed, current)
	}

	deadline := time.Now().Add(5 * time.Second)
	for registry.GetGlobalRegistry().GetModelCount(modelID) != beforeQuota {
		if time.Now().After(deadline) {
			t.Fatalf("replacement retained registry quota: got=%d want=%d", registry.GetGlobalRegistry().GetModelCount(modelID), beforeQuota)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServiceApplyCoreAuthAddOrUpdate_SameChatGPTWebAccountPreservesCooldown(t *testing.T) {
	service := newServiceStaleStateTestService()
	authID := "chatgpt-web-same-account-refresh"
	modelID := "gpt-image-2"
	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "old-token-state",
		},
		Metadata: map[string]any{
			"access_token":    "old-token",
			"email":           "same@example.com",
			"account_id":      "same-account",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	current, ok := service.coreManager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("old ChatGPT Web auth was not installed")
	}
	cooldownUntil := time.Now().Add(time.Hour).UTC()
	current.Status = coreauth.StatusError
	current.Unavailable = true
	current.NextRetryAfter = cooldownUntil
	current.CooldownScope = "auth"
	current.ModelStates = map[string]*coreauth.ModelState{
		modelID: {
			Status:         coreauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: cooldownUntil,
		},
	}
	if _, err := service.coreManager.Update(context.Background(), current); err != nil {
		t.Fatalf("seed same-account cooldown: %v", err)
	}

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "new-token-state",
		},
		Metadata: map[string]any{
			"access_token":    "new-token",
			"email":           "same@example.com",
			"account_id":      "same-account",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})

	updated, ok := service.coreManager.GetByID(authID)
	if !ok || updated == nil {
		t.Fatal("refreshed ChatGPT Web auth was not installed")
	}
	if !updated.Unavailable || updated.CooldownScope != "auth" || !updated.NextRetryAfter.Equal(cooldownUntil) {
		t.Fatalf("auth cooldown = unavailable:%t scope:%q until:%v", updated.Unavailable, updated.CooldownScope, updated.NextRetryAfter)
	}
	state := updated.ModelStates[modelID]
	if state == nil || !state.Unavailable || !state.NextRetryAfter.Equal(cooldownUntil) {
		t.Fatalf("model cooldown = %#v", state)
	}
}

func TestServiceApplyCoreAuthAddOrUpdate_NoEmailJWTRefreshPreservesCooldown(t *testing.T) {
	service := newServiceStaleStateTestService()
	authID := "chatgpt-web-no-email-jwt-refresh"
	modelID := "gpt-image-2"
	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "old-token-state",
		},
		Metadata: map[string]any{
			"access_token":    chatGPTWebTestJWT(t, "account-one", "user-one", "subject-one", "old-jti"),
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	current, ok := service.coreManager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("old ChatGPT Web auth was not installed")
	}
	cooldownUntil := time.Now().Add(time.Hour).UTC()
	current.Status = coreauth.StatusError
	current.Unavailable = true
	current.NextRetryAfter = cooldownUntil
	current.CooldownScope = "auth"
	current.ModelStates = map[string]*coreauth.ModelState{
		modelID: {
			Status:         coreauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: cooldownUntil,
		},
	}
	if _, err := service.coreManager.Update(context.Background(), current); err != nil {
		t.Fatalf("seed no-email cooldown: %v", err)
	}

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "new-token-state",
		},
		Metadata: map[string]any{
			"access_token":    chatGPTWebTestJWT(t, "account-one", "user-one", "subject-one", "new-jti"),
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})

	updated, ok := service.coreManager.GetByID(authID)
	if !ok || updated == nil {
		t.Fatal("refreshed ChatGPT Web auth was not installed")
	}
	if !updated.Unavailable || updated.CooldownScope != "auth" || !updated.NextRetryAfter.Equal(cooldownUntil) {
		t.Fatalf("auth cooldown = unavailable:%t scope:%q until:%v", updated.Unavailable, updated.CooldownScope, updated.NextRetryAfter)
	}
	state := updated.ModelStates[modelID]
	if state == nil || !state.Unavailable || !state.NextRetryAfter.Equal(cooldownUntil) {
		t.Fatalf("model cooldown = %#v", state)
	}
}

func TestServiceApplyCoreAuthAddOrUpdate_NonChatGPTUpdatePreservesModelCooldown(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	authID := "claude-model-cooldown-refresh"
	modelID := registry.GetClaudeModels()[0].ID
	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "old-source",
		},
	})
	current, ok := service.coreManager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("Claude auth was not installed")
	}
	cooldownUntil := time.Now().Add(time.Hour).UTC()
	current.ModelStates = map[string]*coreauth.ModelState{
		modelID: {
			Status:         coreauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: cooldownUntil,
		},
	}
	if _, err := service.coreManager.Update(context.Background(), current); err != nil {
		t.Fatalf("seed Claude model cooldown: %v", err)
	}

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "new-source",
		},
	})

	updated, ok := service.coreManager.GetByID(authID)
	if !ok || updated == nil {
		t.Fatal("updated Claude auth was not installed")
	}
	state := updated.ModelStates[modelID]
	if state == nil || !state.Unavailable || !state.NextRetryAfter.Equal(cooldownUntil) {
		t.Fatalf("Claude model cooldown = %#v", state)
	}
}
