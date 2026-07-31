package auth

import (
	"context"
	"sync"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

type chatGPTWebRefreshMergeBlockingExecutor struct {
	schedulerProviderTestExecutor
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (executor *chatGPTWebRefreshMergeBlockingExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	executor.once.Do(func() {
		close(executor.started)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-executor.release:
	}
	updated := auth.Clone()
	updated.Metadata["access_token"] = "refreshed-access"
	updated.Metadata["plan_type"] = "refresh-plan"
	return updated, nil
}

func TestChatGPTWebForceReloginCleanupUsesOldRuntimeInstance(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	executor := &replaceAwareExecutor{id: chatgptwebauth.Provider}
	manager.RegisterExecutor(executor)
	auth := chatGPTWebIdentityTestAuth("force-relogin-cleanup", "account-a", "user-a")
	registered, errRegister := manager.Register(WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatalf("Register() error: %v", errRegister)
	}
	oldInstanceID := registered.RuntimeInstanceID()
	cleanupDone := registered.RuntimeInstanceCleanupDone()

	updated := registered.Clone()
	updated.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-a", "user-a", "rotated@example.com")
	installed, current, errUpdate := manager.UpdateIfCurrent(
		WithForceRuntimeReplacement(WithSkipPersist(t.Context())),
		registered,
		updated,
	)
	if errUpdate != nil {
		t.Fatalf("UpdateIfCurrent() error: %v", errUpdate)
	}
	if !current || installed == nil {
		t.Fatalf("UpdateIfCurrent() = (%v, %v), want current install", installed, current)
	}
	if installed.RuntimeInstanceID() == oldInstanceID {
		t.Fatal("forced relogin reused the old runtime instance")
	}
	select {
	case <-cleanupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("forced relogin cleanup did not finish")
	}
	assertChatGPTWebRefreshCleanupCall(
		t,
		executor,
		registered.ID,
		oldInstanceID,
		installed.RuntimeInstanceID(),
		"auth_runtime_replaced",
	)
}

func TestChatGPTWebIdentityChangingRefreshCleanupUsesOldRuntimeInstance(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	executor := &replaceAwareExecutor{id: chatgptwebauth.Provider}
	manager.RegisterExecutor(executor)
	auth := chatGPTWebIdentityTestAuth("identity-changing-refresh-cleanup", "account-a", "user-a")
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("Register() error: %v", errRegister)
	}

	manager.mu.RLock()
	expected := manager.auths[auth.ID]
	baseline := expected.Clone()
	manager.mu.RUnlock()
	oldInstanceID := baseline.RuntimeInstanceID()
	updated := baseline.Clone()
	updated.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-b", "user-b", "next@example.com")

	installed, errRefresh := manager.applyRefreshedAuth(
		WithSkipPersist(t.Context()),
		expected,
		baseline,
		updated,
		time.Time{},
	)
	if errRefresh != nil {
		t.Fatalf("applyRefreshedAuth() error: %v", errRefresh)
	}
	if installed == nil {
		t.Fatal("applyRefreshedAuth() returned nil")
	}
	if installed.RuntimeInstanceID() == oldInstanceID {
		t.Fatal("identity-changing refresh reused the old runtime instance")
	}
	assertChatGPTWebRefreshCleanupCall(
		t,
		executor,
		auth.ID,
		oldInstanceID,
		installed.RuntimeInstanceID(),
		"auth_refreshed",
	)
}

func TestChatGPTWebSameIdentityTokenRefreshPreservesRuntimeInstanceState(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	executor := &replaceAwareExecutor{id: chatgptwebauth.Provider}
	manager.RegisterExecutor(executor)
	auth := chatGPTWebIdentityTestAuth("same-identity-refresh-cleanup", "account-a", "user-a")
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("Register() error: %v", errRegister)
	}

	manager.mu.RLock()
	expected := manager.auths[auth.ID]
	baseline := expected.Clone()
	manager.mu.RUnlock()
	updated := baseline.Clone()
	updated.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-a", "user-a", "rotated@example.com")

	installed, errRefresh := manager.applyRefreshedAuth(
		WithSkipPersist(t.Context()),
		expected,
		baseline,
		updated,
		time.Time{},
	)
	if errRefresh != nil {
		t.Fatalf("applyRefreshedAuth() error: %v", errRefresh)
	}
	if installed == nil {
		t.Fatal("applyRefreshedAuth() returned nil")
	}
	if installed.RuntimeInstanceID() != baseline.RuntimeInstanceID() {
		t.Fatal("same-identity token refresh replaced the runtime instance")
	}
	if calls := executor.ClosedAuthInstanceCalls(); len(calls) != 0 {
		t.Fatalf("same-instance token refresh triggered account-info cleanup: %+v", calls)
	}
}

func TestUpdateChatGPTWebReloginRejectsConcurrentCredentialMetadataChange(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := chatGPTWebRefreshMergeAuth(nil)
	auth.ID = "chatgpt-web-relogin-credential-change"
	auth.Metadata["credential_uid"] = "credential-a"
	registered, errRegister := manager.Register(WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	relogin := registered.Clone()
	relogin.Metadata["access_token"] = "relogin-access"

	_, current, errMutate := manager.MutateRuntimeMetadataIfCurrent(t.Context(), registered, func(candidate *Auth) {
		candidate.Metadata["password"] = "concurrent-password"
	})
	if errMutate != nil || !current {
		t.Fatalf("concurrent credential mutation = current %v error %v", current, errMutate)
	}
	installed, current, errRelogin := manager.UpdateChatGPTWebReloginIfCurrent(
		WithSkipPersist(t.Context()),
		registered,
		relogin,
	)
	if errRelogin != nil {
		t.Fatalf("UpdateChatGPTWebReloginIfCurrent() error = %v", errRelogin)
	}
	if current || installed != nil {
		t.Fatalf("UpdateChatGPTWebReloginIfCurrent() = (%v, %v), want superseded", installed, current)
	}
	latest, ok := manager.GetByID(registered.ID)
	if !ok || latest.Metadata["password"] != "concurrent-password" {
		t.Fatalf("latest credential = %#v", latest)
	}
}

func TestUpdateChatGPTWebReloginAllowsConcurrentRuntimeIdentityEnrichment(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := chatGPTWebRefreshMergeAuth(nil)
	auth.ID = "chatgpt-web-relogin-identity-enrichment"
	auth.Metadata["credential_uid"] = "credential-a"
	registered, errRegister := manager.Register(WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	relogin := registered.Clone()
	relogin.Metadata["access_token"] = "relogin-access"

	_, current, errMutate := manager.MutateRuntimeMetadataIfCurrent(t.Context(), registered, func(candidate *Auth) {
		candidate.Metadata["account_id"] = "account-a"
		candidate.Metadata["user_id"] = "user-a"
		candidate.Metadata["plan_type"] = "plus"
	})
	if errMutate != nil || !current {
		t.Fatalf("concurrent identity enrichment = current %v error %v", current, errMutate)
	}
	installed, current, errRelogin := manager.UpdateChatGPTWebReloginIfCurrent(
		WithSkipPersist(t.Context()),
		registered,
		relogin,
	)
	if errRelogin != nil {
		t.Fatalf("UpdateChatGPTWebReloginIfCurrent() error = %v", errRelogin)
	}
	if !current || installed == nil {
		t.Fatalf("UpdateChatGPTWebReloginIfCurrent() = (%v, %v), want current install", installed, current)
	}
	if installed.Metadata["account_id"] != "account-a" ||
		installed.Metadata["user_id"] != "user-a" ||
		installed.Metadata["plan_type"] != "plus" {
		t.Fatalf("runtime identity metadata = %#v", installed.Metadata)
	}
}

func TestUpdateChatGPTWebReloginRejectsConcurrentRuntimeIdentityConflict(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := chatGPTWebRefreshMergeAuth(nil)
	auth.ID = "chatgpt-web-relogin-identity-conflict"
	auth.Metadata["credential_uid"] = "credential-a"
	auth.Metadata["account_id"] = "account-a"
	auth.Metadata["user_id"] = "user-a"
	registered, errRegister := manager.Register(WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	relogin := registered.Clone()
	relogin.Metadata["access_token"] = "relogin-access"

	_, current, errMutate := manager.MutateRuntimeMetadataIfCurrent(t.Context(), registered, func(candidate *Auth) {
		candidate.Metadata["account_id"] = "account-b"
	})
	if errMutate != nil || !current {
		t.Fatalf("concurrent identity conflict = current %v error %v", current, errMutate)
	}
	installed, current, errRelogin := manager.UpdateChatGPTWebReloginIfCurrent(
		WithSkipPersist(t.Context()),
		registered,
		relogin,
	)
	if errRelogin != nil {
		t.Fatalf("UpdateChatGPTWebReloginIfCurrent() error = %v", errRelogin)
	}
	if current || installed != nil {
		t.Fatalf("UpdateChatGPTWebReloginIfCurrent() = (%v, %v), want superseded", installed, current)
	}
}

func assertChatGPTWebRefreshCleanupCall(
	t *testing.T,
	executor *replaceAwareExecutor,
	authID string,
	oldInstanceID string,
	currentInstanceID string,
	reason string,
) {
	t.Helper()
	calls := executor.ClosedAuthInstanceCalls()
	if len(calls) != 1 {
		t.Fatalf("cleanup calls = %+v, want one", calls)
	}
	call := calls[0]
	if call.authID != authID || call.instanceID != oldInstanceID || call.reason != reason {
		t.Fatalf(
			"cleanup call = %+v, want auth %q old instance %q reason %q",
			call,
			authID,
			oldInstanceID,
			reason,
		)
	}
	if call.instanceID == currentInstanceID {
		t.Fatalf("cleanup targeted current runtime instance %q", currentInstanceID)
	}
}

func TestLockCredentialRefreshSerializesCallers(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	releaseFirst, errLock := manager.LockCredentialRefresh(t.Context(), "web.json")
	if errLock != nil {
		t.Fatal(errLock)
	}
	acquired := make(chan struct{})
	go func() {
		releaseSecond, errSecond := manager.LockCredentialRefresh(context.Background(), "web.json")
		if errSecond == nil {
			close(acquired)
			releaseSecond()
		}
	}()
	select {
	case <-acquired:
		t.Fatal("second credential refresh lock acquired before the first was released")
	case <-time.After(50 * time.Millisecond):
	}
	releaseFirst()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second credential refresh lock did not acquire after release")
	}
}

func TestCarryForwardConcurrentRefreshMetadataPreservesCurrentOnlyCookies(t *testing.T) {
	baseline := chatGPTWebRefreshMergeAuth([]chatgptwebauth.Cookie{{Name: "session", Value: "baseline"}})
	current := baseline.Clone()
	currentCredential, errParse := chatgptwebauth.ParseCredential(current.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	currentCredential.Cookies = append(currentCredential.Cookies, chatgptwebauth.Cookie{Name: "runtime", Value: "current"})
	currentCredential.ApplyToMetadata(current.Metadata)
	next := baseline.Clone()

	carryForwardConcurrentRefreshMetadata(baseline, current, next.Clone(), next)
	merged, errParse := chatgptwebauth.ParseCredential(next.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	if len(merged.Cookies) != 2 || merged.Cookies[1].Name != "runtime" || merged.Cookies[1].Value != "current" {
		t.Fatalf("cookies = %+v", merged.Cookies)
	}
}

func TestCarryForwardConcurrentRefreshMetadataMergesAccountProfileAndQuotaFields(t *testing.T) {
	baseline := chatGPTWebRefreshMergeAuth(nil)
	for key, value := range map[string]any{
		"account_id":            "account-baseline",
		"user_id":               "user-baseline",
		"plan_type":             "free",
		"profile_updated_at":    "2026-07-27T10:00:00Z",
		"image_quota_remaining": 5,
		"image_quota_reset_at":  "2026-07-27T11:00:00Z",
		"quota_state":           string(chatgptwebauth.QuotaStateAvailable),
		"quota_updated_at":      "2026-07-27T10:00:00Z",
		"quota_stale":           false,
		"quota_last_error":      "",
	} {
		baseline.Metadata[key] = value
	}
	current := baseline.Clone()
	for key, value := range map[string]any{
		"account_id":           "account-concurrent",
		"user_id":              "user-concurrent",
		"plan_type":            "team-concurrent",
		"profile_updated_at":   "2026-07-27T10:05:00Z",
		"image_quota_reset_at": "2026-07-27T12:00:00Z",
		"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
		"quota_updated_at":     "2026-07-27T10:05:00Z",
		"quota_stale":          true,
		"quota_last_error":     "rate_limited",
	} {
		current.Metadata[key] = value
	}
	delete(current.Metadata, "image_quota_remaining")

	refreshed := baseline.Clone()
	refreshed.Metadata["plan_type"] = "pro-refreshed"
	refreshed.Metadata["quota_last_error"] = "refresh_failed"
	next := refreshed.Clone()
	carryForwardConcurrentRefreshMetadata(baseline, current, refreshed, next)

	for key, want := range map[string]any{
		"account_id":           "account-concurrent",
		"user_id":              "user-concurrent",
		"plan_type":            "pro-refreshed",
		"profile_updated_at":   "2026-07-27T10:05:00Z",
		"image_quota_reset_at": "2026-07-27T12:00:00Z",
		"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
		"quota_updated_at":     "2026-07-27T10:05:00Z",
		"quota_stale":          true,
		"quota_last_error":     "refresh_failed",
	} {
		got, ok := next.Metadata[key]
		if !ok || got != want {
			t.Fatalf("%s = %#v, want %#v", key, got, want)
		}
	}
	if _, ok := next.Metadata["image_quota_remaining"]; ok {
		t.Fatal("concurrent image_quota_remaining deletion was not preserved")
	}
}

func TestCarryForwardConcurrentRefreshMetadataNormalizesQuotaValueTypes(t *testing.T) {
	baseline := chatGPTWebRefreshMergeAuth(nil)
	baseline.Metadata["image_quota_remaining"] = float64(5)
	delete(baseline.Metadata, "quota_state")

	current := baseline.Clone()
	current.Metadata["image_quota_remaining"] = 0
	current.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)

	refreshed := baseline.Clone()
	refreshedCredential, errParse := chatgptwebauth.ParseCredential(refreshed.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	refreshedCredential.ApplyToMetadata(refreshed.Metadata)
	next := refreshed.Clone()

	carryForwardConcurrentRefreshMetadata(baseline, current, refreshed, next)

	if got := next.Metadata["image_quota_remaining"]; got != 0 {
		t.Fatalf("image_quota_remaining = %#v, want concurrent zero", got)
	}
	if got := next.Metadata["quota_state"]; got != string(chatgptwebauth.QuotaStateExhausted) {
		t.Fatalf("quota_state = %#v, want exhausted", got)
	}
}

func TestManagerRefreshPreservesBlockedConcurrentAccountInfoMutation(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	auth := chatGPTWebRefreshMergeAuth(nil)
	auth.ID = "chatgpt-web-concurrent-account-info"
	auth.Metadata["account_id"] = "account-a"
	auth.Metadata["user_id"] = "user-a"
	auth.Metadata["plan_type"] = "free"
	auth.Metadata["profile_updated_at"] = "2026-07-27T10:00:00Z"
	auth.Metadata["image_quota_remaining"] = 5
	auth.Metadata["image_quota_reset_at"] = "2026-07-27T11:00:00Z"
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
	auth.Metadata["quota_updated_at"] = "2026-07-27T10:00:00Z"
	auth.Metadata["quota_stale"] = false
	auth.Metadata["quota_last_error"] = ""
	installed, errRegister := manager.Register(context.Background(), auth)
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	executor := &chatGPTWebRefreshMergeBlockingExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: chatgptwebauth.Provider},
		started:                       make(chan struct{}),
		release:                       make(chan struct{}),
	}
	manager.RegisterExecutor(executor)
	refreshDone := make(chan struct{})
	go func() {
		manager.refreshAuth(context.Background(), auth.ID)
		close(refreshDone)
	}()
	select {
	case <-executor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("credential refresh did not block")
	}

	_, current, errMutate := manager.MutateRuntimeMetadataIfCurrent(context.Background(), installed, func(candidate *Auth) {
		candidate.Metadata["profile_updated_at"] = "2026-07-27T10:05:00Z"
		candidate.Metadata["image_quota_remaining"] = 0
		candidate.Metadata["image_quota_reset_at"] = "2026-07-27T12:00:00Z"
		candidate.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
		candidate.Metadata["quota_updated_at"] = "2026-07-27T10:05:00Z"
		candidate.Metadata["quota_stale"] = true
		candidate.Metadata["quota_last_error"] = "rate_limited"
	})
	if errMutate != nil || !current {
		t.Fatalf("MutateRuntimeMetadataIfCurrent() = current %v, error %v", current, errMutate)
	}
	close(executor.release)
	select {
	case <-refreshDone:
	case <-time.After(5 * time.Second):
		t.Fatal("credential refresh did not finish")
	}

	assertMerged := func(t *testing.T, merged *Auth) {
		t.Helper()
		for key, want := range map[string]any{
			"access_token":          "refreshed-access",
			"plan_type":             "refresh-plan",
			"profile_updated_at":    "2026-07-27T10:05:00Z",
			"image_quota_remaining": 0,
			"image_quota_reset_at":  "2026-07-27T12:00:00Z",
			"quota_state":           string(chatgptwebauth.QuotaStateExhausted),
			"quota_updated_at":      "2026-07-27T10:05:00Z",
			"quota_stale":           true,
			"quota_last_error":      "rate_limited",
		} {
			if got := merged.Metadata[key]; got != want {
				t.Fatalf("%s = %#v, want %#v", key, got, want)
			}
		}
	}
	merged, ok := manager.GetByID(auth.ID)
	if !ok || merged == nil {
		t.Fatal("refreshed auth disappeared")
	}
	assertMerged(t, merged)
	store.mu.Lock()
	persisted := store.records[auth.ID].Clone()
	store.mu.Unlock()
	assertMerged(t, persisted)
}

func TestApplyRefreshedAuthRejectsExternalSourceReplacement(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	expected := registerDependencyTestAuth(t, manager, dependencyTestCodexAuth("source", "uid-a"))
	updated := expected.Clone()
	updated.Metadata["access_token"] = "rotated-access"
	store.replaceOutsideManager(expected.ID, func(replacement *Auth) {
		replacement.Metadata["access_token"] = "external-access"
	})

	installed, errRefresh := manager.applyRefreshedAuth(t.Context(), expected, expected, updated, time.Time{})
	if errRefresh == nil || installed != nil {
		t.Fatalf("apply refreshed auth = %#v, %v; want source-generation conflict", installed, errRefresh)
	}
	if outcome, explicit := SaveOutcomeFromError(errRefresh); !explicit || outcome != SaveOutcomeRolledBack {
		t.Fatalf("refresh outcome = %q, explicit=%v, err=%v", outcome, explicit, errRefresh)
	}
	store.mu.Lock()
	persisted := store.records[expected.ID].Clone()
	store.mu.Unlock()
	if got := chatGPTWebIdentityMetadataString(persisted.Metadata, "access_token"); got != "external-access" {
		t.Fatalf("persisted access token = %q, want external replacement", got)
	}
}

func chatGPTWebRefreshMergeAuth(cookies []chatgptwebauth.Cookie) *Auth {
	credential := &chatgptwebauth.Credential{
		Type: chatgptwebauth.Provider, Email: "person@example.com", AccessToken: "access",
		RefreshStrategy: chatgptwebauth.RefreshStrategyChatGPTSession, Cookies: cookies,
		LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	return &Auth{ID: "web.json", Provider: chatgptwebauth.Provider, Metadata: metadata}
}
