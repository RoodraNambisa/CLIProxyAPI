package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type chatGPTWebReentrantRefreshMarkerHook struct {
	NoopHook
	manager       *Manager
	triggered     atomic.Bool
	updatedFlags  chan bool
	registered    chan bool
	reentrantErrs chan error
}

type chatGPTWebIdentityLoadStore struct {
	auths []*Auth
}

type chatGPTWebCredentialMarkerHook struct {
	NoopHook
	updated chan struct {
		replaced  bool
		refreshed bool
	}
}

func (hook *chatGPTWebCredentialMarkerHook) OnAuthUpdated(ctx context.Context, _ *Auth) {
	hook.updated <- struct {
		replaced  bool
		refreshed bool
	}{
		replaced:  ChatGPTWebCredentialReplaced(ctx),
		refreshed: ChatGPTWebCredentialRefreshed(ctx),
	}
}

func (store *chatGPTWebIdentityLoadStore) List(context.Context) ([]*Auth, error) {
	auths := make([]*Auth, 0, len(store.auths))
	for _, auth := range store.auths {
		auths = append(auths, auth.Clone())
	}
	return auths, nil
}

func (*chatGPTWebIdentityLoadStore) Save(context.Context, *Auth) (string, error) {
	return "", nil
}

func (*chatGPTWebIdentityLoadStore) Delete(context.Context, string) error {
	return nil
}

func (hook *chatGPTWebReentrantRefreshMarkerHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	refreshed := ChatGPTWebCredentialRefreshed(ctx)
	hook.updatedFlags <- refreshed
	if !refreshed || !hook.triggered.CompareAndSwap(false, true) {
		return
	}
	nested := auth.Clone()
	nested.Metadata["note"] = "nested update"
	nestedInstalled, err := hook.manager.Update(ctx, nested)
	if err != nil {
		hook.reentrantErrs <- err
		return
	}
	replacement := nestedInstalled.Clone()
	replacement.Metadata["access_token"] = "nested-replacement-token"
	if _, current, errUpdate := hook.manager.UpdateIfCurrent(ctx, nestedInstalled, replacement); errUpdate != nil || !current {
		if errUpdate == nil {
			errUpdate = errors.New("nested UpdateIfCurrent did not update the current auth")
		}
		hook.reentrantErrs <- errUpdate
		return
	}
	_, err = hook.manager.Register(ctx, &Auth{
		ID:       "opaque-refresh-nested-register",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "nested-token",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if err != nil {
		hook.reentrantErrs <- err
	}
}

func (hook *chatGPTWebReentrantRefreshMarkerHook) OnAuthRegistered(ctx context.Context, auth *Auth) {
	if auth != nil && auth.ID == "opaque-refresh-nested-register" {
		hook.registered <- ChatGPTWebCredentialRefreshed(ctx)
	}
}

func TestChatGPTWebCredentialIdentityStableAcrossTokenRotation(t *testing.T) {
	first := chatGPTWebIdentityTestAuth("identity", "account-a", "user-a")
	second := chatGPTWebIdentityTestAuth("identity", "account-a", "user-a")
	second.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-a", "user-a", "second@example.com")
	different := chatGPTWebIdentityTestAuth("identity", "account-b", "user-b")

	if got, want := ChatGPTWebCredentialIdentity(first), ChatGPTWebCredentialIdentity(second); got == "" || got != want {
		t.Fatalf("same-account identities = (%q, %q)", got, want)
	}
	if ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("same-account token rotation was treated as an identity change")
	}
	if !ChatGPTWebCredentialIdentityChanged(first, different) {
		t.Fatal("different account was not treated as an identity change")
	}
}

func TestChatGPTWebCredentialIdentityAllowsMissingOptionalUserClaim(t *testing.T) {
	first := chatGPTWebIdentityTestAuth("identity-claims", "account-a", "user-a")
	second := first.Clone()
	second.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-a", "", "second@example.com")

	if ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("same account with a missing optional user claim was treated as an identity change")
	}

	differentUser := first.Clone()
	differentUser.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-a", "user-b", "second@example.com")
	if !ChatGPTWebCredentialIdentityChanged(first, differentUser) {
		t.Fatal("different explicit users in the same account were treated as the same credential")
	}

	reference := NewChatGPTWebCredentialReference(first)
	if !reference.Matches(second) {
		t.Fatal("credential reference rejected the same account with a missing optional user claim")
	}
	if reference.Matches(differentUser) {
		t.Fatal("credential reference accepted a different explicit user in the same account")
	}
}

func TestChatGPTWebCredentialIdentityFallsBackWithoutAccountIDOrSourceHash(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]any
	}{
		{name: "user ID", metadata: map[string]any{"user_id": "user-a"}},
		{name: "subject", metadata: map[string]any{"sub": "subject-a"}},
		{name: "email", metadata: map[string]any{"email": "First@Example.com"}},
		{name: "JWT claims", metadata: map[string]any{
			"access_token": chatGPTWebIdentityTestJWT("", "jwt-user-a", "jwt@example.com"),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first := &Auth{Provider: "chatgpt-web", Metadata: test.metadata}
			second := first.Clone()
			if got := ChatGPTWebCredentialIdentity(first); got == "" {
				t.Fatal("fallback identity is empty")
			}
			if ChatGPTWebCredentialIdentityChanged(first, second) {
				t.Fatal("identical fallback metadata was treated as an account change")
			}
		})
	}
}

func TestChatGPTWebCredentialIdentityIgnoresMutableSourceHash(t *testing.T) {
	first := &Auth{
		Provider:   "chatgpt-web",
		Attributes: map[string]string{SourceHashAttributeKey: "first-hash"},
		Metadata: map[string]any{
			"access_token": "opaque-first-token",
			"email":        "person@example.com",
		},
	}
	second := first.Clone()
	second.Attributes[SourceHashAttributeKey] = "second-hash"
	second.Metadata["access_token"] = "opaque-second-token"

	if got, want := ChatGPTWebCredentialIdentity(first), ChatGPTWebCredentialIdentity(second); got == "" || got != want {
		t.Fatalf("source-hash rotation identities = (%q, %q)", got, want)
	}
	if ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("source-hash rotation was treated as an account replacement")
	}
}

func TestChatGPTWebCredentialIdentityUnknownAccountUsesOpaqueCredentialGeneration(t *testing.T) {
	first := &Auth{
		Provider:   "chatgpt-web",
		Attributes: map[string]string{SourceHashAttributeKey: "first-hash"},
		Metadata:   map[string]any{"access_token": "opaque-first-token"},
	}
	second := first.Clone()
	second.Attributes[SourceHashAttributeKey] = "second-hash"

	if got := ChatGPTWebCredentialIdentity(first); got != "" {
		t.Fatalf("unknown account identity = %q, want empty", got)
	}
	if ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("source-hash-only update was treated as an account replacement")
	}

	second.Metadata["access_token"] = "opaque-second-token"
	if !ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("opaque credential generation change was not treated as an account replacement")
	}
	if firstKey, secondKey := ChatGPTWebCatalogCredentialKey(first), ChatGPTWebCatalogCredentialKey(second); firstKey == "" || firstKey == secondKey {
		t.Fatalf("opaque credential catalog keys = (%q, %q), want distinct non-empty keys", firstKey, secondKey)
	}
}

func TestChatGPTWebCredentialIdentitySupportsCamelCaseOpaqueTokens(t *testing.T) {
	first := &Auth{
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"accessToken": "opaque-first-token",
		},
	}
	second := first.Clone()
	second.Metadata["accessToken"] = "opaque-second-token"

	if !ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("camelCase opaque token replacement was treated as the same credential")
	}
	firstKey := ChatGPTWebCatalogCredentialKey(first)
	secondKey := ChatGPTWebCatalogCredentialKey(second)
	if firstKey == "" || secondKey == "" || firstKey == secondKey {
		t.Fatalf("camelCase catalog keys = (%q, %q), want distinct non-empty keys", firstKey, secondKey)
	}
}

func TestChatGPTWebCredentialIdentityTreatsMatchingOpaqueRefreshTokenAsSameAccount(t *testing.T) {
	first := &Auth{
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"access_token":  "opaque-first-token",
			"refresh_token": "shared-refresh-token",
		},
	}
	second := first.Clone()
	second.Metadata["access_token"] = "opaque-second-token"

	if ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("matching opaque refresh token was treated as an account replacement")
	}
}

func TestChatGPTWebCredentialIdentityRefreshTokenConflictOverridesMatchingOpaqueIDToken(t *testing.T) {
	first := &Auth{
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"access_token":  "opaque-first-token",
			"refresh_token": "first-refresh-token",
			"id_token":      "shared-id-token",
		},
	}
	second := first.Clone()
	second.Metadata["access_token"] = "opaque-second-token"
	second.Metadata["refresh_token"] = "second-refresh-token"

	if !ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("changed opaque refresh token was hidden by a stale matching ID token")
	}
}

func TestChatGPTWebCredentialIdentityRefreshTokenConflictOverridesMatchingEmail(t *testing.T) {
	first := &Auth{
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"access_token":  "opaque-first-token",
			"refresh_token": "first-refresh-token",
			"email":         "same@example.com",
		},
	}
	second := first.Clone()
	second.Metadata["access_token"] = "opaque-second-token"
	second.Metadata["refresh_token"] = "second-refresh-token"

	if !ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("changed opaque refresh token was hidden by a matching email fallback")
	}
}

func TestChatGPTWebCredentialIdentityEmailConflictOverridesMatchingRefreshToken(t *testing.T) {
	first := &Auth{
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"access_token":  "opaque-first-token",
			"refresh_token": "shared-refresh-token",
			"email":         "first@example.com",
		},
	}
	second := first.Clone()
	second.Metadata["access_token"] = "opaque-second-token"
	second.Metadata["email"] = "second@example.com"

	if !ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("matching refresh token hid a conflicting email identity")
	}
}

func TestChatGPTWebCredentialIdentityEmailConflictOverridesMatchingOpaqueIDToken(t *testing.T) {
	first := &Auth{
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"access_token": "opaque-first-token",
			"id_token":     "shared-opaque-id-token",
			"email":        "first@example.com",
		},
	}
	second := first.Clone()
	second.Metadata["access_token"] = "opaque-second-token"
	second.Metadata["email"] = "second@example.com"

	if !ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("matching opaque ID token hid a conflicting email identity")
	}
}

func TestChatGPTWebCredentialIdentityCrossTypeConflictOverridesMatchingRefreshToken(t *testing.T) {
	first := &Auth{
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"access_token":  "opaque-first-token",
			"refresh_token": "shared-refresh-token",
			"user_id":       "user-a",
		},
	}
	second := first.Clone()
	delete(second.Metadata, "user_id")
	second.Metadata["access_token"] = "opaque-second-token"
	second.Metadata["email"] = "second@example.com"

	if !ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("matching refresh token hid conflicting fallback identity types")
	}
}

func TestChatGPTWebCredentialIdentityRejectsConflictingAccessAndIDTokenAccounts(t *testing.T) {
	first := &Auth{
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"access_token":  chatGPTWebIdentityTestJWT("account-a", "user-a", "first@example.com"),
			"id_token":      chatGPTWebIdentityTestJWT("account-a", "user-a", "first@example.com"),
			"refresh_token": "refresh-a",
		},
	}
	second := first.Clone()
	second.Metadata["id_token"] = chatGPTWebIdentityTestJWT("account-b", "user-b", "second@example.com")
	second.Metadata["refresh_token"] = "refresh-b"

	if !ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("stale access token hid a conflicting ID token account")
	}
	reference := NewChatGPTWebCredentialReference(first)
	if reference.Matches(second) {
		t.Fatal("credential reference accepted conflicting access and ID token accounts")
	}
}

func TestChatGPTWebCredentialIdentityRejectsConflictingClaimAliases(t *testing.T) {
	tests := []struct {
		name   string
		first  *Auth
		second *Auth
	}{
		{
			name: "metadata aliases",
			first: &Auth{
				Provider: "chatgpt-web",
				Metadata: map[string]any{"account_id": "account-a"},
			},
			second: &Auth{
				Provider: "chatgpt-web",
				Metadata: map[string]any{
					"account_id":         "account-a",
					"chatgpt_account_id": "account-b",
				},
			},
		},
		{
			name: "JWT namespaces",
			first: &Auth{
				Provider: "chatgpt-web",
				Metadata: map[string]any{
					"access_token": chatGPTWebIdentityTestJWT("account-a", "user-a", "first@example.com"),
				},
			},
			second: &Auth{
				Provider: "chatgpt-web",
				Metadata: map[string]any{
					"access_token": chatGPTWebIdentityTestJWTClaims(map[string]any{
						"account_id": "account-b",
						"https://api.openai.com/auth": map[string]any{
							"chatgpt_account_id": "account-a",
							"chatgpt_user_id":    "user-a",
						},
					}),
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !ChatGPTWebCredentialIdentityChanged(test.first, test.second) {
				t.Fatal("conflicting aliases were treated as the same credential")
			}
			if NewChatGPTWebCredentialReference(test.first).Matches(test.second) {
				t.Fatal("credential reference accepted conflicting aliases")
			}
		})
	}
}

func TestChatGPTWebCredentialIdentityOpaqueIDTokenConflictOverridesMatchingEmail(t *testing.T) {
	first := &Auth{
		Provider: "chatgpt-web",
		Metadata: map[string]any{
			"access_token": "opaque-first-token",
			"id_token":     "opaque-first-id-token",
			"email":        "same@example.com",
		},
	}
	second := first.Clone()
	second.Metadata["access_token"] = "opaque-second-token"
	second.Metadata["id_token"] = "opaque-second-id-token"

	if !ChatGPTWebCredentialIdentityChanged(first, second) {
		t.Fatal("changed opaque ID token was hidden by a matching email fallback")
	}
}

func TestChatGPTWebCredentialIdentityTreatsKnownOpaqueTransitionsAsReplacement(t *testing.T) {
	known := chatGPTWebIdentityTestAuth("known-opaque-transition", "account-a", "user-a")
	opaque := known.Clone()
	opaque.Metadata = map[string]any{
		"access_token":    "opaque-token",
		"lifecycle_state": LifecycleStateActive,
	}

	if !ChatGPTWebCredentialIdentityChanged(known, opaque) {
		t.Fatal("known to opaque transition was not treated as an account replacement")
	}
	if !ChatGPTWebCredentialIdentityChanged(opaque, known) {
		t.Fatal("opaque to known transition was not treated as an account replacement")
	}
}

func TestManagerUpdateReplacesChatGPTWebAccountUsingFallbackEmailIdentity(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	first := &Auth{
		ID:       "fallback-email-account",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "opaque-first-token",
			"email":           "first@example.com",
			"lifecycle_state": LifecycleStateActive,
		},
	}
	registered, err := manager.Register(WithSkipPersist(t.Context()), first)
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	next := registered.Clone()
	next.Metadata["access_token"] = "opaque-second-token"
	next.Metadata["email"] = "second@example.com"
	installed, err := manager.Update(WithSkipPersist(t.Context()), next)
	if err != nil {
		t.Fatalf("Update() error: %v", err)
	}
	if installed.RuntimeInstanceID() == registered.RuntimeInstanceID() {
		t.Fatal("different fallback email identity reused the old runtime instance")
	}
	_, release, active := registered.BeginRuntimeExecution(t.Context())
	if active {
		release()
		t.Fatal("old fallback identity runtime remained active")
	}
}

func TestManagerUpdateDifferentChatGPTWebAccountPreservesAdministrativeDisable(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	first := chatGPTWebIdentityTestAuth("disabled-account-replacement", "account-a", "user-a")
	first.Disabled = true
	first.Status = StatusDisabled
	first.Metadata["disabled"] = true
	registered, err := manager.Register(WithSkipPersist(t.Context()), first)
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	replacement := &Auth{
		ID:       registered.ID,
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    chatGPTWebIdentityTestJWT("account-b", "user-b", "next@example.com"),
			"lifecycle_state": LifecycleStateActive,
		},
	}
	installed, err := manager.Update(WithSkipPersist(t.Context()), replacement)
	if err != nil {
		t.Fatalf("Update() error: %v", err)
	}
	if !installed.Disabled || installed.Status != StatusDisabled {
		t.Fatalf("administrative disable was cleared: %#v", installed)
	}
	if disabled, _ := installed.Metadata["disabled"].(bool); !disabled {
		t.Fatalf("persisted administrative disable = %#v", installed.Metadata["disabled"])
	}
}

func TestManagerUpdateReplacesOpaqueChatGPTWebCredentialGeneration(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	retryAt := time.Now().Add(time.Hour).UTC()
	first := &Auth{
		ID:          "opaque-account-replacement",
		Provider:    "chatgpt-web",
		Status:      StatusError,
		Unavailable: true,
		Quota:       QuotaState{Exceeded: true, NextRecoverAt: retryAt},
		Metadata: map[string]any{
			"access_token":    "opaque-first-token",
			"lifecycle_state": LifecycleStateActive,
		},
		NextRetryAfter: retryAt,
		CooldownScope:  cooldownScopeAuth,
		ModelStates: map[string]*ModelState{
			"gpt-image-2": {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: retryAt,
			},
		},
	}
	registered, err := manager.Register(WithSkipPersist(t.Context()), first)
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	replacement := registered.Clone()
	replacement.Metadata["access_token"] = "opaque-second-token"
	replacement.Metadata["lifecycle_state"] = LifecycleStateActive
	installed, err := manager.Update(WithSkipPersist(t.Context()), replacement)
	if err != nil {
		t.Fatalf("Update() error: %v", err)
	}
	if installed.RuntimeInstanceID() == registered.RuntimeInstanceID() {
		t.Fatal("opaque credential replacement reused the old runtime instance")
	}
	assertChatGPTWebReplacementStateCleared(t, installed)
}

func TestManagerOpaqueChatGPTWebReplacementClearsCarriedAccountMetadata(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	first := &Auth{
		ID:       "opaque-account-metadata-replacement",
		Provider: "chatgpt-web",
		Status:   StatusPending,
		Metadata: map[string]any{
			"access_token":         "opaque-first-token",
			"refresh_token":        "opaque-first-refresh",
			"id_token":             "opaque-first-id",
			"password":             "old-password",
			"totp_secret":          "OLDTOTP",
			"cookies":              []any{map[string]any{"name": "session", "value": "old-cookie"}},
			"persona":              map[string]any{"profile": "old-profile"},
			"device_id":            "old-device",
			"session_id":           "old-session",
			"expired":              "2099-01-01T00:00:00Z",
			"lifecycle_state":      LifecycleStateReloginPending,
			"lifecycle_reason":     "old token invalid",
			"lifecycle_updated_at": "2026-01-01T00:00:00Z",
			"last_login_at":        "2026-01-01T00:00:00Z",
			"last_refresh_at":      "2026-01-01T00:00:00Z",
			"last_relogin_at":      "2026-01-01T00:00:00Z",
		},
	}
	registered, err := manager.Register(WithSkipPersist(t.Context()), first)
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	replacement := registered.Clone()
	replacement.Metadata["access_token"] = "opaque-second-token"
	replacement.Metadata["refresh_token"] = "opaque-second-refresh"
	installed, err := manager.Update(WithSkipPersist(t.Context()), replacement)
	if err != nil {
		t.Fatalf("Update() error: %v", err)
	}
	if installed.LifecycleState() != LifecycleStateActive ||
		!installed.LifecycleSelectable() ||
		!installed.LifecycleRefreshable() ||
		installed.Status != StatusActive {
		t.Fatalf("replacement lifecycle = %q status=%q", installed.LifecycleState(), installed.Status)
	}
	for _, key := range []string{
		"id_token",
		"cookies",
		"session_id",
		"expired",
		"lifecycle_reason",
		"last_login_at",
		"last_refresh_at",
		"last_relogin_at",
	} {
		if _, exists := installed.Metadata[key]; exists {
			t.Fatalf("carried account metadata %q was retained", key)
		}
	}
	for key, want := range map[string]string{
		"refresh_token": "opaque-second-refresh",
		"password":      "old-password",
		"totp_secret":   "OLDTOTP",
		"device_id":     "old-device",
	} {
		if got := requestPrepareString(installed.Metadata[key]); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	if _, exists := installed.Metadata["persona"]; !exists {
		t.Fatal("explicitly reusable persona was removed")
	}
	if got := requestPrepareString(installed.Metadata["access_token"]); got != "opaque-second-token" {
		t.Fatalf("access token = %q", got)
	}
}

func TestManagerOpaqueChatGPTWebReplacementPreservesExplicitSameMetadata(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	first := &Auth{
		ID:       "opaque-explicit-metadata-replacement",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "opaque-first-token",
			"refresh_token":   "first-refresh-token",
			"password":        "shared-password",
			"device_id":       "shared-device",
			"lifecycle_state": LifecycleStateActive,
		},
	}
	if _, err := manager.Register(WithSkipPersist(t.Context()), first); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	replacement := &Auth{
		ID:       first.ID,
		Provider: first.Provider,
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "opaque-second-token",
			"refresh_token":   "second-refresh-token",
			"password":        "shared-password",
			"device_id":       "shared-device",
			"lifecycle_state": LifecycleStateActive,
		},
	}
	installed, err := manager.Update(WithSkipPersist(t.Context()), replacement)
	if err != nil {
		t.Fatalf("Update() error: %v", err)
	}
	for key, want := range map[string]string{
		"refresh_token": "second-refresh-token",
		"password":      "shared-password",
		"device_id":     "shared-device",
	} {
		if got := requestPrepareString(installed.Metadata[key]); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
}

func TestManagerLoadedOpaqueChatGPTWebReplacementClearsCarriedAccountMetadata(t *testing.T) {
	store := &chatGPTWebIdentityLoadStore{auths: []*Auth{{
		ID:       "opaque-loaded-metadata-replacement",
		Provider: "chatgpt-web",
		Status:   StatusPending,
		Metadata: map[string]any{
			"access_token":     "opaque-first-token",
			"refresh_token":    "opaque-first-refresh",
			"password":         "old-password",
			"session_id":       "old-session",
			"lifecycle_state":  LifecycleStateReloginPending,
			"lifecycle_reason": "old token invalid",
		},
	}}}
	manager := NewManager(store, &RoundRobinSelector{}, nil)
	if err := manager.Load(t.Context()); err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	current, ok := manager.GetByID("opaque-loaded-metadata-replacement")
	if !ok || current == nil {
		t.Fatal("loaded auth missing")
	}

	replacement := current.Clone()
	replacement.Metadata["access_token"] = "opaque-second-token"
	replacement.Metadata["refresh_token"] = "opaque-second-refresh"
	installed, err := manager.Update(WithSkipPersist(t.Context()), replacement)
	if err != nil {
		t.Fatalf("Update() error: %v", err)
	}
	for _, key := range []string{"session_id", "lifecycle_reason"} {
		if _, exists := installed.Metadata[key]; exists {
			t.Fatalf("loaded carried metadata %q was retained", key)
		}
	}
	if got := requestPrepareString(installed.Metadata["refresh_token"]); got != "opaque-second-refresh" {
		t.Fatalf("refresh token = %q", got)
	}
	if got := requestPrepareString(installed.Metadata["password"]); got != "old-password" {
		t.Fatalf("password = %q", got)
	}
	if installed.LifecycleState() != LifecycleStateActive || installed.Status != StatusActive {
		t.Fatalf("replacement lifecycle = %q status=%q", installed.LifecycleState(), installed.Status)
	}
}

func TestUpdateIfCurrentChatGPTWebSameAccountPreservesRuntimeState(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	retryAt := time.Now().Add(time.Hour).UTC()
	auth := chatGPTWebIdentityTestAuth("same-account", "account-a", "user-a")
	auth.Status = StatusError
	auth.Unavailable = true
	auth.NextRetryAfter = retryAt
	auth.CooldownScope = cooldownScopeAuth
	auth.Quota = QuotaState{Exceeded: true, NextRecoverAt: retryAt}
	registered, err := manager.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	updated := registered.Clone()
	updated.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-a", "user-a", "rotated@example.com")
	installed, current, err := manager.UpdateIfCurrent(t.Context(), registered, updated)
	if err != nil {
		t.Fatalf("UpdateIfCurrent() error: %v", err)
	}
	if !current || installed == nil {
		t.Fatalf("UpdateIfCurrent() = (%v, %v), want current install", installed, current)
	}
	if installed.RuntimeInstanceID() != registered.RuntimeInstanceID() {
		t.Fatal("same-account token rotation replaced the runtime instance")
	}
	if !installed.Unavailable || !installed.NextRetryAfter.Equal(retryAt) || !installed.Quota.Exceeded {
		t.Fatalf("same-account runtime state was not preserved: %#v", installed)
	}
}

func TestUpdateIfCurrentForceRuntimeReplacementForSameChatGPTWebAccount(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	auth := chatGPTWebIdentityTestAuth("same-account-relogin", "account-a", "user-a")
	registered, err := manager.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}
	cleanupDone := registered.RuntimeInstanceCleanupDone()
	refreshFamilyID := registered.requestRefreshFamilyID

	updated := registered.Clone()
	updated.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-a", "user-a", "rotated@example.com")
	installed, current, err := manager.UpdateIfCurrent(
		WithForceRuntimeReplacement(t.Context()),
		registered,
		updated,
	)
	if err != nil {
		t.Fatalf("UpdateIfCurrent() error: %v", err)
	}
	if !current || installed == nil {
		t.Fatalf("UpdateIfCurrent() = (%v, %v), want current install", installed, current)
	}
	if installed.RuntimeInstanceID() == registered.RuntimeInstanceID() {
		t.Fatal("forced same-account update reused the old runtime instance")
	}
	if installed.requestRefreshFamilyID == "" || installed.requestRefreshFamilyID == refreshFamilyID {
		t.Fatalf("request refresh family = %q, want a new family", installed.requestRefreshFamilyID)
	}
	if _, release, active := registered.BeginRuntimeExecution(t.Context()); active {
		release()
		t.Fatal("forced same-account update left the old runtime instance active")
	}
	select {
	case <-cleanupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("forced same-account cleanup did not finish")
	}
}

func TestUpdateIfCurrentChatGPTWebDifferentAccountResetsRuntimeState(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	retryAt := time.Now().Add(time.Hour).UTC()
	auth := chatGPTWebIdentityTestAuth("relogin-account-change", "account-a", "user-a")
	auth.Status = StatusError
	auth.StatusMessage = "old account cooldown"
	auth.Unavailable = true
	auth.NextRefreshAfter = retryAt
	auth.NextRetryAfter = retryAt
	auth.CooldownScope = cooldownScopeAuth
	auth.Quota = QuotaState{Exceeded: true, NextRecoverAt: retryAt}
	auth.ModelStates = map[string]*ModelState{
		"gpt-image-2": {
			Status:         StatusError,
			Unavailable:    true,
			NextRetryAfter: retryAt,
			Quota:          QuotaState{Exceeded: true, NextRecoverAt: retryAt},
		},
	}
	registered, err := manager.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}
	cleanupDone := registered.RuntimeInstanceCleanupDone()

	updated := registered.Clone()
	updated.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-b", "user-b", "next@example.com")
	updated.Metadata["lifecycle_state"] = LifecycleStateActive
	installed, current, err := manager.UpdateIfCurrent(t.Context(), registered, updated)
	if err != nil {
		t.Fatalf("UpdateIfCurrent() error: %v", err)
	}
	if !current || installed == nil {
		t.Fatalf("UpdateIfCurrent() = (%v, %v), want current install", installed, current)
	}
	if installed.RuntimeInstanceID() == registered.RuntimeInstanceID() {
		t.Fatal("different account reused the old runtime instance")
	}
	_, release, active := registered.BeginRuntimeExecution(t.Context())
	if active {
		release()
		t.Fatal("old account instance remained active after replacement")
	}
	assertChatGPTWebReplacementStateCleared(t, installed)
	select {
	case <-cleanupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("old account cleanup did not finish")
	}
}

func TestUpdateIfCurrentChatGPTWebOpaqueCredentialReplacementResetsRuntimeInstance(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	registered, err := manager.Register(WithSkipPersist(t.Context()), &Auth{
		ID:       "opaque-account-replacement",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "opaque-access-a",
			"refresh_token":   "opaque-refresh-a",
			"id_token":        "opaque-id-a",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	updated := registered.Clone()
	updated.Metadata["access_token"] = "opaque-access-b"
	updated.Metadata["refresh_token"] = "opaque-refresh-b"
	updated.Metadata["id_token"] = "opaque-id-b"
	installed, current, err := manager.UpdateIfCurrent(t.Context(), registered, updated)
	if err != nil {
		t.Fatalf("UpdateIfCurrent() error: %v", err)
	}
	if !current || installed == nil {
		t.Fatalf("UpdateIfCurrent() = (%v, %v), want current install", installed, current)
	}
	if installed.RuntimeInstanceID() == registered.RuntimeInstanceID() {
		t.Fatal("opaque credential replacement reused the old runtime instance")
	}
	_, release, active := registered.BeginRuntimeExecution(t.Context())
	if active {
		release()
		t.Fatal("opaque credential replacement left the old runtime instance active")
	}
}

func TestManagerRegisterAndUpdateReplaceDifferentChatGPTWebAccount(t *testing.T) {
	tests := []struct {
		name    string
		replace func(*Manager, *Auth) (*Auth, error)
	}{
		{
			name: "register",
			replace: func(manager *Manager, auth *Auth) (*Auth, error) {
				return manager.Register(WithSkipPersist(t.Context()), auth)
			},
		},
		{
			name: "update",
			replace: func(manager *Manager, auth *Auth) (*Auth, error) {
				return manager.Update(WithSkipPersist(t.Context()), auth)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := NewManager(nil, &RoundRobinSelector{}, nil)
			retryAt := time.Now().Add(time.Hour).UTC()
			auth := chatGPTWebIdentityTestAuth("direct-"+test.name, "account-a", "user-a")
			auth.Status = StatusError
			auth.Unavailable = true
			auth.NextRetryAfter = retryAt
			auth.CooldownScope = cooldownScopeAuth
			auth.Quota = QuotaState{Exceeded: true, NextRecoverAt: retryAt}
			auth.ModelStates = map[string]*ModelState{
				"gpt-image-2": {
					Status:         StatusError,
					Unavailable:    true,
					NextRetryAfter: retryAt,
				},
			}
			registered, err := manager.Register(WithSkipPersist(t.Context()), auth)
			if err != nil {
				t.Fatalf("Register() error: %v", err)
			}
			cleanupDone := registered.RuntimeInstanceCleanupDone()

			replacement := registered.Clone()
			replacement.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-b", "user-b", "next@example.com")
			replacement.Metadata["lifecycle_state"] = LifecycleStateActive
			installed, err := test.replace(manager, replacement)
			if err != nil {
				t.Fatalf("replace auth: %v", err)
			}
			if installed.RuntimeInstanceID() == registered.RuntimeInstanceID() {
				t.Fatal("different account reused the old runtime instance")
			}
			_, release, active := registered.BeginRuntimeExecution(t.Context())
			if active {
				release()
				t.Fatal("old account instance remained active after replacement")
			}
			assertChatGPTWebReplacementStateCleared(t, installed)
			select {
			case <-cleanupDone:
			case <-time.After(5 * time.Second):
				t.Fatal("old account cleanup did not finish")
			}
		})
	}
}

func TestApplyRefreshedChatGPTWebDifferentAccountResetsRuntimeState(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	retryAt := time.Now().Add(time.Hour).UTC()
	auth := chatGPTWebIdentityTestAuth("refresh-account-change", "account-a", "user-a")
	auth.Status = StatusError
	auth.Unavailable = true
	auth.NextRetryAfter = retryAt
	auth.CooldownScope = cooldownScopeAuth
	auth.Quota = QuotaState{Exceeded: true, NextRecoverAt: retryAt}
	auth.ModelStates = map[string]*ModelState{"gpt-5": {Unavailable: true, NextRetryAfter: retryAt}}
	if _, err := manager.Register(WithSkipPersist(t.Context()), auth); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	manager.mu.RLock()
	expected := manager.auths[auth.ID]
	baseline := expected.Clone()
	manager.mu.RUnlock()
	updated := baseline.Clone()
	updated.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-b", "user-b", "next@example.com")
	updated.Metadata["lifecycle_state"] = LifecycleStateActive

	installed, err := manager.applyRefreshedAuth(t.Context(), expected, baseline, updated, time.Time{})
	if err != nil {
		t.Fatalf("applyRefreshedAuth() error: %v", err)
	}
	if installed == nil {
		t.Fatal("applyRefreshedAuth() returned nil")
	}
	if installed.RuntimeInstanceID() == baseline.RuntimeInstanceID() {
		t.Fatal("different account refresh reused the old runtime instance")
	}
	if installed.requestRefreshFamilyID == "" || installed.requestRefreshFamilyID == expected.requestRefreshFamilyID {
		t.Fatalf("different account refresh retained family %q", installed.requestRefreshFamilyID)
	}
	assertChatGPTWebReplacementStateCleared(t, installed)
}

func TestApplyRefreshedOpaqueChatGPTWebCredentialPreservesRuntimeState(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	hook := &chatGPTWebReentrantRefreshMarkerHook{
		manager:       manager,
		updatedFlags:  make(chan bool, 3),
		registered:    make(chan bool, 1),
		reentrantErrs: make(chan error, 1),
	}
	manager.SetHook(hook)
	retryAt := time.Now().Add(time.Hour).UTC()
	auth := &Auth{
		ID:          "opaque-refresh",
		Provider:    "chatgpt-web",
		Status:      StatusError,
		Unavailable: true,
		Quota:       QuotaState{Exceeded: true, NextRecoverAt: retryAt},
		Metadata: map[string]any{
			"access_token":    "opaque-first-token",
			"refresh_token":   "opaque-first-refresh",
			"lifecycle_state": LifecycleStateActive,
		},
		NextRetryAfter: retryAt,
		CooldownScope:  cooldownScopeAuth,
		ModelStates: map[string]*ModelState{
			"gpt-image-2": {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: retryAt,
			},
		},
	}
	if _, err := manager.Register(WithSkipPersist(t.Context()), auth); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	manager.mu.RLock()
	expected := manager.auths[auth.ID]
	baseline := expected.Clone()
	manager.mu.RUnlock()
	updated := baseline.Clone()
	updated.Metadata["access_token"] = "opaque-second-token"
	updated.Metadata["refresh_token"] = "opaque-second-refresh"

	installed, err := manager.applyRefreshedAuth(t.Context(), expected, baseline, updated, time.Time{})
	if err != nil {
		t.Fatalf("applyRefreshedAuth() error: %v", err)
	}
	if installed == nil {
		t.Fatal("applyRefreshedAuth() returned nil")
	}
	if installed.RuntimeInstanceID() != baseline.RuntimeInstanceID() {
		t.Fatal("opaque same-account refresh replaced the runtime instance")
	}
	if installed.requestRefreshFamilyID == "" || installed.requestRefreshFamilyID != expected.requestRefreshFamilyID {
		t.Fatalf("opaque token rotation changed refresh family: %q -> %q", expected.requestRefreshFamilyID, installed.requestRefreshFamilyID)
	}
	if !installed.Unavailable || !installed.NextRetryAfter.Equal(retryAt) ||
		installed.CooldownScope != cooldownScopeAuth || !installed.Quota.Exceeded ||
		len(installed.ModelStates) != 1 {
		t.Fatalf("opaque refresh did not preserve runtime state: %#v", installed)
	}
	for index, want := range []bool{true, false, false} {
		select {
		case got := <-hook.updatedFlags:
			if got != want {
				t.Fatalf("updated hook marker %d = %v, want %v", index, got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for updated hook marker %d", index)
		}
	}
	select {
	case got := <-hook.registered:
		if got {
			t.Fatal("controlled refresh marker leaked into nested Register")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for nested Register hook")
	}
	select {
	case err := <-hook.reentrantErrs:
		t.Fatalf("reentrant hook operation failed: %v", err)
	default:
	}
}

func TestApplyRefreshedFallbackIdentityPreservesRuntimeStateAcrossRefreshTokenRotation(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	retryAt := time.Now().Add(time.Hour).UTC()
	auth := &Auth{
		ID:          "fallback-identity-refresh",
		Provider:    "chatgpt-web",
		Status:      StatusError,
		Unavailable: true,
		Metadata: map[string]any{
			"email":           "same@example.com",
			"access_token":    "opaque-first-token",
			"refresh_token":   "opaque-first-refresh",
			"lifecycle_state": LifecycleStateActive,
		},
		NextRetryAfter: retryAt,
		CooldownScope:  cooldownScopeAuth,
	}
	if _, err := manager.Register(WithSkipPersist(t.Context()), auth); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	manager.mu.RLock()
	expected := manager.auths[auth.ID]
	baseline := expected.Clone()
	manager.mu.RUnlock()
	updated := baseline.Clone()
	updated.Metadata["access_token"] = "opaque-second-token"
	updated.Metadata["refresh_token"] = "opaque-second-refresh"

	installed, err := manager.applyRefreshedAuth(t.Context(), expected, baseline, updated, time.Time{})
	if err != nil {
		t.Fatalf("applyRefreshedAuth() error: %v", err)
	}
	if installed == nil {
		t.Fatal("applyRefreshedAuth() returned nil")
	}
	if !installed.Unavailable || !installed.NextRetryAfter.Equal(retryAt) ||
		installed.CooldownScope != cooldownScopeAuth {
		t.Fatalf("fallback identity refresh did not preserve runtime state: %#v", installed)
	}
}

func TestApplyRefreshedSameAccountMissingOptionalUserPreservesRuntimeState(t *testing.T) {
	hook := &chatGPTWebCredentialMarkerHook{
		updated: make(chan struct {
			replaced  bool
			refreshed bool
		}, 1),
	}
	manager := NewManager(nil, &RoundRobinSelector{}, hook)
	retryAt := time.Now().Add(time.Hour).UTC()
	auth := chatGPTWebIdentityTestAuth("refresh-missing-user", "account-a", "user-a")
	auth.Status = StatusError
	auth.Unavailable = true
	auth.NextRetryAfter = retryAt
	auth.CooldownScope = cooldownScopeAuth
	if _, err := manager.Register(WithSkipPersist(t.Context()), auth); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	manager.mu.RLock()
	expected := manager.auths[auth.ID]
	baseline := expected.Clone()
	manager.mu.RUnlock()
	updated := baseline.Clone()
	updated.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-a", "", "refresh-missing-user@example.com")

	installed, err := manager.applyRefreshedAuth(t.Context(), expected, baseline, updated, time.Time{})
	if err != nil {
		t.Fatalf("applyRefreshedAuth() error: %v", err)
	}
	if installed == nil {
		t.Fatal("applyRefreshedAuth() returned nil")
	}
	if installed.RuntimeInstanceID() != baseline.RuntimeInstanceID() {
		t.Fatal("same-account refresh replaced the runtime instance")
	}
	if _, release, active := baseline.BeginRuntimeExecution(t.Context()); !active {
		t.Fatal("same-account refresh retired the active runtime instance")
	} else {
		release()
	}
	if !installed.Unavailable || !installed.NextRetryAfter.Equal(retryAt) ||
		installed.CooldownScope != cooldownScopeAuth {
		t.Fatalf("same-account refresh did not preserve runtime state: %#v", installed)
	}
	select {
	case marker := <-hook.updated:
		if marker.replaced || !marker.refreshed {
			t.Fatalf("refresh marker = replaced %v, refreshed %v", marker.replaced, marker.refreshed)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refresh marker")
	}
}

func TestApplyRefreshedChatGPTWebAcceptsConcurrentRuntimeMetadataUpdate(t *testing.T) {
	authPath := filepath.Join(t.TempDir(), "chatgpt-web.json")
	manager := NewManager(&hashingFileStore{baseDir: filepath.Dir(authPath)}, &RoundRobinSelector{}, nil)
	auth := chatGPTWebIdentityTestAuth("refresh-runtime-metadata", "account-a", "user-a")
	auth.FileName = authPath
	auth.Attributes = map[string]string{"path": authPath}
	auth.Metadata["session_id"] = "session-before"
	raw, errCanonical := CanonicalMetadataBytes(auth)
	if errCanonical != nil {
		t.Fatalf("CanonicalMetadataBytes() error: %v", errCanonical)
	}
	SetSourceHashAttribute(auth, raw)
	if _, err := manager.Register(WithSkipPersist(t.Context()), auth); err != nil {
		t.Fatalf("Register() error: %v", err)
	}

	manager.mu.RLock()
	expected := manager.auths[auth.ID]
	baseline := expected.Clone()
	manager.mu.RUnlock()
	updated := baseline.Clone()
	updated.Metadata["access_token"] = chatGPTWebIdentityTestJWT("account-a", "user-a", "rotated@example.com")

	if _, current, err := manager.UpdateRuntimeMetadataIfCurrent(t.Context(), expected, map[string]any{
		"session_id": "session-after",
	}); err != nil || !current {
		t.Fatalf("UpdateRuntimeMetadataIfCurrent() = current %v, err %v", current, err)
	}
	currentAfterMetadata, ok := manager.GetByID(auth.ID)
	if !ok || authSourceHash(currentAfterMetadata) == authSourceHash(expected) {
		t.Fatal("runtime metadata persistence did not advance the source hash")
	}

	installed, err := manager.applyRefreshedAuth(t.Context(), expected, baseline, updated, time.Time{})
	if err != nil {
		t.Fatalf("applyRefreshedAuth() error: %v", err)
	}
	if installed == nil {
		t.Fatal("concurrent runtime metadata update discarded refreshed auth")
	}
	if got := requestPrepareString(installed.Metadata["access_token"]); got != requestPrepareString(updated.Metadata["access_token"]) {
		t.Fatalf("access token = %q, want refreshed token", got)
	}
	if got := requestPrepareString(installed.Metadata["session_id"]); got != "session-after" {
		t.Fatalf("session_id = %q, want concurrent runtime value", got)
	}
}

func assertChatGPTWebReplacementStateCleared(t *testing.T, auth *Auth) {
	t.Helper()
	if auth.Status != StatusActive || auth.StatusMessage != "" || auth.Unavailable {
		t.Fatalf("replacement status = %q message=%q unavailable=%v", auth.Status, auth.StatusMessage, auth.Unavailable)
	}
	if auth.LastError != nil || !auth.NextRefreshAfter.IsZero() || !auth.NextRetryAfter.IsZero() || auth.CooldownScope != "" {
		t.Fatalf("replacement retry state was retained: %#v", auth)
	}
	if auth.Quota.Exceeded || len(auth.ModelStates) != 0 || auth.Runtime != nil {
		t.Fatalf("replacement quota/model/runtime state was retained: %#v", auth)
	}
}

func chatGPTWebIdentityTestAuth(id, accountID, userID string) *Auth {
	return &Auth{
		ID:       id,
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "chatgpt-web-source-" + id,
		},
		Metadata: map[string]any{
			"access_token":    chatGPTWebIdentityTestJWT(accountID, userID, id+"@example.com"),
			"lifecycle_state": LifecycleStateActive,
		},
	}
}

func chatGPTWebIdentityTestJWT(accountID, userID, email string) string {
	return chatGPTWebIdentityTestJWTClaims(map[string]any{
		"email": email,
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": accountID,
			"chatgpt_user_id":    userID,
		},
	})
}

func chatGPTWebIdentityTestJWTClaims(claims map[string]any) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload, _ := json.Marshal(claims)
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"
}
