package auth

import (
	"context"
	"strings"
	"testing"
	"time"
)

type lifecycleLoadStore struct {
	auths []*Auth
}

func (store *lifecycleLoadStore) List(context.Context) ([]*Auth, error) {
	return store.auths, nil
}

func (*lifecycleLoadStore) Save(context.Context, *Auth) (string, error) {
	return "", nil
}

func (*lifecycleLoadStore) Delete(context.Context, string) error {
	return nil
}

func TestRuntimeStatusForLifecycle(t *testing.T) {
	tests := []struct {
		name  string
		state string
		want  Status
	}{
		{name: "active", state: LifecycleStateActive, want: StatusActive},
		{name: "active normalized", state: " ACTIVE ", want: StatusActive},
		{name: "refreshing", state: LifecycleStateRefreshing, want: StatusRefreshing},
		{name: "login pending", state: LifecycleStateLoginPending, want: StatusPending},
		{name: "relogin pending", state: LifecycleStateReloginPending, want: StatusPending},
		{name: "reauth required", state: LifecycleStateReauthRequired, want: StatusError},
		{name: "interaction required", state: LifecycleStateInteractionRequired, want: StatusError},
		{name: "dead", state: LifecycleStateDead, want: StatusError},
		{name: "unknown", state: "future_state", want: StatusUnknown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := RuntimeStatusForLifecycle(test.state); got != test.want {
				t.Fatalf("RuntimeStatusForLifecycle(%q) = %q, want %q", test.state, got, test.want)
			}
		})
	}
}

func TestApplyLifecycleRuntimeStatePreservesDisabledStatus(t *testing.T) {
	auth := &Auth{
		Disabled:      true,
		Status:        StatusActive,
		StatusMessage: "stale-error",
		Metadata: map[string]any{
			"lifecycle_state":  LifecycleStateActive,
			"lifecycle_reason": "ready",
		},
	}
	applyLifecycleRuntimeState(auth)
	if auth.Status != StatusDisabled {
		t.Fatalf("disabled lifecycle status = %q, want %q", auth.Status, StatusDisabled)
	}
	if auth.StatusMessage != "" {
		t.Fatalf("active disabled lifecycle message = %q, want empty", auth.StatusMessage)
	}

	auth.Metadata["lifecycle_state"] = LifecycleStateReauthRequired
	auth.Metadata["lifecycle_reason"] = "authentication_failed"
	applyLifecycleRuntimeState(auth)
	if auth.StatusMessage != "authentication_failed" {
		t.Fatalf("terminal disabled lifecycle message = %q, want authentication_failed", auth.StatusMessage)
	}
	delete(auth.Metadata, "lifecycle_reason")
	applyLifecycleRuntimeState(auth)
	if auth.StatusMessage != "" {
		t.Fatalf("disabled lifecycle without current reason retained message %q", auth.StatusMessage)
	}
}

func TestApplyLifecycleRuntimeStateSanitizesDisabledChatGPTWebReason(t *testing.T) {
	auth := &Auth{
		Provider: "chatgpt-web",
		Disabled: true,
		Metadata: map[string]any{
			"lifecycle_state":  LifecycleStateDead,
			"lifecycle_reason": "secret-token-shaped-reason",
		},
	}
	applyLifecycleRuntimeState(auth)
	if auth.Status != StatusDisabled {
		t.Fatalf("status = %q, want %q", auth.Status, StatusDisabled)
	}
	if auth.StatusMessage != "authentication_failed" {
		t.Fatalf("status message = %q, want sanitized reason", auth.StatusMessage)
	}
}

func TestManagerLoadAppliesChatGPTWebLifecycleRuntimeState(t *testing.T) {
	store := &lifecycleLoadStore{auths: []*Auth{{
		ID:       "dead-chatgpt-web",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"lifecycle_state":  LifecycleStateDead,
			"lifecycle_reason": "account_deleted",
		},
	}}}
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(context.Background()); errLoad != nil {
		t.Fatalf("Load() error = %v", errLoad)
	}
	loaded, ok := manager.GetByID("dead-chatgpt-web")
	if !ok || loaded == nil {
		t.Fatal("loaded credential is missing")
	}
	if loaded.Status != StatusError || loaded.StatusMessage != "account_deleted" || loaded.LifecycleSelectable() {
		t.Fatalf("loaded lifecycle state = %+v", loaded)
	}
}

func TestManagerLoadPreservesActiveChatGPTWebCooldownRuntimeState(t *testing.T) {
	retryAt := time.Now().Add(time.Hour).Round(time.Second)
	store := &lifecycleLoadStore{auths: []*Auth{{
		ID:             "cooling-chatgpt-web",
		Provider:       "chatgpt-web",
		Status:         StatusError,
		StatusMessage:  "rate limited",
		Unavailable:    true,
		NextRetryAfter: retryAt,
		CooldownScope:  cooldownScopeModel,
		ModelStates: map[string]*ModelState{
			"gpt-image-2": {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: retryAt,
			},
		},
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateActive,
			"access_token":    "token",
		},
	}}}
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(context.Background()); errLoad != nil {
		t.Fatalf("Load() error = %v", errLoad)
	}
	loaded, ok := manager.GetByID("cooling-chatgpt-web")
	if !ok || loaded == nil {
		t.Fatal("loaded credential is missing")
	}
	if loaded.Status != StatusError || loaded.StatusMessage != "rate limited" || !loaded.Unavailable ||
		!loaded.NextRetryAfter.Equal(retryAt) || loaded.CooldownScope != cooldownScopeModel {
		t.Fatalf("loaded cooldown state = %+v", loaded)
	}
	modelState := loaded.ModelStates["gpt-image-2"]
	if modelState == nil || !modelState.Unavailable || !modelState.NextRetryAfter.Equal(retryAt) {
		t.Fatalf("loaded model cooldown = %+v", modelState)
	}
}

func TestApplyLifecycleRuntimeStateClearsExpiredActiveCooldown(t *testing.T) {
	past := time.Now().Add(-time.Minute)
	auth := &Auth{
		Provider:       "chatgpt-web",
		Status:         StatusError,
		StatusMessage:  "rate limited",
		Unavailable:    true,
		NextRetryAfter: past,
		CooldownScope:  cooldownScopeAuth,
		ModelStates: map[string]*ModelState{
			"gpt-image-2": {Status: StatusError, Unavailable: true, NextRetryAfter: past},
		},
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateActive,
			"access_token":    "token",
		},
	}
	applyLifecycleRuntimeState(auth)
	if auth.Status != StatusActive || auth.StatusMessage != "" || auth.Unavailable || !auth.NextRetryAfter.IsZero() || auth.CooldownScope != "" {
		t.Fatalf("expired cooldown runtime state = %+v", auth)
	}
	modelState := auth.ModelStates["gpt-image-2"]
	if modelState == nil || modelState.Unavailable || !modelState.NextRetryAfter.IsZero() {
		t.Fatalf("expired model cooldown state = %+v", modelState)
	}
}

func TestApplyLifecycleRuntimeStateNormalizesExpiredAuthCooldownBeforeActiveModelCooldown(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Minute)
	future := now.Add(time.Hour)
	auth := &Auth{
		Provider:       "chatgpt-web",
		Status:         StatusError,
		StatusMessage:  "rate limited",
		Unavailable:    true,
		NextRetryAfter: past,
		CooldownScope:  cooldownScopeAuth,
		ModelStates: map[string]*ModelState{
			"expired": {Status: StatusError, Unavailable: true, NextRetryAfter: past},
			"active":  {Status: StatusError, Unavailable: true, NextRetryAfter: future},
		},
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateActive,
			"access_token":    "token",
		},
	}

	applyLifecycleRuntimeState(auth)

	if auth.CooldownScope == cooldownScopeAuth || auth.NextRetryAfter.Equal(past) {
		t.Fatalf("expired auth-wide cooldown was retained: %+v", auth)
	}
	if expired := auth.ModelStates["expired"]; expired.Unavailable || !expired.NextRetryAfter.IsZero() {
		t.Fatalf("expired model cooldown state = %+v", expired)
	}
	if active := auth.ModelStates["active"]; !active.Unavailable || !active.NextRetryAfter.Equal(future) {
		t.Fatalf("active model cooldown state = %+v", active)
	}
}

func TestLifecycleSelectable(t *testing.T) {
	tests := []struct {
		name       string
		state      string
		selectable bool
	}{
		{name: "legacy", selectable: true},
		{name: "active", state: LifecycleStateActive, selectable: true},
		{name: "login pending", state: LifecycleStateLoginPending},
		{name: "refreshing", state: LifecycleStateRefreshing},
		{name: "relogin pending", state: LifecycleStateReloginPending},
		{name: "reauth required", state: LifecycleStateReauthRequired},
		{name: "interaction required", state: LifecycleStateInteractionRequired},
		{name: "dead", state: LifecycleStateDead},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			auth := &Auth{Metadata: map[string]any{"lifecycle_state": test.state}}
			if got := auth.LifecycleSelectable(); got != test.selectable {
				t.Fatalf("LifecycleSelectable() = %v, want %v", got, test.selectable)
			}
			if got := auth.LifecycleRefreshable(); got != test.selectable {
				t.Fatalf("LifecycleRefreshable() = %v, want %v", got, test.selectable)
			}
		})
	}

	chatgptWeb := &Auth{Provider: "chatgpt-web", Metadata: map[string]any{}}
	if chatgptWeb.LifecycleSelectable() || chatgptWeb.LifecycleRefreshable() {
		t.Fatal("chatgpt-web credential without token or state must fail closed")
	}
	chatgptWeb.Metadata["access_token"] = "token"
	if !chatgptWeb.LifecycleSelectable() || !chatgptWeb.LifecycleRefreshable() {
		t.Fatal("legacy chatgpt-web token should derive active lifecycle")
	}
	compat := &Auth{
		Provider:   "chatgpt-web",
		Attributes: map[string]string{"compat_name": "chatgpt-web"},
		Metadata:   map[string]any{},
	}
	if state := compat.LifecycleState(); state != "" {
		t.Fatalf("OpenAI compatibility lifecycle = %q, want legacy behavior", state)
	}
	if !compat.LifecycleSelectable() || !compat.LifecycleRefreshable() {
		t.Fatal("OpenAI compatibility provider named chatgpt-web must not use native lifecycle gating")
	}
	agentIdentity := &Auth{Provider: "codex", Metadata: map[string]any{"auth_mode": "agentIdentity"}}
	if !agentIdentity.LifecycleSelectable() {
		t.Fatal("Agent Identity credential should remain selectable")
	}
	if agentIdentity.LifecycleRefreshable() {
		t.Fatal("Agent Identity credential should not enter OAuth auto-refresh")
	}
	agentIdentity.Metadata["auth_mode"] = "oauth"
	if !agentIdentity.LifecycleRefreshable() {
		t.Fatal("Codex credential switched back to OAuth should re-enter auto-refresh")
	}
	invalid := &Auth{
		Provider: "chatgpt-web",
		Metadata: map[string]any{"lifecycle_state": "secret-shaped-invalid-state"},
	}
	if state := invalid.LifecycleState(); state != LifecycleStateReauthRequired {
		t.Fatalf("invalid ChatGPT Web lifecycle = %q, want fail-closed reauth state", state)
	}
	if status := RuntimeStatusForLifecycle(invalid.LifecycleState()); status != StatusError {
		t.Fatalf("invalid ChatGPT Web lifecycle status = %q, want %q", status, StatusError)
	}
	invalid.Metadata["lifecycle_state"] = 123
	invalid.Metadata["access_token"] = "token"
	if state := invalid.LifecycleState(); state != LifecycleStateReauthRequired {
		t.Fatalf("non-string ChatGPT Web lifecycle = %q, want fail-closed reauth state", state)
	}
}

func TestLifecycleStateBlocksSelectionAndRefresh(t *testing.T) {
	now := time.Now()
	auth := &Auth{
		ID:       "chatgpt-web.json",
		Provider: "chatgpt-web",
		Status:   StatusPending,
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateInteractionRequired,
			"expired":         now.Add(time.Hour).Format(time.RFC3339),
		},
	}
	blocked, reason, retryAt := isAuthBlockedForModel(auth, "gpt-image-2", now)
	if !blocked || reason != blockReasonOther || !retryAt.IsZero() {
		t.Fatalf("selection block = (%v, %v, %v), want blocked without retry time", blocked, reason, retryAt)
	}
	if _, scheduled := nextRefreshCheckAt(now, auth, time.Minute); scheduled {
		t.Fatal("interaction-required credential was scheduled for refresh")
	}
}

func TestChatGPTWebRefreshStateErrorSanitizesLifecycleReason(t *testing.T) {
	auth := &Auth{
		Provider:      "chatgpt-web",
		StatusMessage: "status-message-secret",
		Metadata: map[string]any{
			"lifecycle_state":  LifecycleStateReauthRequired,
			"lifecycle_reason": "metadata-secret-shaped-reason",
		},
	}
	message := newChatGPTWebRefreshStateUnavailableError(auth).Error()
	if strings.Contains(message, "secret") {
		t.Fatalf("refresh state error exposed raw lifecycle reason: %q", message)
	}
	if !strings.Contains(message, "authentication_failed") {
		t.Fatalf("refresh state error = %q, want sanitized reason", message)
	}
}
