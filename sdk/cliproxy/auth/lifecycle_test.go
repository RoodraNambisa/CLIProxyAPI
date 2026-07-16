package auth

import (
	"testing"
	"time"
)

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
