package auth

import (
	"context"
	"fmt"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestChatGPTWebBackgroundRefreshUsesAccessTokenExpiry(t *testing.T) {
	lead := 5 * time.Minute
	setRefreshLeadFactory(t, "chatgpt-web", func() *time.Duration { return &lead })
	now := time.Now().UTC().Truncate(time.Second)
	for _, offset := range []time.Duration{-time.Minute, 4 * time.Minute, time.Hour} {
		expiry := now.Add(offset)
		auth := &Auth{Provider: "chatgpt-web", Metadata: map[string]any{
			"access_token":    expiryTestToken(fmt.Sprint(expiry.Unix())),
			"expired":         now.Add(90 * 24 * time.Hour).Format(time.RFC3339),
			"lifecycle_state": LifecycleStateActive,
		}}
		if got, known := auth.ExpirationTime(); !known || !got.Equal(expiry) {
			t.Fatalf("expiry = %v/%t, want %v", got, known, expiry)
		}
		due, enabled := nextRefreshCheckAt(now, auth, time.Hour)
		wantDue := expiry.Add(-lead)
		if wantDue.Before(now) {
			wantDue = now
		}
		if !enabled || !due.Equal(wantDue) {
			t.Fatalf("background due = %v/%t, want %v", due, enabled, wantDue)
		}
		if (&Manager{}).shouldRefresh(auth, now) != (offset <= lead) {
			t.Fatal("refresh evaluation disagrees with scheduled JWT expiry")
		}
	}
}

func TestChatGPTWebExpiredTokenBlockedWithoutDisablingRecovery(t *testing.T) {
	lead := 5 * time.Minute
	setRefreshLeadFactory(t, "chatgpt-web", func() *time.Duration { return &lead })
	now := time.Now().Truncate(time.Second)
	auth := &Auth{ID: "expired-web", Provider: "chatgpt-web", Status: StatusActive, Metadata: map[string]any{
		"access_token":    expiryTestToken(fmt.Sprint(now.Unix())),
		"expired":         now.Add(90 * 24 * time.Hour).Format(time.RFC3339),
		"lifecycle_state": LifecycleStateActive,
	}}
	if blocked, _, _ := isAuthBlockedForModel(auth, "model", now); !blocked {
		t.Fatal("expired token remained selectable")
	}
	if auth.Disabled || !auth.LifecycleRefreshable() || !(&Manager{}).shouldRefresh(auth, now) {
		t.Fatal("route rejection disabled background recovery")
	}
	for _, state := range []string{LifecycleStateActive, LifecycleStateReloginPending, LifecycleStateReauthRequired} {
		updated := auth.Clone()
		updated.Metadata["access_token"] = expiryTestToken(fmt.Sprint(now.Add(time.Hour).Unix()))
		updated.Metadata["lifecycle_state"] = state
		if blocked, _, _ := isAuthBlockedForModel(updated, "model", now); blocked != (state != LifecycleStateActive) {
			t.Fatalf("new token bypassed lifecycle state %s", state)
		}
		updated.Disabled = true
		if blocked, _, _ := isAuthBlockedForModel(updated, "model", now); !blocked {
			t.Fatal("new token bypassed manual disable")
		}
	}
	auth.Attributes = map[string]string{"compat_name": "web-compatible"}
	if _, known := routingAccessTokenExpiration(auth); known {
		t.Fatal("native guard changed compatible provider behavior")
	}
}

func TestChatGPTWebSchedulerAgesOutExpiredToken(t *testing.T) {
	now := time.Unix(1800000000, 0)
	auth := &Auth{ID: "expiring-web", Provider: "chatgpt-web", Metadata: map[string]any{
		"access_token": expiryTestToken(fmt.Sprint(now.Add(time.Second).Unix())),
	}}
	meta := buildScheduledAuthMetaWithSupportedModels(auth, map[string]struct{}{"model": {}})
	model := &modelScheduler{modelKey: "model", entries: map[string]*scheduledAuth{auth.ID: buildScheduledAuth(meta, "model", now)}}
	model.rebuildIndexesLocked()
	if len(model.readyByPriority) != 1 {
		t.Fatal("unexpired token blocked")
	}
	model.promoteExpiredLocked(now.Add(time.Second))
	if len(model.readyByPriority) != 0 || model.entries[auth.ID].state != scheduledStateBlocked {
		t.Fatal("indexed ready token did not age out at expiry")
	}
	auth.Metadata["access_token"] = expiryTestToken(fmt.Sprint(now.Add(time.Hour).Unix()))
	model.entries[auth.ID].applyMeta(buildScheduledAuthMetaWithSupportedModels(auth, map[string]struct{}{"model": {}}), "model", now.Add(time.Second))
	model.rebuildIndexesLocked()
	if len(model.readyByPriority) != 1 {
		t.Fatal("valid replacement did not restore readiness")
	}
}

type chatGPTWebExpiryRefreshExecutor struct{ ProviderExecutor }

func (chatGPTWebExpiryRefreshExecutor) Identifier() string { return "chatgpt-web" }
func (chatGPTWebExpiryRefreshExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	updated := auth.Clone()
	updated.Metadata["access_token"] = expiryTestToken(fmt.Sprint(time.Now().Add(time.Hour).Unix()))
	return updated, nil
}

func TestChatGPTWebExpiredSelectionAndBackgroundRecovery(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.RegisterExecutor(chatGPTWebExpiryRefreshExecutor{})
			for _, id := range []string{"a-expired", "b-valid"} {
				expiry := time.Now().Add(time.Hour)
				if id == "a-expired" {
					expiry = time.Now().Add(-time.Hour)
				}
				_, err := manager.Register(t.Context(), &Auth{ID: id, Provider: "chatgpt-web", Metadata: map[string]any{
					"access_token": expiryTestToken(fmt.Sprint(expiry.Unix())), "lifecycle_state": LifecycleStateActive,
				}})
				if err != nil {
					t.Fatal(err)
				}
			}
			pick := manager.pickNext
			if legacy {
				pick = manager.pickNextLegacy
			}
			got, _, err := pick(t.Context(), "chatgpt-web", "", cliproxyexecutor.Options{}, nil)
			if err != nil || got.ID != "b-valid" {
				t.Fatalf("expired credential selected: auth=%v err=%v", got, err)
			}
			got, _, err = pick(t.Context(), "chatgpt-web", "", cliproxyexecutor.Options{Metadata: map[string]any{
				cliproxyexecutor.PinnedAuthMetadataKey: "a-expired",
			}}, nil)
			if err == nil || got != nil {
				t.Fatal("fixed credential bypassed expiry")
			}
			manager.refreshAuth(t.Context(), "a-expired")
			got, _, err = pick(t.Context(), "chatgpt-web", "", cliproxyexecutor.Options{}, nil)
			if err != nil || got.ID != "a-expired" {
				t.Fatalf("background recovery did not restore selection: auth=%v err=%v", got, err)
			}
		})
	}
}

func TestChatGPTWebRoutingExpiryEvidence(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	for _, tc := range []struct {
		name     string
		metadata map[string]any
		blocked  bool
	}{
		{"opaque without evidence", map[string]any{"access_token": "opaque"}, false},
		{"expired metadata", map[string]any{"access_token": "opaque", "expired": now.Add(-time.Second).Format(time.RFC3339)}, true},
		{"malformed explicit expiry", map[string]any{"access_token": "opaque", "expired": "invalid"}, true},
		{"valid token over stale metadata", map[string]any{"access_token": expiryTestToken(fmt.Sprint(now.Add(time.Hour).Unix())), "expired": now.Add(-time.Hour).Format(time.RFC3339)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auth := &Auth{Provider: "chatgpt-web", Metadata: tc.metadata}
			if blocked, _, _ := isAuthBlockedForModel(auth, "model", now); blocked != tc.blocked {
				t.Fatalf("blocked = %t, want %t", blocked, tc.blocked)
			}
		})
	}
}

func TestChatGPTWebRefreshBackoffDoesNotOutliveUsableToken(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(codexTransientRefreshExecutor{schedulerProviderTestExecutor{provider: "chatgpt-web"}})
	expiry := time.Now().Add(time.Minute).Truncate(time.Second)
	_, err := manager.Register(t.Context(), &Auth{ID: "expiry-backoff", Provider: "chatgpt-web", Status: StatusActive, Metadata: map[string]any{
		"access_token": expiryTestToken(fmt.Sprint(expiry.Unix())),
	}})
	if err != nil {
		t.Fatal(err)
	}
	manager.refreshAuth(t.Context(), "expiry-backoff")
	current, _ := manager.GetByID("expiry-backoff")
	if !current.NextRefreshAfter.Equal(expiry) || current.Disabled || !current.LifecycleRefreshable() {
		t.Fatal("transient refresh failure delayed expiry recovery or disabled the credential")
	}
}
