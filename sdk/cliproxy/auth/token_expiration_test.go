package auth

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"
)

func expiryTestToken(exp string) string {
	return "e30." + base64.RawURLEncoding.EncodeToString([]byte(`{"exp":`+exp+`}`)) + ".test-signature"
}

func TestCodexTokenExpirationUsesJWTWithoutAssumingAuthorization(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	for _, expires := range []int64{0, now.Unix() - 1, now.Unix(), now.Unix() + 60} {
		credential := &Auth{Provider: "codex", Metadata: map[string]any{"access_token": expiryTestToken(fmt.Sprint(expires)), "expires_at": now.Add(time.Hour).Format(time.RFC3339)}}
		exp, ok := credential.ExpirationTime()
		if !ok || exp.Unix() != expires {
			t.Fatal("stale metadata overrode the JWT expiry")
		}
		blocked, _, _ := isAuthBlockedForModel(credential, "gpt-5.4", now)
		if blocked != (expires <= now.Unix()) {
			t.Fatal("expired access token remained selectable")
		}
	}
	credential := &Auth{Provider: "codex", Unavailable: true, Status: StatusError, CooldownScope: cooldownScopeAuth, NextRetryAfter: now.Add(time.Hour), LastError: &Error{HTTPStatus: 401, Message: "revoked"}, Metadata: map[string]any{"access_token": expiryTestToken(fmt.Sprint(now.Unix() + 60))}}
	if blocked, _, _ := isAuthBlockedForModel(credential, "gpt-5.4", now); !blocked {
		t.Fatal("an unexpired claim bypassed revoked credential state")
	}
	credential.Provider = "chatgpt-web"
	credential.Metadata["expires_at"] = now.Add(2 * time.Hour).Format(time.RFC3339)
	if exp, _ := credential.ExpirationTime(); !exp.Equal(now.Add(2 * time.Hour)) {
		t.Fatal("Codex expiry update changed another provider's expiry precedence")
	}
	credential.Provider = "codex"
	credential.Attributes = map[string]string{"api_key": "test"}
	if _, ok := codexAccessTokenExpiration(credential); ok {
		t.Fatal("API key inherited OAuth expiry")
	}
}

func TestJWTExpirationBoundaryAndMalformedValues(t *testing.T) {
	if expiry, ok := parseJWTExpiration(expiryTestToken("253402300799.5")); !ok || expiry.Year() != 9999 || expiry.Nanosecond() != 500000000 {
		t.Fatal("valid fractional second at the supported upper boundary was rejected")
	}
	for _, raw := range []string{`null`, `true`, `{}`, `[]`, `"NaN"`, `"Inf"`, `1e999`, `9223372036854775808`, `253402300800`, `-62135596801`} {
		if _, ok := parseJWTExpiration(expiryTestToken(raw)); ok {
			t.Fatal("invalid expiry accepted")
		}
	}
	for _, token := range []string{"opaque", "a.invalid-base64.c", "a.e30.extra.part", strings.Repeat("a", (64<<10)+1)} {
		if _, ok := parseJWTExpiration(token); ok {
			t.Fatal("malformed token accepted")
		}
	}
	for _, raw := range []string{`1800000000.5`, `"1800000000.5"`} {
		if exp, ok := parseJWTExpiration(expiryTestToken(raw)); !ok || !exp.Equal(time.Unix(1800000000, 500000000)) {
			t.Fatal("fractional NumericDate changed")
		}
	}
	credential := &Auth{Provider: "codex", Metadata: map[string]any{"access_token": "opaque", "expires_at": "2030-01-01T00:00:00Z"}}
	if exp, ok := credential.ExpirationTime(); !ok || exp.Year() != 2030 {
		t.Fatal("opaque legacy access token lost metadata expiry")
	}
}

func TestSchedulerDemotesExpiredTokenAndRefreshRestoresEligibility(t *testing.T) {
	now := time.Unix(1800000000, 0)
	credential := &Auth{ID: "expiring", Provider: "codex", Metadata: map[string]any{"access_token": expiryTestToken(fmt.Sprint(now.Unix() + 1))}}
	meta := buildScheduledAuthMetaWithSupportedModels(credential, map[string]struct{}{"model": {}})
	model := &modelScheduler{modelKey: "model", entries: map[string]*scheduledAuth{credential.ID: buildScheduledAuth(meta, "model", now)}}
	model.rebuildIndexesLocked()
	if len(model.readyByPriority) != 1 {
		t.Fatal("unexpired token was blocked")
	}
	model.promoteExpiredLocked(now.Add(2 * time.Second))
	if len(model.readyByPriority) != 0 || model.entries[credential.ID].state != scheduledStateBlocked {
		t.Fatal("ready view continued offering an expired token")
	}
	refreshed := credential.Clone()
	refreshed.Metadata["access_token"] = expiryTestToken(fmt.Sprint(now.Unix() + 3600))
	model.entries[credential.ID].applyMeta(buildScheduledAuthMetaWithSupportedModels(refreshed, map[string]struct{}{"model": {}}), "model", now.Add(2*time.Second))
	model.rebuildIndexesLocked()
	if len(model.readyByPriority) != 1 {
		t.Fatal("refreshed token did not restore readiness")
	}
}

type codexTransientRefreshExecutor struct{ schedulerProviderTestExecutor }

func (e codexTransientRefreshExecutor) Refresh(context.Context, *Auth) (*Auth, error) {
	return nil, &Error{HTTPStatus: 503, Message: "temporary refresh failure"}
}

func TestCodexRefreshFailurePreservesUsableStateAndExistingCooldown(t *testing.T) {
	for _, cooling := range []bool{false, true} {
		manager := NewManager(nil, nil, nil)
		manager.RegisterExecutor(codexTransientRefreshExecutor{schedulerProviderTestExecutor{provider: "codex"}})
		expiry := time.Now().Add(time.Minute).Truncate(time.Second)
		credential := &Auth{ID: "refresh-expiry", Provider: "codex", Status: StatusActive, Metadata: map[string]any{"access_token": expiryTestToken(fmt.Sprint(expiry.Unix())), "refresh_token": "fake-refresh"}}
		if cooling {
			credential.Unavailable = true
			credential.Status = StatusError
			credential.CooldownScope = cooldownScopeAuth
			credential.NextRetryAfter = time.Now().Add(time.Hour)
			credential.LastError = &Error{HTTPStatus: 401, Message: "revoked"}
		}
		registered, err := manager.Register(t.Context(), credential)
		if err != nil {
			t.Fatal(err)
		}
		manager.refreshAuth(t.Context(), registered.ID)
		current, _ := manager.GetByID(registered.ID)
		if current.Unavailable != cooling || current.Status != registered.Status || !current.NextRetryAfter.Equal(registered.NextRetryAfter) {
			t.Fatal("refresh failure changed authorization/cooldown state")
		}
		if !current.NextRefreshAfter.Equal(expiry) {
			t.Fatal("refresh backoff outlived the usable access token")
		}
	}
}
