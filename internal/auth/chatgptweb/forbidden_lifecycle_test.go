package chatgptweb

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSessionForbiddenRequiresExplicitLifecycleEvidence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		body        string
		contentType string
		mitigated   string
		wantCode    string
		wantState   LifecycleState
		terminal    bool
	}{
		{"challenge", `<html><script src="/cdn-cgi/challenge-platform/x"></script></html>`, "text/html", "challenge", "cloudflare_challenge", LifecycleActive, false},
		{"unknown HTML", `<html>Request denied</html>`, "text/html", "", "session_refresh_forbidden", LifecycleActive, false},
		{"empty body", "", "text/html", "", "session_refresh_forbidden", LifecycleActive, false},
		{"HTML without edge headers", `<html>Request denied</html>`, "text/html", "", "session_refresh_forbidden", LifecycleActive, false},
		{"empty without edge headers", "", "text/plain", "", "session_refresh_forbidden", LifecycleActive, false},
		{"unknown JSON", `{"error":{"code":"permission_denied"}}`, "application/json", "", "session_refresh_forbidden", LifecycleActive, false},
		{"deactivated behind CF", `{"error":{"code":"account_deactivated"}}`, "application/json", "", "account_deactivated", LifecycleDead, true},
		{"deleted behind CF", `{"error":{"code":"account_deleted"}}`, "application/json", "", "account_deleted", LifecycleDead, true},
		{"explicit session expiry", `{"error":"session expired"}`, "application/json", "", "session_expired", LifecycleReauthRequired, true},
		{"explicit invalid grant", `{"error":"invalid_grant"}`, "application/json", "", "invalid_grant", LifecycleReauthRequired, true},
		{"structured challenge", `{"error":{"code":"cloudflare_challenge"}}`, "application/json", "", "cloudflare_challenge", LifecycleActive, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				if tc.name != "HTML without edge headers" && tc.name != "empty without edge headers" {
					w.Header().Set("CF-Ray", "fixture-ray")
					w.Header().Set("Server", "cloudflare")
				}
				w.Header().Set("CF-Mitigated", tc.mitigated)
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			cookies, err := ParseCookieHeader("next-auth.session-token=fixture", server.URL)
			if err != nil {
				t.Fatal(err)
			}
			service := NewService(Options{SessionBaseURL: server.URL})
			updated, err := service.RefreshSession(t.Context(), Credential{
				Cookies: cookies, AccessToken: "old-token", LifecycleState: LifecycleActive,
			}, "")
			authError, ok := AsAuthError(err)
			if !ok || authError.Code != tc.wantCode || authError.State != tc.wantState ||
				authError.Terminal != tc.terminal || authError.Retryable == tc.terminal {
				t.Fatalf("classification = %#v, want %s / %s terminal=%v", authError, tc.wantCode, tc.wantState, tc.terminal)
			}
			if updated == nil || updated.LifecycleState != tc.wantState || updated.AccessToken != "old-token" || updated.LastRefreshAt != "" {
				t.Fatalf("failed refresh changed token readiness: %#v", updated)
			}
			metadata := make(map[string]any)
			updated.ApplyToMetadata(metadata)
			raw, errMarshal := json.Marshal(metadata)
			if errMarshal != nil {
				t.Fatal(errMarshal)
			}
			reloaded, errDecode := DecodeCredential(raw)
			if errDecode != nil || reloaded.LifecycleFailureCode != tc.wantCode || reloaded.LifecycleFailureStatus != http.StatusForbidden {
				t.Fatalf("persisted classification lost: %s / %v", raw, errDecode)
			}
			wrapped := fmt.Errorf("refresh failed: %w", authError)
			reloaded.RecordLifecycleFailure(wrapped)
			if reloaded.LifecycleFailureCode != tc.wantCode {
				t.Fatal("error wrapping lost classification")
			}
		})
	}
}

func TestLifecycleFailureClearedOnSuccessfulRecovery(t *testing.T) {
	service := NewService(Options{})
	credential := &Credential{Type: Provider, LifecycleState: LifecycleReauthRequired}
	service.applyFailure(credential, newAuthError("cloudflare_challenge", LifecycleReloginPending, 403, true, false, "challenge", nil), true)
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	if metadata["lifecycle_failure_code"] != "cloudflare_challenge" {
		t.Fatal("missing original failure")
	}
	service.updateLifecycle(credential, LifecycleActive, "")
	credential.ApplyToMetadata(metadata)
	if _, exists := metadata["lifecycle_failure_code"]; exists {
		t.Fatal("successful recovery retained stale failure classification")
	}
}

func TestLoginUnknownForbiddenIsNotPermanentAccountEvidence(t *testing.T) {
	for _, stage := range []string{"authorize", "password_verify", "mfa_verify", "token_refresh"} {
		for _, body := range []string{
			"", `<html>Request denied</html>`,
			`<html>This account was deleted or deactivated.</html>`,
			`<html>Access denied. If your account was deleted or deactivated, contact support.</html>`,
			`{"error":{"code":"temporary_failure","message":"Could not check if account was deactivated"}}`,
			`{"page":{"type":"account_deactivated_pending"}}`,
			`{"page":{"type":"not_account_deleted"}}`,
		} {
			authError := classifyHTTPResponse(stage, http.StatusForbidden, []byte(body), LifecycleReloginPending)
			if authError == nil || authError.State != LifecycleReloginPending || authError.Terminal || !authError.Retryable {
				t.Errorf("stage=%s body=%q: %#v, want retryable pending", stage, body, authError)
			}
		}
	}
}
