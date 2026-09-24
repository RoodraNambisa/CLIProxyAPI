package chatgptweb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSessionRefreshRejectsIneffectiveResults(t *testing.T) {
	now := time.Date(2026, time.September, 24, 8, 0, 0, 0, time.UTC)
	expired := testJWT(now.Add(-time.Hour).Unix())
	valid := testJWT(now.Add(time.Hour).Unix())
	for _, tc := range []struct {
		name      string
		token     string
		errValue  any
		recoverAt int
		wantCode  string
		wantCalls int32
		terminal  bool
	}{
		{name: "explicit session refresh failure", token: expired, errValue: "RefreshAccessTokenError", wantCode: "session_refresh_failed", wantCalls: 3, terminal: true},
		{name: "expired without error flag", token: expired, wantCode: "access_token_expired", wantCalls: 3, terminal: true},
		{name: "missing token", wantCode: "access_token_missing", wantCalls: 3, terminal: true},
		{name: "error flag precedes valid token", token: valid, errValue: "RefreshAccessTokenError", wantCode: "session_refresh_failed", wantCalls: 3, terminal: true},
		{name: "eventual valid token", token: expired, errValue: "RefreshAccessTokenError", recoverAt: 3, wantCalls: 3},
		{name: "unchanged valid token", token: valid, wantCalls: 1},
		{name: "unknown error remains transient", token: expired, errValue: map[string]any{"code": "temporary_failure"}, wantCode: "session_response_error", wantCalls: 1},
		{name: "opaque token does not inherit session expiry", token: "opaque-token", wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempt := calls.Add(1)
				if r.URL.Path != "/api/auth/session" || r.URL.Query().Get("refresh") != "true" {
					t.Errorf("unexpected request %s", r.URL)
				}
				cookie, errCookie := r.Cookie("next-auth.session-token")
				if errCookie != nil || cookie.Value != fmt.Sprintf("session-%d", attempt-1) {
					t.Errorf("retry did not retain the updated session cookie")
				}
				http.SetCookie(w, &http.Cookie{Name: "next-auth.session-token", Value: fmt.Sprintf("session-%d", attempt), Path: "/"})
				token, sessionError := tc.token, tc.errValue
				if tc.recoverAt > 0 && int(attempt) >= tc.recoverAt {
					token, sessionError = valid, nil
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"accessToken": token, "error": sessionError,
					"expires": now.Add(90 * 24 * time.Hour).Format(time.RFC3339),
				})
			}))
			defer server.Close()
			cookies, errCookies := ParseCookieHeader("next-auth.session-token=session-0", server.URL)
			if errCookies != nil {
				t.Fatal(errCookies)
			}
			cookies[0].Domain = ""
			cookies[0].Host = strings.TrimPrefix(server.URL, "http://")
			original := Credential{AccessToken: tc.token, Cookies: cookies, Expired: now.Add(-time.Hour).Format(time.RFC3339), LastRefreshAt: "2026-09-01T00:00:00Z", LifecycleState: LifecycleActive}
			service := NewService(Options{SessionBaseURL: server.URL, Now: func() time.Time { return now }})
			updated, errRefresh := service.RefreshSession(t.Context(), original, "")
			if calls.Load() != tc.wantCalls {
				t.Fatalf("requests = %d, want %d", calls.Load(), tc.wantCalls)
			}
			if tc.wantCode == "" {
				if errRefresh != nil || updated.LifecycleState != LifecycleActive || updated.LastRefreshAt != now.Format(time.RFC3339) {
					t.Fatalf("successful refresh: state=%v error=%v", updated.LifecycleState, errRefresh)
				}
				if tc.token == "opaque-token" && updated.Expired != "" {
					t.Fatal("session expiry was incorrectly used as access-token expiry")
				}
				if tc.recoverAt > 0 && updated.AccessToken != valid {
					t.Fatal("recovered token was not installed")
				}
				return
			}
			authError, ok := AsAuthError(errRefresh)
			if !ok || authError.Code != tc.wantCode || authError.Terminal != tc.terminal || authError.Attempts != int(tc.wantCalls) {
				t.Fatalf("refresh error = %#v", authError)
			}
			if updated.LastRefreshAt != original.LastRefreshAt || updated.AccessToken != original.AccessToken {
				t.Fatal("failed refresh was published as a successful token change")
			}
			if tc.terminal && updated.LifecycleState != LifecycleReauthRequired {
				t.Fatalf("state = %s", updated.LifecycleState)
			}
			if !tc.terminal && (!authError.Retryable || updated.LifecycleState != LifecycleActive) {
				t.Fatal("unknown session error was classified as invalid credentials")
			}
			if updated.SessionToken != fmt.Sprintf("session-%d", tc.wantCalls) {
				t.Fatal("latest session cookie was lost on failure")
			}
		})
	}
}

func TestSessionRefreshCancellationDoesNotEscalateToRelogin(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "RefreshAccessTokenError"})
		w.(http.Flusher).Flush()
		cancel()
	}))
	defer server.Close()
	cookies, errCookies := ParseCookieHeader("next-auth.session-token=session-0", server.URL)
	if errCookies != nil {
		t.Fatal(errCookies)
	}
	service := NewService(Options{SessionBaseURL: server.URL})
	updated, err := service.RefreshSession(ctx, Credential{AccessToken: "old", Cookies: cookies, LastRefreshAt: "original"}, "")
	if err == nil || IsTerminal(err) || updated.LastRefreshAt != "original" || calls.Load() != 1 {
		t.Fatalf("canceled refresh: error=%v state=%s calls=%d", err, updated.LifecycleState, calls.Load())
	}
}

func TestClassifyRevokedAccessTokenRequiresExplicitUnauthorizedCode(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   bool
	}{
		{401, `{"error":{"code":"token_revoked"}}`, true},
		{401, `{"detail":{"code":"token_revoked"}}`, true},
		{401, `{"code":"token_revoked"}`, true},
		{403, `{"error":{"code":"token_revoked"}}`, false},
		{401, `{"error":{"code":"token_expired"}}`, false},
		{401, `{"error":{"message":"token_revoked"}}`, false},
		{401, `<html>token_revoked</html>`, false},
	} {
		err := ClassifyRevokedAccessTokenResponse(tc.status, []byte(tc.body))
		if (err != nil) != tc.want {
			t.Fatalf("classify %d %s = %v", tc.status, tc.body, err)
		}
		if err != nil && (err.State != LifecycleReauthRequired || !err.Terminal || SafeLifecycleReason(err.Code) != "token_revoked") {
			t.Fatalf("revocation state = %#v", err)
		}
	}
}
