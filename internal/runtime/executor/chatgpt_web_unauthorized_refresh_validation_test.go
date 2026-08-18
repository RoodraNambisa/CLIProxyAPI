package executor

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func chatGPTWebUnauthorizedRefreshValidationAuth(t *testing.T, accessToken string) *cliproxyauth.Auth {
	t.Helper()
	auth := chatGPTWebRuntimeAuth()
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	credential.AccessToken = accessToken
	credential.AccountID = "account-1"
	credential.LoginMethod = chatgptwebauth.LoginMethodAPI798
	credential.API798URL = "https://api798.com/get_code?email=user%40example.com&auth_code=opaque"
	credential.ApplyToMetadata(auth.Metadata)
	return auth
}

func TestChatGPTWebUnauthorizedRefreshAcceptsSameTokenAfterAuthenticatedProbe(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if got := request.Header.Get("Authorization"); got != "Bearer retained-token" {
			t.Errorf("probe authorization = %q", got)
		}
		http.SetCookie(w, &http.Cookie{Name: "probe-session", Value: "refreshed-cookie", Path: "/"})
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accounts": map[string]any{
				"account-1": map[string]any{"account_id": "account-1", "plan_type": "plus"},
			},
		})
	}))
	defer server.Close()
	executor := &ChatGPTWebExecutor{
		runtimeBaseURL: server.URL,
		now:            testNow,
	}
	executor.cfg.Store(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}})
	previous := chatGPTWebUnauthorizedRefreshValidationAuth(t, "retained-token")
	refreshed := chatGPTWebUnauthorizedRefreshValidationAuth(t, "retained-token")
	refreshedCredential, errCredential := chatgptwebauth.ParseCredential(refreshed.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	refreshedCredential.Persona.HardwareConcurrency = 16
	refreshedCredential.Cookies = []chatgptwebauth.Cookie{{
		Name: "refresh-session", Value: "session-cookie", Path: "/", Host: "127.0.0.1",
	}}
	refreshedCredential.ApplyToMetadata(refreshed.Metadata)

	updated, errValidate := executor.ValidateUnauthorizedRequestRefresh(
		t.Context(),
		"retained-token",
		previous,
		refreshed,
	)
	if errValidate != nil || updated == nil || updated.LifecycleState() != cliproxyauth.LifecycleStateActive {
		t.Fatalf("same-token validation = (%#v, %v)", updated, errValidate)
	}
	verifiedCredential, errCredential := chatgptwebauth.ParseCredential(updated.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	if verifiedCredential.AccessToken != "retained-token" || verifiedCredential.Persona.HardwareConcurrency != 16 {
		t.Fatalf("verified credential = %#v", verifiedCredential)
	}
	installedCookies := make(map[string]string)
	for _, cookie := range verifiedCredential.Cookies {
		installedCookies[cookie.Name] = cookie.Value
	}
	if installedCookies["refresh-session"] != "session-cookie" || installedCookies["probe-session"] != "refreshed-cookie" {
		t.Fatalf("verified cookies = %#v, want refreshed and probe cookies", verifiedCredential.Cookies)
	}
	if calls.Load() != 1 {
		t.Fatalf("authenticated probe calls = %d, want 1", calls.Load())
	}
}

func TestChatGPTWebUnauthorizedRefreshRequiresAuthenticatedProbe(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		status         int
		body           any
		wantState      string
		wantOutcome    string
		wantPersist    bool
		wantToken      string
		wantProbeAuth  string
		wantCooldown   bool
		refreshedToken string
	}{
		{
			name:          "different valid token",
			status:        http.StatusOK,
			body:          map[string]any{"accounts": map[string]any{"account-1": map[string]any{"account_id": "account-1", "plan_type": "plus"}}},
			wantState:     cliproxyauth.LifecycleStateActive,
			wantToken:     "replacement-token",
			wantProbeAuth: "Bearer replacement-token",
		},
		{
			name:           "same valid token",
			status:         http.StatusOK,
			body:           map[string]any{"accounts": map[string]any{"account-1": map[string]any{"account_id": "account-1", "plan_type": "plus"}}},
			wantState:      cliproxyauth.LifecycleStateActive,
			wantToken:      "rejected-token",
			wantProbeAuth:  "Bearer rejected-token",
			refreshedToken: "rejected-token",
		},
		{
			name:          "different unauthorized token",
			status:        http.StatusUnauthorized,
			body:          map[string]any{"error": map[string]any{"code": "invalid_token"}},
			wantState:     cliproxyauth.LifecycleStateReloginPending,
			wantOutcome:   cliproxyauth.ChatGPTWebRequestRefreshOutcomeProbeUnauthorized,
			wantPersist:   true,
			wantProbeAuth: "Bearer replacement-token",
		},
		{
			name:           "same unauthorized token",
			status:         http.StatusUnauthorized,
			body:           map[string]any{"error": map[string]any{"code": "invalid_token"}},
			wantState:      cliproxyauth.LifecycleStateReloginPending,
			wantOutcome:    cliproxyauth.ChatGPTWebRequestRefreshOutcomeProbeUnauthorized,
			wantPersist:    true,
			wantProbeAuth:  "Bearer rejected-token",
			refreshedToken: "rejected-token",
		},
		{
			name:          "different token with mismatched account",
			status:        http.StatusOK,
			body:          map[string]any{"accounts": map[string]any{"account-2": map[string]any{"account_id": "account-2", "plan_type": "plus"}}},
			wantState:     cliproxyauth.LifecycleStateInteractionRequired,
			wantOutcome:   cliproxyauth.ChatGPTWebRequestRefreshOutcomeProbeUnauthorized,
			wantPersist:   true,
			wantProbeAuth: "Bearer replacement-token",
		},
		{
			name:           "same token with mismatched account",
			status:         http.StatusOK,
			body:           map[string]any{"accounts": map[string]any{"account-2": map[string]any{"account_id": "account-2", "plan_type": "plus"}}},
			wantState:      cliproxyauth.LifecycleStateInteractionRequired,
			wantOutcome:    cliproxyauth.ChatGPTWebRequestRefreshOutcomeProbeUnauthorized,
			wantPersist:    true,
			wantProbeAuth:  "Bearer rejected-token",
			refreshedToken: "rejected-token",
		},
		{
			name:          "cloudflare challenge remains transient",
			status:        http.StatusForbidden,
			body:          "<html><title>Just a moment...</title><p>Cloudflare</p></html>",
			wantState:     cliproxyauth.LifecycleStateActive,
			wantOutcome:   cliproxyauth.ChatGPTWebRequestRefreshOutcomeProbeTransient,
			wantPersist:   true,
			wantToken:     "replacement-token",
			wantProbeAuth: "Bearer replacement-token",
			wantCooldown:  true,
		},
		{
			name:           "same token with cloudflare challenge remains transient",
			status:         http.StatusForbidden,
			body:           "<html><title>Just a moment...</title><p>Cloudflare</p></html>",
			wantState:      cliproxyauth.LifecycleStateActive,
			wantOutcome:    cliproxyauth.ChatGPTWebRequestRefreshOutcomeProbeTransient,
			wantPersist:    true,
			wantToken:      "rejected-token",
			wantProbeAuth:  "Bearer rejected-token",
			wantCooldown:   true,
			refreshedToken: "rejected-token",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var authorization string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				authorization = request.Header.Get("Authorization")
				if testCase.status == http.StatusForbidden {
					w.Header().Set("Content-Type", "text/html")
					w.Header().Set("Server", "cloudflare")
				}
				w.WriteHeader(testCase.status)
				switch body := testCase.body.(type) {
				case string:
					_, _ = w.Write([]byte(body))
				default:
					_ = json.NewEncoder(w).Encode(body)
				}
			}))
			defer server.Close()

			executor := &ChatGPTWebExecutor{
				runtimeBaseURL: server.URL,
				now:            testNow,
			}
			executor.cfg.Store(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}})
			previous := chatGPTWebUnauthorizedRefreshValidationAuth(t, "rejected-token")
			refreshedToken := testCase.refreshedToken
			if refreshedToken == "" {
				refreshedToken = "replacement-token"
			}
			refreshed := chatGPTWebUnauthorizedRefreshValidationAuth(t, refreshedToken)
			updated, errValidate := executor.ValidateUnauthorizedRequestRefresh(
				t.Context(),
				"rejected-token",
				previous,
				refreshed,
			)
			if authorization != testCase.wantProbeAuth {
				t.Fatalf("probe authorization = %q", authorization)
			}
			if testCase.wantState == "" {
				if updated != nil {
					t.Fatalf("updated = %#v, want no installable auth", updated)
				}
			} else if updated == nil || updated.LifecycleState() != testCase.wantState {
				t.Fatalf("lifecycle = %#v, want %q", updated, testCase.wantState)
			}
			if testCase.wantToken != "" {
				credential, errCredential := chatgptwebauth.ParseCredential(updated.Metadata)
				if errCredential != nil || credential.AccessToken != testCase.wantToken {
					t.Fatalf("verified credential = %#v, %v", credential, errCredential)
				}
			}
			if got := chatGPTWebRequestRefreshValidationOutcome(errValidate); got != testCase.wantOutcome {
				t.Fatalf("validation outcome = %q, want %q (error %v)", got, testCase.wantOutcome, errValidate)
			}
			if got := persistAuthUpdateForExecutorError(errValidate); got != testCase.wantPersist {
				t.Fatalf("persist update = %t, want %t", got, testCase.wantPersist)
			}
			if testCase.wantCooldown {
				if updated == nil || !updated.Unavailable || updated.CooldownScope != "auth" ||
					!updated.NextRetryAfter.After(testNow()) || !updated.NextRefreshAfter.After(testNow()) || updated.LastError == nil ||
					updated.LastError.Code != "unauthorized_refresh_probe_transient" {
					t.Fatalf("transient probe cooldown = %#v", updated)
				}
			}
		})
	}
}

func persistAuthUpdateForExecutorError(err error) bool {
	type provider interface {
		PersistAuthUpdateOnError() bool
	}
	var target provider
	return errors.As(err, &target) && target.PersistAuthUpdateOnError()
}

func chatGPTWebRequestRefreshValidationOutcome(err error) string {
	type provider interface {
		ChatGPTWebRequestRefreshOutcome() string
	}
	var target provider
	if errors.As(err, &target) {
		return target.ChatGPTWebRequestRefreshOutcome()
	}
	return ""
}

func testNow() time.Time {
	return time.Unix(1_700_000_000, 0).UTC()
}
