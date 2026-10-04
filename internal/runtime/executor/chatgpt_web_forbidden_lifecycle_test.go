package executor

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

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebSessionForbiddenLifecycleEndToEnd(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		code     string
		wantDead bool
	}{
		{"CF challenge", `<html><script src="/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1"></script></html>`, "cloudflare_challenge", false},
		{"unknown HTML", `<html>Request denied</html>`, "session_refresh_forbidden", false},
		{"empty", "", "session_refresh_forbidden", false},
		{"unknown JSON", `{"error":"forbidden"}`, "session_refresh_forbidden", false},
		{"deactivated JSON", `{"error":{"code":"account_deactivated"}}`, "account_deactivated", true},
		{"deleted JSON", `{"error":{"code":"account_deleted"}}`, "account_deleted", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("CF-Ray", "fixture-ray")
				w.Header().Set("Server", "cloudflare")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			store := &chatGPTWebLifecycleCaptureStore{}
			manager := cliproxyauth.NewManager(store, nil, nil)
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
				SessionCookieRefreshOnTokenFailure: true,
			}}, manager)
			executor.authService = chatgptwebauth.NewService(chatgptwebauth.Options{SessionBaseURL: server.URL})
			manager.RegisterExecutor(executor)
			t.Cleanup(func() { _ = manager.CloseExecutors() })
			auth := chatGPTWebTestAuth("session-forbidden")
			delete(auth.Metadata, "password")
			delete(auth.Metadata, "totp_secret")
			auth.Metadata["refresh_strategy"] = string(chatgptwebauth.RefreshStrategyChatGPTSession)
			auth.Metadata["expired"] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
			auth.Metadata["cookies"] = []chatgptwebauth.Cookie{{Name: "next-auth.session-token", Value: "fixture", Host: strings.TrimPrefix(server.URL, "http://"), Path: "/"}}
			current, err := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
			if err != nil {
				t.Fatal(err)
			}
			_, err = manager.RefreshChatGPTWebForRequest(t.Context(), current)
			authError, ok := chatgptwebauth.AsAuthError(err)
			if !ok || authError.Code != tc.code {
				t.Fatalf("wrapped classification = %v, want %s", err, tc.code)
			}
			providerError := cliproxyauth.NewProviderError(current, err)
			if providerError.Diagnostic == nil || providerError.Diagnostic.Code != tc.code || providerError.Diagnostic.HTTPStatus != 403 {
				t.Fatalf("provider diagnostic lost: %#v", providerError)
			}
			current, _ = manager.GetByID(auth.ID)
			if (current.LifecycleState() == cliproxyauth.LifecycleStateDead) != tc.wantDead {
				t.Fatalf("lifecycle = %s, want dead=%v", current.LifecycleState(), tc.wantDead)
			}
			if tc.wantDead {
				persisted := store.Saved()
				if persisted == nil || persisted.LifecycleSelectable() || !chatgptwebauth.DeadLifecycleDeletionAllowed(persisted.Metadata) {
					t.Fatal("explicit account termination was not durably quarantined and eligible")
				}
				if persisted.Metadata["lifecycle_failure_code"] != tc.code {
					t.Fatal("persisted lifecycle lost original error")
				}
			} else {
				if chatgptwebauth.DeadLifecycleDeletionAllowed(current.Metadata) || store.Saved() != nil {
					t.Fatal("ambiguous 403 persisted a destructive lifecycle")
				}
				expiry, known := current.AccessTokenExpirationTime()
				if !known || expiry.After(time.Now()) || !executor.ShouldRefresh(time.Now(), current) {
					t.Fatal("failed refresh made expired credentials ready or disabled later recovery")
				}
			}
		})
	}
}

func TestChatGPTWebSessionPromotionRejectsConflictingDiagnostics(t *testing.T) {
	for _, code := range []string{"cloudflare_challenge", "session_refresh_forbidden", "network_error"} {
		err := &chatgptwebauth.AuthError{
			Code: "session_expired", DiagnosticCode: code, StatusCode: 403,
			State: chatgptwebauth.LifecycleReauthRequired, Terminal: true,
		}
		credential := &chatgptwebauth.Credential{LifecycleState: chatgptwebauth.LifecycleReauthRequired}
		updated, failure, terminal := classifyChatGPTWebSessionCookieRefresh(credential, fmt.Errorf("wrapped: %w", err), false)
		if terminal || failure == nil || updated.LifecycleState == chatgptwebauth.LifecycleDead || err.State == chatgptwebauth.LifecycleDead {
			t.Fatalf("conflicting diagnostic %s was promoted to dead", code)
		}
	}
}

func TestChatGPTWebBackgroundForbiddenExhaustionKeepsQuarantine(t *testing.T) {
	for _, tc := range []struct{ name, body, code string }{
		{"CF", `<html><script src="/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1"></script></html>`, "cloudflare_challenge"},
		{"unknown HTML", `<html>Access denied</html>`, "authentication_forbidden"},
		{"empty", "", "authentication_forbidden"},
		{"network", "", "authentication_network_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &chatGPTWebLifecycleCaptureStore{}
			manager := cliproxyauth.NewManager(store, nil, nil)
			expected := registerChatGPTWebPendingAuth(t, manager, "background-forbidden")
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				current, _ := manager.GetByID(expected.ID)
				if current.LifecycleSelectable() {
					t.Error("credential selectable during recovery")
				}
				w.Header().Set("CF-Ray", "fixture-ray")
				w.Header().Set("Server", "cloudflare")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			maxRetries, jitter := 2, 0
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
				AutoRelogin: true, AutoReloginMaxRetries: &maxRetries, AutoReloginJitterPercent: &jitter,
			}}, manager)
			executor.authService = chatgptwebauth.NewService(chatgptwebauth.Options{
				AuthBaseURL: server.URL, SessionBaseURL: server.URL, SentinelBaseURL: server.URL,
			})
			if tc.name == "network" {
				executor.authService = &fakeChatGPTWebAuthService{loginFn: func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
					calls.Add(1)
					return nil, fmt.Errorf("login: %w", &chatgptwebauth.AuthError{Code: tc.code, Retryable: true})
				}}
			}
			executor.reloginBackoff = func(int) time.Duration { return 0 }
			t.Cleanup(func() { _ = executor.Close() })
			if !executor.TriggerBackgroundRelogin(expected) {
				t.Fatal("relogin was not queued")
			}
			waitForChatGPTWebCondition(t, 5*time.Second, func() bool {
				return executor.BackgroundReloginSnapshot().Exhausted == 1
			})
			current, _ := manager.GetByID(expected.ID)
			if calls.Load() != 3 || current.LifecycleState() != cliproxyauth.LifecycleStateReauthRequired || current.LifecycleSelectable() {
				t.Fatalf("exhaustion: calls=%d lifecycle=%s", calls.Load(), current.LifecycleState())
			}
			if current.LastError == nil || current.LastError.Diagnostic == nil || current.LastError.Diagnostic.Code != tc.code {
				t.Fatalf("last recovery failure lost: %#v", current.LastError)
			}
			raw, err := json.Marshal(store.Saved().Metadata)
			if err != nil {
				t.Fatal(err)
			}
			var metadata map[string]any
			if err = json.Unmarshal(raw, &metadata); err != nil {
				t.Fatal(err)
			}
			if metadata["lifecycle_reason"] != "auto_relogin_exhausted" || metadata["lifecycle_failure_code"] != tc.code || chatgptwebauth.DeadLifecycleDeletionAllowed(metadata) {
				t.Fatalf("persisted exhaustion lost classification: %s", raw)
			}
			if executor.TriggerBackgroundRelogin(current) || executor.BackgroundReloginSnapshot().Dead != 0 {
				t.Fatal("exhausted transient failure was requeued or counted as dead")
			}
		})
	}
}

func TestChatGPTWebTransientReloginOnlyRestoresSelectionAfterSuccess(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "challenge-then-success")
	maxRetries, jitter := 2, 0
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AutoRelogin: true, AutoReloginMaxRetries: &maxRetries, AutoReloginJitterPercent: &jitter,
	}}, manager)
	t.Cleanup(func() { _ = executor.Close() })
	secondStarted, release := make(chan struct{}), make(chan struct{}, 1)
	defer close(release)
	var calls atomic.Int32
	executor.authService = &fakeChatGPTWebAuthService{loginFn: func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if calls.Add(1) == 1 {
			return nil, &chatgptwebauth.AuthError{Code: "cloudflare_challenge", StatusCode: 403, Retryable: true}
		}
		close(secondStarted)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
		updated := cloneChatGPTWebCredential(input.Credential)
		updated.AccessToken = "recovered-token"
		updated.LifecycleState = chatgptwebauth.LifecycleActive
		updated.LifecycleReason = ""
		return updated, nil
	}}
	executor.reloginBackoff = func(int) time.Duration { return 0 }
	if !executor.TriggerBackgroundRelogin(expected) {
		t.Fatal("relogin not queued")
	}
	select {
	case <-secondStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("second attempt did not start")
	}
	current, _ := manager.GetByID(expected.ID)
	if current.LifecycleSelectable() || chatgptwebauth.DeadLifecycleDeletionAllowed(current.Metadata) {
		t.Fatal("retry exposed or deleted the recovering credential")
	}
	release <- struct{}{}
	waitForChatGPTWebCondition(t, 5*time.Second, func() bool {
		return executor.BackgroundReloginSnapshot().Succeeded == 1
	})
	current, _ = manager.GetByID(expected.ID)
	if !current.LifecycleSelectable() || current.Metadata["access_token"] != "recovered-token" || current.Metadata["lifecycle_failure_code"] != nil {
		t.Fatal("successful recovery did not restore a clean active credential")
	}
}

func TestChatGPTWebProxiedChallengeExhaustionIsFinite(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "proxied-challenge")
	maxRetries, jitter := 2, 0
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AutoRelogin: true, AutoReloginMaxRetries: &maxRetries, AutoReloginJitterPercent: &jitter,
		LoginProxy: config.ChatGPTWebLoginProxyConfig{Enabled: true, URLTemplate: "http://127.0.0.1:1"},
	}}, manager)
	t.Cleanup(func() { _ = executor.Close() })
	fake := &fakeChatGPTWebAuthService{loginFn: func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if !input.LoginProxy.Enabled {
			t.Error("login-only proxy was not enabled")
		}
		credential := cloneChatGPTWebCredential(input.Credential)
		credential.LifecycleState = chatgptwebauth.LifecycleReloginPending
		credential.LifecycleReason = "cloudflare_challenge"
		return credential, &chatgptwebauth.AuthError{
			Code: "cloudflare_challenge", State: chatgptwebauth.LifecycleReloginPending,
			StatusCode: 403, Retryable: true,
		}
	}}
	executor.authService = fake
	executor.reloginBackoff = func(int) time.Duration { return 0 }
	if !executor.TriggerBackgroundRelogin(expected) {
		t.Fatal("relogin not queued")
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		snapshot := executor.BackgroundReloginSnapshot()
		return snapshot.Running == 0 && snapshot.Queued == 0 && snapshot.Delayed == 0
	})
	current, _ := manager.GetByID(expected.ID)
	if fake.loginCalls.Load() != 3 || current.LifecycleState() != cliproxyauth.LifecycleStateReauthRequired ||
		current.LifecycleSelectable() || executor.BackgroundReloginSnapshot().Exhausted != 1 {
		t.Fatalf("proxied recovery: calls=%d state=%s snapshot=%+v", fake.loginCalls.Load(), current.LifecycleState(), executor.BackgroundReloginSnapshot())
	}
}
