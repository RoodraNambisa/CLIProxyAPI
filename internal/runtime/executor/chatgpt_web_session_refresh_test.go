package executor

import (
	"encoding/base64"
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

func sessionRefreshTestJWT(exp time.Time) string {
	return "e30." + base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, exp.Unix()))) + ".signature"
}

func TestChatGPTWebRefreshExpiryMatchesBackground(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.now = func() time.Time { return now }
	t.Cleanup(func() { _ = executor.Close() })
	for _, offset := range []time.Duration{-time.Second, 5 * time.Minute, 5*time.Minute + time.Second, time.Hour} {
		auth := chatGPTWebTestAuth("expiry")
		auth.Metadata["access_token"] = sessionRefreshTestJWT(now.Add(offset))
		auth.Metadata["expired"] = now.Add(90 * 24 * time.Hour).Format(time.RFC3339)
		credential, err := chatgptwebauth.ParseCredential(auth.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		requestExpiry, requestKnown := chatGPTWebCredentialExpiry(credential)
		backgroundExpiry, backgroundKnown := auth.ExpirationTime()
		if !requestKnown || !backgroundKnown || !requestExpiry.Equal(backgroundExpiry) || !requestExpiry.Equal(now.Add(offset)) {
			t.Fatal("request and background expiry disagree")
		}
		if executor.ShouldPrepareRequestAuth(auth) != (offset <= 5*time.Minute) || executor.ShouldRefresh(now, auth) != (offset <= 5*time.Minute) {
			t.Fatal("refresh scheduling ignored access-token expiry")
		}
	}
}

func TestChatGPTWebFailedSessionRefreshTransitionsAfterBoundedAttempts(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, _ = w.Write([]byte(`{"error":"RefreshAccessTokenError","accessToken":"old-token"}`))
			}))
			defer server.Close()
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: enabled}}, nil)
			executor.authService = chatgptwebauth.NewService(chatgptwebauth.Options{SessionBaseURL: server.URL})
			t.Cleanup(func() { _ = executor.Close() })
			auth := chatGPTWebTestAuth("expired-session")
			auth.Metadata["refresh_strategy"] = string(chatgptwebauth.RefreshStrategyChatGPTSession)
			auth.Metadata["expired"] = time.Now().Add(-time.Hour).Format(time.RFC3339)
			auth.Metadata["last_refresh_at"] = "2026-09-01T00:00:00Z"
			auth.Metadata["cookies"] = []chatgptwebauth.Cookie{{Name: "next-auth.session-token", Value: "session", Host: strings.TrimPrefix(server.URL, "http://"), Path: "/"}}
			updated, err := executor.PrepareRequestAuth(t.Context(), auth)
			wantState := cliproxyauth.LifecycleStateReauthRequired
			if enabled {
				wantState = cliproxyauth.LifecycleStateReloginPending
			}
			if err == nil || !persistAuthUpdateForExecutorError(err) || updated == nil || updated.LifecycleState() != wantState || updated.LifecycleSelectable() {
				t.Fatalf("state=%v err=%v", updated, err)
			}
			if requests.Load() != 3 || updated.Metadata["last_refresh_at"] != auth.Metadata["last_refresh_at"] || updated.Metadata["lifecycle_reason"] != "session_refresh_failed" {
				t.Fatal("ineffective refresh became successful or exceeded its attempt limit")
			}
		})
	}
}

func TestChatGPTWebRevokedTokenUsesConfiguredRelogin(t *testing.T) {
	for _, tc := range []struct {
		enabled   bool
		materials bool
		want      string
	}{
		{true, true, cliproxyauth.LifecycleStateReloginPending},
		{false, true, cliproxyauth.LifecycleStateReauthRequired},
		{true, false, cliproxyauth.LifecycleStateReauthRequired},
	} {
		executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: tc.enabled}}, nil)
		t.Cleanup(func() { _ = executor.Close() })
		auth := chatGPTWebTestAuth("revoked")
		if !tc.materials {
			delete(auth.Metadata, "password")
			delete(auth.Metadata, "totp_secret")
		}
		updated, err := executor.RejectRevokedAccessToken(auth)
		if err == nil || !persistAuthUpdateForExecutorError(err) || updated.LifecycleState() != tc.want || updated.LifecycleSelectable() || updated.Metadata["lifecycle_reason"] != "token_revoked" {
			t.Fatalf("revocation state=%s error=%v", updated.LifecycleState(), err)
		}
	}
}

func TestChatGPTWebModelRefreshRevocationRecoversWithoutWaiting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			_, _ = w.Write([]byte("<html></html>"))
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":"token_revoked"}}`))
	}))
	defer server.Close()
	manager := cliproxyauth.NewManager(nil, nil, nil)
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.runtimeBaseURL = server.URL
	service := &fakeChatGPTWebAuthService{}
	executor.authService = service
	manager.RegisterExecutor(executor)
	t.Cleanup(func() { _ = manager.CloseExecutors() })
	auth, err := manager.Register(t.Context(), chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.FetchModels(t.Context(), auth)
	if statusCodeFromError(err) != http.StatusUnauthorized {
		t.Fatalf("FetchModels error = %v", err)
	}
	waitForChatGPTWebCondition(t, 5*time.Second, func() bool {
		current, _ := manager.GetByID(auth.ID)
		return current.LifecycleState() == cliproxyauth.LifecycleStateReauthRequired
	})
	current, _ := manager.GetByID(auth.ID)
	if current.LifecycleSelectable() || current.Metadata["lifecycle_reason"] != "token_revoked" || service.refreshCalls.Load() != 0 || service.refreshSessionCalls.Load() != 0 {
		t.Fatal("model refresh did not quarantine the revoked token directly")
	}
}

func TestChatGPTWebAccountInfoRevocationDoesNotRefreshTheRevokedToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"code":"token_revoked"}}`))
	}))
	defer server.Close()
	manager := cliproxyauth.NewManager(nil, nil, nil)
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.runtimeBaseURL = server.URL
	service := &fakeChatGPTWebAuthService{}
	executor.authService = service
	manager.RegisterExecutor(executor)
	t.Cleanup(func() { _ = manager.CloseExecutors() })
	auth, err := manager.Register(t.Context(), chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	executor.refreshChatGPTWebAccountInfo(t.Context(), auth.ID, true)
	waitForChatGPTWebCondition(t, 5*time.Second, func() bool {
		current, _ := manager.GetByID(auth.ID)
		return current.LifecycleState() == cliproxyauth.LifecycleStateReauthRequired
	})
	current, _ := manager.GetByID(auth.ID)
	if current.Metadata["lifecycle_reason"] != "token_revoked" || service.refreshCalls.Load() != 0 || service.refreshSessionCalls.Load() != 0 {
		t.Fatal("account-info refresh ignored the explicit revocation")
	}
}
