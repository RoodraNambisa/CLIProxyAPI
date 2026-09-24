package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebExpiredTokenCannotReachRuntimeEndpoints(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	manager := cliproxyauth.NewManager(nil, nil, nil)
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.runtimeBaseURL = server.URL
	t.Cleanup(func() { _ = executor.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	executor.now = func() time.Time { return now }
	auth := chatGPTWebRuntimeAuth()
	auth.Metadata["access_token"] = sessionRefreshTestJWT(now)
	// Session expiry must not mask an already-expired access token.
	auth.Metadata["expired"] = now.Add(90 * 24 * time.Hour).Format(time.RFC3339)
	_, modelErr := executor.FetchModels(t.Context(), auth)
	_, _, profileErr, quotaErr, _, _, _, _ := executor.fetchChatGPTWebAccountInfo(t.Context(), auth)
	_, _, runtimeErr := executor.newRuntimeClientForRequest(t.Context(), auth)
	for _, err := range []error{modelErr, profileErr, quotaErr, runtimeErr} {
		var expired *chatgptwebauth.AccessTokenExpiredError
		if !errors.As(err, &expired) {
			t.Fatalf("expiry rejection missing: %v", err)
		}
		if code, retryable := classifyChatGPTWebAccountInfoError(err); code != "access_token_expired" || retryable {
			t.Fatalf("expiry classified as upstream failure: %s/%t", code, retryable)
		}
	}
	if requests.Load() != 0 {
		t.Fatal("expired token reached an upstream endpoint")
	}
	if auth.Disabled || auth.Metadata["lifecycle_state"] != "active" {
		t.Fatal("local guard disabled recovery")
	}
}

func TestChatGPTWebRuntimeClientGuardsLaterExpiryAndCommittedRetry(t *testing.T) {
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	executor.now = func() time.Time { return now }
	auth := chatGPTWebRuntimeAuth()
	auth.Metadata["access_token"] = sessionRefreshTestJWT(now.Add(time.Second))
	client, credential, err := executor.newRuntimeClient(auth)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	now = now.Add(time.Second)
	_, _, err = client.DoNoRedirect(t.Context(), http.MethodGet, "http://127.0.0.1:1/backend-api/files", executor.chatGPTWebHeaders(credential, "/backend-api/files", nil), nil)
	var expired *chatgptwebauth.AccessTokenExpiredError
	if !errors.As(err, &expired) {
		t.Fatalf("runtime did not bind the expiry guard: %v", err)
	}
	diagnostic := helps.ClassifyChatGPTWebTransportDiagnostic(err, "https://chatgpt.com/backend-api/files")
	if diagnostic.Code != "access_token_expired" || diagnostic.HTTPStatus != 0 || diagnostic.Stage != "credential_preparation" {
		t.Fatalf("local expiry misreported: %+v", diagnostic)
	}
	committed := chatGPTWebCommittedRequestError(t.Context(), err)
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(committed, &retry) || retry.RetryOtherAuth() {
		t.Fatal("expired token allowed a committed image request to be replayed")
	}
}

func TestChatGPTWebExpiredRefreshResultCannotRestoreSelection(t *testing.T) {
	for _, strategy := range []chatgptwebauth.RefreshStrategy{chatgptwebauth.RefreshStrategyWebOAuthRT, chatgptwebauth.RefreshStrategyChatGPTSession} {
		t.Run(string(strategy), func(t *testing.T) {
			executor := NewChatGPTWebExecutor(&config.Config{}, nil)
			t.Cleanup(func() { _ = executor.Close() })
			now := time.Now().UTC().Truncate(time.Second)
			executor.now = func() time.Time { return now }
			auth := chatGPTWebTestAuth("expired-refresh-result")
			auth.Metadata["refresh_strategy"] = string(strategy)
			auth.Metadata["access_token"] = sessionRefreshTestJWT(now.Add(-time.Hour))
			auth.Metadata["last_refresh_at"] = now.Add(-24 * time.Hour).Format(time.RFC3339)
			refresh := func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
				credential.RefreshToken = "rotated-refresh-token"
				credential.LastRefreshAt = now.Format(time.RFC3339)
				return &credential, nil
			}
			executor.authService = &fakeChatGPTWebAuthService{refreshFn: refresh, refreshSessionFn: refresh}
			updated, err, terminal := executor.refreshCredential(t.Context(), auth, true)
			if err == nil || !terminal || updated == nil || updated.LifecycleSelectable() {
				t.Fatalf("expired refresh reported success: err=%v terminal=%t", err, terminal)
			}
			if updated.Metadata["refresh_token"] != "rotated-refresh-token" || updated.Metadata["last_refresh_at"] != auth.Metadata["last_refresh_at"] {
				t.Fatal("lost rotated recovery material or advanced last-success time")
			}
		})
	}
}
