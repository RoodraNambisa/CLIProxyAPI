package xai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/singleflight"
)

func resetXAIRefreshGroupForTest() {
	xaiRefreshGroup = singleflight.Group{}
}

func allowTestXAIEndpoint(rawURL, _ string) (string, error) {
	return strings.TrimSpace(rawURL), nil
}

func TestValidateOAuthEndpoint(t *testing.T) {
	if _, err := ValidateOAuthEndpoint("https://auth.x.ai/oauth2/token", "token_endpoint"); err != nil {
		t.Fatalf("ValidateOAuthEndpoint(xai) error = %v", err)
	}
	if _, err := ValidateOAuthEndpoint("http://auth.x.ai/oauth2/token", "token_endpoint"); err == nil {
		t.Fatal("expected non-HTTPS endpoint to be rejected")
	}
	if _, err := ValidateOAuthEndpoint("https://evil.example/oauth/token", "token_endpoint"); err == nil {
		t.Fatal("expected non-xAI endpoint to be rejected")
	}
}

func TestRequestDeviceCodePostsClientIDAndScope(t *testing.T) {
	var gotForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm() error = %v", err)
		}
		gotForm = r.PostForm
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code":               "device-abc",
			"user_code":                 "ABCD-1234",
			"verification_uri":          "https://accounts.x.ai/oauth2/device",
			"verification_uri_complete": "https://accounts.x.ai/oauth2/device?user_code=ABCD-1234",
			"expires_in":                1800,
			"interval":                  5,
		})
	}))
	defer server.Close()

	auth := NewXAIAuth(nil)
	deviceCode, err := auth.RequestDeviceCode(context.Background(), server.URL, "https://auth.x.ai/oauth2/token")
	if err != nil {
		t.Fatalf("RequestDeviceCode() error = %v", err)
	}
	if deviceCode.DeviceCode != "device-abc" || deviceCode.UserCode != "ABCD-1234" {
		t.Fatalf("unexpected device response: %#v", deviceCode)
	}
	if gotForm.Get("client_id") != ClientID {
		t.Fatalf("client_id = %q, want %q", gotForm.Get("client_id"), ClientID)
	}
	if gotForm.Get("scope") != Scope {
		t.Fatalf("scope = %q, want %q", gotForm.Get("scope"), Scope)
	}
}

func TestPollForTokenExchangesDeviceCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm() error = %v", err)
		}
		if got := r.PostForm.Get("grant_type"); got != DeviceCodeGrantType {
			t.Fatalf("grant_type = %q, want %q", got, DeviceCodeGrantType)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "access-1",
			"refresh_token": "refresh-1",
			"token_type":    "Bearer",
			"expires_in":    3600,
			"id_token":      fakeJWTWithIdentity("user@x.ai", "sub-1"),
		})
	}))
	defer server.Close()

	auth := NewXAIAuth(nil)
	tokenData, err := auth.PollForToken(context.Background(), &DeviceCodeResponse{
		DeviceCode:    "device-abc",
		UserCode:      "ABCD-1234",
		ExpiresIn:     60,
		TokenEndpoint: server.URL,
	})
	if err != nil {
		t.Fatalf("PollForToken() error = %v", err)
	}
	if tokenData.AccessToken != "access-1" || tokenData.RefreshToken != "refresh-1" {
		t.Fatalf("unexpected token data: %#v", tokenData)
	}
	if tokenData.Email != "user@x.ai" || tokenData.Subject != "sub-1" {
		t.Fatalf("unexpected identity: %#v", tokenData)
	}
}

func TestPollForTokenHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	auth := NewXAIAuth(nil)
	_, err := auth.PollForToken(ctx, &DeviceCodeResponse{
		DeviceCode:    "device-abc",
		UserCode:      "ABCD-1234",
		TokenEndpoint: "https://auth.x.ai/oauth2/token",
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "context cancel") {
		t.Fatalf("PollForToken() error = %v, want context cancellation", err)
	}
}

func TestRefreshTokensPostsClientIDAndRefreshToken(t *testing.T) {
	resetXAIRefreshGroupForTest()
	t.Cleanup(resetXAIRefreshGroupForTest)

	var gotForm url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm() error = %v", err)
		}
		gotForm = r.PostForm
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "new-access",
			"refresh_token": "new-refresh",
			"token_type":    "Bearer",
			"expires_in":    3600,
		})
	}))
	defer server.Close()

	auth := NewXAIAuth(nil)
	auth.validateOAuthEndpoint = allowTestXAIEndpoint
	tokenData, err := auth.RefreshTokens(context.Background(), "old-refresh", server.URL)
	if err != nil {
		t.Fatalf("RefreshTokens() error = %v", err)
	}
	if tokenData.AccessToken != "new-access" {
		t.Fatalf("access token = %q, want new-access", tokenData.AccessToken)
	}
	if gotForm.Get("grant_type") != "refresh_token" || gotForm.Get("client_id") != ClientID || gotForm.Get("refresh_token") != "old-refresh" {
		t.Fatalf("unexpected refresh form: %v", gotForm)
	}
}

func TestRefreshTokensDeduplicatesConcurrentRefresh(t *testing.T) {
	resetXAIRefreshGroupForTest()
	t.Cleanup(resetXAIRefreshGroupForTest)

	var calls int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		once.Do(func() { close(started) })
		<-release
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "new-access",
			"refresh_token": "new-refresh",
			"expires_in":    3600,
		})
	}))
	defer server.Close()

	results := make(chan *TokenData, 2)
	errs := make(chan error, 2)
	runRefresh := func(auth *XAIAuth) {
		auth.validateOAuthEndpoint = allowTestXAIEndpoint
		tokenData, errRefresh := auth.RefreshTokens(context.Background(), "shared-refresh-token", server.URL)
		results <- tokenData
		errs <- errRefresh
	}

	go runRefresh(NewXAIAuth(nil))
	<-started
	go runRefresh(NewXAIAuth(nil))
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("concurrent refresh calls = %d, want 1", got)
	}
	close(release)

	for i := 0; i < 2; i++ {
		if errRefresh := <-errs; errRefresh != nil {
			t.Fatalf("RefreshTokens() error = %v", errRefresh)
		}
		if tokenData := <-results; tokenData == nil || tokenData.AccessToken != "new-access" {
			t.Fatalf("unexpected token data: %#v", tokenData)
		}
	}
}

func TestRefreshTokensRejectsUntrustedEndpoint(t *testing.T) {
	resetXAIRefreshGroupForTest()
	t.Cleanup(resetXAIRefreshGroupForTest)

	auth := NewXAIAuth(nil)
	_, err := auth.RefreshTokens(context.Background(), "refresh-secret", "https://evil.example/oauth/token")
	if err == nil || !strings.Contains(err.Error(), "not on x.ai") {
		t.Fatalf("RefreshTokens() error = %v, want untrusted endpoint rejection", err)
	}
}

func TestCreateTokenStorageAndCredentialFileName(t *testing.T) {
	auth := NewXAIAuth(nil)
	storage := auth.CreateTokenStorage(&AuthBundle{
		TokenData: TokenData{AccessToken: "access", Email: "user@example.com", Subject: "sub-1"},
	})
	if storage == nil || storage.BaseURL != DefaultAPIBaseURL || storage.UsingAPI || storage.Websockets {
		t.Fatalf("unexpected token storage defaults: %#v", storage)
	}
	if got := CredentialFileName("user@example.com", "sub-1"); got != "xai-user@example.com.json" {
		t.Fatalf("CredentialFileName() = %q", got)
	}
	if got := CredentialFileName("", "sub:/1"); got != "xai-sub--1.json" {
		t.Fatalf("CredentialFileName(subject) = %q", got)
	}

	path := t.TempDir() + "/xai.json"
	if err := storage.SaveTokenToFile(path); err != nil {
		t.Fatalf("SaveTokenToFile() error = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	var persisted map[string]any
	if err = json.Unmarshal(raw, &persisted); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got, ok := persisted["using_api"].(bool); !ok || got {
		t.Fatalf("using_api = %#v, want false", persisted["using_api"])
	}
	if got, ok := persisted["websockets"].(bool); !ok || got {
		t.Fatalf("websockets = %#v, want false", persisted["websockets"])
	}
}

func fakeJWTWithIdentity(email, subject string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"email":"` + email + `","sub":"` + subject + `"}`))
	return header + "." + payload + ".sig"
}
