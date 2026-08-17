package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type fakeChatGPTWebAuthService struct {
	loginFn                   func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error)
	loginAcquisitionTimeoutFn func(chatgptwebauth.LoginInput) time.Duration
	refreshFn                 func(context.Context, chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error)
	refreshSessionFn          func(context.Context, chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error)
	loginCalls                atomic.Int32
	refreshCalls              atomic.Int32
	refreshSessionCalls       atomic.Int32
}

type chatGPTWebReloginSourceHashStore struct{}

func (*chatGPTWebReloginSourceHashStore) List(context.Context) ([]*cliproxyauth.Auth, error) {
	return nil, nil
}

func (*chatGPTWebReloginSourceHashStore) Save(_ context.Context, auth *cliproxyauth.Auth) (string, error) {
	return "", cliproxyauth.SetCanonicalSourceHashAttribute(auth)
}

func (*chatGPTWebReloginSourceHashStore) Delete(context.Context, string) error {
	return nil
}

type chatGPTWebLeaseResolver struct {
	active atomic.Int32
}

type linkedCodexRuntimeExecutor struct {
	refreshCalls atomic.Int32
}

func (*linkedCodexRuntimeExecutor) Identifier() string { return "codex" }

func (*linkedCodexRuntimeExecutor) Execute(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("unexpected codex execution")
}

func (*linkedCodexRuntimeExecutor) ExecuteStream(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, errors.New("unexpected codex stream execution")
}

func (executor *linkedCodexRuntimeExecutor) Refresh(_ context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	executor.refreshCalls.Add(1)
	updated := auth.Clone()
	updated.Metadata["access_token"] = "source-new"
	updated.Metadata["expired"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	return updated, nil
}

func (*linkedCodexRuntimeExecutor) CountTokens(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("unexpected codex token count")
}

func (*linkedCodexRuntimeExecutor) HttpRequest(context.Context, *cliproxyauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected codex HTTP request")
}

type linkedChatGPTWebRuntimeExecutor struct {
	*ChatGPTWebExecutor
	unauthorizedOnce bool
	executeCalls     atomic.Int32
}

func (executor *linkedChatGPTWebRuntimeExecutor) Execute(_ context.Context, auth *cliproxyauth.Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	call := executor.executeCalls.Add(1)
	if executor.unauthorizedOnce && call == 1 {
		return cliproxyexecutor.Response{}, &cliproxyauth.Error{
			Code:       "unauthorized",
			Message:    "access token expired",
			HTTPStatus: http.StatusUnauthorized,
		}
	}
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		return cliproxyexecutor.Response{}, errCredential
	}
	return cliproxyexecutor.Response{Payload: []byte(credential.AccessToken)}, nil
}

func (resolver *chatGPTWebLeaseResolver) Resolve(context.Context, *cliproxyauth.Auth) (cliproxyauth.ResolvedProxy, error) {
	if resolver.active.Load() == 0 {
		return cliproxyauth.ResolvedProxy{}, errors.New("proxy binding lease is not active")
	}
	return cliproxyauth.ResolvedProxy{URL: "http://proxy.example:8080", BindingID: "binding-a"}, nil
}

func (*chatGPTWebLeaseResolver) ReportFailure(_ context.Context, _ *cliproxyauth.Auth, err error) error {
	return err
}

func (resolver *chatGPTWebLeaseResolver) HoldBinding(string) func() {
	resolver.active.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() { resolver.active.Add(-1) })
	}
}

func (service *fakeChatGPTWebAuthService) Login(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
	service.loginCalls.Add(1)
	if service.loginFn == nil {
		return nil, errors.New("unexpected login")
	}
	return service.loginFn(ctx, input)
}

func (service *fakeChatGPTWebAuthService) LoginAcquisitionTimeout(input chatgptwebauth.LoginInput) time.Duration {
	if service.loginAcquisitionTimeoutFn != nil {
		return service.loginAcquisitionTimeoutFn(input)
	}
	return chatgptwebauth.DefaultAcquisitionTimeout
}

func (service *fakeChatGPTWebAuthService) Refresh(ctx context.Context, credential chatgptwebauth.Credential, proxyURL string) (*chatgptwebauth.Credential, error) {
	service.refreshCalls.Add(1)
	if service.refreshFn == nil {
		return nil, errors.New("unexpected refresh")
	}
	return service.refreshFn(ctx, credential, proxyURL)
}

func (service *fakeChatGPTWebAuthService) RefreshSession(ctx context.Context, credential chatgptwebauth.Credential, proxyURL string) (*chatgptwebauth.Credential, error) {
	service.refreshSessionCalls.Add(1)
	if service.refreshSessionFn == nil {
		return nil, errors.New("unexpected session refresh")
	}
	return service.refreshSessionFn(ctx, credential, proxyURL)
}

func TestChatGPTWebExecutorAppliesDedicatedProxyOnlyToLogin(t *testing.T) {
	rotate := true
	requestAttempts := 4
	flowAttempts := 3
	retryDelay := 250
	timeout := 120
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{API798AutoLoginEnabled: true, LoginProxy: config.ChatGPTWebLoginProxyConfig{
		Enabled:                   true,
		URLTemplate:               "http://session-{8}:secret@proxy.example:8080",
		PlaceholderCharset:        "abc123",
		RotateOnRetry:             &rotate,
		RequestAttempts:           &requestAttempts,
		FlowAttempts:              &flowAttempts,
		RetryDelayMilliseconds:    &retryDelay,
		AcquisitionTimeoutSeconds: &timeout,
	}}}
	var receivedLogin chatgptwebauth.LoginInput
	var receivedRefreshProxy string
	fake := &fakeChatGPTWebAuthService{
		loginFn: func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
			receivedLogin = input
			return &chatgptwebauth.Credential{LifecycleState: chatgptwebauth.LifecycleActive}, nil
		},
		refreshFn: func(_ context.Context, credential chatgptwebauth.Credential, proxyURL string) (*chatgptwebauth.Credential, error) {
			receivedRefreshProxy = proxyURL
			credential.AccessToken = "refreshed"
			credential.LifecycleState = chatgptwebauth.LifecycleActive
			return &credential, nil
		},
	}
	executor := NewChatGPTWebExecutor(cfg, nil)
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Fatal(errClose)
		}
	}()
	executor.authService = fake
	if _, errLogin := executor.Login(t.Context(), chatgptwebauth.LoginInput{
		Email:    "person@example.com",
		Password: "secret",
		ProxyURL: "http://normal-proxy.example:8080",
	}); errLogin != nil {
		t.Fatalf("Login() error = %v", errLogin)
	}
	if receivedLogin.ProxyURL != "" || !receivedLogin.LoginProxy.Enabled || !receivedLogin.AllowAutoAPI798 ||
		receivedLogin.BeginSentinelObserver == nil ||
		receivedLogin.LoginProxy.URLTemplate != cfg.ChatGPTWeb.LoginProxy.URLTemplate ||
		receivedLogin.LoginProxy.RequestAttempts != requestAttempts ||
		receivedLogin.LoginProxy.FlowAttempts != flowAttempts ||
		receivedLogin.LoginProxy.RetryDelay != 250*time.Millisecond ||
		receivedLogin.LoginProxy.AcquisitionTimeout != 120*time.Second {
		t.Fatalf("login input = %#v", receivedLogin)
	}

	auth := chatGPTWebTestAuth("login-proxy-refresh")
	auth.ProxyURL = "http://credential-proxy.example:8080"
	if _, errRefresh := executor.Refresh(t.Context(), auth); errRefresh != nil {
		t.Fatalf("Refresh() error = %v", errRefresh)
	}
	if receivedRefreshProxy != auth.ProxyURL {
		t.Fatalf("refresh proxy = %q, want existing credential proxy %q", receivedRefreshProxy, auth.ProxyURL)
	}
}

func TestChatGPTWebExecutorRefreshUsesActualTargetForEnvironmentProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("http_proxy", "")
	t.Setenv("HTTPS_PROXY", "http://proxy.example:8080")
	t.Setenv("https_proxy", "")
	t.Setenv("ALL_PROXY", "")
	t.Setenv("all_proxy", "")
	t.Setenv("NO_PROXY", "chatgpt.com")
	t.Setenv("no_proxy", "")

	var oauthProxy, sessionProxy string
	service := &fakeChatGPTWebAuthService{
		refreshFn: func(_ context.Context, credential chatgptwebauth.Credential, proxyURL string) (*chatgptwebauth.Credential, error) {
			oauthProxy = proxyURL
			return &credential, nil
		},
		refreshSessionFn: func(_ context.Context, credential chatgptwebauth.Credential, proxyURL string) (*chatgptwebauth.Credential, error) {
			sessionProxy = proxyURL
			return &credential, nil
		},
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = service
	auth := chatGPTWebTestAuth("environment-proxy-target")
	credential := &chatgptwebauth.Credential{RefreshStrategy: chatgptwebauth.RefreshStrategyWebOAuthRT}
	if _, errRefresh, _ := executor.refreshByStrategy(t.Context(), auth, credential); errRefresh != nil {
		t.Fatal(errRefresh)
	}
	credential.RefreshStrategy = chatgptwebauth.RefreshStrategyChatGPTSession
	if _, errRefresh, _ := executor.refreshByStrategy(t.Context(), auth, credential); errRefresh != nil {
		t.Fatal(errRefresh)
	}
	if oauthProxy != "http://proxy.example:8080" {
		t.Fatalf("OAuth proxy = %q, want environment proxy", oauthProxy)
	}
	if sessionProxy != "" {
		t.Fatalf("session proxy = %q, want NO_PROXY bypass", sessionProxy)
	}
}

func TestChatGPTWebExecutorImportSessionRefreshCanSkipValidAccessToken(t *testing.T) {
	now := time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)
	forceRefresh := false
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		ForceSessionRefreshOnImport: &forceRefresh,
	}}
	service := &fakeChatGPTWebAuthService{
		refreshSessionFn: func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
			credential.AccessToken = "refreshed-access"
			credential.Expired = now.Add(time.Hour).Format(time.RFC3339)
			return &credential, nil
		},
	}
	executor := NewChatGPTWebExecutor(cfg, nil)
	t.Cleanup(func() { _ = executor.Close() })
	executor.authService = service
	executor.now = func() time.Time { return now }

	valid := &chatgptwebauth.Credential{
		AccessToken:     "uploaded-access",
		Expired:         now.Add(time.Hour).Format(time.RFC3339),
		RefreshStrategy: chatgptwebauth.RefreshStrategyChatGPTSession,
	}
	normalized, errNormalize := executor.NormalizeImportedCredential(t.Context(), valid, "")
	if errNormalize != nil {
		t.Fatalf("NormalizeImportedCredential(valid) error = %v", errNormalize)
	}
	if normalized.AccessToken != "uploaded-access" || service.refreshSessionCalls.Load() != 0 {
		t.Fatalf("valid import = %+v calls=%d", normalized, service.refreshSessionCalls.Load())
	}

	expired := cloneChatGPTWebCredential(valid)
	expired.Expired = now.Add(-time.Second).Format(time.RFC3339)
	normalized, errNormalize = executor.NormalizeImportedCredential(t.Context(), expired, "")
	if errNormalize != nil {
		t.Fatalf("NormalizeImportedCredential(expired) error = %v", errNormalize)
	}
	if normalized.AccessToken != "refreshed-access" || service.refreshSessionCalls.Load() != 1 {
		t.Fatalf("expired import = %+v calls=%d", normalized, service.refreshSessionCalls.Load())
	}
}

func TestChatGPTWebExecutorImportSessionRefreshDefaultsToForced(t *testing.T) {
	service := &fakeChatGPTWebAuthService{
		refreshSessionFn: func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
			credential.AccessToken = "refreshed-access"
			return &credential, nil
		},
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	executor.authService = service

	credential := &chatgptwebauth.Credential{
		AccessToken:     "uploaded-access",
		Expired:         time.Now().Add(time.Hour).Format(time.RFC3339),
		RefreshStrategy: chatgptwebauth.RefreshStrategyChatGPTSession,
	}
	normalized, errNormalize := executor.NormalizeImportedCredential(t.Context(), credential, "")
	if errNormalize != nil {
		t.Fatalf("NormalizeImportedCredential() error = %v", errNormalize)
	}
	if normalized.AccessToken != "refreshed-access" || service.refreshSessionCalls.Load() != 1 {
		t.Fatalf("default import = %+v calls=%d", normalized, service.refreshSessionCalls.Load())
	}
}

func TestChatGPTWebExecutorShouldPrepareExpiringCredential(t *testing.T) {
	now := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.now = func() time.Time { return now }

	auth := chatGPTWebTestAuth("prepare")
	auth.Metadata["expired"] = now.Add(chatgptwebauth.DefaultRefreshLead).Format(time.RFC3339)
	if !executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("credential at refresh lead boundary should be prepared")
	}
	auth.Metadata["expired"] = now.Add(chatgptwebauth.DefaultRefreshLead + time.Second).Format(time.RFC3339)
	if executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("credential outside refresh lead should not be prepared")
	}
	auth.Metadata["expired"] = "invalid"
	if !executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("malformed explicit expiry should fail closed")
	}
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateDead
	if executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("dead credential should not be refreshed")
	}
}

func TestChatGPTWebExecutorShouldRefreshTokenOnlyAtExpiry(t *testing.T) {
	now := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	auth := chatGPTWebTestAuth("token-only-refresh")
	auth.Metadata["refresh_strategy"] = string(chatgptwebauth.RefreshStrategyTokenOnly)
	auth.Metadata["refresh_token"] = ""
	auth.Metadata["expired"] = now.Add(time.Minute).Format(time.RFC3339)
	if executor.ShouldRefresh(now, auth) {
		t.Fatal("token-only credential was refreshed before expiry")
	}
	auth.Metadata["expired"] = now.Format(time.RFC3339)
	if !executor.ShouldRefresh(now, auth) {
		t.Fatal("expired token-only credential was not refreshed")
	}
	auth.Metadata["expired"] = ""
	auth.Metadata["access_token"] = "opaque-token"
	if executor.ShouldRefresh(now, auth) {
		t.Fatal("opaque token-only credential without expiry was refreshed")
	}
}

func TestChatGPTWebExecutorPersistsMissingCodexSourceAsReauthRequired(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	t.Cleanup(func() { _ = executor.Close() })
	auth := chatGPTWebTestAuth("missing-source")
	credential, errParse := chatgptwebauth.ParseCredential(auth.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	credential.RefreshStrategy = chatgptwebauth.RefreshStrategyCodexSource
	credential.SourceAuthID = "missing-codex.json"
	credential.SourceCredentialUID = "missing-uid"
	credential.SourceIdentity = "v1:account:user:subject:identity"
	credential.RefreshToken = ""
	credential.ApplyToMetadata(auth.Metadata)

	updated, errRefresh := executor.Refresh(t.Context(), auth)
	if errRefresh == nil || updated == nil {
		t.Fatalf("Refresh() = %#v, %v", updated, errRefresh)
	}
	refreshed, errParse := chatgptwebauth.ParseCredential(updated.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	if refreshed.LifecycleState != chatgptwebauth.LifecycleReauthRequired || refreshed.LifecycleReason != "source_auth_missing" {
		t.Fatalf("credential state = %q/%q", refreshed.LifecycleState, refreshed.LifecycleReason)
	}
}

func TestChatGPTWebExecutorLinkedCodexRequestPreparationPreservesRefreshLocks(t *testing.T) {
	manager, codexExecutor, webExecutor, web, model := newLinkedChatGPTWebRuntime(t, true, false)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	response, errExecute := manager.Execute(
		ctx,
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: model, Payload: []byte(`{"model":"gpt-5","input":"hello"}`)},
		cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex},
	)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	if got := string(response.Payload); got != "source-new" {
		t.Fatalf("response token = %q, want source-new", got)
	}
	if got := codexExecutor.refreshCalls.Load(); got != 1 {
		t.Fatalf("codex refresh calls = %d, want 1", got)
	}
	if got := webExecutor.executeCalls.Load(); got != 1 {
		t.Fatalf("web execute calls = %d, want 1", got)
	}
	current, ok := manager.GetByID(web.ID)
	if !ok || current == nil || current.Metadata["access_token"] != "source-new" {
		t.Fatalf("current Web credential = %#v, want refreshed source token", current)
	}
}

func TestChatGPTWebExecutorLinkedCodexUnauthorizedBackgroundRefreshPreservesRefreshLocks(t *testing.T) {
	manager, codexExecutor, webExecutor, web, model := newLinkedChatGPTWebRuntime(t, false, true)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, errExecute := manager.Execute(
		ctx,
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: model, Payload: []byte(`{"model":"gpt-5","input":"hello"}`)},
		cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex},
	)
	var statusErr cliproxyexecutor.StatusError
	if !errors.As(errExecute, &statusErr) || statusErr.StatusCode() != http.StatusUnauthorized {
		t.Fatalf("first request error = %v, want original 401", errExecute)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		current, ok := manager.GetByID(web.ID)
		if ok && current != nil && current.Metadata["access_token"] == "source-new" && !current.Unavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background refresh did not install linked source token: %#v", current)
		}
		time.Sleep(time.Millisecond)
	}

	response, errExecute := manager.Execute(
		ctx,
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: model, Payload: []byte(`{"model":"gpt-5","input":"hello"}`)},
		cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex},
	)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	if got := string(response.Payload); got != "source-new" {
		t.Fatalf("second response token = %q, want source-new", got)
	}
	if got := codexExecutor.refreshCalls.Load(); got != 1 {
		t.Fatalf("codex refresh calls = %d, want 1", got)
	}
	if got := webExecutor.executeCalls.Load(); got != 2 {
		t.Fatalf("web execute calls = %d, want 2", got)
	}
}

func TestChatGPTWebExecutorLinkedCodexBackgroundRefreshPreservesRefreshLocks(t *testing.T) {
	manager, codexExecutor, _, web, _ := newLinkedChatGPTWebRuntime(t, true, false)
	manager.StartAutoRefresh(t.Context(), 5*time.Millisecond)
	t.Cleanup(manager.StopAutoRefresh)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		current, ok := manager.GetByID(web.ID)
		if ok && current != nil && current.Metadata["access_token"] == "source-new" {
			if got := codexExecutor.refreshCalls.Load(); got != 1 {
				t.Fatalf("codex refresh calls = %d, want 1", got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	current, _ := manager.GetByID(web.ID)
	t.Fatalf("background refresh did not update linked Web credential: %#v", current)
}

func newLinkedChatGPTWebRuntime(
	t *testing.T,
	expired bool,
	unauthorizedOnce bool,
) (*cliproxyauth.Manager, *linkedCodexRuntimeExecutor, *linkedChatGPTWebRuntimeExecutor, *cliproxyauth.Auth, string) {
	t.Helper()
	manager := cliproxyauth.NewManager(nil, &cliproxyauth.FillFirstSelector{}, nil)
	codexExecutor := &linkedCodexRuntimeExecutor{}
	webExecutor := &linkedChatGPTWebRuntimeExecutor{
		ChatGPTWebExecutor: NewChatGPTWebExecutor(&config.Config{}, manager),
		unauthorizedOnce:   unauthorizedOnce,
	}
	probeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != chatgptwebauth.AccountCheckPath {
			http.NotFound(w, request)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"accounts":{"linked-account":{"account_id":"linked-account","plan_type":"plus"}}}`)
	}))
	t.Cleanup(probeServer.Close)
	webExecutor.runtimeBaseURL = probeServer.URL
	manager.RegisterExecutor(codexExecutor)
	manager.RegisterExecutor(webExecutor)
	t.Cleanup(func() {
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("close executors: %v", errClose)
		}
	})

	source, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), &cliproxyauth.Auth{
		ID:       "linked-codex-source.json",
		FileName: "linked-codex-source.json",
		Provider: "codex",
		Status:   cliproxyauth.StatusActive,
		Metadata: map[string]any{
			"type":           "codex",
			"credential_uid": "linked-source-uid",
			"account_id":     "linked-account",
			"user_id":        "linked-user",
			"email":          "linked@example.com",
			"access_token":   "source-old",
			"expired":        time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	identitySource := source.Clone()
	identitySource.Provider = chatgptwebauth.Provider
	expiresAt := time.Now().Add(time.Hour)
	if expired {
		expiresAt = time.Now().Add(-time.Minute)
	}
	web, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), &cliproxyauth.Auth{
		ID:       "linked-chatgpt-web.json",
		FileName: "linked-chatgpt-web.json",
		Provider: chatgptwebauth.Provider,
		Status:   cliproxyauth.StatusActive,
		Metadata: map[string]any{
			"type":                  chatgptwebauth.Provider,
			"email":                 "linked@example.com",
			"access_token":          "source-old",
			"expired":               expiresAt.UTC().Format(time.RFC3339),
			"refresh_strategy":      string(chatgptwebauth.RefreshStrategyCodexSource),
			"source_auth_id":        source.ID,
			"source_credential_uid": "linked-source-uid",
			"source_identity":       cliproxyauth.ChatGPTWebCredentialReferenceValue(identitySource),
			"lifecycle_state":       cliproxyauth.LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	model := "linked-chatgpt-web-model"
	registry.GetGlobalRegistry().RegisterClient(
		web.ID,
		web.Provider,
		[]*registry.ModelInfo{{ID: model}},
	)
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(web.ID) })
	return manager, codexExecutor, webExecutor, web, model
}

func TestChatGPTWebExecutorRefreshUsesStableLegacyRuntimeIdentity(t *testing.T) {
	auth := chatGPTWebTestAuth("legacy-identity")
	delete(auth.Metadata, "device_id")
	delete(auth.Metadata, "session_id")
	auth.Metadata["email"] = "person@example.com"
	expected, err := chatgptwebauth.ParseCredential(auth.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err = chatgptwebauth.EnsureCredentialRuntimeIDs(expected, chatgptwebauth.CredentialRuntimeIdentityReader(auth.ID, expected)); err != nil {
		t.Fatal(err)
	}

	var received chatgptwebauth.Credential
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		received = credential
		credential.AccessToken = "refreshed-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake

	updated, err := executor.Refresh(t.Context(), auth)
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if received.DeviceID != expected.DeviceID || received.SessionID != expected.SessionID {
		t.Fatalf("refresh identity = %q/%q, want %q/%q", received.DeviceID, received.SessionID, expected.DeviceID, expected.SessionID)
	}
	if updated.Metadata["device_id"] != expected.DeviceID || updated.Metadata["session_id"] != expected.SessionID {
		t.Fatalf("persisted identity = %v/%v", updated.Metadata["device_id"], updated.Metadata["session_id"])
	}
}

func TestChatGPTWebExecutorSuccessfulRefreshClearsImportSessionIntent(t *testing.T) {
	fake := &fakeChatGPTWebAuthService{
		refreshSessionFn: func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
			credential.AccessToken = "refreshed-token"
			credential.Expired = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			credential.LifecycleState = chatgptwebauth.LifecycleActive
			return &credential, nil
		},
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake
	auth := chatGPTWebTestAuth("import-session-intent")
	auth.Metadata["refresh_strategy"] = string(chatgptwebauth.RefreshStrategyChatGPTSession)
	auth.Metadata["import_session_refresh_pending"] = true
	auth.Metadata["cookies"] = []any{map[string]any{
		"name": "__Secure-next-auth.session-token", "value": "session-token",
		"domain": ".chatgpt.com", "path": "/", "secure": true, "http_only": true,
	}}

	updated, errRefresh := executor.Refresh(t.Context(), auth)
	if errRefresh != nil {
		t.Fatalf("Refresh() error = %v", errRefresh)
	}
	credential, errParse := chatgptwebauth.ParseCredential(updated.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	if credential.ImportSessionPending {
		t.Fatal("successful refresh retained import session intent")
	}
	if executor.ShouldRefresh(time.Now(), updated) {
		t.Fatal("successful refresh remained immediately eligible only because of the import intent")
	}
}

func TestChatGPTWebExecutorImportSessionIntentDoesNotBlockUsableAccessToken(t *testing.T) {
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	auth := chatGPTWebTestAuth("import-session-background")
	auth.Metadata["refresh_strategy"] = string(chatgptwebauth.RefreshStrategyChatGPTSession)
	auth.Metadata["import_session_refresh_pending"] = true
	auth.Metadata["expired"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)

	if executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("usable access token was synchronously blocked by background import maintenance")
	}
	if !executor.ShouldRefresh(time.Now(), auth) {
		t.Fatal("background import maintenance was not scheduled")
	}
}

func TestChatGPTWebExecutorTerminalRefreshLifecycle(t *testing.T) {
	for _, test := range []struct {
		name              string
		autoRelogin       bool
		withoutPassword   bool
		email             string
		api798URL         string
		loginMethod       chatgptwebauth.LoginMethod
		autoAPI798Enabled bool
		failure           chatgptwebauth.LifecycleState
		want              string
	}{
		{name: "reauth", failure: chatgptwebauth.LifecycleReauthRequired, want: cliproxyauth.LifecycleStateReauthRequired},
		{name: "auto relogin", autoRelogin: true, failure: chatgptwebauth.LifecycleReauthRequired, want: cliproxyauth.LifecycleStateReloginPending},
		{name: "imported credential cannot auto relogin", autoRelogin: true, withoutPassword: true, failure: chatgptwebauth.LifecycleReauthRequired, want: cliproxyauth.LifecycleStateReauthRequired},
		{
			name:            "explicit API798 can auto relogin",
			autoRelogin:     true,
			withoutPassword: true,
			email:           "explicit-api798@example.com",
			api798URL:       "https://api798.com/get_code?email=explicit-api798%40example.com&auth_code=opaque",
			loginMethod:     chatgptwebauth.LoginMethodAPI798,
			failure:         chatgptwebauth.LifecycleReauthRequired,
			want:            cliproxyauth.LifecycleStateReloginPending,
		},
		{
			name:              "automatic API798 fallback can auto relogin when enabled",
			autoRelogin:       true,
			withoutPassword:   true,
			email:             "auto-api798@example.com",
			api798URL:         "https://api798.com/get_code?email=auto-api798%40example.com&auth_code=opaque",
			autoAPI798Enabled: true,
			failure:           chatgptwebauth.LifecycleReauthRequired,
			want:              cliproxyauth.LifecycleStateReloginPending,
		},
		{
			name:            "automatic API798 fallback stays disabled by default",
			autoRelogin:     true,
			withoutPassword: true,
			email:           "disabled-api798@example.com",
			api798URL:       "https://api798.com/get_code?email=disabled-api798%40example.com&auth_code=opaque",
			failure:         chatgptwebauth.LifecycleReauthRequired,
			want:            cliproxyauth.LifecycleStateReauthRequired,
		},
		{name: "dead never relogins", autoRelogin: true, failure: chatgptwebauth.LifecycleDead, want: cliproxyauth.LifecycleStateDead},
		{name: "interaction never relogins", autoRelogin: true, failure: chatgptwebauth.LifecycleInteractionRequired, want: cliproxyauth.LifecycleStateInteractionRequired},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeChatGPTWebAuthService{}
			fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
				credential.LifecycleState = test.failure
				return &credential, &chatgptwebauth.AuthError{Code: "terminal_failure", State: test.failure, LifecycleState: test.failure, Terminal: true}
			}
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
				AutoRelogin:            test.autoRelogin,
				API798AutoLoginEnabled: test.autoAPI798Enabled,
			}}, nil)
			executor.authService = fake
			auth := chatGPTWebTestAuth(test.name)
			if test.withoutPassword {
				auth.Metadata["password"] = ""
				auth.Metadata["totp_secret"] = ""
			}
			if test.api798URL != "" {
				auth.Metadata["email"] = test.email
				auth.Metadata["api798_url"] = test.api798URL
				auth.Metadata["login_method"] = string(test.loginMethod)
			}
			updated, errRefresh := executor.Refresh(t.Context(), auth)
			if errRefresh == nil || updated == nil {
				t.Fatalf("Refresh() = (%v, %v), want persisted terminal update", updated, errRefresh)
			}
			if got := updated.LifecycleState(); got != test.want {
				t.Fatalf("lifecycle = %q, want %q", got, test.want)
			}
			persist, ok := errRefresh.(interface{ PersistAuthUpdateOnError() bool })
			if !ok || !persist.PersistAuthUpdateOnError() {
				t.Fatal("terminal refresh error must request lifecycle persistence")
			}
			if skipper, ok := errRefresh.(interface{ SkipAuthResult() bool }); !ok || !skipper.SkipAuthResult() {
				t.Fatal("terminal refresh must not record a credential failure")
			}
		})
	}
}

func TestChatGPTWebExecutorTransientRefreshDoesNotPersist(t *testing.T) {
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		return &credential, &chatgptwebauth.AuthError{Code: "network_error", State: chatgptwebauth.LifecycleActive, Retryable: true}
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake
	updated, errRefresh := executor.Refresh(t.Context(), chatGPTWebTestAuth("transient"))
	if updated != nil || errRefresh == nil {
		t.Fatalf("Refresh() = (%v, %v), want transient error without update", updated, errRefresh)
	}
	if persist, ok := errRefresh.(interface{ PersistAuthUpdateOnError() bool }); ok && persist.PersistAuthUpdateOnError() {
		t.Fatal("transient refresh error must not persist a lifecycle transition")
	}
}

func TestChatGPTWebExecutorSessionCookieFallbackEligibility(t *testing.T) {
	tests := []struct {
		name             string
		enabled          bool
		completePassword bool
		cookies          []chatgptwebauth.Cookie
		wantSessionCalls int32
		wantOAuthCalls   int32
	}{
		{
			name:             "disabled preserves configured refresh strategy",
			completePassword: false,
			cookies:          chatGPTWebCompleteSessionCookies(),
			wantOAuthCalls:   1,
		},
		{
			name:             "incomplete login uses session cookie",
			enabled:          true,
			completePassword: false,
			cookies:          chatGPTWebCompleteSessionCookies(),
			wantSessionCalls: 1,
		},
		{
			name:             "complete password login preserves oauth refresh",
			enabled:          true,
			completePassword: true,
			cookies:          chatGPTWebCompleteSessionCookies(),
			wantOAuthCalls:   1,
		},
		{
			name:             "incomplete cookie chunks do not enter session fallback",
			enabled:          true,
			completePassword: false,
			cookies: []chatgptwebauth.Cookie{{
				Name:   "__Secure-next-auth.session-token.0",
				Value:  "first-half",
				Domain: "chatgpt.com",
				Path:   "/",
				Secure: true,
			}},
			wantOAuthCalls: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeChatGPTWebAuthService{
				refreshFn: func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
					credential.AccessToken = "oauth-token"
					return &credential, nil
				},
				refreshSessionFn: func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
					credential.RefreshStrategy = chatgptwebauth.RefreshStrategyChatGPTSession
					credential.AccessToken = "session-token"
					return &credential, nil
				},
			}
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
				SessionCookieRefreshOnTokenFailure: test.enabled,
			}}, nil)
			t.Cleanup(func() { _ = executor.Close() })
			executor.authService = service
			auth := chatGPTWebTestAuth("session-eligibility-" + strings.ReplaceAll(test.name, " ", "-"))
			credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
			if errCredential != nil {
				t.Fatal(errCredential)
			}
			credential.Cookies = test.cookies
			if !test.completePassword {
				credential.TOTPSecret = ""
			}
			credential.RefreshStrategy = chatgptwebauth.RefreshStrategyWebOAuthRT
			credential.ApplyToMetadata(auth.Metadata)

			updated, errRefresh := executor.Refresh(t.Context(), auth)
			if errRefresh != nil || updated == nil {
				t.Fatalf("Refresh() = (%v, %v)", updated, errRefresh)
			}
			if got := service.refreshSessionCalls.Load(); got != test.wantSessionCalls {
				t.Fatalf("session refresh calls = %d, want %d", got, test.wantSessionCalls)
			}
			if got := service.refreshCalls.Load(); got != test.wantOAuthCalls {
				t.Fatalf("OAuth refresh calls = %d, want %d", got, test.wantOAuthCalls)
			}
		})
	}
}

func TestChatGPTWebExecutorSessionCookieFallbackPreservesCodexSource(t *testing.T) {
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		SessionCookieRefreshOnTokenFailure: true,
	}}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	credential := &chatgptwebauth.Credential{
		RefreshStrategy: chatgptwebauth.RefreshStrategyCodexSource,
		SourceAuthID:    "source-auth",
		Cookies:         chatGPTWebCompleteSessionCookies(),
	}
	if executor.sessionCookieRefreshEligible(credential) {
		t.Fatal("linked Codex source was replaced by Session Cookie fallback")
	}
}

func TestChatGPTWebExecutorSessionCookieFallbackLifecycle(t *testing.T) {
	tests := []struct {
		name         string
		errorCode    string
		state        chatgptwebauth.LifecycleState
		retryable    bool
		terminal     bool
		withPassword bool
		withPasskey  bool
		autoRelogin  bool
		wantUpdate   bool
		wantState    string
		wantPersist  bool
	}{
		{
			name:        "expired session without recovery is dead",
			errorCode:   "session_expired",
			state:       chatgptwebauth.LifecycleReauthRequired,
			terminal:    true,
			wantUpdate:  true,
			wantState:   cliproxyauth.LifecycleStateDead,
			wantPersist: true,
		},
		{
			name:         "expired session with password requires reauthentication",
			errorCode:    "session_expired",
			state:        chatgptwebauth.LifecycleReauthRequired,
			terminal:     true,
			withPassword: true,
			wantUpdate:   true,
			wantState:    cliproxyauth.LifecycleStateReauthRequired,
			wantPersist:  true,
		},
		{
			name:        "expired session with Passkey requires reauthentication",
			errorCode:   "session_expired",
			state:       chatgptwebauth.LifecycleReauthRequired,
			terminal:    true,
			withPasskey: true,
			wantUpdate:  true,
			wantState:   cliproxyauth.LifecycleStateReauthRequired,
			wantPersist: true,
		},
		{
			name:        "expired session with Passkey schedules automatic re-login",
			errorCode:   "session_expired",
			state:       chatgptwebauth.LifecycleReauthRequired,
			terminal:    true,
			withPasskey: true,
			autoRelogin: true,
			wantUpdate:  true,
			wantState:   cliproxyauth.LifecycleStateReloginPending,
			wantPersist: true,
		},
		{
			name:        "valid session without token is dead",
			errorCode:   "access_token_missing",
			state:       chatgptwebauth.LifecycleReauthRequired,
			terminal:    true,
			wantUpdate:  true,
			wantState:   cliproxyauth.LifecycleStateDead,
			wantPersist: true,
		},
		{
			name:      "network failure remains transient",
			errorCode: "session_refresh_network_error",
			state:     chatgptwebauth.LifecycleActive,
			retryable: true,
		},
		{
			name:      "Cloudflare challenge remains transient",
			errorCode: "cloudflare_challenge",
			state:     chatgptwebauth.LifecycleActive,
			retryable: true,
		},
		{
			name:        "identity conflict requires reauthentication but is not dead",
			errorCode:   "identity_conflict",
			state:       chatgptwebauth.LifecycleReauthRequired,
			terminal:    true,
			wantUpdate:  true,
			wantState:   cliproxyauth.LifecycleStateReauthRequired,
			wantPersist: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeChatGPTWebAuthService{
				refreshSessionFn: func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
					credential.RefreshStrategy = chatgptwebauth.RefreshStrategyChatGPTSession
					credential.LifecycleState = test.state
					return &credential, &chatgptwebauth.AuthError{
						Code:           test.errorCode,
						State:          test.state,
						LifecycleState: test.state,
						Retryable:      test.retryable,
						Terminal:       test.terminal,
					}
				},
			}
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
				SessionCookieRefreshOnTokenFailure: true,
				AutoRelogin:                        test.autoRelogin,
			}}, nil)
			t.Cleanup(func() { _ = executor.Close() })
			executor.authService = service
			authID := "session-lifecycle-" + strings.ReplaceAll(test.name, " ", "-")
			auth := chatGPTWebTestAuth(authID)
			if test.withPasskey {
				auth = chatGPTWebPasskeyTestAuth(t, authID, 0)
			}
			credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
			if errCredential != nil {
				t.Fatal(errCredential)
			}
			if !test.withPassword {
				credential.Password = ""
			}
			credential.TOTPSecret = ""
			credential.RefreshStrategy = chatgptwebauth.RefreshStrategyTokenOnly
			credential.Cookies = chatGPTWebCompleteSessionCookies()
			credential.ApplyToMetadata(auth.Metadata)

			updated, errRefresh := executor.Refresh(t.Context(), auth)
			if errRefresh == nil {
				t.Fatal("Refresh() error = nil")
			}
			if (updated != nil) != test.wantUpdate {
				t.Fatalf("Refresh() update = %v, want update %t", updated, test.wantUpdate)
			}
			if updated != nil && updated.LifecycleState() != test.wantState {
				t.Fatalf("lifecycle = %q, want %q", updated.LifecycleState(), test.wantState)
			}
			persist, okPersist := errRefresh.(interface{ PersistAuthUpdateOnError() bool })
			if got := okPersist && persist.PersistAuthUpdateOnError(); got != test.wantPersist {
				t.Fatalf("PersistAuthUpdateOnError() = %t, want %t", got, test.wantPersist)
			}
		})
	}
}

func TestClassifyChatGPTWebSessionCookieRefreshRecognizesAPI798Recovery(t *testing.T) {
	const api798URL = "https://api798.com/get_code?email=api798-recovery%40example.com&auth_code=opaque"
	for _, test := range []struct {
		name            string
		loginMethod     chatgptwebauth.LoginMethod
		allowAutoAPI798 bool
		wantState       chatgptwebauth.LifecycleState
	}{
		{
			name:        "explicit API798",
			loginMethod: chatgptwebauth.LoginMethodAPI798,
			wantState:   chatgptwebauth.LifecycleReauthRequired,
		},
		{
			name:            "automatic API798 enabled",
			allowAutoAPI798: true,
			wantState:       chatgptwebauth.LifecycleReauthRequired,
		},
		{
			name:      "automatic API798 disabled",
			wantState: chatgptwebauth.LifecycleDead,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			credential := &chatgptwebauth.Credential{
				Email:           "api798-recovery@example.com",
				LoginMethod:     test.loginMethod,
				API798URL:       api798URL,
				RefreshStrategy: chatgptwebauth.RefreshStrategyTokenOnly,
			}
			failure := &chatgptwebauth.AuthError{
				Code:           "session_expired",
				State:          chatgptwebauth.LifecycleReauthRequired,
				LifecycleState: chatgptwebauth.LifecycleReauthRequired,
				Terminal:       true,
			}
			updated, errRefresh, terminal := classifyChatGPTWebSessionCookieRefresh(credential, failure, test.allowAutoAPI798)
			if !terminal || errRefresh == nil || updated == nil {
				t.Fatalf("classifyChatGPTWebSessionCookieRefresh() = (%v, %v, %t)", updated, errRefresh, terminal)
			}
			if updated.LifecycleState != test.wantState {
				t.Fatalf("lifecycle = %q, want %q", updated.LifecycleState, test.wantState)
			}
		})
	}
}

func TestChatGPTWebExecutorRefreshSingleflight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		if fake.refreshCalls.Load() == 1 {
			close(started)
		}
		<-release
		credential.AccessToken = "refreshed-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake
	auth := chatGPTWebTestAuth("singleflight")

	var group sync.WaitGroup
	errs := make(chan error, 2)
	group.Add(1)
	go func() {
		defer group.Done()
		_, errPrepare := executor.PrepareRequestAuth(t.Context(), auth)
		errs <- errPrepare
	}()
	<-started
	secondEntered := make(chan struct{})
	group.Add(1)
	go func() {
		defer group.Done()
		close(secondEntered)
		_, errPrepare := executor.PrepareRequestAuth(t.Context(), auth)
		errs <- errPrepare
	}()
	<-secondEntered
	// Keep the first refresh in flight long enough for the second caller to
	// join the same singleflight operation.
	time.Sleep(20 * time.Millisecond)
	close(release)
	group.Wait()
	close(errs)
	for errPrepare := range errs {
		if errPrepare != nil {
			t.Fatalf("PrepareRequestAuth() error: %v", errPrepare)
		}
	}
	if got := fake.refreshCalls.Load(); got != 1 {
		t.Fatalf("refresh calls = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorRefreshSingleflightCallerCancellation(t *testing.T) {
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(ctx context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		started <- ctx
		select {
		case <-release:
			credential.AccessToken = "refreshed-token"
			return &credential, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake
	auth := chatGPTWebTestAuth("cancel")

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstDone := make(chan error, 1)
	go func() {
		_, errPrepare := executor.PrepareRequestAuth(firstCtx, auth)
		firstDone <- errPrepare
	}()
	acquisitionCtx := <-started

	secondDone := make(chan error, 1)
	secondEntered := make(chan struct{})
	go func() {
		close(secondEntered)
		_, errPrepare := executor.PrepareRequestAuth(t.Context(), auth)
		secondDone <- errPrepare
	}()
	<-secondEntered
	time.Sleep(20 * time.Millisecond)
	cancelFirst()
	if errPrepare := <-firstDone; !errors.Is(errPrepare, context.Canceled) {
		t.Fatalf("first PrepareRequestAuth() error = %v, want context canceled", errPrepare)
	}
	select {
	case <-acquisitionCtx.Done():
		t.Fatal("canceling one waiter canceled the shared refresh acquisition")
	default:
	}
	close(release)
	if errPrepare := <-secondDone; errPrepare != nil {
		t.Fatalf("second PrepareRequestAuth() error: %v", errPrepare)
	}
	if got := fake.refreshCalls.Load(); got != 1 {
		t.Fatalf("refresh calls = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorCloseWaitsForRefreshAndDiscardsLateResult(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential.AccessToken = "late-refreshed-token"
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake

	type refreshOutput struct {
		auth *cliproxyauth.Auth
		err  error
	}
	refreshDone := make(chan refreshOutput, 1)
	go func() {
		updated, errRefresh := executor.Refresh(context.Background(), chatGPTWebTestAuth("shutdown-refresh"))
		refreshDone <- refreshOutput{auth: updated, err: errRefresh}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- executor.Close() }()
	select {
	case errClose := <-closeDone:
		t.Fatalf("Close() returned before refresh exited: %v", errClose)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case errClose := <-closeDone:
		if errClose != nil {
			t.Fatalf("Close() error = %v", errClose)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not finish after refresh exited")
	}
	select {
	case output := <-refreshDone:
		if output.auth != nil || !errors.Is(output.err, context.Canceled) {
			t.Fatalf("late refresh result = (%v, %v), want canceled without update", output.auth, output.err)
		}
	case <-time.After(time.Second):
		t.Fatal("refresh caller did not finish")
	}
}

func TestChatGPTWebExecutorRefreshSingleflightStopsWhenAuthRetires(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("refresh-retirement")
	if _, err := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); err != nil {
		t.Fatal(err)
	}
	installed, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatal("registered auth not found")
	}

	started := make(chan struct{})
	canceled := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(ctx context.Context, _ chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake

	refreshDone := make(chan error, 1)
	go func() {
		_, err := executor.PrepareRequestAuth(t.Context(), installed)
		refreshDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}

	replacement := installed.Clone()
	replacement.Metadata["access_token"] = "replacement-token"
	replacement.Attributes[cliproxyauth.SourceHashAttributeKey] = "replacement-source"
	if _, err := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("retiring the auth did not cancel the shared refresh acquisition")
	}
	select {
	case err := <-refreshDone:
		if err == nil {
			t.Fatal("retired auth refresh returned no error")
		}
	case <-time.After(time.Second):
		t.Fatal("retired auth refresh did not finish")
	}
}

func TestChatGPTWebExecutorRefreshSingleflightClonesCredential(t *testing.T) {
	const waiters = 8
	started := make(chan struct{})
	release := make(chan struct{})
	sharedCredential := &chatgptwebauth.Credential{
		Type:           chatgptwebauth.Provider,
		AccessToken:    "shared-token",
		RefreshToken:   "refresh-token",
		Cookies:        []chatgptwebauth.Cookie{{Name: "session", Value: "shared-cookie"}},
		Persona:        chatgptwebauth.DefaultPersona(),
		LifecycleState: chatgptwebauth.LifecycleReauthRequired,
	}
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, _ chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		return sharedCredential, &chatgptwebauth.AuthError{
			Code:           "terminal_failure",
			State:          chatgptwebauth.LifecycleReauthRequired,
			LifecycleState: chatgptwebauth.LifecycleReauthRequired,
			Terminal:       true,
		}
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake
	auth := chatGPTWebTestAuth("clone")

	type refreshOutput struct {
		auth *cliproxyauth.Auth
		err  error
	}
	outputs := make(chan refreshOutput, waiters)
	go func() {
		updated, errRefresh := executor.Refresh(t.Context(), auth)
		outputs <- refreshOutput{auth: updated, err: errRefresh}
	}()
	<-started
	for index := 1; index < waiters; index++ {
		go func() {
			updated, errRefresh := executor.Refresh(t.Context(), auth)
			outputs <- refreshOutput{auth: updated, err: errRefresh}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(release)

	results := make([]*cliproxyauth.Auth, 0, waiters)
	for index := 0; index < waiters; index++ {
		output := <-outputs
		if output.auth == nil || output.err == nil {
			t.Fatalf("Refresh() = (%v, %v), want terminal update", output.auth, output.err)
		}
		results = append(results, output.auth)
	}
	if got := fake.refreshCalls.Load(); got != 1 {
		t.Fatalf("refresh calls = %d, want 1", got)
	}
	firstCookies := results[0].Metadata["cookies"].([]chatgptwebauth.Cookie)
	firstCookies[0].Value = "caller-mutation"
	for index, result := range results[1:] {
		cookies := result.Metadata["cookies"].([]chatgptwebauth.Cookie)
		if cookies[0].Value != "shared-cookie" {
			t.Fatalf("caller %d cookie = %q, want independent clone", index+1, cookies[0].Value)
		}
	}
	if sharedCredential.Cookies[0].Value != "shared-cookie" {
		t.Fatal("caller mutation changed the singleflight credential")
	}
}

func TestChatGPTWebExecutorRefreshToCompletionOutlivesCallerCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseRefresh := func() {
		releaseOnce.Do(func() { close(release) })
	}
	t.Cleanup(releaseRefresh)

	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential.AccessToken = "refreshed-access-token"
		credential.RefreshToken = "rotated-refresh-token"
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	type refreshResult struct {
		auth *cliproxyauth.Auth
		err  error
	}
	result := make(chan refreshResult, 1)
	go func() {
		updated, errRefresh := executor.RefreshToCompletion(ctx, chatGPTWebTestAuth("durable-refresh"))
		result <- refreshResult{auth: updated, err: errRefresh}
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("durable refresh did not start")
	}
	cancel()
	select {
	case early := <-result:
		releaseRefresh()
		t.Fatalf("durable refresh returned before provider completion: %#v", early)
	case <-time.After(25 * time.Millisecond):
	}
	releaseRefresh()

	select {
	case completed := <-result:
		if completed.err != nil || completed.auth == nil {
			t.Fatalf("RefreshToCompletion() = (%v, %v)", completed.auth, completed.err)
		}
		credential, errCredential := chatgptwebauth.ParseCredential(completed.auth.Metadata)
		if errCredential != nil {
			t.Fatal(errCredential)
		}
		if credential.AccessToken != "refreshed-access-token" || credential.RefreshToken != "rotated-refresh-token" {
			t.Fatalf("durable credential = access %q refresh %q", credential.AccessToken, credential.RefreshToken)
		}
	case <-time.After(time.Second):
		t.Fatal("durable refresh did not return after provider completion")
	}
}

func TestChatGPTWebExecutorRefreshSingleflightSpansInstallationUpdates(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("refresh-installation")
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	first, _ := manager.GetByID(auth.ID)
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseRefresh := func() { releaseOnce.Do(func() { close(release) }) }
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential.AccessToken = "refreshed-token"
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })
	t.Cleanup(releaseRefresh)

	type refreshResult struct {
		auth *cliproxyauth.Auth
		err  error
	}
	results := make(chan refreshResult, 2)
	go func() {
		updated, errRefresh, _ := executor.refreshCredential(t.Context(), first, false)
		results <- refreshResult{auth: updated, err: errRefresh}
	}()
	<-started

	replacement := first.Clone()
	if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	second, _ := manager.GetByID(auth.ID)
	if second.RuntimeInstanceID() != first.RuntimeInstanceID() || second.RuntimeInstallationID() == first.RuntimeInstallationID() {
		t.Fatalf("installation update identities = runtime %q/%q installation %q/%q",
			first.RuntimeInstanceID(), second.RuntimeInstanceID(), first.RuntimeInstallationID(), second.RuntimeInstallationID())
	}
	go func() {
		updated, errRefresh, _ := executor.refreshCredential(t.Context(), second, false)
		results <- refreshResult{auth: updated, err: errRefresh}
	}()
	time.Sleep(20 * time.Millisecond)
	releaseRefresh()
	for range 2 {
		result := <-results
		if result.err != nil || result.auth == nil {
			t.Fatalf("refresh result = (%v, %v)", result.auth, result.err)
		}
	}
	if got := fake.refreshCalls.Load(); got != 1 {
		t.Fatalf("refresh calls across installation update = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorManualAndBackgroundReloginSingleflight(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "shared-relogin")
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential := *input.Credential
		credential.AccessToken = "relogin-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	if !executor.TriggerBackgroundRelogin(expected) {
		t.Fatalf("TriggerBackgroundRelogin() rejected task: %+v", executor.BackgroundReloginSnapshot())
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		current, _ := manager.GetByID(expected.ID)
		t.Fatalf("background re-login did not start: snapshot=%+v lifecycle=%q reason=%q", executor.BackgroundReloginSnapshot(), current.LifecycleState(), chatGPTWebLifecycleReason(current))
	}

	manualDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(t.Context(), expected)
		manualDone <- errRelogin
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	if errRelogin := <-manualDone; errRelogin != nil {
		t.Fatalf("ReloginCurrent() error: %v", errRelogin)
	}
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls = %d, want 1", got)
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateActive
	})
}

func TestChatGPTWebExecutorReloginUsesResolvedAPI798AcquisitionTimeout(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("api798-timeout")
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	credential.LoginMethod = chatgptwebauth.LoginMethodAPI798
	credential.API798URL = "https://api798.com/get_code?email=api798-timeout%40example.com&auth_code=opaque"
	credential.Password = ""
	credential.TOTPSecret = ""
	credential.ApplyToMetadata(auth.Metadata)
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatalf("registered auth %q not found", auth.ID)
	}

	fake := &fakeChatGPTWebAuthService{}
	fake.loginAcquisitionTimeoutFn = func(input chatgptwebauth.LoginInput) time.Duration {
		if input.Credential == nil || input.Credential.LoginMethod != chatgptwebauth.LoginMethodAPI798 {
			t.Fatalf("timeout input = %#v", input.Credential)
		}
		return chatgptwebauth.DefaultAPI798AcquisitionTimeout
	}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		deadline, okDeadline := ctx.Deadline()
		if !okDeadline || time.Until(deadline) < time.Minute {
			return nil, fmt.Errorf("re-login deadline = %v, want API798 acquisition window", deadline)
		}
		updated := *input.Credential
		updated.AccessToken = "api798-relogin-token"
		updated.LifecycleState = chatgptwebauth.LifecycleActive
		return &updated, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if errRelogin != nil || !current || updated == nil {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v)", updated, current, errRelogin)
	}
}

func TestChatGPTWebExecutorReloginKeepsOneRuntimeConfigSnapshot(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "relogin-runtime-snapshot")
	timeout := 120
	initialConfig := &config.Config{SDKConfig: config.SDKConfig{ProxyURL: "http://initial-proxy.example:8080"}, ChatGPTWeb: config.ChatGPTWebConfig{
		API798AutoLoginEnabled:       true,
		InvalidPasskeyResponseAsDead: true,
		LoginProxy: config.ChatGPTWebLoginProxyConfig{
			Enabled:                   true,
			URLTemplate:               "http://initial-login-proxy.example:8080",
			AcquisitionTimeoutSeconds: &timeout,
		},
	}}
	replacementConfig := &config.Config{SDKConfig: config.SDKConfig{ProxyURL: "http://replacement-proxy.example:8080"}}

	var executor *ChatGPTWebExecutor
	fake := &fakeChatGPTWebAuthService{}
	fake.loginAcquisitionTimeoutFn = func(input chatgptwebauth.LoginInput) time.Duration {
		if !input.AllowAutoAPI798 || !input.LoginProxy.Enabled ||
			input.LoginProxy.URLTemplate != "http://initial-login-proxy.example:8080" {
			t.Fatalf("timeout input = %#v, want initial runtime snapshot", input)
		}
		executor.UpdateConfig(replacementConfig)
		return time.Second
	}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if !input.AllowAutoAPI798 || !input.RetryInvalidPasskeyResponse || !input.LoginProxy.Enabled ||
			input.LoginProxy.URLTemplate != "http://initial-login-proxy.example:8080" || input.ProxyURL != "" {
			t.Fatalf("login input = %#v, want the same initial runtime snapshot", input)
		}
		updated := *input.Credential
		updated.AccessToken = "runtime-snapshot-token"
		updated.LifecycleState = chatgptwebauth.LifecycleActive
		return &updated, nil
	}
	executor = NewChatGPTWebExecutor(initialConfig, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if errRelogin != nil || !current || updated == nil {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v)", updated, current, errRelogin)
	}
}

func TestChatGPTWebExecutorReloginAcquisitionTimeoutStartsAfterQueue(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("api798-queued-timeout")
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	credential.LoginMethod = chatgptwebauth.LoginMethodAPI798
	credential.API798URL = "https://api798.com/get_code?email=api798-queued-timeout%40example.com&auth_code=opaque"
	credential.Password = ""
	credential.TOTPSecret = ""
	credential.ApplyToMetadata(auth.Metadata)
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatalf("registered auth %q not found", auth.ID)
	}

	const acquisitionTimeout = 100 * time.Millisecond
	var timeoutCalls atomic.Int32
	fake := &fakeChatGPTWebAuthService{}
	fake.loginAcquisitionTimeoutFn = func(chatgptwebauth.LoginInput) time.Duration {
		timeoutCalls.Add(1)
		return acquisitionTimeout
	}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		deadline, okDeadline := ctx.Deadline()
		if !okDeadline || time.Until(deadline) <= 0 {
			return nil, fmt.Errorf("re-login deadline = %v, want a fresh acquisition window", deadline)
		}
		updated := *input.Credential
		updated.AccessToken = "api798-queued-token"
		updated.LifecycleState = chatgptwebauth.LifecycleActive
		return &updated, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	executor.reloginSlots = make(chan struct{}, 1)
	executor.reloginSlots <- struct{}{}
	t.Cleanup(func() { _ = executor.Close() })

	type reloginResult struct {
		auth    *cliproxyauth.Auth
		current bool
		err     error
	}
	done := make(chan reloginResult, 1)
	go func() {
		updated, current, errRelogin := executor.reloginCurrentWithMode(t.Context(), expected, true)
		done <- reloginResult{auth: updated, current: current, err: errRelogin}
	}()
	waitForChatGPTWebReloginWaiters(t, executor, expected, 1)
	time.Sleep(2 * acquisitionTimeout)
	if got := timeoutCalls.Load(); got != 0 {
		t.Fatalf("acquisition timeout resolved while queued = %d, want 0", got)
	}
	select {
	case result := <-done:
		t.Fatalf("queued re-login completed before a slot was available: %#v", result)
	default:
	}

	<-executor.reloginSlots
	select {
	case result := <-done:
		if result.err != nil || !result.current || result.auth == nil {
			t.Fatalf("queued re-login = (%v, %v, %v)", result.auth, result.current, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued re-login did not complete after a slot became available")
	}
	if got := timeoutCalls.Load(); got != 1 {
		t.Fatalf("acquisition timeout resolutions = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorBackgroundReloginRetriesClassifiedAcquisitionTimeout(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "classified-timeout-retry")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginAcquisitionTimeoutFn = func(chatgptwebauth.LoginInput) time.Duration {
		return 20 * time.Millisecond
	}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if fake.loginCalls.Load() == 1 {
			<-ctx.Done()
			return nil, &chatgptwebauth.AuthError{
				Code:           "api798_timeout",
				State:          chatgptwebauth.LifecycleReloginPending,
				LifecycleState: chatgptwebauth.LifecycleReloginPending,
				Retryable:      true,
				Cause:          ctx.Err(),
			}
		}
		updated := *input.Credential
		updated.AccessToken = "retried-timeout-token"
		updated.LifecycleState = chatgptwebauth.LifecycleActive
		return &updated, nil
	}
	maxRetries := 1
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AutoRelogin:           true,
		AutoReloginMaxRetries: &maxRetries,
	}}, manager)
	executor.authService = fake
	executor.reloginBackoff = func(int) time.Duration { return 0 }
	t.Cleanup(func() { _ = executor.Close() })

	executor.runBackgroundRelogin(expected)
	if got := fake.loginCalls.Load(); got != 2 {
		t.Fatalf("login calls = %d, want 2", got)
	}
	current, ok := manager.GetByID(expected.ID)
	if !ok || current.LifecycleState() != cliproxyauth.LifecycleStateActive {
		t.Fatalf("current auth = %#v, want active", current)
	}
}

func TestChatGPTWebExecutorReloginPersistsClassifiedTerminalErrorAtDeadline(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "classified-terminal-deadline")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginAcquisitionTimeoutFn = func(chatgptwebauth.LoginInput) time.Duration {
		return 20 * time.Millisecond
	}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		<-ctx.Done()
		updated := *input.Credential
		updated.LifecycleState = chatgptwebauth.LifecycleReauthRequired
		updated.LifecycleReason = "invalid_grant"
		return &updated, &chatgptwebauth.AuthError{
			Code:           "invalid_grant",
			State:          chatgptwebauth.LifecycleReauthRequired,
			LifecycleState: chatgptwebauth.LifecycleReauthRequired,
			Terminal:       true,
			Cause:          ctx.Err(),
		}
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	updated, current, errRelogin := executor.reloginCurrentWithMode(t.Context(), expected, true)
	if errRelogin == nil || chatGPTWebErrorCode(errRelogin) != "invalid_grant" {
		t.Fatalf("re-login error = %v, want classified invalid_grant", errRelogin)
	}
	if !current || updated == nil || updated.LifecycleState() != cliproxyauth.LifecycleStateReauthRequired {
		t.Fatalf("re-login result = (%#v, %v), want current reauth_required", updated, current)
	}
	installed, ok := manager.GetByID(expected.ID)
	if !ok || installed.LifecycleState() != cliproxyauth.LifecycleStateReauthRequired {
		t.Fatalf("installed auth = %#v, want reauth_required", installed)
	}
}

func TestChatGPTWebExecutorReloginPersistsSuccessAtAcquisitionDeadline(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "success-at-acquisition-deadline")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginAcquisitionTimeoutFn = func(chatgptwebauth.LoginInput) time.Duration {
		return 20 * time.Millisecond
	}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		<-ctx.Done()
		updated := *input.Credential
		updated.AccessToken = "deadline-success-token"
		updated.LifecycleState = chatgptwebauth.LifecycleActive
		return &updated, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	updated, current, errRelogin := executor.reloginCurrentWithMode(t.Context(), expected, true)
	if errRelogin != nil || !current || updated == nil || updated.LifecycleState() != cliproxyauth.LifecycleStateActive {
		t.Fatalf("re-login result = (%#v, %v, %v), want current active", updated, current, errRelogin)
	}
	installed, ok := manager.GetByID(expected.ID)
	if !ok || installed.LifecycleState() != cliproxyauth.LifecycleStateActive {
		t.Fatalf("installed auth = %#v, want active", installed)
	}
}

func TestChatGPTWebExecutorCanceledReloginDoesNotPersistTerminalError(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "canceled-terminal-relogin")
	started := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		updated := *input.Credential
		updated.LifecycleState = chatgptwebauth.LifecycleReauthRequired
		updated.LifecycleReason = "invalid_grant"
		return &updated, &chatgptwebauth.AuthError{
			Code:           "invalid_grant",
			State:          chatgptwebauth.LifecycleReauthRequired,
			LifecycleState: chatgptwebauth.LifecycleReauthRequired,
			Terminal:       true,
			Cause:          ctx.Err(),
		}
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		done <- errRelogin
	}()
	<-started
	cancel()
	select {
	case errRelogin := <-done:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("re-login error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled re-login did not return")
	}
	installed, ok := manager.GetByID(expected.ID)
	if !ok || installed.LifecycleState() != cliproxyauth.LifecycleStateReloginPending {
		t.Fatalf("installed auth = %#v, want relogin_pending", installed)
	}
}

func TestChatGPTWebExecutorCallerDeadlineDoesNotPersistTerminalError(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "caller-deadline-terminal-relogin")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginAcquisitionTimeoutFn = func(chatgptwebauth.LoginInput) time.Duration {
		return time.Second
	}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		<-ctx.Done()
		updated := *input.Credential
		updated.LifecycleState = chatgptwebauth.LifecycleReauthRequired
		updated.LifecycleReason = "invalid_grant"
		return &updated, &chatgptwebauth.AuthError{
			Code:           "invalid_grant",
			State:          chatgptwebauth.LifecycleReauthRequired,
			LifecycleState: chatgptwebauth.LifecycleReauthRequired,
			Terminal:       true,
			Cause:          ctx.Err(),
		}
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
	if !errors.Is(errRelogin, context.DeadlineExceeded) {
		t.Fatalf("re-login error = %v, want caller deadline exceeded", errRelogin)
	}
	installed, ok := manager.GetByID(expected.ID)
	if !ok || installed.LifecycleState() != cliproxyauth.LifecycleStateReloginPending {
		t.Fatalf("installed auth = %#v, want relogin_pending", installed)
	}
}

func TestChatGPTWebExecutorReloginPersistsSafePasskeyDiagnostic(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "passkey-diagnostic")
	fake := &fakeChatGPTWebAuthService{loginFn: func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		credential := *input.Credential
		credential.LifecycleState = chatgptwebauth.LifecycleReauthRequired
		credential.LifecycleReason = "passkey_verification_failed"
		return &credential, &chatgptwebauth.AuthError{
			Code:           "passkey_verification_failed",
			DiagnosticCode: "invalid_passkey_response",
			State:          chatgptwebauth.LifecycleReauthRequired,
			LifecycleState: chatgptwebauth.LifecycleReauthRequired,
			Status:         http.StatusBadRequest,
			StatusCode:     http.StatusBadRequest,
			FailureStage:   "passkey_verify",
			Attempts:       2,
			ResponseType:   "json",
			ContentType:    "application/json",
			TargetHost:     "auth.openai.com",
			TargetPath:     "/api/accounts/passkey/verify",
			Message:        "Passkey credential was rejected",
			Terminal:       true,
		}
	}}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if errRelogin == nil || !current || updated == nil {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v)", updated, current, errRelogin)
	}
	installed, ok := manager.GetByID(expected.ID)
	if !ok || installed.LastError == nil || installed.LastError.Diagnostic == nil {
		t.Fatalf("installed auth = %#v", installed)
	}
	diagnostic := installed.LastError.Diagnostic
	if diagnostic.Code != "invalid_passkey_response" || diagnostic.Stage != "passkey_verify" || diagnostic.Attempts != 2 || diagnostic.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("diagnostic = %#v", diagnostic)
	}
}

func TestChatGPTWebExecutorManualPromotesQueuedBackgroundRelogin(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "manual-promotes-background")
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential := *input.Credential
		credential.AccessToken = "promoted-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	executor.reloginSlots = make(chan struct{}, 1)
	executor.reloginSlots <- struct{}{}
	t.Cleanup(func() { _ = executor.Close() })

	backgroundDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.reloginCurrentWithMode(t.Context(), expected, true)
		backgroundDone <- errRelogin
	}()
	waitForChatGPTWebReloginWaiters(t, executor, expected, 1)

	manualDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(t.Context(), expected)
		manualDone <- errRelogin
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("manual re-login did not promote the queued background flight")
	}
	close(release)
	if errRelogin := <-manualDone; errRelogin != nil {
		t.Fatalf("manual ReloginCurrent() error: %v", errRelogin)
	}
	if errRelogin := <-backgroundDone; errRelogin != nil {
		t.Fatalf("background re-login error: %v", errRelogin)
	}
	if got := len(executor.reloginSlots); got != 1 {
		t.Fatalf("background slot occupancy = %d, want original blocker only", got)
	}
}

func TestChatGPTWebExecutorReloginHoldsProxyBindingLease(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "proxy-lease")
	resolver := &chatGPTWebLeaseResolver{}
	manager.SetProxyResolver(resolver)
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if resolver.active.Load() == 0 {
			return nil, errors.New("proxy binding lease was released during login")
		}
		credential := *input.Credential
		credential.AccessToken = "relogin-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake

	updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if errRelogin != nil || !current || updated == nil {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v)", updated, current, errRelogin)
	}
	if active := resolver.active.Load(); active != 0 {
		t.Fatalf("proxy binding lease count after re-login = %d, want 0", active)
	}
}

func TestChatGPTWebExecutorReloginCancellationStopsAcquisition(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "cancel-relogin")
	started := make(chan struct{})
	acquisitionCanceled := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		close(acquisitionCanceled)
		return nil, ctx.Err()
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		result <- errRelogin
	}()
	<-started
	cancel()
	select {
	case errRelogin := <-result:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("ReloginCurrent() did not stop after cancellation")
	}
	select {
	case <-acquisitionCanceled:
	case <-time.After(time.Second):
		t.Fatal("re-login acquisition did not observe cancellation")
	}
}

func TestChatGPTWebExecutorReloginCancellationDoesNotWaitForStuckAcquisition(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "stuck-cancel-relogin")
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		return nil, context.Canceled
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		result <- errRelogin
	}()
	<-started
	cancel()
	select {
	case errRelogin := <-result:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("ReloginCurrent() waited for an acquisition that ignored cancellation")
	}

	close(release)
	if errClose := executor.Close(); errClose != nil {
		t.Fatalf("Close() error = %v", errClose)
	}
}

func TestChatGPTWebExecutorCanceledReloginDoesNotCommitReturnedCredential(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "cancel-returned-credential")
	started := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		credential := *input.Credential
		credential.AccessToken = "late-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		result <- errRelogin
	}()
	<-started
	cancel()
	select {
	case errRelogin := <-result:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("ReloginCurrent() did not stop after cancellation")
	}
	current, ok := manager.GetByID(expected.ID)
	if !ok {
		t.Fatal("current auth is missing")
	}
	if token := current.Metadata["access_token"]; token != "access-token" {
		t.Fatalf("access token after canceled re-login = %v, want original", token)
	}
	if state := current.LifecycleState(); state != cliproxyauth.LifecycleStateReloginPending {
		t.Fatalf("lifecycle after canceled re-login = %q, want pending", state)
	}
}

func TestChatGPTWebExecutorCanceledReloginWaiterKeepsSharedOperationTracked(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "shared-cancel-relogin")
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential := *input.Credential
		credential.AccessToken = "shared-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()
	executor.TriggerBackgroundRelogin(expected)
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	manualDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		manualDone <- errRelogin
	}()
	waitForChatGPTWebReloginWaiters(t, executor, expected, 2)
	cancel()
	select {
	case errRelogin := <-manualDone:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("shared ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled shared ReloginCurrent() did not return")
	}
	close(release)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateActive
	})
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorCanceledManualReloginRestartsBackgroundWithinLimit(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "first-cancel-relogin")
	firstStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	secondStarted := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if fake.loginCalls.Load() == 1 {
			close(firstStarted)
			<-ctx.Done()
			close(firstCanceled)
			return nil, ctx.Err()
		}
		close(secondStarted)
		credential := *input.Credential
		credential.AccessToken = "background-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	executor.reloginBackoff = func(int) time.Duration { return 0 }
	executor.reloginSlots = make(chan struct{}, 1)
	executor.reloginSlots <- struct{}{}
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	manualDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		manualDone <- errRelogin
	}()
	<-firstStarted
	backgroundDone := make(chan struct{})
	go func() {
		executor.runBackgroundRelogin(expected)
		close(backgroundDone)
	}()
	waitForChatGPTWebReloginWaiters(t, executor, expected, 2)
	cancel()
	select {
	case errRelogin := <-manualDone:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("manual ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled manual ReloginCurrent() did not return")
	}

	<-firstCanceled
	select {
	case <-secondStarted:
		t.Fatal("background retry bypassed the full background slot limit")
	case <-time.After(50 * time.Millisecond):
	}
	<-executor.reloginSlots
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("background retry did not start after a slot became available")
	}
	select {
	case <-backgroundDone:
	case <-time.After(time.Second):
		t.Fatal("background retry did not finish")
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateActive
	})
	if got := fake.loginCalls.Load(); got != 2 {
		t.Fatalf("login calls = %d, want 2", got)
	}
}

func TestChatGPTWebExecutorBackgroundRetriesAfterJoiningCanceledFlight(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "canceling-flight-relogin")
	firstStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	releaseFirst := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if fake.loginCalls.Load() == 1 {
			close(firstStarted)
			<-ctx.Done()
			close(firstCanceled)
			<-releaseFirst
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "background-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	manualDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		manualDone <- errRelogin
	}()
	<-firstStarted
	cancel()
	<-firstCanceled
	executor.TriggerBackgroundRelogin(expected)
	close(releaseFirst)
	if errRelogin := <-manualDone; !errors.Is(errRelogin, context.Canceled) {
		t.Fatalf("manual ReloginCurrent() error = %v, want context canceled", errRelogin)
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateActive
	})
	if got := fake.loginCalls.Load(); got != 2 {
		t.Fatalf("login calls = %d, want 2", got)
	}
}

func TestChatGPTWebExecutorWaitingForCanceledFlightHonorsCallerCancellation(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "wait-canceled-flight")
	firstStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	releaseFirst := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(firstStarted)
		<-ctx.Done()
		close(firstCanceled)
		<-releaseFirst
		return nil, ctx.Err()
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(firstCtx, expected)
		firstDone <- errRelogin
	}()
	<-firstStarted
	cancelFirst()
	<-firstCanceled

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(secondCtx, expected)
		secondDone <- errRelogin
	}()
	cancelSecond()
	select {
	case errRelogin := <-secondDone:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("second ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("second ReloginCurrent() ignored caller cancellation")
	}
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls = %d, want 1", got)
	}

	close(releaseFirst)
	if errRelogin := <-firstDone; !errors.Is(errRelogin, context.Canceled) {
		t.Fatalf("first ReloginCurrent() error = %v, want context canceled", errRelogin)
	}
}

func TestChatGPTWebExecutorCloseWaitsForManualReloginFlight(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "close-manual-flight")
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil, ctx.Err()
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	reloginDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(context.Background(), expected)
		reloginDone <- errRelogin
	}()
	<-started

	closeDone := make(chan error, 1)
	go func() { closeDone <- executor.Close() }()
	<-canceled
	select {
	case errClose := <-closeDone:
		t.Fatalf("Close() returned before manual flight stopped: %v", errClose)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if errClose := <-closeDone; errClose != nil {
		t.Fatalf("Close() error = %v", errClose)
	}
	if errRelogin := <-reloginDone; !errors.Is(errRelogin, context.Canceled) {
		t.Fatalf("ReloginCurrent() error = %v, want context canceled", errRelogin)
	}
}

func TestChatGPTWebExecutorCloseCancelsBackgroundRelogin(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "close-background")
	started := make(chan struct{})
	canceled := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	executor.TriggerBackgroundRelogin(expected)
	<-started
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Close() did not cancel the background re-login")
	}
	executor.TriggerBackgroundRelogin(expected)
	time.Sleep(20 * time.Millisecond)
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls after Close() = %d, want 1", got)
	}
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestChatGPTWebExecutorBackgroundReloginContinuesUntilSuccess(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "retry-until-success")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if fake.loginCalls.Load() == 4 {
			credential := *input.Credential
			credential.AccessToken = "retried-token"
			credential.LifecycleState = chatgptwebauth.LifecycleActive
			return &credential, nil
		}
		return nil, &chatgptwebauth.AuthError{Code: "network_error", Retryable: true}
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	executor.reloginBackoff = func(int) time.Duration { return 0 }
	executor.TriggerBackgroundRelogin(expected)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateActive
	})
	if got := fake.loginCalls.Load(); got != 4 {
		t.Fatalf("login calls = %d, want 4", got)
	}
}

func TestChatGPTWebExecutorBackgroundReloginAllowsNewInstallationAfterSupersededFlight(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "superseded-background")
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if fake.loginCalls.Load() == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		credential := input.Credential
		credential.AccessToken = "relogin-token-1"
		if fake.loginCalls.Load() == 2 {
			credential.AccessToken = "relogin-token-2"
		}
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	executor.TriggerBackgroundRelogin(expected)
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first background re-login did not start")
	}

	replacement, ok := manager.GetByID(expected.ID)
	if !ok {
		t.Fatalf("auth %q not found", expected.ID)
	}
	if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	current, ok := manager.GetByID(expected.ID)
	if !ok {
		t.Fatalf("replacement auth %q not found", expected.ID)
	}
	if current.RuntimeInstanceID() != expected.RuntimeInstanceID() {
		t.Fatalf("test setup replaced runtime instance: got %q, want %q", current.RuntimeInstanceID(), expected.RuntimeInstanceID())
	}

	executor.TriggerBackgroundRelogin(current)
	close(releaseFirst)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return fake.loginCalls.Load() == 2
	})
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		installed, found := manager.GetByID(expected.ID)
		return found && installed.LifecycleState() == cliproxyauth.LifecycleStateActive && installed.Metadata["access_token"] == "relogin-token-2"
	})
}

func TestChatGPTWebExecutorBackgroundReloginStopsOnClose(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "retry-until-close")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return nil, &chatgptwebauth.AuthError{Code: "network_error", Retryable: true}
	}
	maxRetries := 10
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AutoRelogin:           true,
		AutoReloginMaxRetries: &maxRetries,
	}}, manager)
	executor.authService = fake
	executor.reloginBackoff = func(int) time.Duration { return time.Millisecond }
	executor.TriggerBackgroundRelogin(expected)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return fake.loginCalls.Load() >= 4
	})
	if errClose := executor.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	calls := fake.loginCalls.Load()
	time.Sleep(20 * time.Millisecond)
	if got := fake.loginCalls.Load(); got != calls {
		t.Fatalf("login calls after Close() = %d, want %d", got, calls)
	}
}

func TestChatGPTWebBackgroundReloginRetryable(t *testing.T) {
	if !chatGPTWebBackgroundReloginRetryable(context.DeadlineExceeded) {
		t.Fatal("acquisition deadline was not retried")
	}
	if chatGPTWebBackgroundReloginRetryable(context.Canceled) {
		t.Fatal("caller cancellation was retried")
	}
	if !chatGPTWebBackgroundReloginRetryable(&chatgptwebauth.AuthError{Retryable: true}) {
		t.Fatal("retryable auth error was not retried")
	}
	if !chatGPTWebBackgroundReloginRetryable(&proxypool.UnavailableError{Pool: "test"}) {
		t.Fatal("proxy unavailable error was not retried")
	}
	if chatGPTWebBackgroundReloginRetryable(cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, errors.New("write failed"))) {
		t.Fatal("rolled-back persistence error was retried")
	}
	if chatGPTWebBackgroundReloginRetryable(cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeUncertain, errors.New("write uncertain"))) {
		t.Fatal("uncertain persistence error was retried")
	}
	if chatGPTWebBackgroundReloginRetryable(&chatGPTWebCredentialUnavailableError{cause: errors.New("request routing failure")}) {
		t.Fatal("generic request-routing error was retried")
	}
	if chatGPTWebBackgroundReloginRetryable(errors.New("invalid credentials")) {
		t.Fatal("non-retryable error was retried")
	}
}

func TestChatGPTWebExecutorUpdateConfigIsConcurrentSafe(t *testing.T) {
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	enabled := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}
	disabled := &config.Config{}
	var workers sync.WaitGroup
	for index := 0; index < 8; index++ {
		workers.Add(1)
		go func(writer bool) {
			defer workers.Done()
			for iteration := 0; iteration < 1000; iteration++ {
				if writer {
					if iteration%2 == 0 {
						executor.UpdateConfig(enabled)
					} else {
						executor.UpdateConfig(disabled)
					}
					continue
				}
				_ = executor.AutoReloginEnabled()
			}
		}(index%2 == 0)
	}
	workers.Wait()
	executor.UpdateConfig(enabled)
	if !executor.AutoReloginEnabled() {
		t.Fatal("updated config was not observed")
	}
}

func TestChatGPTWebExecutorUpdateConfigPublishesImmutableSnapshot(t *testing.T) {
	headers := map[string]string{"X-Test": "before"}
	items := []map[string]any{{"name": "before"}}
	defaultRaw := []byte(`{"default":true}`)
	overrideRaw := json.RawMessage(`{"override":true}`)
	cfg := &config.Config{
		SDKConfig: config.SDKConfig{
			ProxyURL:   "http://proxy-before.example",
			RequestLog: true,
		},
		ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true},
		Payload: config.PayloadConfig{
			DefaultRaw: []config.PayloadRule{{
				Params: map[string]any{"raw": defaultRaw},
			}},
			Override: []config.PayloadRule{{
				Models: []config.PayloadModelRule{{Name: "gpt-*", Protocol: "responses"}},
				Params: map[string]any{
					"temperature": 0.5,
					"headers":     headers,
					"items":       items,
					"large":       int64(9007199254740993),
				},
			}},
			OverrideRaw: []config.PayloadRule{{
				Params: map[string]any{"raw": overrideRaw},
			}},
		},
	}
	executor := NewChatGPTWebExecutor(cfg, nil)

	cfg.ProxyURL = "http://proxy-after.example"
	cfg.RequestLog = false
	cfg.ChatGPTWeb.AutoRelogin = false
	cfg.Payload.Override[0].Models[0].Name = "mutated-*"
	cfg.Payload.Override[0].Params["temperature"] = 1.0
	headers["X-Test"] = "after"
	items[0]["name"] = "after"
	defaultRaw[2] = 'X'
	overrideRaw[2] = 'X'

	snapshot := executor.configSnapshot()
	if snapshot == nil {
		t.Fatal("configuration snapshot is nil")
	}
	if snapshot.ProxyURL != "http://proxy-before.example" || !snapshot.RequestLog || !snapshot.ChatGPTWeb.AutoRelogin {
		t.Fatalf("configuration snapshot changed with caller-owned config: %+v", snapshot)
	}
	rule := snapshot.Payload.Override[0]
	if rule.Models[0].Name != "gpt-*" || rule.Params["temperature"] != json.Number("0.5") {
		t.Fatalf("payload snapshot changed with caller-owned config: %+v", rule)
	}
	snapshotHeaders, okHeaders := rule.Params["headers"].(map[string]any)
	snapshotItems, okItems := rule.Params["items"].([]any)
	if !okHeaders || snapshotHeaders["X-Test"] != "before" || !okItems || snapshotItems[0].(map[string]any)["name"] != "before" {
		t.Fatalf("nested payload snapshot changed with caller-owned config: %+v", rule.Params)
	}
	if rule.Params["large"] != json.Number("9007199254740993") {
		t.Fatalf("large payload number lost precision: %#v", rule.Params["large"])
	}
	snapshotDefaultRaw, okDefaultRaw := snapshot.Payload.DefaultRaw[0].Params["raw"].([]byte)
	snapshotOverrideRaw, okOverrideRaw := snapshot.Payload.OverrideRaw[0].Params["raw"].(json.RawMessage)
	if !okDefaultRaw || string(snapshotDefaultRaw) != `{"default":true}` {
		t.Fatalf("default raw payload type/value = %T %q", snapshot.Payload.DefaultRaw[0].Params["raw"], snapshotDefaultRaw)
	}
	if !okOverrideRaw || string(snapshotOverrideRaw) != `{"override":true}` {
		t.Fatalf("override raw payload type/value = %T %q", snapshot.Payload.OverrideRaw[0].Params["raw"], snapshotOverrideRaw)
	}
}

func TestChatGPTWebExecutorTimezoneUsesCurrentConfigAndDST(t *testing.T) {
	executor := &ChatGPTWebExecutor{
		now: func() time.Time {
			return time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
		},
	}
	executor.cfg.Store(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		Timezone: "America/New_York",
	}})
	resolved := executor.chatGPTWebTimezone()
	if resolved.Timezone != "America/New_York" || resolved.OffsetMinutes != 240 {
		t.Fatalf("New York timezone = %#v", resolved)
	}

	zero := 0
	executor.cfg.Store(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		Timezone:              "UTC",
		TimezoneOffsetMinutes: &zero,
	}})
	resolved = executor.chatGPTWebTimezone()
	if resolved.Timezone != "UTC" || resolved.OffsetMinutes != 0 {
		t.Fatalf("updated UTC timezone = %#v", resolved)
	}
}

func TestChatGPTWebExecutorBackgroundReloginStopsWhenDisabled(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "stop-disabled")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return nil, &chatgptwebauth.AuthError{Code: "network_error", Retryable: true}
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	backoffStarted := make(chan struct{})
	var backoffOnce sync.Once
	executor.reloginBackoff = func(int) time.Duration {
		backoffOnce.Do(func() { close(backoffStarted) })
		return 500 * time.Millisecond
	}
	done := make(chan struct{})
	go func() {
		executor.runBackgroundRelogin(expected)
		close(done)
	}()
	<-backoffStarted
	executor.UpdateConfig(&config.Config{})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("background re-login did not stop after auto-relogin was disabled")
	}
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorBackgroundReloginStopsWhenCurrentChanges(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*cliproxyauth.Auth)
	}{
		{name: "lifecycle", change: func(auth *cliproxyauth.Auth) {
			auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateActive
		}},
		{name: "generation", change: func(auth *cliproxyauth.Auth) {
			auth.Attributes[cliproxyauth.SourceHashAttributeKey] = "replacement-generation"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := cliproxyauth.NewManager(nil, nil, nil)
			expected := registerChatGPTWebPendingAuth(t, manager, "stop-"+test.name)
			fake := &fakeChatGPTWebAuthService{}
			fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
				return nil, &chatgptwebauth.AuthError{Code: "network_error", Retryable: true}
			}
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
			executor.authService = fake
			backoffStarted := make(chan struct{})
			var backoffOnce sync.Once
			executor.reloginBackoff = func(int) time.Duration {
				backoffOnce.Do(func() { close(backoffStarted) })
				return 500 * time.Millisecond
			}
			done := make(chan struct{})
			go func() {
				executor.runBackgroundRelogin(expected)
				close(done)
			}()
			<-backoffStarted
			current, _ := manager.GetByID(expected.ID)
			test.change(current)
			if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), current); errUpdate != nil {
				t.Fatal(errUpdate)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("background re-login did not stop after auth changed")
			}
			if got := fake.loginCalls.Load(); got != 1 {
				t.Fatalf("login calls = %d, want 1", got)
			}
		})
	}
}

func TestChatGPTWebExecutorBackgroundReloginReleasesSlotWhenPendingChangesAfterAcquire(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "slot-lifecycle-race")
	fake := &fakeChatGPTWebAuthService{}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	initialSlots := len(chatGPTWebBackgroundReloginSlots)
	executor.reloginSlotAcquired = func() {
		current, ok := manager.GetByID(expected.ID)
		if !ok {
			t.Fatalf("auth %q not found", expected.ID)
		}
		current.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateActive
		if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), current); errUpdate != nil {
			t.Fatal(errUpdate)
		}
	}

	executor.runBackgroundRelogin(expected)
	if got := fake.loginCalls.Load(); got != 0 {
		t.Fatalf("login calls = %d, want 0 after lifecycle changed", got)
	}
	if got := len(chatGPTWebBackgroundReloginSlots); got != initialSlots {
		t.Fatalf("occupied background slots = %d, want %d", got, initialSlots)
	}
}

func TestChatGPTWebExecutorBackgroundReloginGlobalConcurrencyLimit(t *testing.T) {
	const authCount = chatGPTWebBackgroundReloginConcurrency * 2
	manager := cliproxyauth.NewManager(nil, nil, nil)
	fake := &fakeChatGPTWebAuthService{}
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var active atomic.Int32
	var maximum atomic.Int32
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := maximum.Load(); current > previous && !maximum.CompareAndSwap(previous, current); previous = maximum.Load() {
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "limited-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executors := []*ChatGPTWebExecutor{
		NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager),
		NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager),
	}
	t.Cleanup(func() {
		for _, executor := range executors {
			_ = executor.Close()
		}
	})
	for _, executor := range executors {
		executor.authService = fake
	}
	for index := 0; index < authCount; index++ {
		expected := registerChatGPTWebPendingAuth(t, manager, "limit-"+string(rune('a'+index)))
		executors[index%len(executors)].TriggerBackgroundRelogin(expected)
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return fake.loginCalls.Load() == chatGPTWebBackgroundReloginConcurrency
	})
	time.Sleep(30 * time.Millisecond)
	if got := fake.loginCalls.Load(); got != chatGPTWebBackgroundReloginConcurrency {
		t.Fatalf("login calls before release = %d, want %d", got, chatGPTWebBackgroundReloginConcurrency)
	}
	if got := maximum.Load(); got > chatGPTWebBackgroundReloginConcurrency {
		t.Fatalf("maximum background concurrency = %d, limit %d", got, chatGPTWebBackgroundReloginConcurrency)
	}
	releaseOnce.Do(func() { close(release) })
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return fake.loginCalls.Load() == authCount
	})
}

func TestChatGPTWebExecutorManagementLoginWaitsForBackgroundRelogin(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "management-serialization")
	fake := &fakeChatGPTWebAuthService{}
	started := make(chan struct{})
	continueLogin := make(chan struct{})
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		select {
		case <-continueLogin:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "serialized-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	executor.TriggerBackgroundRelogin(expected)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background re-login did not start")
	}

	type operationResult struct {
		release func()
		err     error
	}
	acquired := make(chan operationResult, 1)
	email, _ := expected.Metadata["email"].(string)
	go func() {
		_, release, errAcquire := executor.BeginLoginOperation(t.Context(), email)
		acquired <- operationResult{release: release, err: errAcquire}
	}()
	select {
	case result := <-acquired:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("management login acquired while background re-login was active: %v", result.err)
	case <-time.After(50 * time.Millisecond):
	}

	close(continueLogin)
	select {
	case result := <-acquired:
		if result.err != nil {
			t.Fatalf("BeginLoginOperation() error = %v", result.err)
		}
		result.release()
	case <-time.After(time.Second):
		t.Fatal("management login did not acquire after background re-login finished")
	}
}

func TestChatGPTWebExecutorLoginCoordinatorSurvivesReplacement(t *testing.T) {
	coordinator := NewChatGPTWebLoginCoordinator()
	previous := NewChatGPTWebExecutorWithLoginCoordinator(&config.Config{}, nil, coordinator)
	replacement := NewChatGPTWebExecutorWithLoginCoordinator(&config.Config{}, nil, coordinator)
	t.Cleanup(func() {
		_ = previous.Close()
		_ = replacement.Close()
	})

	_, releasePrevious, errPrevious := previous.BeginLoginOperation(t.Context(), "shared@example.com")
	if errPrevious != nil {
		t.Fatal(errPrevious)
	}
	type operationResult struct {
		release func()
		err     error
	}
	acquired := make(chan operationResult, 1)
	go func() {
		_, release, errAcquire := replacement.BeginLoginOperation(t.Context(), "SHARED@example.com")
		acquired <- operationResult{release: release, err: errAcquire}
	}()
	select {
	case result := <-acquired:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("replacement executor bypassed previous login operation: %v", result.err)
	case <-time.After(50 * time.Millisecond):
	}

	releasePrevious()
	select {
	case result := <-acquired:
		if result.err != nil {
			t.Fatalf("replacement BeginLoginOperation() error = %v", result.err)
		}
		result.release()
	case <-time.After(time.Second):
		t.Fatal("replacement executor did not acquire shared login coordinator")
	}
}

func TestChatGPTWebExecutorBackgroundReloginRechecksGenerationAfterAccountGate(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "queued-generation")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		t.Fatal("superseded queued re-login must not reach the login service")
		return nil, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	email, _ := expected.Metadata["email"].(string)
	_, releaseGate, errGate := executor.BeginLoginOperation(t.Context(), email)
	if errGate != nil {
		t.Fatal(errGate)
	}
	slotAcquired := make(chan struct{}, 1)
	executor.reloginSlotAcquired = func() { slotAcquired <- struct{}{} }
	type reloginResult struct {
		auth    *cliproxyauth.Auth
		current bool
		err     error
	}
	done := make(chan reloginResult, 1)
	go func() {
		updated, current, errRelogin := executor.reloginCurrentWithMode(t.Context(), expected, true)
		done <- reloginResult{auth: updated, current: current, err: errRelogin}
	}()
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		executor.loginCoordinator.mu.Lock()
		defer executor.loginCoordinator.mu.Unlock()
		gate := executor.loginCoordinator.gates[strings.ToLower(email)]
		return gate != nil && gate.refs == 2
	})
	select {
	case <-slotAcquired:
		t.Fatal("queued re-login consumed a global slot before acquiring its account gate")
	default:
	}

	for range 2 {
		replacement, _ := manager.GetByID(expected.ID)
		if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); errUpdate != nil {
			t.Fatal(errUpdate)
		}
	}
	releaseGate()
	result := <-done
	if result.current || !errors.Is(result.err, chatgptwebauth.ErrCredentialSuperseded) {
		t.Fatalf("queued re-login = (%v, %v, %v), want superseded", result.auth, result.current, result.err)
	}
	if got := fake.loginCalls.Load(); got != 0 {
		t.Fatalf("login calls = %d, want 0", got)
	}
	select {
	case <-slotAcquired:
		t.Fatal("superseded queued re-login consumed a global slot")
	default:
	}
}

func TestChatGPTWebExecutorReloginUsesConditionalUpdate(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("relogin")
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, _ := manager.GetByID(auth.ID)

	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential := *input.Credential
		credential.AccessToken = "stale-login-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake

	type reloginResult struct {
		auth    *cliproxyauth.Auth
		current bool
		err     error
	}
	result := make(chan reloginResult, 1)
	go func() {
		updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
		result <- reloginResult{auth: updated, current: current, err: errRelogin}
	}()
	<-started
	replacement := expected.Clone()
	replacement.Metadata["access_token"] = "manual-token"
	replacement.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateActive
	replacement.Attributes[cliproxyauth.SourceHashAttributeKey] = "manual-source"
	if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	close(release)
	relogin := <-result
	if relogin.current {
		t.Fatal("stale re-login unexpectedly replaced the current auth")
	}
	if !errors.Is(relogin.err, chatgptwebauth.ErrCredentialSuperseded) {
		t.Fatalf("stale re-login error = %v, want superseded", relogin.err)
	}
	if relogin.auth == nil || relogin.auth.Metadata["access_token"] != "manual-token" {
		t.Fatalf("stale re-login latest auth = %#v", relogin.auth)
	}
	got, _ := manager.GetByID(auth.ID)
	if got.Metadata["access_token"] != "manual-token" {
		t.Fatalf("access token = %v, want manual-token", got.Metadata["access_token"])
	}
}

func TestChatGPTWebExecutorReloginMergesConcurrentRuntimeCookieRotation(t *testing.T) {
	manager := cliproxyauth.NewManager(&chatGPTWebReloginSourceHashStore{}, nil, nil)
	auth := chatGPTWebTestAuth("relogin-cookie-rotation")
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	remaining := 24
	credential.ImageQuotaRemaining = &remaining
	credential.Cookies = []chatgptwebauth.Cookie{
		{Name: "__Secure-next-auth.session-token.0", Value: "baseline-a", Domain: ".chatgpt.com", Path: "/"},
		{Name: "__Secure-next-auth.session-token.1", Value: "baseline-b", Domain: ".chatgpt.com", Path: "/"},
	}
	credential.LifecycleState = chatgptwebauth.LifecycleReloginPending
	credential.ApplyToMetadata(auth.Metadata)
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, _ := manager.GetByID(auth.ID)
	expectedSourceHash := expected.Attributes[cliproxyauth.SourceHashAttributeKey]
	expectedGeneration := chatGPTWebReloginGenerationKey(expected)

	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		result := cloneChatGPTWebCredential(input.Credential)
		result.AccessToken = "fresh-login-token"
		result.Cookies = []chatgptwebauth.Cookie{
			{Name: "__Secure-next-auth.session-token.0", Value: "relogin-a", Domain: ".chatgpt.com", Path: "/"},
			{Name: "__Secure-next-auth.session-token.1", Value: "relogin-b", Domain: ".chatgpt.com", Path: "/"},
		}
		result.LifecycleState = chatgptwebauth.LifecycleActive
		return result, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake

	type reloginResult struct {
		auth    *cliproxyauth.Auth
		current bool
		err     error
	}
	done := make(chan reloginResult, 1)
	go func() {
		updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
		done <- reloginResult{auth: updated, current: current, err: errRelogin}
	}()
	<-started

	concurrent, current, errMutate := manager.MutateRuntimeMetadataIfCurrent(t.Context(), expected, func(candidate *cliproxyauth.Auth) {
		currentCredential, errParse := chatgptwebauth.ParseCredential(candidate.Metadata)
		if errParse != nil {
			t.Errorf("parse concurrent credential: %v", errParse)
			return
		}
		concurrentRemaining := 23
		currentCredential.AccountID = "account-a"
		currentCredential.UserID = "user-a"
		currentCredential.ImageQuotaRemaining = &concurrentRemaining
		currentCredential.Cookies = []chatgptwebauth.Cookie{
			{Name: "__Secure-next-auth.session-token.0", Value: "runtime-a", Domain: ".chatgpt.com", Path: "/"},
			{Name: "__Secure-next-auth.session-token.1", Value: "runtime-b", Domain: ".chatgpt.com", Path: "/"},
			{Name: "concurrent", Value: "kept", Domain: ".chatgpt.com", Path: "/"},
		}
		currentCredential.ApplyToMetadata(candidate.Metadata)
	})
	if errMutate != nil || !current || concurrent == nil {
		t.Fatalf("concurrent mutation = (%v, %v, %v)", concurrent, current, errMutate)
	}
	if concurrent.Attributes[cliproxyauth.SourceHashAttributeKey] == expectedSourceHash {
		t.Fatal("concurrent cookie rotation did not advance the persisted source hash")
	}
	if got := chatGPTWebReloginGenerationKey(concurrent); got != expectedGeneration {
		t.Fatalf("re-login generation changed after runtime cookie rotation: %q != %q", got, expectedGeneration)
	}
	close(release)

	relogin := <-done
	if relogin.err != nil || !relogin.current || relogin.auth == nil {
		t.Fatalf("re-login = (%v, %v, %v)", relogin.auth, relogin.current, relogin.err)
	}
	installedCredential, errParse := chatgptwebauth.ParseCredential(relogin.auth.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	if installedCredential.AccessToken != "fresh-login-token" {
		t.Fatalf("access token = %q", installedCredential.AccessToken)
	}
	if installedCredential.ImageQuotaRemaining == nil || *installedCredential.ImageQuotaRemaining != 23 {
		t.Fatalf("image quota remaining = %v, want 23", installedCredential.ImageQuotaRemaining)
	}
	if installedCredential.AccountID != "account-a" || installedCredential.UserID != "user-a" {
		t.Fatalf("runtime identity = (%q, %q)", installedCredential.AccountID, installedCredential.UserID)
	}
	cookieValues := make(map[string]string, len(installedCredential.Cookies))
	for _, cookie := range installedCredential.Cookies {
		cookieValues[cookie.Name] = cookie.Value
	}
	if cookieValues["__Secure-next-auth.session-token.0"] != "relogin-a" ||
		cookieValues["__Secure-next-auth.session-token.1"] != "relogin-b" ||
		cookieValues["concurrent"] != "kept" {
		t.Fatalf("merged cookies = %#v", installedCredential.Cookies)
	}
}

func TestChatGPTWebExecutorReloginRejectsSupersededAuthBeforeFlightStarts(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("relogin-before-flight")
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, _ := manager.GetByID(auth.ID)

	replacement := expected.Clone()
	replacement.Metadata["access_token"] = "manual-token"
	replacement.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateActive
	replacement.Attributes[cliproxyauth.SourceHashAttributeKey] = "manual-source"
	if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatal(errUpdate)
	}

	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		t.Fatal("superseded auth must not start a re-login")
		return nil, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if current || !errors.Is(errRelogin, chatgptwebauth.ErrCredentialSuperseded) {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v), want superseded", updated, current, errRelogin)
	}
	if updated == nil || updated.Metadata["access_token"] != "manual-token" {
		t.Fatalf("latest auth = %#v, want replacement", updated)
	}
}

func TestChatGPTWebExecutorReloginResultDoesNotShareCredentialMetadata(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "clone-relogin")
	cleanupDone := expected.RuntimeInstanceCleanupDone()
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		credential := cloneChatGPTWebCredential(input.Credential)
		credential.AccessToken = "fresh-token"
		credential.Cookies = []chatgptwebauth.Cookie{{Name: "session", Value: "fresh-cookie"}}
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if errRelogin != nil || !current || updated == nil {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v)", updated, current, errRelogin)
	}
	if updated.RuntimeInstanceID() == expected.RuntimeInstanceID() {
		t.Fatal("successful re-login reused the previous runtime instance")
	}
	if _, release, active := expected.BeginRuntimeExecution(t.Context()); active {
		release()
		t.Fatal("successful re-login left the previous runtime instance active")
	}
	cookies := updated.Metadata["cookies"].([]chatgptwebauth.Cookie)
	cookies[0].Value = "caller-mutation"
	installed, _ := manager.GetByID(expected.ID)
	installedCookies := installed.Metadata["cookies"].([]chatgptwebauth.Cookie)
	if installedCookies[0].Value != "fresh-cookie" {
		t.Fatalf("installed cookie = %q, want independent metadata", installedCookies[0].Value)
	}
	select {
	case <-cleanupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("successful re-login cleanup did not finish")
	}
}

func chatGPTWebTestAuth(id string) *cliproxyauth.Auth {
	credential := &chatgptwebauth.Credential{
		Type:           chatgptwebauth.Provider,
		Email:          id + "@example.com",
		Password:       "password",
		TOTPSecret:     "JBSWY3DPEHPK3PXP",
		AccessToken:    "access-token",
		RefreshToken:   "refresh-token",
		Persona:        chatgptwebauth.DefaultPersona(),
		LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	return &cliproxyauth.Auth{
		ID:         "chatgpt-web-" + id,
		Provider:   chatgptwebauth.Provider,
		Status:     cliproxyauth.StatusActive,
		Attributes: map[string]string{cliproxyauth.SourceHashAttributeKey: "source-" + id},
		Metadata:   metadata,
	}
}

func chatGPTWebCompleteSessionCookies() []chatgptwebauth.Cookie {
	return []chatgptwebauth.Cookie{
		{
			Name:     "__Secure-next-auth.session-token.0",
			Value:    "first-half",
			Domain:   "chatgpt.com",
			Path:     "/",
			Secure:   true,
			HTTPOnly: true,
		},
		{
			Name:     "__Secure-next-auth.session-token.1",
			Value:    "second-half",
			Domain:   "chatgpt.com",
			Path:     "/",
			Secure:   true,
			HTTPOnly: true,
		},
	}
}

func registerChatGPTWebPendingAuth(t *testing.T, manager *cliproxyauth.Manager, id string) *cliproxyauth.Auth {
	t.Helper()
	auth := chatGPTWebTestAuth(id)
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatalf("registered auth %q not found", auth.ID)
	}
	return expected
}

func waitForChatGPTWebReloginWaiters(t *testing.T, executor *ChatGPTWebExecutor, auth *cliproxyauth.Auth, want int) {
	t.Helper()
	key := chatGPTWebReloginGenerationKey(auth)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		executor.reloginMu.Lock()
		defer executor.reloginMu.Unlock()
		flight := executor.reloginFlights[key]
		return flight != nil && flight.waiters == want
	})
}

func waitForChatGPTWebCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for condition")
		case <-ticker.C:
		}
	}
}
