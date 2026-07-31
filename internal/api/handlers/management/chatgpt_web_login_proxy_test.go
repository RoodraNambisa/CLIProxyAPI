package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type loginProxyConfigUpdateTestExecutor struct {
	coreauth.ProviderExecutor
	updates  int
	resolved config.ResolvedChatGPTWebLoginProxyConfig
}

func (executor *loginProxyConfigUpdateTestExecutor) Identifier() string {
	return chatgptwebauth.Provider
}

func (executor *loginProxyConfigUpdateTestExecutor) UpdateConfig(cfg *config.Config) {
	executor.updates++
	executor.resolved = cfg.ChatGPTWeb.LoginProxy.Resolved()
}

func TestGetChatGPTWebLoginProxyReturnsCompleteTemplateWithoutCaching(t *testing.T) {
	const template = "http://session-{12}:plain-secret@proxy.example:59999"
	handler := &Handler{cfg: &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		LoginProxy: config.ChatGPTWebLoginProxyConfig{Enabled: true, URLTemplate: template},
	}}}
	ctx, recorder := newChatGPTWebLoginProxyRequest(http.MethodGet, "")
	handler.GetChatGPTWebLoginProxy(ctx)

	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("GET response = %d, Cache-Control=%q", recorder.Code, recorder.Header().Get("Cache-Control"))
	}
	response := decodeChatGPTWebLoginProxyResponse(t, recorder)
	if response.URLTemplate != template || !strings.Contains(recorder.Body.String(), "plain-secret") {
		t.Fatalf("GET masked or changed the URL template: %s", recorder.Body.String())
	}
	if response.RequestAttempts != 3 || response.FlowAttempts != 2 ||
		response.RetryDelayMilliseconds != 800 || response.AcquisitionTimeoutSeconds != 90 {
		t.Fatalf("GET defaults = %#v", response)
	}
}

func TestPutChatGPTWebLoginProxyPersistsAndHotReloads(t *testing.T) {
	handler, configPath := newPersistedChatGPTWebLoginProxyHandler(t, config.ChatGPTWebLoginProxyConfig{})
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &loginProxyConfigUpdateTestExecutor{}
	manager.RegisterExecutor(executor)
	handler.authManager = manager

	body := `{
		"enabled":true,
		"url-template":"socks5h://session-{8}:secret@proxy.example:1080",
		"placeholder-charset":"abc123",
		"rotate-on-retry":false,
		"request-attempts":4,
		"flow-attempts":3,
		"retry-delay-milliseconds":0,
		"acquisition-timeout-seconds":120
	}`
	ctx, recorder := newChatGPTWebLoginProxyRequest(http.MethodPut, body)
	handler.PutChatGPTWebLoginProxy(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("PUT status = %d: %s", recorder.Code, recorder.Body.String())
	}
	resolved := handler.cfg.ChatGPTWeb.LoginProxy.Resolved()
	if !resolved.Enabled || resolved.RotateOnRetry || resolved.RequestAttempts != 4 ||
		resolved.FlowAttempts != 3 || resolved.RetryDelayMilliseconds != 0 ||
		resolved.AcquisitionTimeoutSeconds != 120 {
		t.Fatalf("resolved config = %#v", resolved)
	}
	if executor.updates != 1 || executor.resolved.URLTemplate != resolved.URLTemplate {
		t.Fatalf("runtime update = %d %#v", executor.updates, executor.resolved)
	}
	loaded, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	if loaded.ChatGPTWeb.LoginProxy.Resolved() != resolved {
		t.Fatalf("persisted config = %#v, want %#v", loaded.ChatGPTWeb.LoginProxy.Resolved(), resolved)
	}
}

func TestPatchChatGPTWebLoginProxyIsStrictAndPreservesOtherFields(t *testing.T) {
	rotate := true
	attempts := 4
	flows := 3
	delay := 200
	timeout := 120
	initial := config.ChatGPTWebLoginProxyConfig{
		Enabled:                   true,
		URLTemplate:               "http://session-{8}:secret@proxy.example:8080",
		PlaceholderCharset:        "abc123",
		RotateOnRetry:             &rotate,
		RequestAttempts:           &attempts,
		FlowAttempts:              &flows,
		RetryDelayMilliseconds:    &delay,
		AcquisitionTimeoutSeconds: &timeout,
	}
	handler, _ := newPersistedChatGPTWebLoginProxyHandler(t, initial)
	ctx, recorder := newChatGPTWebLoginProxyRequest(http.MethodPatch, `{"request-attempts":2}`)
	handler.PatchChatGPTWebLoginProxy(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d: %s", recorder.Code, recorder.Body.String())
	}
	resolved := handler.cfg.ChatGPTWeb.LoginProxy.Resolved()
	if resolved.RequestAttempts != 2 || resolved.FlowAttempts != 3 || resolved.URLTemplate != initial.URLTemplate {
		t.Fatalf("PATCH replaced unsubmitted values: %#v", resolved)
	}

	for _, body := range []string{
		`{"unknown":true}`,
		`{"request-attempts":11}`,
		`{"url-template":"http://proxy-{3}.example:8080"}`,
		`{"enabled":null}`,
	} {
		ctx, recorder = newChatGPTWebLoginProxyRequest(http.MethodPatch, body)
		handler.PatchChatGPTWebLoginProxy(ctx)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("PATCH %s status = %d, want 400: %s", body, recorder.Code, recorder.Body.String())
		}
	}
	ctx, recorder = newChatGPTWebLoginProxyRequest(http.MethodPut, `{"enabled":false}`)
	handler.PutChatGPTWebLoginProxy(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("incomplete PUT status = %d, want 400", recorder.Code)
	}
}

func TestChatGPTWebLoginProxyRollsBackAfterPersistFailure(t *testing.T) {
	initial := config.ChatGPTWebLoginProxyConfig{URLTemplate: "http://user:secret@proxy.example:8080"}
	handler := &Handler{
		cfg:            &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{LoginProxy: initial}},
		configFilePath: filepath.Join(t.TempDir(), "missing", "config.yaml"),
	}
	ctx, recorder := newChatGPTWebLoginProxyRequest(http.MethodPatch, `{"request-attempts":4}`)
	handler.PatchChatGPTWebLoginProxy(ctx)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("PATCH status = %d, want 500: %s", recorder.Code, recorder.Body.String())
	}
	if handler.cfg.ChatGPTWeb.LoginProxy.Resolved().RequestAttempts != 3 {
		t.Fatalf("in-memory config was not rolled back: %#v", handler.cfg.ChatGPTWeb.LoginProxy.Resolved())
	}
}

func TestChatGPTWebManagementCloudflareDiagnosticsAreSafe(t *testing.T) {
	errLogin := &chatgptwebauth.AuthError{
		Code:           "cloudflare_challenge",
		State:          chatgptwebauth.LifecycleLoginPending,
		LifecycleState: chatgptwebauth.LifecycleLoginPending,
		StatusCode:     http.StatusForbidden,
		Retryable:      true,
		FailureStage:   "authorize",
		Attempts:       2,
	}
	category, message, status, lifecycle := classifyChatGPTWebManagementError(errLogin)
	failureStage, attempts := chatGPTWebManagementErrorDiagnostics(errLogin)
	if category != "cloudflare_challenge" ||
		message != "Cloudflare challenge blocked authentication" ||
		status != http.StatusServiceUnavailable ||
		lifecycle != string(chatgptwebauth.LifecycleLoginPending) ||
		failureStage != "authorize" ||
		attempts != 2 {
		t.Fatalf(
			"classification = %q %q %d %q %q %d",
			category,
			message,
			status,
			lifecycle,
			failureStage,
			attempts,
		)
	}
}

func newPersistedChatGPTWebLoginProxyHandler(t *testing.T, loginProxy config.ChatGPTWebLoginProxyConfig) (*Handler, string) {
	t.Helper()
	configPath := writeTestConfigFile(t)
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{LoginProxy: loginProxy}}
	return &Handler{cfg: cfg, configFilePath: configPath}, configPath
}

func newChatGPTWebLoginProxyRequest(method, body string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(method, "/v0/management/chatgpt-web/login-proxy", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	return ctx, recorder
}

func decodeChatGPTWebLoginProxyResponse(t *testing.T, recorder *httptest.ResponseRecorder) config.ResolvedChatGPTWebLoginProxyConfig {
	t.Helper()
	var response config.ResolvedChatGPTWebLoginProxyConfig
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("json.Unmarshal() error = %v; body=%s", errDecode, recorder.Body.String())
	}
	return response
}
