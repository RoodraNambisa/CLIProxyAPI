package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestListAuthFilesIncludesSafeChatGPTWebLifecycleAndProxyBinding(t *testing.T) {
	const (
		proxyPassword  = "proxy-password-secret"
		passwordSecret = "credential-password-secret"
		tokenSecret    = "credential-token-secret"
	)
	authDir := t.TempDir()
	authPath := filepath.Join(authDir, "chatgpt-web-summary.json")
	now := time.Now().UTC()
	metadata := map[string]any{
		"type":                 chatgptwebauth.Provider,
		"email":                "summary@example.com",
		"password":             passwordSecret,
		"totp_secret":          "JBSWY3DPEHPK3PXP",
		"access_token":         tokenSecret,
		"refresh_token":        "refresh-token-secret",
		"expired":              now.Add(-time.Minute).Format(time.RFC3339),
		"lifecycle_state":      string(chatgptwebauth.LifecycleDead),
		"lifecycle_reason":     "account_deleted",
		"lifecycle_updated_at": now.Add(-time.Hour).Format(time.RFC3339),
		"last_login_at":        now.Add(-2 * time.Hour).Format(time.RFC3339),
		"last_refresh_at":      now.Add(-90 * time.Minute).Format(time.RFC3339),
		"last_relogin_at":      now.Add(-time.Hour).Format(time.RFC3339),
	}
	data, errMarshal := json.Marshal(metadata)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	if errWrite := os.WriteFile(authPath, data, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	authManager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:            filepath.Base(authPath),
		Provider:      chatgptwebauth.Provider,
		FileName:      filepath.Base(authPath),
		Label:         "summary@example.com",
		Status:        coreauth.RuntimeStatusForLifecycle(coreauth.LifecycleStateDead),
		StatusMessage: "account_deleted",
		Attributes:    map[string]string{"path": authPath, "source": authPath},
		Metadata:      metadata,
	}
	if _, errRegister := authManager.Register(t.Context(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}

	traceProxy := newProxyRuntimeTraceProxy(t)
	proxyURL := proxyURLWithCredential(t, traceProxy, proxyPassword)
	cfg := &config.Config{
		AuthDir: authDir,
		SDKConfig: config.SDKConfig{
			ProxyPools: []config.ProxyPoolConfig{{
				Name:         "chatgpt-web",
				BindAttempts: 1,
				Entries: []config.ProxyPoolEntryConfig{{
					ID:          "node",
					URLTemplate: proxyURL,
				}},
			}},
			ProxyRules: []config.ProxyRuleConfig{{
				Name:      "chatgpt-web",
				Pool:      "chatgpt-web",
				Providers: []string{chatgptwebauth.Provider},
			}},
		},
	}
	proxyManager, errProxyManager := proxypool.NewManager("", cfg)
	if errProxyManager != nil {
		t.Fatal(errProxyManager)
	}
	proxyManager.SetAuthSource(authManager)
	authManager.SetProxyResolver(proxyManager)
	if _, errResolve := authManager.ResolveProxyAuth(context.Background(), auth); errResolve != nil {
		t.Fatalf("resolve proxy: %v", errResolve)
	}

	h := NewHandler(cfg, "", authManager)
	h.SetProxyPoolManager(proxyManager)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if errShutdown := h.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown management handler: %v", errShutdown)
		}
	})
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/auth-files", h.ListAuthFiles)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/auth-files", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	assertChatGPTWebManagementSecretsAbsent(t, recorder.Body.String(), proxyPassword, passwordSecret, tokenSecret, "refresh-token-secret", "JBSWY3DPEHPK3PXP")
	var response struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatal(errDecode)
	}
	if len(response.Files) != 1 {
		t.Fatalf("files = %+v", response.Files)
	}
	entry := response.Files[0]
	if entry["lifecycle_state"] != string(chatgptwebauth.LifecycleDead) || entry["reason"] != "account_deleted" {
		t.Fatalf("lifecycle summary = %+v", entry)
	}
	if entry["token_expired"] != true || entry["token_refreshable"] != true {
		t.Fatalf("token summary = %+v", entry)
	}
	binding, ok := entry["proxy_binding"].(map[string]any)
	if !ok || binding["pool"] != "chatgpt-web" {
		t.Fatalf("proxy binding = %#v", entry["proxy_binding"])
	}
	proxyText, _ := binding["proxy_url"].(string)
	if !strings.Contains(proxyText, "********") || strings.Contains(proxyText, proxyPassword) {
		t.Fatalf("masked proxy = %q", proxyText)
	}
}

func TestApplyChatGPTWebMetadataSummarySanitizesLifecycleReason(t *testing.T) {
	entry := gin.H{"status_message": "tokenLikeABC123"}
	applyChatGPTWebMetadataSummary(
		entry,
		map[string]any{"lifecycle_reason": "tokenLikeABC123"},
		string(chatgptwebauth.LifecycleReauthRequired),
		time.Now(),
	)
	for _, key := range []string{"lifecycle_reason", "reason"} {
		if entry[key] != "authentication_failed" {
			t.Fatalf("%s = %v", key, entry[key])
		}
	}
	if entry["status_message"] != "authentication_failed" {
		t.Fatalf("status_message = %v, want sanitized lifecycle reason", entry["status_message"])
	}
	applyChatGPTWebMetadataSummary(
		entry,
		map[string]any{"lifecycle_reason": "account_deleted"},
		"tokenLikeStateABC123",
		time.Now(),
	)
	if entry["lifecycle_state"] != string(chatgptwebauth.LifecycleReauthRequired) {
		t.Fatalf("lifecycle_state = %v", entry["lifecycle_state"])
	}
}

func TestAuthFileRuntimeSummaryForLinkedWebUsesSourceBinding(t *testing.T) {
	source := &coreauth.Auth{ID: "codex-source.json", Provider: "codex", Metadata: map[string]any{
		"credential_uid": "uid-a",
	}}
	web := &coreauth.Auth{ID: "web.json", Provider: chatgptwebauth.Provider, Metadata: map[string]any{
		"refresh_strategy": "codex_source", "source_auth_id": source.ID, "source_credential_uid": "uid-a",
	}}
	binding := &proxypool.BindingStatus{AuthID: source.ID, CredentialUID: "uid-a", Pool: "residential", BindingID: "binding-a"}
	summaries := map[string]authFileRuntimeSummary{
		source.ID: {proxyBinding: binding},
		web.ID:    {proxyBinding: &proxypool.BindingStatus{AuthID: web.ID, Pool: "wrong", BindingID: "binding-web"}},
	}
	graph := coreauth.BuildChatGPTWebDependencyGraph([]*coreauth.Auth{source, web})
	got := authFileRuntimeSummaryForAuth(web, graph, summaries)
	if got.proxyBinding == nil || got.proxyBinding.BindingID != "binding-a" || got.proxyBinding.AuthID != source.ID {
		t.Fatalf("linked runtime summary = %+v", got.proxyBinding)
	}
}

func TestAuthFileRuntimeSummaryForLinkedWebRejectsReplacedSourceIdentity(t *testing.T) {
	original := &coreauth.Auth{ID: "codex-source.json", Provider: "codex", Metadata: map[string]any{
		"credential_uid": "uid-a", "account_id": "account-a",
	}}
	identitySource := original.Clone()
	identitySource.Provider = chatgptwebauth.Provider
	web := &coreauth.Auth{ID: "web.json", Provider: chatgptwebauth.Provider, Metadata: map[string]any{
		"refresh_strategy": "codex_source", "source_auth_id": original.ID, "source_credential_uid": "uid-a",
		"source_identity": coreauth.ChatGPTWebCredentialReferenceValue(identitySource),
	}}
	replacement := original.Clone()
	replacement.Metadata = map[string]any{"credential_uid": "uid-a", "account_id": "account-b"}
	graph := coreauth.BuildChatGPTWebDependencyGraph([]*coreauth.Auth{replacement, web})
	got := authFileRuntimeSummaryForAuth(web, graph, map[string]authFileRuntimeSummary{
		replacement.ID: {proxyBinding: &proxypool.BindingStatus{AuthID: replacement.ID, CredentialUID: "uid-a", BindingID: "wrong-account"}},
	})
	if got.proxyBinding != nil {
		t.Fatalf("replaced source runtime summary = %+v", got.proxyBinding)
	}
	if !retainedSourceMissing(web, graph) {
		t.Fatal("replaced source identity was not marked missing")
	}
}

func TestAuthFileRuntimeSummaryForLinkedWebUsesMissingSourceBindingGeneration(t *testing.T) {
	web := &coreauth.Auth{ID: "web.json", Provider: chatgptwebauth.Provider, Metadata: map[string]any{
		"refresh_strategy": "codex_source", "source_auth_id": "missing-source.json", "source_credential_uid": "uid-a",
	}}
	matching := &proxypool.BindingStatus{AuthID: "missing-source.json", CredentialUID: "uid-a", BindingID: "binding-a"}
	graph := coreauth.BuildChatGPTWebDependencyGraph([]*coreauth.Auth{web})
	got := authFileRuntimeSummaryForAuth(web, graph, map[string]authFileRuntimeSummary{
		"missing-source.json": {proxyBinding: matching},
	})
	if got.proxyBinding == nil || got.proxyBinding.BindingID != "binding-a" {
		t.Fatalf("missing-source runtime summary = %+v", got.proxyBinding)
	}
	wrong := authFileRuntimeSummaryForAuth(web, graph, map[string]authFileRuntimeSummary{
		"missing-source.json": {proxyBinding: &proxypool.BindingStatus{AuthID: "missing-source.json", CredentialUID: "uid-b"}},
	})
	if wrong.proxyBinding != nil {
		t.Fatalf("mismatched generation runtime summary = %+v", wrong.proxyBinding)
	}
}

func TestApplyChatGPTWebAuthFileSummarySanitizesLastError(t *testing.T) {
	entry := gin.H{
		"status_message": "secret-token-in-status",
		"last_error":     "secret-token-in-error",
	}
	auth := &coreauth.Auth{
		Provider:      chatgptwebauth.Provider,
		StatusMessage: "secret-token-in-status",
		Attributes:    map[string]string{"runtime_only": "true"},
		Metadata: map[string]any{
			"lifecycle_state":  string(chatgptwebauth.LifecycleDead),
			"lifecycle_reason": "account_deleted",
		},
		LastError: &coreauth.Error{
			Code:       "upstream-secret-code",
			Message:    "secret-token-in-error",
			HTTPStatus: http.StatusUnauthorized,
		},
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	entry = h.buildAuthFileEntryAtWithRuntime(auth, time.Now(), authFileRuntimeSummary{})

	if entry["status_message"] != "account_deleted" {
		t.Fatalf("status_message = %v", entry["status_message"])
	}
	lastError, ok := entry["last_error"].(*coreauth.Error)
	if !ok {
		t.Fatalf("last_error = %#v", entry["last_error"])
	}
	if lastError.Code != "account_deleted" || lastError.Message != "account is deleted" || lastError.HTTPStatus != http.StatusUnauthorized {
		t.Fatalf("last_error = %+v", lastError)
	}
	raw, errMarshal := json.Marshal(entry)
	if errMarshal != nil {
		t.Fatalf("marshal entry: %v", errMarshal)
	}
	if strings.Contains(string(raw), "secret-token") {
		t.Fatalf("entry leaked runtime error: %s", raw)
	}
}

func TestApplyChatGPTWebAuthFileSummaryClassifiesRateLimitCooldown(t *testing.T) {
	now := time.Now()
	auth := &coreauth.Auth{
		Provider:    chatgptwebauth.Provider,
		Status:      coreauth.StatusError,
		Unavailable: true,
		ModelStates: map[string]*coreauth.ModelState{
			"gpt-image-2": {Status: coreauth.StatusError, Unavailable: true, NextRetryAfter: now.Add(time.Minute)},
		},
		Metadata: map[string]any{"lifecycle_state": string(chatgptwebauth.LifecycleActive)},
		LastError: &coreauth.Error{
			HTTPStatus: http.StatusTooManyRequests,
			Message:    "upstream-secret-message",
		},
	}
	entry := gin.H{}
	applyChatGPTWebAuthFileSummary(entry, auth, now)
	lastError, ok := entry["last_error"].(*coreauth.Error)
	if !ok || lastError.Code != "rate_limited" || lastError.Message != "credential was rate limited" {
		t.Fatalf("last_error = %#v", entry["last_error"])
	}
}

func TestParseLastRefreshValueRejectsJSONUnsafeTimestamps(t *testing.T) {
	for _, value := range []any{
		float64(1_000_000_000_000),
		int64(1_000_000_000_000),
		1_000_000_000_000,
		json.Number("1000000000000"),
		"1000000000000",
		"10000-01-01T00:00:00Z",
	} {
		if timestamp, ok := parseLastRefreshValue(value); ok {
			t.Fatalf("parseLastRefreshValue(%v) = %v, true", value, timestamp)
		}
	}
	if timestamp, ok := parseLastRefreshValue(int64(1_700_000_000)); !ok || timestamp.Unix() != 1_700_000_000 {
		t.Fatalf("valid timestamp = %v, %v", timestamp, ok)
	}
}

func TestListAuthFilesFromDiskPreservesDisabledChatGPTWebStatus(t *testing.T) {
	authDir := t.TempDir()
	data := []byte(`{
		"type":"chatgpt-web",
		"email":"disabled@example.com",
		"access_token":"token",
		"lifecycle_state":"active",
		"disabled":true
	}`)
	if errWrite := os.WriteFile(filepath.Join(authDir, "disabled.json"), data, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	h := NewHandler(&config.Config{AuthDir: authDir}, "", nil)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/auth-files", h.ListAuthFiles)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/auth-files", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatal(errDecode)
	}
	if len(response.Files) != 1 {
		t.Fatalf("files = %+v", response.Files)
	}
	entry := response.Files[0]
	if entry["disabled"] != true || entry["status"] != string(coreauth.StatusDisabled) {
		t.Fatalf("disabled disk entry = %+v", entry)
	}
	if entry["lifecycle_state"] != string(chatgptwebauth.LifecycleActive) {
		t.Fatalf("lifecycle state = %v, want active", entry["lifecycle_state"])
	}
}
