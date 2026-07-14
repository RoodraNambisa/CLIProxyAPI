package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gin "github.com/gin-gonic/gin"
	proxyconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	internallogging "github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func newTestServer(t *testing.T) *Server {
	return newTestServerWithConfig(t, nil)
}

func newTestServerWithConfig(t *testing.T, mutate func(*proxyconfig.Config)) *Server {
	t.Helper()

	gin.SetMode(gin.TestMode)

	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o700); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}

	cfg := &proxyconfig.Config{
		SDKConfig: sdkconfig.SDKConfig{
			APIKeys: []string{"test-key"},
		},
		Port:                   0,
		AuthDir:                authDir,
		Debug:                  true,
		LoggingToFile:          false,
		UsageStatisticsEnabled: false,
	}
	if mutate != nil {
		mutate(cfg)
	}

	authManager := auth.NewManager(nil, nil, nil)
	accessManager := sdkaccess.NewManager()

	configPath := filepath.Join(tmpDir, "config.yaml")
	return NewServer(cfg, authManager, accessManager, configPath)
}

func TestHealthz(t *testing.T) {
	server := newTestServer(t)

	t.Run("GET", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rr := httptest.NewRecorder()
		server.engine.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("unexpected status code: got %d want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
		}

		var resp struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response JSON: %v; body=%s", err, rr.Body.String())
		}
		if resp.Status != "ok" {
			t.Fatalf("unexpected response status: got %q want %q", resp.Status, "ok")
		}
	})

	t.Run("HEAD", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/healthz", nil)
		rr := httptest.NewRecorder()
		server.engine.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("unexpected status code: got %d want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
		}
		if rr.Body.Len() != 0 {
			t.Fatalf("expected empty body for HEAD request, got %q", rr.Body.String())
		}
	})
}

func TestInteractionsRoutesAreRegistered(t *testing.T) {
	server := newTestServer(t)
	for _, path := range []string{"/v1/interactions", "/v1beta/interactions"} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"gemini-3.5-flash","input":"hi"}`))
			req.Header.Set("Authorization", "Bearer test-key")
			recorder := httptest.NewRecorder()
			server.engine.ServeHTTP(recorder, req)
			if recorder.Code == http.StatusNotFound {
				t.Fatalf("route %s is not registered", path)
			}
		})
	}
}

func TestLightweightUsageAnalyticsRoutesAreRegistered(t *testing.T) {
	server := newTestServerWithConfig(t, func(cfg *proxyconfig.Config) {
		cfg.RemoteManagement.SecretKey = "secret"
	})
	want := map[string]bool{
		http.MethodGet + " /v0/management/usage/health": false,
		http.MethodGet + " /v0/management/usage/rates":  false,
		http.MethodGet + " /v0/management/usage/tokens": false,
	}
	for _, route := range server.engine.Routes() {
		key := route.Method + " " + route.Path
		if _, ok := want[key]; ok {
			want[key] = true
		}
	}
	for route, registered := range want {
		if !registered {
			t.Errorf("route %s is not registered", route)
		}
	}
}

func TestAmpRoutesAreRemoved(t *testing.T) {
	server := newTestServer(t)
	tests := []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/threads"},
		{method: http.MethodGet, path: "/threads/example"},
		{method: http.MethodGet, path: "/docs"},
		{method: http.MethodGet, path: "/docs/example"},
		{method: http.MethodGet, path: "/settings"},
		{method: http.MethodGet, path: "/settings/example"},
		{method: http.MethodGet, path: "/threads.rss"},
		{method: http.MethodGet, path: "/news.rss"},
		{method: http.MethodPost, path: "/auth"},
		{method: http.MethodPost, path: "/auth/token"},
		{method: http.MethodPost, path: "/api/auth"},
		{method: http.MethodPost, path: "/api/auth/token"},
		{method: http.MethodPost, path: "/api/user"},
		{method: http.MethodPost, path: "/api/threads"},
		{method: http.MethodPost, path: "/api/telemetry"},
		{method: http.MethodPost, path: "/api/provider/openai/v1/chat/completions"},
	}
	for _, test := range tests {
		req := httptest.NewRequest(test.method, test.path, strings.NewReader(`{"token":"secret"}`))
		rr := httptest.NewRecorder()
		server.engine.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s %s status = %d, want %d", test.method, test.path, rr.Code, http.StatusNotFound)
		}
	}

	managementServer := newTestServerWithConfig(t, func(cfg *proxyconfig.Config) {
		cfg.RemoteManagement.SecretKey = "secret"
	})
	req := httptest.NewRequest(http.MethodGet, "/v0/management/ampcode", nil)
	rr := httptest.NewRecorder()
	managementServer.engine.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("GET /v0/management/ampcode status = %d, want %d", rr.Code, http.StatusNotFound)
	}

	for _, route := range managementServer.engine.Routes() {
		if strings.Contains(route.Path, "/ampcode") || util.IsRetiredAmpPath(route.Path) {
			t.Fatalf("retired Amp route is still registered: %s %s", route.Method, route.Path)
		}
	}
}

func TestXAIVideoRoutesRegistered(t *testing.T) {
	server := newTestServer(t)
	want := map[string]bool{
		http.MethodPost + " /v1/videos":             false,
		http.MethodPost + " /v1/videos/generations": false,
		http.MethodPost + " /v1/videos/edits":       false,
		http.MethodPost + " /v1/videos/extensions":  false,
		http.MethodGet + " /v1/videos/:request_id":  false,
	}

	for _, route := range server.engine.Routes() {
		key := route.Method + " " + route.Path
		if _, ok := want[key]; ok {
			want[key] = true
		}
	}
	for route, registered := range want {
		if !registered {
			t.Errorf("route %s is not registered", route)
		}
	}
}

func TestV1InternalMethodIsNotRegistered(t *testing.T) {
	server := newTestServer(t)
	req := httptest.NewRequest(http.MethodPost, "/v1internal:method", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	recorder := httptest.NewRecorder()
	server.engine.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}
}

func TestGeminiCLIRuntimeRoutesAreRemovedAndLoginReturnsGone(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "secret")
	server := newTestServerWithConfig(t, func(cfg *proxyconfig.Config) {
		cfg.RemoteManagement.SecretKey = "secret"
		cfg.RemoteManagement.AllowRemote = true
	})
	for _, test := range []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/v1internal:method"},
		{method: http.MethodGet, path: "/google/callback"},
	} {
		req := httptest.NewRequest(test.method, test.path, nil)
		req.Header.Set("Authorization", "Bearer test-key")
		req.Header.Set("X-Management-Key", "secret")
		recorder := httptest.NewRecorder()
		server.engine.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("%s %s status = %d, want %d", test.method, test.path, recorder.Code, http.StatusNotFound)
		}
	}
	loginReq := httptest.NewRequest(http.MethodGet, "/v0/management/gemini-cli-auth-url", nil)
	loginReq.Header.Set("X-Management-Key", "secret")
	loginRecorder := httptest.NewRecorder()
	server.engine.ServeHTTP(loginRecorder, loginReq)
	if loginRecorder.Code != http.StatusGone {
		t.Fatalf("Gemini CLI login status = %d, want %d; body=%s", loginRecorder.Code, http.StatusGone, loginRecorder.Body.String())
	}
	for _, route := range server.engine.Routes() {
		if route.Path == "/v1internal:method" || route.Path == "/google/callback" {
			t.Fatalf("retired Gemini CLI route remains registered: %s %s", route.Method, route.Path)
		}
	}
}

func TestManagementAccessPathPrefixesManagementRoutesAndCallbacks(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "secret")

	server := newTestServerWithConfig(t, func(cfg *proxyconfig.Config) {
		cfg.RemoteManagement.AccessPath = "secret-token"
	})

	req := httptest.NewRequest(http.MethodGet, "/secret-token/v0/management/config", nil)
	req.Header.Set("X-Management-Key", "secret")
	rr := httptest.NewRecorder()
	server.engine.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("prefixed management route status = %d, want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}

	oldReq := httptest.NewRequest(http.MethodGet, "/v0/management/config", nil)
	oldReq.Header.Set("X-Management-Key", "secret")
	oldRec := httptest.NewRecorder()
	server.engine.ServeHTTP(oldRec, oldReq)
	if oldRec.Code != http.StatusNotFound {
		t.Fatalf("unprefixed management route status = %d, want %d; body=%s", oldRec.Code, http.StatusNotFound, oldRec.Body.String())
	}

	callbackReq := httptest.NewRequest(http.MethodGet, "/secret-token/codex/callback", nil)
	callbackRec := httptest.NewRecorder()
	server.engine.ServeHTTP(callbackRec, callbackReq)
	if callbackRec.Code != http.StatusOK {
		t.Fatalf("prefixed callback status = %d, want %d; body=%s", callbackRec.Code, http.StatusOK, callbackRec.Body.String())
	}

	oldCallbackReq := httptest.NewRequest(http.MethodGet, "/codex/callback", nil)
	oldCallbackRec := httptest.NewRecorder()
	server.engine.ServeHTTP(oldCallbackRec, oldCallbackReq)
	if oldCallbackRec.Code != http.StatusNotFound {
		t.Fatalf("unprefixed callback status = %d, want %d; body=%s", oldCallbackRec.Code, http.StatusNotFound, oldCallbackRec.Body.String())
	}

	updatedCfg := *server.cfg
	updatedCfg.RemoteManagement.AccessPath = "new-secret-token"
	server.UpdateClients(&updatedCfg)

	staleReq := httptest.NewRequest(http.MethodGet, "/secret-token/v0/management/config", nil)
	staleReq.Header.Set("X-Management-Key", "secret")
	staleRec := httptest.NewRecorder()
	server.engine.ServeHTTP(staleRec, staleReq)
	if staleRec.Code != http.StatusNotFound {
		t.Fatalf("stale prefixed management route status = %d, want %d; body=%s", staleRec.Code, http.StatusNotFound, staleRec.Body.String())
	}

	newReq := httptest.NewRequest(http.MethodGet, "/new-secret-token/v0/management/config", nil)
	newReq.Header.Set("X-Management-Key", "secret")
	newRec := httptest.NewRecorder()
	server.engine.ServeHTTP(newRec, newReq)
	if newRec.Code != http.StatusOK {
		t.Fatalf("updated prefixed management route status = %d, want %d; body=%s", newRec.Code, http.StatusOK, newRec.Body.String())
	}
}

func TestDefaultRequestLoggerFactory_UsesResolvedLogDirectory(t *testing.T) {
	t.Setenv("WRITABLE_PATH", "")
	t.Setenv("writable_path", "")

	originalWD, errGetwd := os.Getwd()
	if errGetwd != nil {
		t.Fatalf("failed to get current working directory: %v", errGetwd)
	}

	tmpDir := t.TempDir()
	if errChdir := os.Chdir(tmpDir); errChdir != nil {
		t.Fatalf("failed to switch working directory: %v", errChdir)
	}
	defer func() {
		if errChdirBack := os.Chdir(originalWD); errChdirBack != nil {
			t.Fatalf("failed to restore working directory: %v", errChdirBack)
		}
	}()

	// Force ResolveLogDirectory to fallback to auth-dir/logs by making ./logs not a writable directory.
	if errWriteFile := os.WriteFile(filepath.Join(tmpDir, "logs"), []byte("not-a-directory"), 0o644); errWriteFile != nil {
		t.Fatalf("failed to create blocking logs file: %v", errWriteFile)
	}

	configDir := filepath.Join(tmpDir, "config")
	if errMkdirConfig := os.MkdirAll(configDir, 0o755); errMkdirConfig != nil {
		t.Fatalf("failed to create config dir: %v", errMkdirConfig)
	}
	configPath := filepath.Join(configDir, "config.yaml")

	authDir := filepath.Join(tmpDir, "auth")
	if errMkdirAuth := os.MkdirAll(authDir, 0o700); errMkdirAuth != nil {
		t.Fatalf("failed to create auth dir: %v", errMkdirAuth)
	}

	cfg := &proxyconfig.Config{
		SDKConfig: proxyconfig.SDKConfig{
			RequestLog: false,
		},
		AuthDir:           authDir,
		ErrorLogsMaxFiles: 10,
	}

	logger := defaultRequestLoggerFactory(cfg, configPath)
	fileLogger, ok := logger.(*internallogging.FileRequestLogger)
	if !ok {
		t.Fatalf("expected *FileRequestLogger, got %T", logger)
	}

	errLog := fileLogger.LogRequestWithOptions(
		"/v1/chat/completions",
		http.MethodPost,
		map[string][]string{"Content-Type": []string{"application/json"}},
		[]byte(`{"input":"hello"}`),
		http.StatusBadGateway,
		map[string][]string{"Content-Type": []string{"application/json"}},
		[]byte(`{"error":"upstream failure"}`),
		nil,
		nil,
		nil,
		nil,
		nil,
		true,
		"issue-1711",
		time.Now(),
		time.Now(),
	)
	if errLog != nil {
		t.Fatalf("failed to write forced error request log: %v", errLog)
	}

	authLogsDir := filepath.Join(authDir, "logs")
	authEntries, errReadAuthDir := os.ReadDir(authLogsDir)
	if errReadAuthDir != nil {
		t.Fatalf("failed to read auth logs dir %s: %v", authLogsDir, errReadAuthDir)
	}
	foundErrorLogInAuthDir := false
	for _, entry := range authEntries {
		if strings.HasPrefix(entry.Name(), "error-") && strings.HasSuffix(entry.Name(), ".log") {
			foundErrorLogInAuthDir = true
			break
		}
	}
	if !foundErrorLogInAuthDir {
		t.Fatalf("expected forced error log in auth fallback dir %s, got entries: %+v", authLogsDir, authEntries)
	}

	configLogsDir := filepath.Join(configDir, "logs")
	configEntries, errReadConfigDir := os.ReadDir(configLogsDir)
	if errReadConfigDir != nil && !os.IsNotExist(errReadConfigDir) {
		t.Fatalf("failed to inspect config logs dir %s: %v", configLogsDir, errReadConfigDir)
	}
	for _, entry := range configEntries {
		if strings.HasPrefix(entry.Name(), "error-") && strings.HasSuffix(entry.Name(), ".log") {
			t.Fatalf("unexpected forced error log in config dir %s", configLogsDir)
		}
	}
}
