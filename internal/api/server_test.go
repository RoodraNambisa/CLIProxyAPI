package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gin "github.com/gin-gonic/gin"
	proxyconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	internallogging "github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

func newTestServer(t *testing.T) *Server {
	return newTestServerWithConfig(t, nil)
}

func newTestServerWithConfig(t *testing.T, mutate func(*proxyconfig.Config)) *Server {
	return newTestServerWithConfigAndOptions(t, mutate)
}

func newTestServerWithConfigAndOptions(t *testing.T, mutate func(*proxyconfig.Config), opts ...ServerOption) *Server {
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
	server := NewServer(cfg, authManager, accessManager, configPath, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if errShutdown := server.mgmt.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown management handler: %v", errShutdown)
		}
	})
	return server
}

func TestServerCurrentConfigConcurrentReplacement(t *testing.T) {
	server := &Server{cfg: &proxyconfig.Config{Port: 1}}
	var wait sync.WaitGroup
	wait.Add(5)
	go func() {
		defer wait.Done()
		for port := 2; port <= 2_000; port++ {
			server.setCurrentConfig(&proxyconfig.Config{Port: port})
		}
	}()
	for range 4 {
		go func() {
			defer wait.Done()
			for range 2_000 {
				if current := server.currentConfig(); current == nil || current.Port <= 0 {
					t.Errorf("currentConfig() = %#v", current)
					return
				}
			}
		}()
	}
	wait.Wait()
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

func TestStartupReadinessGatesProxyAndCredentialManagement(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "secret")
	startup := NewStartupState()
	startup.SetPhase(StartupPhaseAuthLoading)
	server := newTestServerWithConfigAndOptions(t, func(cfg *proxyconfig.Config) {
		cfg.RemoteManagement.SecretKey = "secret"
		cfg.RemoteManagement.AllowRemote = true
	}, WithStartupState(startup))

	readyRequest := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyRecorder := httptest.NewRecorder()
	server.engine.ServeHTTP(readyRecorder, readyRequest)
	if readyRecorder.Code != http.StatusServiceUnavailable || readyRecorder.Header().Get("Retry-After") != "2" {
		t.Fatalf("initial readyz = %d retry=%q, want 503 retry=2: %s", readyRecorder.Code, readyRecorder.Header().Get("Retry-After"), readyRecorder.Body.String())
	}
	headReadyRequest := httptest.NewRequest(http.MethodHead, "/readyz", nil)
	headReadyRecorder := httptest.NewRecorder()
	server.engine.ServeHTTP(headReadyRecorder, headReadyRequest)
	if headReadyRecorder.Code != http.StatusServiceUnavailable || headReadyRecorder.Body.Len() != 0 {
		t.Fatalf("initial HEAD readyz = %d body=%q, want empty 503", headReadyRecorder.Code, headReadyRecorder.Body.String())
	}

	unauthorized := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	unauthorizedRecorder := httptest.NewRecorder()
	server.engine.ServeHTTP(unauthorizedRecorder, unauthorized)
	if unauthorizedRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized proxy status = %d, want 401: %s", unauthorizedRecorder.Code, unauthorizedRecorder.Body.String())
	}

	proxyRequest := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxyRequest.Header.Set("Authorization", "Bearer test-key")
	proxyRecorder := httptest.NewRecorder()
	server.engine.ServeHTTP(proxyRecorder, proxyRequest)
	if proxyRecorder.Code != http.StatusServiceUnavailable || !strings.Contains(proxyRecorder.Body.String(), `"code":"service_initializing"`) {
		t.Fatalf("initial proxy response = %d %s, want service_initializing 503", proxyRecorder.Code, proxyRecorder.Body.String())
	}

	for _, path := range []string{"/v0/management/config", "/v0/management/startup/status", "/v0/management/storage/history"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("X-Management-Key", "secret")
		recorder := httptest.NewRecorder()
		server.engine.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("initial management %s status = %d, want 200: %s", path, recorder.Code, recorder.Body.String())
		}
	}
	pruneTaskRequest := httptest.NewRequest(http.MethodGet, "/v0/management/usage/prune/missing", nil)
	pruneTaskRequest.Header.Set("X-Management-Key", "secret")
	pruneTaskRecorder := httptest.NewRecorder()
	server.engine.ServeHTTP(pruneTaskRecorder, pruneTaskRequest)
	if pruneTaskRecorder.Code != http.StatusNotFound || strings.Contains(pruneTaskRecorder.Body.String(), "service_initializing") {
		t.Fatalf("initial prune task query = %d %s, want handler 404 without startup gate", pruneTaskRecorder.Code, pruneTaskRecorder.Body.String())
	}

	for _, testCase := range []struct {
		method string
		path   string
	}{
		{method: http.MethodPut, path: "/v0/management/config.yaml"},
		{method: http.MethodPatch, path: "/v0/management/debug"},
		{method: http.MethodPost, path: "/v0/management/control-panel/update"},
		{method: http.MethodDelete, path: "/v0/management/usage"},
	} {
		request := httptest.NewRequest(testCase.method, testCase.path, nil)
		request.Header.Set("X-Management-Key", "secret")
		recorder := httptest.NewRecorder()
		server.engine.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"code":"service_initializing"`) {
			t.Fatalf("initial management write %s %s = %d %s, want service_initializing 503", testCase.method, testCase.path, recorder.Code, recorder.Body.String())
		}
	}

	authFilesRequest := httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	authFilesRequest.Header.Set("X-Management-Key", "secret")
	authFilesRecorder := httptest.NewRecorder()
	server.engine.ServeHTTP(authFilesRecorder, authFilesRequest)
	if authFilesRecorder.Code != http.StatusServiceUnavailable || !strings.Contains(authFilesRecorder.Body.String(), "service_initializing") {
		t.Fatalf("initial auth-files response = %d %s, want 503", authFilesRecorder.Code, authFilesRecorder.Body.String())
	}

	startup.MarkReady()
	readyRecorder = httptest.NewRecorder()
	server.engine.ServeHTTP(readyRecorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readyRecorder.Code != http.StatusOK {
		t.Fatalf("readyz after MarkReady = %d, want 200: %s", readyRecorder.Code, readyRecorder.Body.String())
	}

	proxyRecorder = httptest.NewRecorder()
	proxyRequest = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	proxyRequest.Header.Set("Authorization", "Bearer test-key")
	server.engine.ServeHTTP(proxyRecorder, proxyRequest)
	if proxyRecorder.Code == http.StatusServiceUnavailable && strings.Contains(proxyRecorder.Body.String(), "service_initializing") {
		t.Fatalf("proxy remained startup-gated after MarkReady: %s", proxyRecorder.Body.String())
	}
}

func TestFailedStartupKeepsSafeReadsAndRejectsWrites(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "secret")
	startup := NewStartupState()
	startup.AddIssue("watcher_initial_sync", "watcher_initial_sync_failed", "error")
	startup.MarkFailed()
	server := newTestServerWithConfigAndOptions(t, func(cfg *proxyconfig.Config) {
		cfg.RemoteManagement.SecretKey = "secret"
		cfg.RemoteManagement.AllowRemote = true
	}, WithStartupState(startup))

	read := httptest.NewRequest(http.MethodGet, "/v0/management/startup/status", nil)
	read.Header.Set("X-Management-Key", "secret")
	readRecorder := httptest.NewRecorder()
	server.engine.ServeHTTP(readRecorder, read)
	if readRecorder.Code != http.StatusOK || !strings.Contains(readRecorder.Body.String(), `"status":"failed"`) {
		t.Fatalf("failed startup safe read = %d %s", readRecorder.Code, readRecorder.Body.String())
	}

	write := httptest.NewRequest(http.MethodPut, "/v0/management/config.yaml", strings.NewReader("debug: true\n"))
	write.Header.Set("X-Management-Key", "secret")
	writeRecorder := httptest.NewRecorder()
	server.engine.ServeHTTP(writeRecorder, write)
	if writeRecorder.Code != http.StatusServiceUnavailable || !strings.Contains(writeRecorder.Body.String(), `"code":"service_startup_failed"`) {
		t.Fatalf("failed startup write = %d %s", writeRecorder.Code, writeRecorder.Body.String())
	}
}

func TestStartListeningBindsBeforeReturning(t *testing.T) {
	server := newTestServerWithConfig(t, func(cfg *proxyconfig.Config) {
		cfg.Host = "127.0.0.1"
		cfg.Port = 0
	})
	addr, serverErr, errStart := server.StartListening()
	if errStart != nil {
		t.Fatalf("StartListening() error = %v", errStart)
	}
	if addr == nil {
		t.Fatal("StartListening() returned nil address")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if errStop := server.Stop(ctx); errStop != nil {
			t.Errorf("Stop() error = %v", errStop)
		}
		if errServe := <-serverErr; errServe != nil {
			t.Errorf("Serve() error = %v", errServe)
		}
	})

	response, errGet := http.Get("http://" + addr.String() + "/healthz")
	if errGet != nil {
		t.Fatalf("health request immediately after StartListening: %v", errGet)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want 200", response.StatusCode)
	}
}

func TestServerWithoutStartupStateRemainsReady(t *testing.T) {
	server := newTestServer(t)
	recorder := httptest.NewRecorder()
	server.engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("default readyz = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
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
		http.MethodGet + " /v0/management/usage/health":           false,
		http.MethodGet + " /v0/management/usage/rates":            false,
		http.MethodGet + " /v0/management/usage/tokens":           false,
		http.MethodGet + " /v0/management/usage/costs":            false,
		http.MethodGet + " /v0/management/usage/prices":           false,
		http.MethodPut + " /v0/management/usage/prices":           false,
		http.MethodPatch + " /v0/management/usage/prices":         false,
		http.MethodDelete + " /v0/management/usage/prices":        false,
		http.MethodDelete + " /v0/management/usage/prices/*model": false,
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

func TestPerAuthRequestLimitManagementRoutesAreRegistered(t *testing.T) {
	server := newTestServerWithConfig(t, func(cfg *proxyconfig.Config) {
		cfg.RemoteManagement.SecretKey = "secret"
	})
	want := map[string]bool{
		http.MethodGet + " /v0/management/routing/diagnostics":                       false,
		http.MethodGet + " /v0/management/usage/failures/summary":                    false,
		http.MethodGet + " /v0/management/routing/per-auth-request-limit":            false,
		http.MethodPut + " /v0/management/routing/per-auth-request-limit":            false,
		http.MethodPatch + " /v0/management/routing/per-auth-request-limit":          false,
		http.MethodGet + " /v0/management/routing/per-auth-request-window-minutes":   false,
		http.MethodPut + " /v0/management/routing/per-auth-request-window-minutes":   false,
		http.MethodPatch + " /v0/management/routing/per-auth-request-window-minutes": false,
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

func TestChatGPTWebManagementRoutesAreRegistered(t *testing.T) {
	server := newTestServerWithConfig(t, func(cfg *proxyconfig.Config) {
		cfg.RemoteManagement.SecretKey = "secret"
	})
	want := map[string]bool{
		http.MethodPost + " /v0/management/chatgpt-web/login-tasks":                      false,
		http.MethodGet + " /v0/management/chatgpt-web/login-tasks/:id":                   false,
		http.MethodDelete + " /v0/management/chatgpt-web/login-tasks/:id":                false,
		http.MethodPost + " /v0/management/chatgpt-web/import-tasks":                     false,
		http.MethodGet + " /v0/management/chatgpt-web/import-tasks/:id":                  false,
		http.MethodDelete + " /v0/management/chatgpt-web/import-tasks/:id":               false,
		http.MethodGet + " /v0/management/chatgpt-web/capabilities":                      false,
		http.MethodGet + " /v0/management/chatgpt-web/import":                            false,
		http.MethodPut + " /v0/management/chatgpt-web/import":                            false,
		http.MethodPatch + " /v0/management/chatgpt-web/import":                          false,
		http.MethodPost + " /v0/management/chatgpt-web/conversion-tasks":                 false,
		http.MethodGet + " /v0/management/chatgpt-web/conversion-tasks/:id":              false,
		http.MethodDelete + " /v0/management/chatgpt-web/conversion-tasks/:id":           false,
		http.MethodPost + " /v0/management/chatgpt-web/auth-files/:name/relogin":         false,
		http.MethodGet + " /v0/management/chatgpt-web/sentinel":                          false,
		http.MethodPut + " /v0/management/chatgpt-web/sentinel":                          false,
		http.MethodPatch + " /v0/management/chatgpt-web/sentinel":                        false,
		http.MethodGet + " /v0/management/chatgpt-web/account-info":                      false,
		http.MethodPut + " /v0/management/chatgpt-web/account-info":                      false,
		http.MethodPatch + " /v0/management/chatgpt-web/account-info":                    false,
		http.MethodGet + " /v0/management/chatgpt-web/account-info/diagnostics":          false,
		http.MethodDelete + " /v0/management/chatgpt-web/account-info/diagnostics":       false,
		http.MethodPost + " /v0/management/chatgpt-web/account-info/refresh-tasks":       false,
		http.MethodGet + " /v0/management/chatgpt-web/account-info/refresh-tasks/:id":    false,
		http.MethodDelete + " /v0/management/chatgpt-web/account-info/refresh-tasks/:id": false,
		http.MethodPost + " /v0/management/auth-files/restore":                           false,
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

func TestUsagePriceDeleteRouteHandlesModelPath(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "secret")
	server := newTestServerWithConfig(t, func(cfg *proxyconfig.Config) {
		cfg.RemoteManagement.SecretKey = "secret"
		cfg.RemoteManagement.AllowRemote = true
		cfg.UsagePricing = proxyconfig.UsagePricingConfig{Models: map[string]proxyconfig.UsageModelPrice{
			"provider/model-a": {InputPerMillion: 1},
		}}
	})
	if err := os.WriteFile(server.configFilePath, []byte("usage-pricing:\n  models:\n    provider/model-a:\n      input-per-million: 1\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodDelete, "/v0/management/usage/prices/provider/model-a", nil)
	req.Header.Set("X-Management-Key", "secret")
	recorder := httptest.NewRecorder()
	server.engine.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("DELETE model status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if len(server.cfg.UsagePricing.Models) != 0 {
		t.Fatalf("models after DELETE = %#v, want empty", server.cfg.UsagePricing.Models)
	}

	missing := httptest.NewRequest(http.MethodDelete, "/v0/management/usage/prices/provider/missing", nil)
	missing.Header.Set("X-Management-Key", "secret")
	missingRecorder := httptest.NewRecorder()
	server.engine.ServeHTTP(missingRecorder, missing)
	if missingRecorder.Code != http.StatusNotFound {
		t.Fatalf("DELETE missing status = %d, want 404: %s", missingRecorder.Code, missingRecorder.Body.String())
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

	updatedCfg := *server.currentConfig()
	updatedCfg.RemoteManagement.AccessPath = "new-secret-token"
	if errUpdate := server.UpdateClients(&updatedCfg); errUpdate != nil {
		t.Fatalf("UpdateClients() error = %v", errUpdate)
	}

	staleReq := httptest.NewRequest(http.MethodGet, "/secret-token/v0/management/config", nil)
	staleReq.Header.Set("X-Management-Key", "secret")
	staleRec := httptest.NewRecorder()
	server.engine.ServeHTTP(staleRec, staleReq)
	if staleRec.Code != http.StatusOK {
		t.Fatalf("runtime management route status = %d, want %d; body=%s", staleRec.Code, http.StatusOK, staleRec.Body.String())
	}

	newReq := httptest.NewRequest(http.MethodGet, "/new-secret-token/v0/management/config", nil)
	newReq.Header.Set("X-Management-Key", "secret")
	newRec := httptest.NewRecorder()
	server.engine.ServeHTTP(newRec, newReq)
	if newRec.Code != http.StatusNotFound {
		t.Fatalf("unregistered management route status = %d, want %d; body=%s", newRec.Code, http.StatusNotFound, newRec.Body.String())
	}
}

func TestManagementRoutesCanBeEnabledWithoutRuntimeRegistration(t *testing.T) {
	server := newTestServer(t)
	if server.managementRoutesEnabled.Load() {
		t.Fatal("management routes unexpectedly enabled")
	}

	disabledReq := httptest.NewRequest(http.MethodGet, "/v0/management/config", nil)
	disabledRec := httptest.NewRecorder()
	server.engine.ServeHTTP(disabledRec, disabledReq)
	if disabledRec.Code != http.StatusNotFound {
		t.Fatalf("disabled management status = %d, want %d", disabledRec.Code, http.StatusNotFound)
	}

	updatedCfg := *server.currentConfig()
	secretHash, errHash := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	if errHash != nil {
		t.Fatalf("GenerateFromPassword() error = %v", errHash)
	}
	updatedCfg.RemoteManagement.SecretKey = string(secretHash)
	updatedCfg.RemoteManagement.AllowRemote = true
	if errUpdate := server.UpdateClients(&updatedCfg); errUpdate != nil {
		t.Fatalf("UpdateClients() error = %v", errUpdate)
	}

	enabledReq := httptest.NewRequest(http.MethodGet, "/v0/management/config", nil)
	enabledReq.Header.Set("X-Management-Key", "secret")
	enabledRec := httptest.NewRecorder()
	server.engine.ServeHTTP(enabledRec, enabledReq)
	if enabledRec.Code != http.StatusOK {
		t.Fatalf("enabled management status = %d, want %d; body=%s", enabledRec.Code, http.StatusOK, enabledRec.Body.String())
	}
}

func TestUpdateClientsRollsBackEarlierSideEffectsAfterLaterFailure(t *testing.T) {
	server := newTestServer(t)

	var requestLogStates []bool
	server.loggerToggle = func(enabled bool) {
		requestLogStates = append(requestLogStates, enabled)
	}

	blockedPath := filepath.Join(t.TempDir(), "not-a-directory")
	if errWrite := os.WriteFile(blockedPath, []byte("blocked"), 0o600); errWrite != nil {
		t.Fatalf("write blocked path: %v", errWrite)
	}
	t.Setenv("WRITABLE_PATH", blockedPath)
	t.Setenv("writable_path", "")

	updatedCfg := *server.currentConfig()
	updatedCfg.RequestLog = true
	updatedCfg.LoggingToFile = true
	if errUpdate := server.UpdateClients(&updatedCfg); errUpdate == nil {
		t.Fatal("UpdateClients() error = nil, want logging setup failure")
	}

	if len(requestLogStates) != 2 || !requestLogStates[0] || requestLogStates[1] {
		t.Fatalf("request log states = %v, want [true false]", requestLogStates)
	}
	current := server.currentConfig()
	if current.RequestLog || current.LoggingToFile {
		t.Fatalf("runtime config was not rolled back: request_log=%t logging_to_file=%t", current.RequestLog, current.LoggingToFile)
	}
	var snapshot proxyconfig.Config
	if errUnmarshal := yaml.Unmarshal(server.oldConfigYaml, &snapshot); errUnmarshal != nil {
		t.Fatalf("unmarshal runtime snapshot: %v", errUnmarshal)
	}
	if snapshot.RequestLog || snapshot.LoggingToFile {
		t.Fatalf("runtime snapshot was not rolled back: request_log=%t logging_to_file=%t", snapshot.RequestLog, snapshot.LoggingToFile)
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
