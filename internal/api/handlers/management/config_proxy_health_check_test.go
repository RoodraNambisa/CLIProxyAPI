package management

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
)

func TestProxyHealthCheckManagementHotAppliesAndPersists(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("proxy-pools: []\nproxy-rules: []\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{}
	manager, errManager := proxypool.NewManager(configPath, cfg)
	if errManager != nil {
		t.Fatalf("NewManager() error = %v", errManager)
	}
	h := NewHandler(cfg, configPath, nil)
	h.SetProxyPoolManager(manager)
	router := gin.New()
	router.GET("/proxy-health-check", h.GetProxyHealthCheck)
	router.PATCH("/proxy-health-check", h.PatchProxyHealthCheck)

	get := performProxyConfigRequest(router, http.MethodGet, "/proxy-health-check", "")
	if get.Code != http.StatusOK {
		t.Fatalf("get status = %d; body=%s", get.Code, get.Body.String())
	}
	patch := performProxyConfigRequest(router, http.MethodPatch, "/proxy-health-check", `{
		"concurrency":12,
		"endpoint-timeout-seconds":5,
		"failure-threshold":3,
		"endpoints":[{"name":"primary","url":"https://example.test/health","mode":"http-status"}]
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("patch status = %d; body=%s", patch.Code, patch.Body.String())
	}
	if snapshot := manager.CheckAdmissionSnapshot(); snapshot.Limit != 12 {
		t.Fatalf("runtime admission = %+v", snapshot)
	}
	reloaded, errReload := config.LoadConfig(configPath)
	if errReload != nil {
		t.Fatalf("LoadConfig() error = %v", errReload)
	}
	if reloaded.ProxyHealthCheck.Concurrency != 12 || reloaded.ProxyHealthCheck.FailureThreshold != 3 || len(reloaded.ProxyHealthCheck.Endpoints) != 1 {
		t.Fatalf("persisted health configuration = %#v", reloaded.ProxyHealthCheck)
	}

	invalid := performProxyConfigRequest(router, http.MethodPatch, "/proxy-health-check", `{"concurrency":0}`)
	if invalid.Code != http.StatusBadRequest || manager.CheckAdmissionSnapshot().Limit != 12 {
		t.Fatalf("invalid patch = %d/%s; runtime=%+v", invalid.Code, invalid.Body.String(), manager.CheckAdmissionSnapshot())
	}
}

func TestProxyHealthCheckEndpointTestReturnsOnlySafeMetadata(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer endpoint.Close()
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		ProxyHealthCheck: config.ProxyHealthCheckConfig{
			EndpointTimeoutSeconds: 1,
			Endpoints: []config.ProxyHealthCheckEndpointConfig{{
				Name: "primary",
				URL:  endpoint.URL + "/health?secret=must-not-be-returned",
				Mode: config.ProxyHealthCheckModeHTTPStatus,
			}},
		},
	}}
	h := NewHandler(cfg, "", nil)
	router := gin.New()
	router.POST("/proxy-health-check/test", h.PostProxyHealthCheckTest)

	recorder := performProxyConfigRequest(router, http.MethodPost, "/proxy-health-check/test", `{}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"ok":true`) {
		t.Fatalf("test response = %d/%s", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "must-not-be-returned") || strings.Contains(recorder.Body.String(), endpoint.URL) {
		t.Fatalf("test response exposed configured endpoint: %s", recorder.Body.String())
	}
}
