package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type chatGPTWebUsageCacheTestExecutor struct {
	coreauth.ProviderExecutor
	snapshot helps.ChatGPTWebUsageCacheSnapshot
	updates  int
	resolved config.ResolvedChatGPTWebUsageCacheConfig
}

func (executor *chatGPTWebUsageCacheTestExecutor) Identifier() string {
	return chatgptwebauth.Provider
}

func (executor *chatGPTWebUsageCacheTestExecutor) UsageCacheSnapshot() helps.ChatGPTWebUsageCacheSnapshot {
	return executor.snapshot
}

func (executor *chatGPTWebUsageCacheTestExecutor) UpdateConfig(cfg *config.Config) {
	executor.updates++
	executor.resolved = cfg.ChatGPTWeb.UsageCache.Resolved()
}

func TestGetChatGPTWebUsageCacheReturnsDefaultsAndRuntimeStats(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &chatGPTWebUsageCacheTestExecutor{snapshot: helps.ChatGPTWebUsageCacheSnapshot{
		ActiveDiskBytes:        42,
		PeakDiskBytes:          84,
		SuccessfulCalculations: 3,
	}}
	manager.RegisterExecutor(executor)
	handler := &Handler{cfg: &config.Config{}, authManager: manager}
	ctx, recorder := newChatGPTWebUsageCacheRequest(http.MethodGet, "")
	handler.GetChatGPTWebUsageCache(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	response := decodeChatGPTWebUsageCacheResponse(t, recorder)
	if !response.EstimateTokenUsage || response.UsageCache.Enabled ||
		response.UsageCache.DiskThresholdMB != config.DefaultChatGPTWebUsageCacheThresholdMB ||
		response.UsageCache.MaxDiskSizeMB != config.DefaultChatGPTWebUsageCacheMaxDiskSizeMB ||
		response.ImageUsage.AutoOutputQuality != "medium" {
		t.Fatalf("GET response config = %#v", response)
	}
	if response.Stats.ActiveDiskBytes != 42 || response.Stats.PeakDiskBytes != 84 || response.Stats.SuccessfulCalculations != 3 {
		t.Fatalf("GET response stats = %#v", response.Stats)
	}
}

func TestPatchChatGPTWebUsageCachePersistsAndUpdatesRuntime(t *testing.T) {
	configPath := writeTestConfigFile(t)
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &chatGPTWebUsageCacheTestExecutor{}
	manager.RegisterExecutor(executor)
	handler := &Handler{cfg: &config.Config{}, configFilePath: configPath, authManager: manager}
	body := `{
		"estimate-token-usage":false,
		"usage-cache":{"enabled":true,"disk-threshold-mb":2,"max-disk-size-mb":16,"path":"/tmp/web-usage"},
		"image-usage":{"auto-output-quality":"high"}
	}`
	ctx, recorder := newChatGPTWebUsageCacheRequest(http.MethodPatch, body)
	handler.PatchChatGPTWebUsageCache(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if executor.updates != 1 || !executor.resolved.Enabled || executor.resolved.DiskThresholdMB != 2 {
		t.Fatalf("runtime update = count %d, config %#v", executor.updates, executor.resolved)
	}
	reloaded, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	if reloaded.ChatGPTWeb.TokenUsageEstimationEnabled() || !reloaded.ChatGPTWeb.UsageCache.Resolved().Enabled ||
		reloaded.ChatGPTWeb.ImageUsage.ResolvedAutoOutputQuality() != "high" {
		t.Fatalf("persisted config = %#v", reloaded.ChatGPTWeb)
	}
}

func TestChatGPTWebUsageCacheRejectsInvalidThresholdWithoutMutation(t *testing.T) {
	threshold := int64(2)
	maximum := int64(16)
	enabled := true
	handler := &Handler{cfg: &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		UsageCache: config.ChatGPTWebUsageCacheConfig{
			Enabled: &enabled, DiskThresholdMB: &threshold, MaxDiskSizeMB: &maximum,
		},
	}}}
	ctx, recorder := newChatGPTWebUsageCacheRequest(http.MethodPatch, `{"usage-cache":{"disk-threshold-mb":32}}`)
	handler.PatchChatGPTWebUsageCache(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("PATCH status = %d, want 400: %s", recorder.Code, recorder.Body.String())
	}
	if got := handler.cfg.ChatGPTWeb.UsageCache.Resolved(); got.DiskThresholdMB != 2 || got.MaxDiskSizeMB != 16 {
		t.Fatalf("config mutated after rejection: %#v", got)
	}
}

func TestPutChatGPTWebUsageCacheRequiresCompleteBody(t *testing.T) {
	handler := &Handler{cfg: &config.Config{}}
	ctx, recorder := newChatGPTWebUsageCacheRequest(http.MethodPut, `{"estimate-token-usage":true}`)
	handler.PutChatGPTWebUsageCache(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("PUT status = %d, want 400: %s", recorder.Code, recorder.Body.String())
	}
}

func newChatGPTWebUsageCacheRequest(method, body string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(method, "/v0/management/chatgpt-web/usage-cache", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	return ctx, recorder
}

func decodeChatGPTWebUsageCacheResponse(t *testing.T, recorder *httptest.ResponseRecorder) chatGPTWebUsageCacheResponse {
	t.Helper()
	var response chatGPTWebUsageCacheResponse
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("json.Unmarshal() error = %v; body=%s", errDecode, recorder.Body.String())
	}
	return response
}
