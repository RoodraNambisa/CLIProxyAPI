package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"gopkg.in/yaml.v3"
)

func TestProxyPoolManagementMasksSecretsAndMigratesRuleOnRename(t *testing.T) {
	gin.SetMode(gin.TestMode)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("proxy-pools: []\nproxy-rules: []\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{}
	h := NewHandler(cfg, configPath, nil)
	router := gin.New()
	router.POST("/proxy-pools", h.PostProxyPool)
	router.GET("/proxy-pools", h.GetProxyPools)
	router.PATCH("/proxy-pools/:name", h.PatchProxyPool)
	router.PUT("/proxy-rules", h.PutProxyRules)

	create := performProxyConfigRequest(router, http.MethodPost, "/proxy-pools", `{
		"name":"residential",
		"entries":[
			{"id":"geo","url-template":"http://user:secret@proxy.example","ports":"3334,3336-3338"},
			{"id":"backup","url-template":"http://backup.example:8080"}
		]
	}`)
	if create.Code != http.StatusOK {
		t.Fatalf("create status = %d; body=%s", create.Code, create.Body.String())
	}

	get := performProxyConfigRequest(router, http.MethodGet, "/proxy-pools", "")
	if strings.Contains(get.Body.String(), "secret") || !strings.Contains(get.Body.String(), "********") {
		t.Fatalf("GET response did not mask password: %s", get.Body.String())
	}
	if cfg.ProxyPools[0].Entries[0].URLTemplate != "http://user:secret@proxy.example" {
		t.Fatalf("stored URL was mutated: %q", cfg.ProxyPools[0].Entries[0].URLTemplate)
	}
	updateEntry := performProxyConfigRequest(router, http.MethodPatch, "/proxy-pools/residential", `{
		"entries":[{"id":"geo","url-template":"http://user:********@proxy.example","ports":"9000-9002"}]
	}`)
	if updateEntry.Code != http.StatusOK {
		t.Fatalf("entry patch status = %d; body=%s", updateEntry.Code, updateEntry.Body.String())
	}
	if got := cfg.ProxyPools[0].Entries[0]; got.URLTemplate != "http://user:secret@proxy.example" || got.Ports != "9000-9002" {
		t.Fatalf("entry patch = %#v", got)
	}
	if len(cfg.ProxyPools[0].Entries) != 2 || cfg.ProxyPools[0].Entries[1].ID != "backup" {
		t.Fatalf("partial entry patch removed an entry: %#v", cfg.ProxyPools[0].Entries)
	}
	deleteEntry := performProxyConfigRequest(router, http.MethodPatch, "/proxy-pools/residential", `{"delete-entry-ids":["backup"]}`)
	if deleteEntry.Code != http.StatusOK {
		t.Fatalf("entry delete status = %d; body=%s", deleteEntry.Code, deleteEntry.Body.String())
	}
	if len(cfg.ProxyPools[0].Entries) != 1 || cfg.ProxyPools[0].Entries[0].ID != "geo" {
		t.Fatalf("entry delete result = %#v", cfg.ProxyPools[0].Entries)
	}

	rules := performProxyConfigRequest(router, http.MethodPut, "/proxy-rules", `[{"name":"web","pool":"residential","providers":["chatgpt-web"]}]`)
	if rules.Code != http.StatusOK {
		t.Fatalf("put rules status = %d; body=%s", rules.Code, rules.Body.String())
	}
	rename := performProxyConfigRequest(router, http.MethodPatch, "/proxy-pools/residential", `{"name":"primary"}`)
	if rename.Code != http.StatusOK {
		t.Fatalf("rename status = %d; body=%s", rename.Code, rename.Body.String())
	}
	if got := cfg.ProxyRules[0].Pool; got != "primary" {
		t.Fatalf("rule pool after rename = %q, want primary", got)
	}
}

func TestDeleteProxyPoolRejectsReferencedPool(t *testing.T) {
	gin.SetMode(gin.TestMode)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("proxy-pools: []\nproxy-rules: []\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		ProxyPools: []config.ProxyPoolConfig{{Name: "pool", Entries: []config.ProxyPoolEntryConfig{{ID: "node", URLTemplate: "http://proxy.example:8080"}}}},
		ProxyRules: []config.ProxyRuleConfig{{Name: "rule", Pool: "pool"}},
	}}
	h := NewHandler(cfg, configPath, nil)
	router := gin.New()
	router.DELETE("/proxy-pools/:name", h.DeleteProxyPool)
	response := performProxyConfigRequest(router, http.MethodDelete, "/proxy-pools/pool", "")
	if response.Code != http.StatusConflict {
		t.Fatalf("delete status = %d, want 409; body=%s", response.Code, response.Body.String())
	}
	if len(cfg.ProxyPools) != 1 {
		t.Fatal("referenced pool was deleted")
	}
}

func TestGetConfigMasksGlobalAndPoolProxyPasswords(t *testing.T) {
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		ProxyURL: "http://global:secret@proxy.example:8080",
		ProxyPools: []config.ProxyPoolConfig{{Name: "pool", Entries: []config.ProxyPoolEntryConfig{{
			ID: "node", URLTemplate: "socks5h://pool:secret@proxy.example:1080",
		}}}},
	}, GeminiKey: []config.GeminiKey{{ProxyURL: "http://gemini:secret@proxy.example:8080"}},
		InteractionsKey: []config.GeminiKey{{ProxyURL: "http://interactions:secret@proxy.example:8080"}},
		CodexKey:        []config.CodexKey{{ProxyURL: "http://codex:secret@proxy.example:8080"}},
		ClaudeKey:       []config.ClaudeKey{{ProxyURL: "http://claude:secret@proxy.example:8080"}},
		VertexCompatAPIKey: []config.VertexCompatKey{{
			ProxyURL: "http://vertex:secret@proxy.example:8080",
		}},
		OpenAICompatibility: []config.OpenAICompatibility{{APIKeyEntries: []config.OpenAICompatibilityAPIKey{{
			ProxyURL: "http://compat:secret@proxy.example:8080",
		}}}},
	}
	h := NewHandlerWithoutConfigFilePath(cfg, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/config", nil)
	h.GetConfig(ctx)
	if strings.Contains(recorder.Body.String(), "secret") {
		t.Fatalf("config response leaked proxy password: %s", recorder.Body.String())
	}
	var body map[string]any
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &body); errDecode != nil {
		t.Fatalf("decode config response: %v", errDecode)
	}
	if !strings.Contains(body["proxy-url"].(string), "********") {
		t.Fatalf("proxy-url was not masked: %#v", body["proxy-url"])
	}
	if cfg.GeminiKey[0].ProxyURL != "http://gemini:secret@proxy.example:8080" ||
		cfg.OpenAICompatibility[0].APIKeyEntries[0].ProxyURL != "http://compat:secret@proxy.example:8080" {
		t.Fatal("GetConfig mutated nested proxy URLs")
	}
}

func TestPutProxyURLPreservesMaskedPassword(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("proxy-url: http://user:secret@proxy.example:8080\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{ProxyURL: "http://user:secret@proxy.example:8080"}}
	h := NewHandler(cfg, configPath, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPut, "/proxy-url", strings.NewReader(`{"value":"http://user:********@proxy.example:8080"}`))
	ctx.Request.Header.Set("Content-Type", "application/json")
	h.PutProxyURL(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if cfg.ProxyURL != "http://user:secret@proxy.example:8080" {
		t.Fatalf("ProxyURL changed to %q", cfg.ProxyURL)
	}

	replaceRecorder := httptest.NewRecorder()
	replaceContext, _ := gin.CreateTestContext(replaceRecorder)
	replaceContext.Request = httptest.NewRequest(http.MethodPut, "/proxy-url?replace-masked-proxy=true", strings.NewReader(`{"value":"http://user:********@proxy.example:8080"}`))
	replaceContext.Request.Header.Set("Content-Type", "application/json")
	h.PutProxyURL(replaceContext)
	if replaceRecorder.Code != http.StatusOK {
		t.Fatalf("explicit replacement status = %d; body=%s", replaceRecorder.Code, replaceRecorder.Body.String())
	}
	if cfg.ProxyURL != "http://user:********@proxy.example:8080" {
		t.Fatalf("explicit replacement ProxyURL = %q", cfg.ProxyURL)
	}
}

func TestProxyPoolAllowsLiteralMaskedPassword(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("proxy-pools: []\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{}
	h := NewHandler(cfg, configPath, nil)
	router := gin.New()
	router.POST("/proxy-pools", h.PostProxyPool)
	router.PATCH("/proxy-pools/:name", h.PatchProxyPool)

	response := performProxyConfigRequest(router, http.MethodPost, "/proxy-pools", `{
		"name":"literal",
		"entries":[{"id":"node","url-template":"http://user:********@proxy.example:8080"}]
	}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
	}
	response = performProxyConfigRequest(router, http.MethodPost, "/proxy-pools?replace-masked-proxy=true", `{
		"name":"literal",
		"entries":[{"id":"node","url-template":"http://user:********@proxy.example:8080"}]
	}`)
	if response.Code != http.StatusOK {
		t.Fatalf("explicit replacement status = %d; body=%s", response.Code, response.Body.String())
	}
	if got := cfg.ProxyPools[0].Entries[0].URLTemplate; got != "http://user:********@proxy.example:8080" {
		t.Fatalf("stored URL = %q", got)
	}
	reloaded, errReload := config.LoadConfig(configPath)
	if errReload != nil {
		t.Fatalf("reload config: %v", errReload)
	}
	if got := reloaded.ProxyPools[0].Entries[0].URLTemplate; got != "http://user:********@proxy.example:8080" {
		t.Fatalf("reloaded URL = %q", got)
	}
	patch := performProxyConfigRequest(router, http.MethodPatch, "/proxy-pools/literal?replace-masked-proxy=true", `{
		"entries":[{"id":"node","url-template":"http://other:********@proxy.example:8080"}]
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("explicit patch status = %d; body=%s", patch.Code, patch.Body.String())
	}
	if got := cfg.ProxyPools[0].Entries[0].URLTemplate; got != "http://other:********@proxy.example:8080" {
		t.Fatalf("patched URL = %q", got)
	}
	reloaded, errReload = config.LoadConfig(configPath)
	if errReload != nil {
		t.Fatalf("reload patched config: %v", errReload)
	}
	if got := reloaded.ProxyPools[0].Entries[0].URLTemplate; got != "http://other:********@proxy.example:8080" {
		t.Fatalf("reloaded patched URL = %q", got)
	}
}

func TestProxyPoolEmptyListsPersistAcrossReload(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	raw := `proxy-pools:
  - name: pool
    entries:
      - id: node
        url-template: http://proxy.example:8080
proxy-rules:
  - name: rule
    pool: pool
`
	if errWrite := os.WriteFile(configPath, []byte(raw), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatalf("load config: %v", errLoad)
	}
	h := NewHandler(cfg, configPath, nil)
	router := gin.New()
	router.PUT("/proxy-rules", h.PutProxyRules)
	router.DELETE("/proxy-pools/:name", h.DeleteProxyPool)

	clearRules := performProxyConfigRequest(router, http.MethodPut, "/proxy-rules", `[]`)
	if clearRules.Code != http.StatusOK {
		t.Fatalf("clear rules status = %d; body=%s", clearRules.Code, clearRules.Body.String())
	}
	deletePool := performProxyConfigRequest(router, http.MethodDelete, "/proxy-pools/pool", "")
	if deletePool.Code != http.StatusOK {
		t.Fatalf("delete pool status = %d; body=%s", deletePool.Code, deletePool.Body.String())
	}
	reloaded, errReload := config.LoadConfig(configPath)
	if errReload != nil {
		t.Fatalf("reload config: %v", errReload)
	}
	if len(reloaded.ProxyPools) != 0 || len(reloaded.ProxyRules) != 0 {
		t.Fatalf("reloaded proxy config = pools:%#v rules:%#v", reloaded.ProxyPools, reloaded.ProxyRules)
	}
	persisted, errRead := os.ReadFile(configPath)
	if errRead != nil {
		t.Fatalf("read persisted config: %v", errRead)
	}
	var document map[string]any
	if errYAML := yaml.Unmarshal(persisted, &document); errYAML != nil {
		t.Fatalf("decode persisted config: %v", errYAML)
	}
	for _, key := range []string{"proxy-pools", "proxy-rules"} {
		value, exists := document[key]
		if !exists {
			t.Fatalf("persisted config omitted %s: %s", key, persisted)
		}
		items, isSequence := value.([]any)
		if !isSequence || len(items) != 0 {
			t.Fatalf("persisted %s = %#v, want explicit empty sequence", key, value)
		}
	}
	info, errStat := os.Stat(configPath)
	if errStat != nil {
		t.Fatalf("stat config: %v", errStat)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("config mode = %o, want 600", got)
	}
}

func TestProxyPoolCreateRollsBackOnPersistenceFailure(t *testing.T) {
	cfg := &config.Config{}
	h := NewHandler(cfg, filepath.Join(t.TempDir(), "missing", "config.yaml"), nil)
	router := gin.New()
	router.POST("/proxy-pools", h.PostProxyPool)
	response := performProxyConfigRequest(router, http.MethodPost, "/proxy-pools", `{
		"name":"pool",
		"entries":[{"id":"node","url-template":"http://proxy.example:8080"}]
	}`)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s", response.Code, response.Body.String())
	}
	if len(cfg.ProxyPools) != 0 || len(cfg.ProxyRules) != 0 {
		t.Fatalf("configuration was not rolled back: %#v %#v", cfg.ProxyPools, cfg.ProxyRules)
	}
}

func TestProxyPoolMutationsRollBackOnPersistenceFailure(t *testing.T) {
	missingConfigPath := func(t *testing.T) string {
		t.Helper()
		return filepath.Join(t.TempDir(), "missing", "config.yaml")
	}

	t.Run("patch rename", func(t *testing.T) {
		cfg := &config.Config{SDKConfig: config.SDKConfig{
			ProxyPools: []config.ProxyPoolConfig{{Name: "old", Entries: []config.ProxyPoolEntryConfig{{ID: "node", URLTemplate: "http://proxy.example:8080"}}}},
			ProxyRules: []config.ProxyRuleConfig{{Name: "rule", Pool: "old"}},
		}}
		h := NewHandler(cfg, missingConfigPath(t), nil)
		router := gin.New()
		router.PATCH("/proxy-pools/:name", h.PatchProxyPool)
		response := performProxyConfigRequest(router, http.MethodPatch, "/proxy-pools/old", `{"name":"new"}`)
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body=%s", response.Code, response.Body.String())
		}
		if cfg.ProxyPools[0].Name != "old" || cfg.ProxyRules[0].Pool != "old" {
			t.Fatalf("configuration was not rolled back: %#v %#v", cfg.ProxyPools, cfg.ProxyRules)
		}
	})

	t.Run("patch entries", func(t *testing.T) {
		cfg := &config.Config{SDKConfig: config.SDKConfig{ProxyPools: []config.ProxyPoolConfig{{
			Name: "pool",
			Entries: []config.ProxyPoolEntryConfig{
				{ID: "first", URLTemplate: "http://first.example:8080"},
				{ID: "second", URLTemplate: "http://second.example:8080"},
			},
		}}}}
		h := NewHandler(cfg, missingConfigPath(t), nil)
		router := gin.New()
		router.PATCH("/proxy-pools/:name", h.PatchProxyPool)
		response := performProxyConfigRequest(router, http.MethodPatch, "/proxy-pools/pool", `{
			"entries":[{"id":"first","ports":"9000"}],
			"delete-entry-ids":["second"]
		}`)
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body=%s", response.Code, response.Body.String())
		}
		if len(cfg.ProxyPools[0].Entries) != 2 || cfg.ProxyPools[0].Entries[0].Ports != "" || cfg.ProxyPools[0].Entries[1].ID != "second" {
			t.Fatalf("nested entries were not rolled back: %#v", cfg.ProxyPools[0].Entries)
		}
	})

	t.Run("delete", func(t *testing.T) {
		cfg := &config.Config{SDKConfig: config.SDKConfig{ProxyPools: []config.ProxyPoolConfig{{
			Name: "pool", Entries: []config.ProxyPoolEntryConfig{{ID: "node", URLTemplate: "http://proxy.example:8080"}},
		}}}}
		h := NewHandler(cfg, missingConfigPath(t), nil)
		router := gin.New()
		router.DELETE("/proxy-pools/:name", h.DeleteProxyPool)
		response := performProxyConfigRequest(router, http.MethodDelete, "/proxy-pools/pool", "")
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body=%s", response.Code, response.Body.String())
		}
		if len(cfg.ProxyPools) != 1 || cfg.ProxyPools[0].Name != "pool" {
			t.Fatalf("configuration was not rolled back: %#v", cfg.ProxyPools)
		}
	})

	t.Run("put rules", func(t *testing.T) {
		cfg := &config.Config{SDKConfig: config.SDKConfig{
			ProxyPools: []config.ProxyPoolConfig{{Name: "pool", Entries: []config.ProxyPoolEntryConfig{{ID: "node", URLTemplate: "http://proxy.example:8080"}}}},
			ProxyRules: []config.ProxyRuleConfig{{Name: "old", Pool: "pool", Providers: []string{"codex"}}},
		}}
		h := NewHandler(cfg, missingConfigPath(t), nil)
		router := gin.New()
		router.PUT("/proxy-rules", h.PutProxyRules)
		response := performProxyConfigRequest(router, http.MethodPut, "/proxy-rules", `[{"name":"new","pool":"pool","providers":["xai"]}]`)
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body=%s", response.Code, response.Body.String())
		}
		if len(cfg.ProxyRules) != 1 || cfg.ProxyRules[0].Name != "old" || cfg.ProxyRules[0].Providers[0] != "codex" {
			t.Fatalf("configuration was not rolled back: %#v", cfg.ProxyRules)
		}
	})
}

func TestProxyPoolValidationErrorDoesNotLeakPassword(t *testing.T) {
	cfg := &config.Config{}
	h := NewHandlerWithoutConfigFilePath(cfg, nil)
	router := gin.New()
	router.POST("/proxy-pools", h.PostProxyPool)
	response := performProxyConfigRequest(router, http.MethodPost, "/proxy-pools", `{
		"name":"pool",
		"entries":[{"id":"node","url-template":"http://user:sec%ret@proxy.example:8080"}]
	}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "sec%ret") {
		t.Fatalf("validation response leaked password: %s", response.Body.String())
	}
}

func TestProviderPatchRejectsAmbiguousMatch(t *testing.T) {
	cfg := &config.Config{ClaudeKey: []config.ClaudeKey{{APIKey: "key"}, {APIKey: "key"}}}
	h := NewHandlerWithoutConfigFilePath(cfg, nil)
	router := gin.New()
	router.PATCH("/claude", h.PatchClaudeKey)
	response := performProxyConfigRequest(router, http.MethodPatch, "/claude", `{"match":"key","value":{"prefix":"changed"}}`)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "multiple") {
		t.Fatalf("status = %d, want ambiguous 400; body=%s", response.Code, response.Body.String())
	}
	if cfg.ClaudeKey[0].Prefix != "" || cfg.ClaudeKey[1].Prefix != "" {
		t.Fatalf("ambiguous patch modified entries: %#v", cfg.ClaudeKey)
	}
}

func TestOpenAICompatibilityDeleteRejectsAmbiguousName(t *testing.T) {
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{
		{Name: "duplicate", BaseURL: "https://first.example"},
		{Name: "duplicate", BaseURL: "https://second.example"},
	}}
	h := NewHandlerWithoutConfigFilePath(cfg, nil)
	router := gin.New()
	router.DELETE("/openai", h.DeleteOpenAICompat)
	response := performProxyConfigRequest(router, http.MethodDelete, "/openai?name=duplicate", "")
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "multiple") {
		t.Fatalf("status = %d, want ambiguous 400; body=%s", response.Code, response.Body.String())
	}
	if len(cfg.OpenAICompatibility) != 2 {
		t.Fatalf("ambiguous delete removed providers: %#v", cfg.OpenAICompatibility)
	}
}

func TestProviderProxyPasswordIsMaskedAndMaskedPatchIsPreserved(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("gemini-api-key: []\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{GeminiKey: []config.GeminiKey{{
		APIKey: "key", ProxyURL: "http://user:secret@proxy.example:8080",
	}}}
	h := NewHandler(cfg, configPath, nil)
	router := gin.New()
	router.GET("/gemini", h.GetGeminiKeys)
	router.PATCH("/gemini", h.PatchGeminiKey)
	router.PUT("/gemini", h.PutGeminiKeys)

	get := performProxyConfigRequest(router, http.MethodGet, "/gemini", "")
	if strings.Contains(get.Body.String(), "secret") || !strings.Contains(get.Body.String(), "********") {
		t.Fatalf("GET leaked proxy password: %s", get.Body.String())
	}
	patch := performProxyConfigRequest(router, http.MethodPatch, "/gemini", `{
		"index":0,
		"value":{"prefix":"group","proxy-url":"http://user:********@proxy.example:8080"}
	}`)
	if patch.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d; body=%s", patch.Code, patch.Body.String())
	}
	if got := cfg.GeminiKey[0].ProxyURL; got != "http://user:secret@proxy.example:8080" {
		t.Fatalf("PATCH replaced proxy password: %q", got)
	}
	put := performProxyConfigRequest(router, http.MethodPut, "/gemini", `[{"api-key":"key","proxy-url":"http://user:********@proxy.example:8080"}]`)
	if put.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200; body=%s", put.Code, put.Body.String())
	}
	if got := cfg.GeminiKey[0].ProxyURL; got != "http://user:secret@proxy.example:8080" {
		t.Fatalf("PUT replaced proxy password: %q", got)
	}
	forcePatch := performProxyConfigRequest(router, http.MethodPatch, "/gemini?replace-masked-proxy=true", `{
		"index":0,
		"value":{"proxy-url":"http://user:********@proxy.example:8080"}
	}`)
	if forcePatch.Code != http.StatusOK {
		t.Fatalf("explicit PATCH status = %d; body=%s", forcePatch.Code, forcePatch.Body.String())
	}
	if got := cfg.GeminiKey[0].ProxyURL; got != "http://user:********@proxy.example:8080" {
		t.Fatalf("explicit PATCH proxy URL = %q", got)
	}
}

func TestProviderProxyMutationsRollBackOnPersistenceFailure(t *testing.T) {
	t.Run("put", func(t *testing.T) {
		cfg := &config.Config{GeminiKey: []config.GeminiKey{{
			APIKey: "old-key", ProxyURL: "http://user:secret@proxy.example:8080",
		}}}
		h := NewHandler(cfg, filepath.Join(t.TempDir(), "missing", "config.yaml"), nil)
		router := gin.New()
		router.PUT("/gemini", h.PutGeminiKeys)

		response := performProxyConfigRequest(router, http.MethodPut, "/gemini", `[{"api-key":"new-key"}]`)
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body=%s", response.Code, response.Body.String())
		}
		if len(cfg.GeminiKey) != 1 || cfg.GeminiKey[0].APIKey != "old-key" || cfg.GeminiKey[0].ProxyURL != "http://user:secret@proxy.example:8080" {
			t.Fatalf("configuration was not rolled back: %#v", cfg.GeminiKey)
		}
	})

	t.Run("patch nested proxy", func(t *testing.T) {
		cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{
			Name:    "provider",
			BaseURL: "https://api.example",
			APIKeyEntries: []config.OpenAICompatibilityAPIKey{{
				APIKey: "key", ProxyURL: "http://user:secret@proxy.example:8080",
			}},
		}}}
		h := NewHandler(cfg, filepath.Join(t.TempDir(), "missing", "config.yaml"), nil)
		router := gin.New()
		router.PATCH("/openai", h.PatchOpenAICompat)

		response := performProxyConfigRequest(router, http.MethodPatch, "/openai", `{
			"index":0,
			"value":{"api-key-entries":[{"api-key":"key","proxy-url":"http://user:new-secret@proxy.example:8080"}]}
		}`)
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body=%s", response.Code, response.Body.String())
		}
		entries := cfg.OpenAICompatibility[0].APIKeyEntries
		if len(entries) != 1 || entries[0].ProxyURL != "http://user:secret@proxy.example:8080" {
			t.Fatalf("configuration was not rolled back: %#v", entries)
		}
	})

	t.Run("delete", func(t *testing.T) {
		cfg := &config.Config{GeminiKey: []config.GeminiKey{{APIKey: "key"}}}
		h := NewHandler(cfg, filepath.Join(t.TempDir(), "missing", "config.yaml"), nil)
		router := gin.New()
		router.DELETE("/gemini", h.DeleteGeminiKey)

		response := performProxyConfigRequest(router, http.MethodDelete, "/gemini?index=0", "")
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500; body=%s", response.Code, response.Body.String())
		}
		if len(cfg.GeminiKey) != 1 || cfg.GeminiKey[0].APIKey != "key" {
			t.Fatalf("configuration was not rolled back: %#v", cfg.GeminiKey)
		}
	})
}

func TestProviderMaskedProxyRestoreRejectsAmbiguousCredentials(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("gemini-api-key: []\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{GeminiKey: []config.GeminiKey{
		{APIKey: "key", ProxyURL: "http://user:first@proxy.example:8080"},
		{APIKey: "key", ProxyURL: "http://user:second@proxy.example:8080"},
	}}
	h := NewHandler(cfg, configPath, nil)
	router := gin.New()
	router.PUT("/gemini", h.PutGeminiKeys)

	response := performProxyConfigRequest(router, http.MethodPut, "/gemini", `[
		{"api-key":"key","proxy-url":"http://user:********@proxy.example:8080"},
		{"api-key":"key","proxy-url":"http://user:********@proxy.example:8080"}
	]`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "ambiguous") {
		t.Fatalf("response did not report ambiguity: %s", response.Body.String())
	}
	if cfg.GeminiKey[0].ProxyURL != "http://user:first@proxy.example:8080" || cfg.GeminiKey[1].ProxyURL != "http://user:second@proxy.example:8080" {
		t.Fatalf("ambiguous credentials were modified: %#v", cfg.GeminiKey)
	}
}

func performProxyConfigRequest(router http.Handler, method, target, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	router.ServeHTTP(recorder, request)
	return recorder
}
