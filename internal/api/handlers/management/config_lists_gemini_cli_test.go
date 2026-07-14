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
)

func TestPatchOAuthExcludedModelsRejectsRetiredGeminiCLI(t *testing.T) {
	cfg := &config.Config{OAuthExcludedModels: map[string][]string{"codex": {"gpt-old"}}}
	h := NewHandlerWithoutConfigFilePath(cfg, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/oauth-excluded-models", strings.NewReader(`{"provider":"Gemini-CLI","models":["legacy-model"]}`))
	ctx.Request.Header.Set("Content-Type", "application/json")

	h.PatchOAuthExcludedModels(ctx)

	if recorder.Code != http.StatusGone {
		t.Fatalf("status = %d, want 410; body=%s", recorder.Code, recorder.Body.String())
	}
	if _, exists := cfg.OAuthExcludedModels["gemini-cli"]; exists {
		t.Fatal("retired Gemini CLI exclusion was written to runtime config")
	}
	if len(cfg.OAuthExcludedModels["codex"]) != 1 {
		t.Fatalf("supported exclusions changed: %#v", cfg.OAuthExcludedModels)
	}
}

func TestDeleteRetiredGeminiCLIModelConfigurationIsIdempotentCleanup(t *testing.T) {
	tests := []struct {
		name string
		path string
		call func(*Handler, *gin.Context)
	}{
		{
			name: "excluded models",
			path: "/v0/management/oauth-excluded-models?provider=gemini-cli",
			call: func(h *Handler, c *gin.Context) { h.DeleteOAuthExcludedModels(c) },
		},
		{
			name: "model aliases",
			path: "/v0/management/oauth-model-alias?channel=gemini-cli",
			call: func(h *Handler, c *gin.Context) { h.DeleteOAuthModelAlias(c) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			original := `port: 8317
oauth-excluded-models:
  gemini-cli: [legacy]
  codex: [gpt-old]
oauth-model-alias:
  gemini-cli:
    - name: old
      alias: legacy
  codex:
    - name: gpt-old
      alias: gpt-new
`
			if errWrite := os.WriteFile(configPath, []byte(original), 0o600); errWrite != nil {
				t.Fatalf("write config: %v", errWrite)
			}
			cfg := &config.Config{
				Port:                8317,
				OAuthExcludedModels: map[string][]string{"codex": {"gpt-old"}},
				OAuthModelAlias: map[string][]config.OAuthModelAlias{
					"codex": {{Name: "gpt-old", Alias: "gpt-new"}},
				},
			}
			h := NewHandler(cfg, configPath, nil)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodDelete, test.path, nil)

			test.call(h, ctx)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
			}
			saved, errRead := os.ReadFile(configPath)
			if errRead != nil {
				t.Fatalf("read config: %v", errRead)
			}
			if strings.Contains(strings.ToLower(string(saved)), "gemini-cli:") {
				t.Fatalf("retired Gemini CLI configuration survived delete:\n%s", saved)
			}
		})
	}
}

func TestGetStaticModelDefinitionsRetiredGeminiCLIReturnsGone(t *testing.T) {
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "channel", Value: "Gemini-CLI"}}
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/model-definitions/Gemini-CLI", nil)

	h.GetStaticModelDefinitions(ctx)

	if recorder.Code != http.StatusGone {
		t.Fatalf("status = %d, want 410; body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestOAuthModelConfigurationMutationsHandleRetiredGeminiCLICompatibly(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		call          func(*Handler, *gin.Context)
		wantStatus    int
		wantExclusion string
		wantAlias     string
	}{
		{
			name:          "put excluded models",
			body:          `{"Gemini-CLI":["legacy"],"codex":["replacement"]}`,
			call:          func(h *Handler, c *gin.Context) { h.PutOAuthExcludedModels(c) },
			wantStatus:    http.StatusOK,
			wantExclusion: "replacement",
			wantAlias:     "original-alias",
		},
		{
			name:          "patch excluded models",
			body:          `{"provider":"gemini-cli","models":["legacy"]}`,
			call:          func(h *Handler, c *gin.Context) { h.PatchOAuthExcludedModels(c) },
			wantStatus:    http.StatusGone,
			wantExclusion: "original-model",
			wantAlias:     "original-alias",
		},
		{
			name:          "put model aliases",
			body:          `{"Gemini-CLI":[{"name":"old","alias":"legacy"}],"codex":[{"name":"gpt-old","alias":"replacement"}]}`,
			call:          func(h *Handler, c *gin.Context) { h.PutOAuthModelAlias(c) },
			wantStatus:    http.StatusOK,
			wantExclusion: "original-model",
			wantAlias:     "replacement",
		},
		{
			name:          "patch model aliases",
			body:          `{"channel":"gemini-cli","aliases":[{"name":"old","alias":"legacy"}]}`,
			call:          func(h *Handler, c *gin.Context) { h.PatchOAuthModelAlias(c) },
			wantStatus:    http.StatusGone,
			wantExclusion: "original-model",
			wantAlias:     "original-alias",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			if errWrite := os.WriteFile(configPath, []byte("port: 8317\n"), 0o600); errWrite != nil {
				t.Fatalf("write config: %v", errWrite)
			}
			cfg := &config.Config{
				Port:                8317,
				OAuthExcludedModels: map[string][]string{"codex": {"original-model"}},
				OAuthModelAlias: map[string][]config.OAuthModelAlias{
					"codex": {{Name: "gpt-old", Alias: "original-alias"}},
				},
			}
			h := NewHandler(cfg, configPath, nil)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodPut, "/v0/management/config", strings.NewReader(test.body))
			ctx.Request.Header.Set("Content-Type", "application/json")

			test.call(h, ctx)

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			if got := cfg.OAuthExcludedModels["codex"]; len(got) != 1 || got[0] != test.wantExclusion {
				t.Fatalf("supported exclusions changed: %#v", cfg.OAuthExcludedModels)
			}
			if got := cfg.OAuthModelAlias["codex"]; len(got) != 1 || got[0].Alias != test.wantAlias {
				t.Fatalf("supported aliases changed: %#v", cfg.OAuthModelAlias)
			}
			for key := range cfg.OAuthExcludedModels {
				if strings.EqualFold(strings.TrimSpace(key), "gemini-cli") {
					t.Fatal("retired Gemini CLI exclusion was written")
				}
			}
			for key := range cfg.OAuthModelAlias {
				if strings.EqualFold(strings.TrimSpace(key), "gemini-cli") {
					t.Fatal("retired Gemini CLI alias was written")
				}
			}
		})
	}
}
