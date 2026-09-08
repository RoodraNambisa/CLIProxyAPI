package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers/claude"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers/gemini"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers/openai"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestModelEndpointsUseProviderScopedMetadata(t *testing.T) {
	const modelID = "provider-scoped-catalog-model"
	r := registry.GetGlobalRegistry()
	r.RegisterClient("catalog-allowed", "codex", []*registry.ModelInfo{{ID: modelID, DisplayName: "Allowed label", OwnedBy: "allowed", ContextLength: 131072, MaxContextLength: 131072, InputTokenLimit: 131072}})
	r.RegisterClient("catalog-forbidden", "xai", []*registry.ModelInfo{{ID: modelID, DisplayName: "Forbidden label", OwnedBy: "forbidden", ContextLength: 1048576, MaxContextLength: 1048576, InputTokenLimit: 1048576}})
	t.Cleanup(func() { r.UnregisterClient("catalog-allowed"); r.UnregisterClient("catalog-forbidden") })
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	for _, tc := range []struct {
		name, path, models, identity, label string
		handler                             func(*gin.Context)
	}{
		{"openai", "/v1/models", "data", "id", "owned_by", openai.NewOpenAIAPIHandler(base).OpenAIModels},
		{"responses", "/v1/models", "data", "id", "display_name", openai.NewOpenAIResponsesAPIHandler(base).OpenAIResponsesModels},
		{"codex", "/v1/models?client_version=0.153.4", "models", "slug", "display_name", openai.NewOpenAIAPIHandler(base).OpenAIModels},
		{"claude", "/v1/models", "data", "id", "display_name", claude.NewClaudeCodeAPIHandler(base).ClaudeModels},
		{"gemini-list", "/v1beta/models", "models", "name", "displayName", gemini.NewGeminiAPIHandler(base).GeminiModels},
		{"gemini-get", "/v1beta/models/" + modelID, "", "name", "displayName", gemini.NewGeminiAPIHandler(base).GeminiGetHandler},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, scope := range []string{"codex", "xai", "unknown", ""} {
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodGet, tc.path, nil)
				c.Params = gin.Params{{Key: "action", Value: modelID}}
				c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: scope})
				tc.handler(c)
				result := gjson.ParseBytes(recorder.Body.Bytes())
				if tc.models != "" {
					var selected gjson.Result
					for _, model := range result.Get(tc.models).Array() {
						if strings.TrimPrefix(model.Get(tc.identity).String(), "models/") == modelID {
							selected = model
							break
						}
					}
					result = selected
				}
				if scope == "unknown" {
					if result.Get(tc.identity).Exists() {
						t.Fatal("forbidden model was listed")
					}
					continue
				}
				wantLabel, wantContext := "Forbidden label", int64(1048576)
				if scope == "codex" {
					wantLabel, wantContext = "Allowed label", 131072
				}
				if tc.label == "owned_by" {
					wantLabel = strings.ToLower(strings.Fields(wantLabel)[0])
				}
				if recorder.Code != http.StatusOK || result.Get(tc.label).String() != wantLabel {
					t.Fatalf("scope %q status %d: label %q, want %q", scope, recorder.Code, result.Get(tc.label).String(), wantLabel)
				}
				if tc.name == "codex" && (result.Get("context_window").Int() != wantContext || result.Get("max_context_window").Int() != wantContext) {
					t.Fatalf("scope %q has wrong Codex context", scope)
				}
				if strings.HasPrefix(tc.name, "gemini") && result.Get("inputTokenLimit").Int() != wantContext {
					t.Fatalf("scope %q has wrong Gemini context", scope)
				}
			}
		})
	}
}
