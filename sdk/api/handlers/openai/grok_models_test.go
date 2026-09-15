package openai

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/tidwall/gjson"
)

func TestGrokBuildCatalogRetainsProviderAccessAndCapabilities(t *testing.T) {
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient("grok-allowed", "xai", []*registry.ModelInfo{{ID: "grok-allowed", DisplayName: "Grok Allowed", ContextLength: 1234, Thinking: &registry.ThinkingSupport{Levels: []string{"low", "high"}}}})
	reg.RegisterClient("grok-hidden", "codex", []*registry.ModelInfo{{ID: "codex-hidden"}})
	t.Cleanup(func() { reg.UnregisterClient("grok-allowed"); reg.UnregisterClient("grok-hidden") })
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	c.Request.Header.Set("User-Agent", "grok-shell/0.2.120")
	c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: "xai"})
	(&OpenAIAPIHandler{}).OpenAIModels(c)
	body := w.Body.Bytes()
	if w.Code != 200 || gjson.GetBytes(body, "data.#").Int() != 1 || gjson.GetBytes(body, "data.0.model").String() != "grok-allowed" || gjson.GetBytes(body, "data.0.context_window").Int() != 1234 || gjson.GetBytes(body, "data.0.api_backend").String() != "responses" || gjson.GetBytes(body, "data.0.reasoning_efforts.#").Int() != 2 {
		t.Fatalf("Build catalog: %s", body)
	}
}
