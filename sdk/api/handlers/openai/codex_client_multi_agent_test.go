package openai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestCodexClientMultiAgentPolicyPreservesCatalogWhenDisabled(t *testing.T) {
	models := []map[string]any{{"id": "gpt-5.5"}, {"id": "gpt-6-astra"}, {"id": "fixture-custom-model"}}
	for _, version := range []string{"0.143.0", "0.153.4"} {
		baseline := CodexClientModelsResponseForClient(models, version)["models"].([]map[string]any)
		for _, enabled := range []bool{false, true, false} {
			got := codexClientModelsResponseForClient(models, version, nil, enabled)["models"].([]map[string]any)
			if len(got) != len(baseline) {
				t.Fatal("policy changed model availability")
			}
			for index, model := range got {
				want := cloneCodexClientModelMap(baseline[index])
				if enabled {
					want["multi_agent_version"] = "v2"
				}
				if !reflect.DeepEqual(model, want) {
					t.Fatalf("policy changed unrelated capability metadata for %s", stringModelValue(model, "slug"))
				}
				if !enabled && model["slug"] == "gpt-6-astra" && model["multi_agent_version"] != "v2" {
					t.Fatal("disabled optimization downgraded Astra's existing capability")
				}
			}
		}
	}
}

func TestCodexClientMultiAgentModelsEndpointUsesCurrentConfig(t *testing.T) {
	modelRegistry := registry.GetGlobalRegistry()
	clientID := t.Name()
	modelRegistry.RegisterClient(clientID, "codex", []*registry.ModelInfo{{ID: "gpt-5.5"}, {ID: "gpt-6-astra"}})
	t.Cleanup(func() { modelRegistry.UnregisterClient(clientID) })
	handler := NewOpenAIAPIHandler(handlers.NewBaseAPIHandlers(&config.SDKConfig{}, nil))
	for _, enabled := range []bool{false, true, false} {
		handler.UpdateClients(&config.SDKConfig{CodexOptimizeMultiAgentV2: enabled})
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.153.4", nil)
		handler.OpenAIModels(ctx)
		var payload codexClientModelsPayload
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		found := 0
		for _, model := range payload.Models {
			id := stringModelValue(model, "slug")
			if id != "gpt-5.5" && id != "gpt-6-astra" {
				continue
			}
			found++
			want := CodexClientModelsResponseForClient([]map[string]any{{"id": id}}, "0.153.4")["models"].([]map[string]any)[0]["multi_agent_version"]
			if enabled {
				want = "v2"
			}
			if model["multi_agent_version"] != want {
				t.Fatal("models endpoint ignored the installed configuration")
			}
		}
		if found != 2 {
			t.Fatal("registered models missing")
		}
	}
}
