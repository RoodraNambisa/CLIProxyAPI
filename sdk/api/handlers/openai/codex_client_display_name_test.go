package openai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestCodexNativeTemplatePreservesExplicitDisplayName(t *testing.T) {
	for _, id := range []string{"gpt-5.5", "gpt-6-astra"} {
		original := CodexClientModelsResponseForClient([]map[string]any{{"id": id}}, "0.153.4")["models"].([]map[string]any)[0]
		for _, name := range []any{" My model name ", "", "  ", nil, 123} {
			model := map[string]any{"id": id, "display_name": name}
			got := CodexClientModelsResponseForClient([]map[string]any{model}, "0.153.4")["models"].([]map[string]any)[0]
			expected := cloneCodexClientModelMap(original)
			if name == " My model name " {
				expected["display_name"] = "My model name"
			}
			if !reflect.DeepEqual(got, expected) {
				t.Fatal("native template lost the display override or changed unrelated capabilities")
			}
			if !reflect.DeepEqual(model["display_name"], name) {
				t.Fatal("display override mutated input metadata")
			}
		}
		after := CodexClientModelsResponseForClient([]map[string]any{{"id": id}}, "0.153.4")["models"].([]map[string]any)[0]
		if !reflect.DeepEqual(after, original) {
			t.Fatal("display override mutated the stored template")
		}
	}
}

func TestCodexNativeHTTPModelUsesRegistryDisplayName(t *testing.T) {
	modelRegistry := registry.GetGlobalRegistry()
	clientID := t.Name()
	modelRegistry.RegisterClient(clientID, "codex", []*registry.ModelInfo{{ID: "gpt-5.5", DisplayName: "My local Codex"}})
	t.Cleanup(func() { modelRegistry.UnregisterClient(clientID) })
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.153.4", nil)
	(&OpenAIAPIHandler{}).OpenAIModels(ctx)
	var payload codexClientModelsPayload
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	for _, model := range payload.Models {
		if model["slug"] == "gpt-5.5" {
			if model["display_name"] != "My local Codex" {
				t.Fatal("HTTP catalog replaced the local display name")
			}
			return
		}
	}
	t.Fatal("registered native model missing")
}

func TestCodexClientDisplayNameUsesQueryMetadata(t *testing.T) {
	for _, id := range []string{"gpt-5.5", "display-scoped-model"} {
		t.Run(id, func(t *testing.T) {
			clientID := "display-label-" + id
			registry.GetGlobalRegistry().RegisterClient(clientID, "openai", []*registry.ModelInfo{{ID: id, DisplayName: "Other Registry Label"}})
			defer registry.GetGlobalRegistry().UnregisterClient(clientID)
			for _, name := range []string{"Caller Label", "另一个名称"} {
				source := map[string]any{"id": id, "display_name": name}
				result := buildCodexClientModels([]map[string]any{source}, nil)
				if len(result) != 1 || result[0]["display_name"] != name || result[0]["slug"] != id {
					t.Fatal("catalog replaced query label or model id")
				}
				if source["display_name"] != name {
					t.Fatal("query mutated source metadata")
				}
			}
			stored := registry.GetGlobalRegistry().GetModelsForClient(clientID)
			if len(stored) != 1 || stored[0].DisplayName != "Other Registry Label" {
				t.Fatal("query mutated another registry entry")
			}
		})
	}
}

func TestCodexClientDisplayNameFallsBackWhenNotSpecified(t *testing.T) {
	const id = "display-fallback-model"
	const clientID = "display-fallback-client"
	registry.GetGlobalRegistry().RegisterClient(clientID, "openai", []*registry.ModelInfo{{ID: id, DisplayName: "Registry Fallback"}})
	defer registry.GetGlobalRegistry().UnregisterClient(clientID)
	models := buildCodexClientModels([]map[string]any{{"id": id}}, nil)
	if len(models) != 1 || models[0]["display_name"] != "Registry Fallback" {
		t.Fatal("missing display name lost fallback")
	}
}
