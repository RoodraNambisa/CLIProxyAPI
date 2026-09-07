package openai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestCodexClientSynthesizedModelDoesNotInheritToolSearch(t *testing.T) {
	response := CodexClientModelsResponse([]map[string]any{{"id": "fixture-custom-model"}})
	models := response["models"].([]map[string]any)
	if len(models) != 1 || models[0]["supports_search_tool"] != false {
		t.Fatal("synthesized model inherited tool search")
	}
}

func TestCodexClientSearchSupportRequiresTemplateAndKnownProviders(t *testing.T) {
	for _, scenario := range []struct {
		name                         string
		template, advertised, lookup bool
		providers                    []string
		want                         bool
	}{
		{"native SDK", true, true, false, nil, true},
		{"synthesized SDK", false, true, false, nil, false},
		{"normalized Codex providers", true, true, true, []string{" CoDeX ", "codex"}, true},
		{"no provider", true, true, true, nil, false},
		{"unknown provider", true, true, true, []string{""}, false},
		{"mixed provider", true, true, true, []string{"codex", "claude"}, false},
		{"unsupported template", true, false, true, []string{"codex"}, false},
		{"custom template fallback", false, true, true, []string{"codex"}, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			calls := 0
			var lookup codexClientModelProvidersFunc
			if scenario.lookup {
				lookup = func(id string) []string {
					if id != "fixture" {
						t.Fatal("wrong model scope")
					}
					calls++
					return scenario.providers
				}
			}
			model := map[string]any{"supports_search_tool": scenario.advertised, "display_name": "retained"}
			applyCodexClientSearchToolSupport(model, "fixture", scenario.template, lookup)
			if model["supports_search_tool"] != scenario.want || model["display_name"] != "retained" {
				t.Fatal("capability gate changed an unrelated field or returned the wrong support")
			}
			wantCalls := 0
			if scenario.lookup && scenario.template && scenario.advertised {
				wantCalls = 1
			}
			if calls != wantCalls {
				t.Fatal("unnecessary provider lookup")
			}
		})
	}
}

func TestCodexClientToolSearchTracksActualModelProviders(t *testing.T) {
	modelRegistry := registry.GetGlobalRegistry()
	id := "gpt-6-astra"
	codexID, otherID := t.Name()+"-codex", t.Name()+"-other"
	modelRegistry.RegisterClient(codexID, "codex", []*registry.ModelInfo{{ID: id}})
	t.Cleanup(func() { modelRegistry.UnregisterClient(codexID); modelRegistry.UnregisterClient(otherID) })
	response := func() bool {
		t.Helper()
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.153.4", nil)
		(&OpenAIAPIHandler{}).OpenAIModels(ctx)
		var payload codexClientModelsPayload
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
		for _, model := range payload.Models {
			if model["slug"] == id {
				supported, _ := model["supports_search_tool"].(bool)
				return supported
			}
		}
		t.Fatal("registered model missing")
		return false
	}
	if !response() {
		t.Fatal("native Codex model lost tool search")
	}
	modelRegistry.RegisterClient(otherID, "claude", []*registry.ModelInfo{{ID: id}})
	if response() {
		t.Fatal("mixed-provider model advertised Codex-only tool search")
	}
	modelRegistry.UnregisterClient(otherID)
	if !response() {
		t.Fatal("provider update or prior response changed the cached template")
	}
	modelRegistry.UnregisterClient(codexID)
	modelRegistry.RegisterClient(otherID, "openai", []*registry.ModelInfo{{ID: id}})
	if response() {
		t.Fatal("non-Codex provider inherited tool search")
	}
	direct := CodexClientModelsResponseForClient([]map[string]any{{"id": id}}, "0.153.4")["models"].([]map[string]any)
	if direct[0]["supports_search_tool"] != true {
		t.Fatal("HTTP filtering mutated the stored template")
	}
}
