package openai

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestCodexClientCatalogDoesNotInventResponsesTemplatesForRealtime(t *testing.T) {
	reg := registry.GetGlobalRegistry()
	clientID := "realtime-catalog-" + t.Name()
	models := registry.GetCodexRealtimeModels()
	models = append(models, &registry.ModelInfo{ID: "team/voice-catalog-fixture", UpstreamID: registry.CodexLiveModelID, Type: registry.CodexRealtimeModelType})
	models = append(models, &registry.ModelInfo{ID: "ordinary-codex-catalog-fixture", Object: "model", OwnedBy: "openai", Type: "codex"})
	reg.RegisterClient(clientID, "codex", models)
	defer reg.UnregisterClient(clientID)
	response := CodexClientModelsResponse(reg.GetAvailableModels("openai"))
	ordinaryFound := false
	for _, entry := range response["models"].([]map[string]any) {
		slug, _ := entry["slug"].(string)
		if slug == registry.CodexLiveModelID || slug == registry.CodexRealtimeModelID || slug == "team/voice-catalog-fixture" {
			t.Fatal("native realtime model received a Responses prompt template")
		}
		if slug == "ordinary-codex-catalog-fixture" {
			ordinaryFound = true
		}
	}
	if !ordinaryFound {
		t.Fatal("ordinary Codex model disappeared from the client catalog")
	}
	for _, id := range []string{registry.CodexLiveModelID, registry.CodexRealtimeModelID} {
		if !reg.ClientSupportsModel(clientID, id) {
			t.Fatal("catalog filtering removed the native routing model")
		}
	}
}
