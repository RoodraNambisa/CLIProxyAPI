package openai

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

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
