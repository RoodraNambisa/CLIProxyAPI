package registry

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestCodexCatalogAstraAvailabilityAndWireCapabilities(t *testing.T) {
	for tier, models := range map[string][]*ModelInfo{
		"free": GetCodexFreeModels(), "plus": GetCodexPlusModels(),
		"team": GetCodexTeamModels(), "pro": GetCodexProModels(),
	} {
		var astra *ModelInfo
		for _, model := range models {
			if model != nil && model.ID == "gpt-6-astra" {
				astra = model
			}
		}
		if tier == "free" {
			if astra != nil {
				t.Fatal("Astra was added to free credentials")
			}
			continue
		}
		if astra == nil || astra.ContextLength != 272000 || astra.MaxCompletionTokens != 128000 {
			t.Fatalf("%s Astra availability or capacity differs from the reviewed catalog", tier)
		}
		if !reflect.DeepEqual(astra.SupportedInputModalities, []string{"text", "image"}) || !reflect.DeepEqual(astra.SupportedOutputModalities, []string{"text"}) {
			t.Fatal("Astra modalities were not loaded from the catalog")
		}
		// Ultra is a client delegation mode; the official client sends a supported
		// wire effort. Do not extend the proxy's canonical effort enum for it.
		if astra.Thinking == nil || !reflect.DeepEqual(astra.Thinking.Levels, []string{"low", "medium", "high", "xhigh", "max"}) {
			t.Fatal("Astra wire reasoning capabilities differ from the reviewed catalog")
		}
	}
}

func TestCodexEmbeddedCatalogDoesNotImportExcludedModelHeaders(t *testing.T) {
	var groups map[string][]map[string]json.RawMessage
	if err := json.Unmarshal(embeddedModelsJSON, &groups); err != nil {
		t.Fatal(err)
	}
	for name, models := range groups {
		if !strings.HasPrefix(name, "codex-") {
			continue
		}
		for _, model := range models {
			var settings map[string]json.RawMessage
			if raw := model["config"]; len(raw) > 0 {
				if err := json.Unmarshal(raw, &settings); err != nil {
					t.Fatal(err)
				}
			}
			if _, exists := settings["override_header"]; exists {
				t.Fatal("excluded per-model header overrides entered the embedded catalog")
			}
		}
	}
}
