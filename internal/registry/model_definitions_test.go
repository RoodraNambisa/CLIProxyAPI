package registry

import (
	"slices"
	"testing"
)

func assertCodexModelsDoNotContain(t *testing.T, models []*ModelInfo, modelID string) {
	t.Helper()
	for _, model := range models {
		if model == nil {
			continue
		}
		if model.ID == modelID {
			t.Fatalf("expected codex models to exclude %q", modelID)
		}
	}
}

func assertCodexModelsContain(t *testing.T, models []*ModelInfo, modelID string) {
	t.Helper()
	for _, model := range models {
		if model == nil {
			continue
		}
		if model.ID == modelID {
			return
		}
	}
	t.Fatalf("expected codex models to include %q", modelID)
}

func TestGetCodexFreeModels_NoLongerIncludesBuiltInImageModel(t *testing.T) {
	assertCodexModelsDoNotContain(t, GetCodexFreeModels(), "gpt-image-2")
}

func TestGetCodexTeamModels_NoLongerIncludesBuiltInImageModel(t *testing.T) {
	assertCodexModelsDoNotContain(t, GetCodexTeamModels(), "gpt-image-2")
}

func TestGetCodexPlusModels_NoLongerIncludesBuiltInImageModel(t *testing.T) {
	assertCodexModelsDoNotContain(t, GetCodexPlusModels(), "gpt-image-2")
}

func TestGetCodexProModels_StillIncludesSpark(t *testing.T) {
	models := GetCodexProModels()
	for _, model := range models {
		if model == nil {
			continue
		}
		if model.ID == codexSparkModelID {
			return
		}
	}
	t.Fatalf("expected codex pro models to include %q", codexSparkModelID)
}

func TestGetCodexProModels_NoLongerIncludesBuiltInImageModel(t *testing.T) {
	assertCodexModelsDoNotContain(t, GetCodexProModels(), "gpt-image-2")
}

func TestGetCodexPlusModels_ExcludesSpark(t *testing.T) {
	models := GetCodexPlusModels()
	if len(models) == 0 {
		t.Fatal("expected codex plus models to be non-empty")
	}
	for _, model := range models {
		if model == nil {
			continue
		}
		if model.ID == codexSparkModelID {
			t.Fatalf("expected codex plus models to exclude %q", codexSparkModelID)
		}
	}
}

func TestGetCodexModels_GPT55Availability(t *testing.T) {
	assertCodexModelsContain(t, GetCodexFreeModels(), "gpt-5.5")
	assertCodexModelsContain(t, GetCodexPlusModels(), "gpt-5.5")
	assertCodexModelsContain(t, GetCodexTeamModels(), "gpt-5.5")
	assertCodexModelsContain(t, GetCodexProModels(), "gpt-5.5")
}

func TestGetCodexModels_GPT56Availability(t *testing.T) {
	assertCodexModelsDoNotContain(t, GetCodexFreeModels(), "gpt-5.6-sol")
	assertCodexModelsContain(t, GetCodexFreeModels(), "gpt-5.6-terra")
	assertCodexModelsContain(t, GetCodexFreeModels(), "gpt-5.6-luna")
	assertCodexModelsContain(t, GetCodexPlusModels(), "gpt-5.6-sol")
	assertCodexModelsContain(t, GetCodexTeamModels(), "gpt-5.6-sol")
	assertCodexModelsContain(t, GetCodexProModels(), "gpt-5.6-sol")
}

func TestGetCodexModels_AutoReviewAvailability(t *testing.T) {
	assertCodexModelsContain(t, GetCodexFreeModels(), "codex-auto-review")
	assertCodexModelsContain(t, GetCodexPlusModels(), "codex-auto-review")
	assertCodexModelsContain(t, GetCodexTeamModels(), "codex-auto-review")
	assertCodexModelsContain(t, GetCodexProModels(), "codex-auto-review")
}

func TestGetXAIModelsIncludesMediaBuiltins(t *testing.T) {
	models := GetXAIModels()
	want := map[string]bool{
		xaiBuiltinImageModelID:          false,
		xaiBuiltinImageQualityModelID:   false,
		xaiBuiltinVideoModelID:          false,
		xaiBuiltinVideo15PreviewModelID: false,
	}

	for _, model := range models {
		if model == nil {
			continue
		}
		if _, ok := want[model.ID]; ok {
			want[model.ID] = true
		}
	}

	for modelID, found := range want {
		if !found {
			t.Fatalf("expected xAI models to include %q", modelID)
		}
	}
}

func TestGetStaticModelDefinitionsByChannel_XAIAliases(t *testing.T) {
	for _, channel := range []string{"xai", "x-ai", "grok"} {
		if models := GetStaticModelDefinitionsByChannel(channel); len(models) == 0 {
			t.Fatalf("expected %q channel to return xAI models", channel)
		}
	}
}

func TestGetAntigravityModelsMatchesUpstreamCatalog(t *testing.T) {
	want := []string{
		"claude-opus-4-6-thinking",
		"claude-sonnet-4-6",
		"gemini-3-flash",
		"gemini-3-flash-agent",
		"gemini-3.1-flash-image",
		"gemini-pro-agent",
		"gemini-3.1-pro-low",
		"gpt-oss-120b-medium",
		"gemini-3.1-flash-lite",
		"gemini-3.5-flash-low",
		"gemini-3.5-flash-extra-low",
	}
	models := GetAntigravityModels()
	if len(models) != len(want) {
		t.Fatalf("Antigravity model count = %d, want %d", len(models), len(want))
	}
	for i, model := range models {
		if model == nil {
			t.Fatalf("Antigravity model %d is nil", i)
		}
		if model.ID != want[i] {
			t.Fatalf("Antigravity model %d = %q, want %q", i, model.ID, want[i])
		}
		if model.Object != "model" || model.OwnedBy != "antigravity" || model.Type != "antigravity" {
			t.Fatalf("Antigravity model %q ownership metadata = (%q, %q, %q)", model.ID, model.Object, model.OwnedBy, model.Type)
		}
	}

	type metadata struct {
		displayName string
		context     int
		maxTokens   int
		thinkingMin int
		thinkingMax int
		levels      []string
	}
	wantMetadata := map[string]metadata{
		"gemini-3-flash-agent": {
			displayName: "Gemini 3.5 Flash (High)", context: 1048576, maxTokens: 65536,
			thinkingMin: 128, thinkingMax: 32768, levels: []string{"minimal", "low", "medium", "high"},
		},
		"gemini-pro-agent": {
			displayName: "Gemini 3.1 Pro (High)", context: 1048576, maxTokens: 65535,
			thinkingMin: 1, thinkingMax: 65535, levels: []string{"low", "medium", "high"},
		},
		"gemini-3.5-flash-low": {
			displayName: "Gemini 3.5 Flash (Medium)", context: 1048576, maxTokens: 65535,
			thinkingMin: 1, thinkingMax: 65535, levels: []string{"low", "medium", "high"},
		},
		"gemini-3.5-flash-extra-low": {
			displayName: "Gemini 3.5 Flash (Low)", context: 1048576, maxTokens: 65535,
			thinkingMin: 1, thinkingMax: 65535, levels: []string{"low", "medium", "high"},
		},
	}
	for _, model := range models {
		wantModel, ok := wantMetadata[model.ID]
		if !ok {
			continue
		}
		if model.Name != model.ID || model.DisplayName != wantModel.displayName || model.Description != wantModel.displayName {
			t.Fatalf("Antigravity model %q names = (%q, %q, %q)", model.ID, model.Name, model.DisplayName, model.Description)
		}
		if model.ContextLength != wantModel.context || model.MaxCompletionTokens != wantModel.maxTokens {
			t.Fatalf("Antigravity model %q limits = (%d, %d)", model.ID, model.ContextLength, model.MaxCompletionTokens)
		}
		if model.Thinking == nil || model.Thinking.Min != wantModel.thinkingMin || model.Thinking.Max != wantModel.thinkingMax || !model.Thinking.DynamicAllowed || !slices.Equal(model.Thinking.Levels, wantModel.levels) {
			t.Fatalf("Antigravity model %q thinking = %#v", model.ID, model.Thinking)
		}
	}
}
