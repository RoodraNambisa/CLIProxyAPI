package helps

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexCollaborationModelsPreserveDeclaredCapabilitiesAndOrder(t *testing.T) {
	models := []map[string]any{
		{"slug": "native", "description": "First\n model", "default_reasoning_level": "medium",
			"supported_reasoning_levels": []any{map[string]any{"effort": "minimal"}, map[string]any{"effort": "medium"}, map[string]any{"effort": "medium"}, map[string]any{"effort": "invented"}},
			"service_tiers":              []any{map[string]any{"id": "priority"}, map[string]any{"id": "priority"}}},
		{"slug": "custom", "description": "Second!", "supported_reasoning_levels": []any{}},
		{"slug": "native", "description": "duplicate"},
		{"slug": "empty", "supported_reasoning_levels": []any{map[string]any{"effort": "none"}}, "default_reasoning_level": "unknown"},
		nil, {"slug": "bad\nid"}, {"slug": 12},
	}
	got := FormatCodexCollaborationModels(models)
	want := "- `native`: First model. Reasoning efforts: minimal, medium (default). Service tiers: priority.\n- `custom`: Second!\n- `empty`: Reasoning efforts: none (default)."
	if got != want {
		t.Fatalf("unexpected model description:\n%s", got)
	}
	if FormatCodexCollaborationModels(nil) != "" {
		t.Fatal("empty catalog gained a fallback model")
	}
	if models[0]["description"] != "First\n model" || len(models[0]["supported_reasoning_levels"].([]any)) != 4 {
		t.Fatal("formatter mutated the caller's model snapshot")
	}
	if got := codexMarkdownCode("a``b"); got != "``` a``b ```" {
		t.Fatal("model ID backticks broke the inline code delimiter")
	}
}

func TestCodexCollaborationDescriptionReplacementPreservesInstructions(t *testing.T) {
	modelList := "- `model-a`: Model A."
	for _, description := range []string{
		"    " + codexSpawnAgentModelsHeading + "\n- old\n- old\n    Spawns an agent to work.\nKeep later instructions.",
		codexSpawnAgentModelsHeading + "\n- first old\n\n" + codexSpawnAgentModelsHeading + "\n- second old\nSpawns an agent to work.",
		"Create a worker.", "", "Create a worker.\n",
	} {
		got := replaceCodexSpawnAgentModels(description, modelList)
		if strings.Count(got, codexSpawnAgentModelsHeading) != 1 || strings.Count(got, "`model-a`") != 1 || strings.Contains(got, " old") {
			t.Fatal("stale or duplicate model sections survived")
		}
		if strings.Contains(description, "Keep later instructions.") && !strings.Contains(got, "Keep later instructions.") {
			t.Fatal("instructions following the model section were deleted")
		}
		if again := replaceCodexSpawnAgentModels(got, modelList); again != got {
			t.Fatal("repeated preparation changed the description")
		}
	}
}

func TestCodexCollaborationToolsPrepareKnownSchemasWithOrWithoutModels(t *testing.T) {
	raw := []byte(`{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","description":"Create a worker.","parameters":{"properties":{"message":{"encrypted":true}}}},{"type":"function","name":"send_message","description":"Send.","parameters":{"properties":{"message":{"encrypted":true}}}}]},{"type":"namespace","name":"other","tools":[{"type":"function","name":"spawn_agent","description":"Foreign","parameters":{"properties":{"message":{"encrypted":true}}}}]}],"metadata":{"counter":9007199254740993},"prompt_cache_key":"cache"}`)
	original := bytes.Clone(raw)
	for _, list := range []string{"", "- model-a"} {
		got := PrepareCodexCollaborationTools(raw, list)
		for _, path := range []string{"tools.0.tools.0.parameters.properties.message.encrypted", "tools.0.tools.1.parameters.properties.message.encrypted"} {
			if gjson.GetBytes(got, path).Exists() {
				t.Fatal("message schema encryption marker survived")
			}
		}
		description := gjson.GetBytes(got, "tools.0.tools.0.description").String()
		if list == "" && description != "Create a worker." || list != "" && !strings.Contains(description, list) {
			t.Fatal("description fallback or replacement failed")
		}
		for _, path := range []string{"tools.0.name", "tools.0.tools.1.description", "tools.1", "metadata", "prompt_cache_key"} {
			if gjson.GetBytes(got, path).Raw != gjson.GetBytes(raw, path).Raw {
				t.Fatalf("unrelated data changed: %s", path)
			}
		}
	}
	if !bytes.Equal(raw, original) {
		t.Fatal("preparation mutated request")
	}
	conflict := []byte(`{"tools":[{"type":"function","name":"spawn_agent","description":"Keep.","parameters":{"properties":{"message":{"encrypted":true}}}},{"type":"namespace","name":"collaboration-optimize"}]}`)
	got := PrepareCodexCollaborationTools(conflict, "- model-a")
	if gjson.GetBytes(got, "tools.0.description").String() != "Keep." || gjson.GetBytes(got, "tools.0.parameters.properties.message.encrypted").Exists() {
		t.Fatal("conflict changed description or prevented schema normalization")
	}
}
