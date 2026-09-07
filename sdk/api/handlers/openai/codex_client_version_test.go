package openai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/tidwall/gjson"
)

func TestCodexClientReasoningVersionFiltering(t *testing.T) {
	models := []map[string]any{{"id": "gpt-5.6-sol"}}
	for version, extended := range map[string]bool{"0.143.0": false, "0.143.9-beta": false, "v0.143.0": false, "0.144.0": true, "0.153.4": true, "1.0.0": true, "": true, "unknown": true, "0.143.bad": true} {
		response := CodexClientModelsResponseForClient(models, version)
		entries := response["models"].([]map[string]any)
		found := false
		for _, level := range entries[0]["supported_reasoning_levels"].([]any) {
			name := level.(map[string]any)["effort"]
			found = found || name == "max" || name == "ultra"
		}
		if found != extended {
			t.Fatalf("version %s has extended levels = %t, want %t", version, found, extended)
		}
	}
	// Filtering returns copies and must not remove capabilities from later clients.
	response := CodexClientModelsResponse(models)
	if len(response["models"].([]map[string]any)[0]["supported_reasoning_levels"].([]any)) < 6 {
		t.Fatal("legacy filtering mutated the cached template")
	}
}

func TestCodexClientReasoningEmptyArrayRemainsAnArray(t *testing.T) {
	for _, metadata := range []*registry.ThinkingSupport{{Levels: []string{"unsupported"}}} {
		entry := map[string]any{"supported_reasoning_levels": []any{map[string]any{"effort": "high"}}, "default_reasoning_level": "high"}
		applyCodexClientThinkingMetadata(entry, metadata)
		sanitizeCodexClientReasoningMetadata(entry)
		raw, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != `{"supported_reasoning_levels":[]}` {
			t.Fatalf("empty capability response = %s", raw)
		}
	}
	entry := map[string]any{"supported_reasoning_levels": []any{}}
	sanitizeCodexClientReasoningMetadata(entry)
	if levels, ok := entry["supported_reasoning_levels"].([]any); !ok || levels == nil || len(levels) != 0 {
		t.Fatal("explicit empty reasoning array was removed or made null")
	}
}

func TestCodexClientReasoningRetainsBudgetOnlyInheritance(t *testing.T) {
	for _, metadata := range []*registry.ThinkingSupport{nil, {}, {Min: 1024, Max: 32768, DynamicAllowed: true}, {Levels: []string{}}} {
		entry := map[string]any{"supported_reasoning_levels": []any{map[string]any{"effort": "high"}}, "default_reasoning_level": "high"}
		applyCodexClientThinkingMetadata(entry, metadata)
		sanitizeCodexClientReasoningMetadata(entry)
		levels := entry["supported_reasoning_levels"].([]any)
		if len(levels) != 1 || levels[0].(map[string]any)["effort"] != "high" || entry["default_reasoning_level"] != "high" {
			t.Fatal("unspecified discrete levels erased budget-model inheritance")
		}
	}
}

func TestCodexClientLegacyFilterDoesNotRestoreFallbackEfforts(t *testing.T) {
	clientID := "legacy-effort-catalog"
	models := []*registry.ModelInfo{
		{ID: "only-extended-effort-test", Thinking: &registry.ThinkingSupport{Levels: []string{"max", "ultra"}}},
		{ID: "mixed-effort-test", Thinking: &registry.ThinkingSupport{Levels: []string{"minimal", "max"}}},
	}
	modelRegistry := registry.GetGlobalRegistry()
	modelRegistry.RegisterClient(clientID, "openai-compatibility", models)
	t.Cleanup(func() { modelRegistry.UnregisterClient(clientID) })
	for _, tc := range []struct {
		model, version string
		want           []string
	}{
		{"only-extended-effort-test", "0.143.9", nil},
		{"only-extended-effort-test", "0.144.0", []string{"max", "ultra"}},
		{"mixed-effort-test", "0.143.9", []string{"minimal"}},
	} {
		response := CodexClientModelsResponseForClient([]map[string]any{{"id": tc.model}}, tc.version)
		entry := response["models"].([]map[string]any)[0]
		levels, ok := entry["supported_reasoning_levels"].([]any)
		if !ok || levels == nil || len(levels) != len(tc.want) {
			t.Fatal("filtered capabilities inherited unrelated fallback levels or lost their array")
		}
		for index, want := range tc.want {
			if levels[index].(map[string]any)["effort"] != want {
				t.Fatal("model-specific effort was replaced by a fallback choice")
			}
		}
		if len(tc.want) == 0 {
			if _, exists := entry["default_reasoning_level"]; exists {
				t.Fatal("empty reasoning list retained a default")
			}
		} else if entry["default_reasoning_level"] != tc.want[0] {
			t.Fatal("filtered default is not a supported effort")
		}
	}
}

func TestCodexClientMinimalReasoningAndUppercaseVersionPrefix(t *testing.T) {
	if supportsExtendedCodexClientReasoning("V0.143.9") {
		t.Error("uppercase prefix exposed extended levels to an old client")
	}
	entry := map[string]any{"supported_reasoning_levels": []any{map[string]any{"effort": "minimal"}}, "default_reasoning_level": "minimal"}
	sanitizeCodexClientReasoningMetadata(entry)
	levels := entry["supported_reasoning_levels"].([]any)
	if len(levels) != 1 || levels[0].(map[string]any)["effort"] != "minimal" || entry["default_reasoning_level"] != "minimal" {
		t.Fatal("valid minimal reasoning effort was removed")
	}
}

func TestCodexClientVersionQueryControlsModelResponse(t *testing.T) {
	clientID := "version-query-catalog"
	modelRegistry := registry.GetGlobalRegistry()
	modelRegistry.RegisterClient(clientID, "openai", []*registry.ModelInfo{{ID: "gpt-5.6-sol", Object: "model", OwnedBy: "openai"}})
	t.Cleanup(func() { modelRegistry.UnregisterClient(clientID) })
	for _, version := range []string{"0.143.9", "V0.143.9", "0.153.4"} {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version="+version, nil)
		c.Request.Header.Set("User-Agent", "codex_cli_rs/0.153.4")
		if version == "0.153.4" {
			c.Request.Header.Set("User-Agent", "codex_cli_rs/0.137.0")
		}
		(&OpenAIAPIHandler{}).OpenAIModels(c)
		foundModel, extended := false, false
		for _, model := range gjson.GetBytes(recorder.Body.Bytes(), "models").Array() {
			if model.Get("slug").String() != "gpt-5.6-sol" {
				continue
			}
			foundModel = true
			for _, level := range model.Get("supported_reasoning_levels").Array() {
				effort := level.Get("effort").String()
				extended = extended || effort == "max" || effort == "ultra"
			}
		}
		if recorder.Code != http.StatusOK || !foundModel || extended != (version == "0.153.4") {
			t.Fatalf("client_version=%s was not applied independently of software headers", version)
		}
	}
}

func TestCodexClientAstraCatalogUsesReviewedTemplateAndCapabilities(t *testing.T) {
	response := CodexClientModelsResponseForClient([]map[string]any{{"id": "gpt-6-astra"}}, "0.153.4")
	models := response["models"].([]map[string]any)
	if len(models) != 1 {
		t.Fatal("Astra model was omitted")
	}
	astra := models[0]
	if astra["minimal_client_version"] != "0.153.0" || astra["use_responses_lite"] != true || astra["tool_mode"] != "code_mode_only" || astra["multi_agent_version"] != "v2" {
		t.Fatal("Astra protocol capabilities are incorrect")
	}
	message := astra["model_messages"].(map[string]any)
	template := message["instructions_template"].(string)
	if strings.Contains(template, "functions.send_user_message_async") || !strings.Contains(template, "functions.request_user_input_async") {
		t.Fatal("Astra still uses the obsolete template paragraph")
	}
	if !strings.Contains(message["persistent_instructions"].(string), "functions.send_user_message_async") {
		t.Fatal("unrelated official instruction fields were globally rewritten")
	}
}
