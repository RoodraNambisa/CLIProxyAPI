package cliproxy

import (
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers/openai"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestConfiguredAliasesInheritCatalogCapabilitiesWithoutRequestLimitChanges(t *testing.T) {
	entries := []modelEntry{
		config.CodexModel{Name: "gpt-5.5", Alias: "codex-alias"},
		config.ClaudeModel{Name: "claude-opus-4-6", Alias: "claude-alias"},
		config.GeminiModel{Name: "gemini-2.5-pro", Alias: "gemini-alias"},
		config.VertexCompatModel{Name: "gemini-2.5-pro", Alias: "vertex-alias"},
	}
	for _, info := range buildConfigModels(entries, "fixture", "fixture") {
		source := registry.LookupStaticModelInfo(info.UpstreamID)
		output := source.OutputTokenLimit
		if output <= 0 {
			output = source.MaxCompletionTokens
		}
		if !reflect.DeepEqual(info.SupportedInputModalities, source.SupportedInputModalities) || !reflect.DeepEqual(info.SupportedOutputModalities, source.SupportedOutputModalities) || info.OutputTokenLimit != output {
			t.Fatalf("%s lost catalog capabilities", info.ID)
		}
		if !info.UserDefined || info.MaxCompletionTokens != 0 || !reflect.DeepEqual(info.Thinking, source.Thinking) {
			t.Fatalf("%s changed request-limit or thinking behavior", info.ID)
		}
		prefixed := applyModelPrefixes([]*ModelInfo{info}, "tenant", true)[0]
		r := registry.GetGlobalRegistry()
		client := t.Name() + info.ID
		r.RegisterClient(client, "fixture", []*ModelInfo{prefixed})
		t.Cleanup(func() { r.UnregisterClient(client) })
		response := openai.CodexClientModelsResponse(r.GetAvailableModels("openai"))
		var selected map[string]any
		for _, model := range response["models"].([]map[string]any) {
			if model["slug"] == prefixed.ID {
				selected = model
				break
			}
		}
		if selected == nil || selected["max_tokens"] != output {
			t.Fatalf("%s lost output capacity in the client catalog", info.ID)
		}
		if info.SupportedInputModalities != nil {
			info.SupportedInputModalities[0] = "mutated"
		}
		if !reflect.DeepEqual(registry.LookupStaticModelInfo(info.UpstreamID).SupportedInputModalities, source.SupportedInputModalities) {
			t.Fatal("configured alias mutated the source catalog")
		}
	}
	unknown := buildConfigModels([]config.CodexModel{{Name: "unknown-capability-model", Alias: "unknown"}}, "fixture", "fixture")[0]
	if unknown.OutputTokenLimit != 0 || len(unknown.SupportedInputModalities) != 0 {
		t.Fatal("unknown model acquired invented capabilities")
	}
}

func TestCompatibilityAliasCatalogKeepsConfiguredThinking(t *testing.T) {
	const client = "compat-catalog-inheritance"
	for _, thinking := range []*registry.ThinkingSupport{nil, {Levels: []string{"high"}}} {
		s := &Service{cfg: &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: client, Models: []config.OpenAICompatibilityModel{{Name: "kimi-k2", Alias: "text-alias", Thinking: thinking}}}}}}
		a := &coreauth.Auth{ID: client, Provider: "openai-compatibility", Attributes: map[string]string{"compat_name": client, "provider_key": client}}
		s.registerModelsForAuth(a)
		r := registry.GetGlobalRegistry()
		t.Cleanup(func() { r.UnregisterClient(client) })
		models := r.GetModelsForClient(client)
		if len(models) != 1 || !reflect.DeepEqual(models[0].SupportedInputModalities, []string{"text"}) || models[0].OutputTokenLimit <= 0 {
			t.Fatal("compatibility alias lost source capabilities")
		}
		want := []string{"low", "medium", "high"}
		if thinking != nil {
			want = thinking.Levels
		}
		if models[0].UserDefined || models[0].MaxCompletionTokens != 0 || models[0].Thinking == nil || !reflect.DeepEqual(models[0].Thinking.Levels, want) {
			t.Fatal("catalog inheritance changed compatibility request defaults")
		}
	}
}
