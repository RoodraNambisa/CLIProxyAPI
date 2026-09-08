package cliproxy

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestConfiguredModelContextLengthUsesUpstreamCatalogAndOverride(t *testing.T) {
	for _, limit := range []int{0, 131072, config.MaxModelContextLength} {
		entries := []modelEntry{
			config.CodexModel{Name: "gpt-5.5", Alias: "codex-local", MaxContextLength: limit},
			config.ClaudeModel{Name: "claude-haiku-4-5-20251001", Alias: "claude-local", MaxContextLength: limit},
			config.GeminiModel{Name: "gemini-2.5-pro", Alias: "gemini-local", MaxContextLength: limit},
			config.VertexCompatModel{Name: "gemini-2.5-pro", Alias: "vertex-local", MaxContextLength: limit},
			config.OpenAICompatibilityModel{Name: "gpt-5.5", Alias: "compat-local", MaxContextLength: limit},
		}
		models := buildConfigModels(entries, "fixture", "openai")
		for index, model := range models {
			upstream := registry.LookupStaticModelInfo(entries[index].GetName())
			if upstream == nil {
				t.Fatal("fixture upstream model missing")
			}
			want := limit
			if limit == 0 {
				want = upstream.ContextLength
				if want == 0 {
					want = upstream.InputTokenLimit
				}
			}
			if model.ContextLength != want || model.MaxContextLength != limit || model.ID != entries[index].GetAlias() || model.UpstreamID != entries[index].GetName() {
				t.Fatal("configured model lost inheritance, override or routing identity")
			}
			prefixed := applyModelPrefixes([]*ModelInfo{model}, "tenant", true)
			if len(prefixed) != 1 || prefixed[0].ID != "tenant/"+model.ID || prefixed[0].MaxContextLength != limit || prefixed[0].ContextLength != want {
				t.Fatal("prefix lost configured context metadata")
			}
			if current := registry.LookupStaticModelInfo(entries[index].GetName()); current.ContextLength != upstream.ContextLength || current.MaxContextLength != 0 {
				t.Fatal("override mutated static catalog")
			}
		}
	}
	unknown := buildConfigModels([]config.CodexModel{{Name: "context-unknown", Alias: "local"}}, "fixture", "openai")
	if unknown[0].ContextLength != 0 || unknown[0].MaxContextLength != 0 {
		t.Fatal("unknown model acquired invented capacity")
	}
}

func TestOpenAICompatibilityRegistrationUsesContextOverride(t *testing.T) {
	const authID = "compat-context-fixture"
	s := &Service{cfg: &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat-context", Models: []config.OpenAICompatibilityModel{{Name: "gpt-5.5", Alias: "local", MaxContextLength: 131072}}}}}}
	s.cfg.ForceModelPrefix = true
	a := &coreauth.Auth{ID: authID, Provider: "openai-compatibility", Prefix: "tenant", Attributes: map[string]string{"compat_name": "compat-context", "provider_key": "compat-context"}}
	s.registerModelsForAuth(a)
	r := registry.GetGlobalRegistry()
	t.Cleanup(func() { r.UnregisterClient(authID) })
	models := r.GetModelsForClient(authID)
	if len(models) != 1 || models[0].ContextLength != 131072 || models[0].MaxContextLength != 131072 || models[0].ID != "tenant/local" || models[0].UpstreamID != "gpt-5.5" {
		t.Fatal("compat registration lost override or prefix")
	}
	s.cfg.OpenAICompatibility[0].Models[0].MaxContextLength = 0
	s.registerModelsForAuthPreservingState(a)
	models = r.GetModelsForClient(authID)
	if len(models) != 1 || models[0].MaxContextLength != 0 || models[0].ContextLength != registry.LookupStaticModelInfo("gpt-5.5").ContextLength {
		t.Fatal("clearing compatibility override failed to restore directory inheritance")
	}
}
