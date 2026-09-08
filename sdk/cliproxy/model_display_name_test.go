package cliproxy

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestConfiguredModelDisplayLabelsDoNotChangeRouting(t *testing.T) {
	entries := []modelEntry{
		config.CodexModel{Name: "upstream", Alias: "codex-alias", DisplayName: " Codex Label "},
		config.ClaudeModel{Name: "upstream", Alias: "claude-alias", DisplayName: "Claude Label"},
		config.GeminiModel{Name: "upstream", Alias: "gemini-alias", DisplayName: "Gemini Label"},
		config.VertexCompatModel{Name: "upstream", Alias: "vertex-alias", DisplayName: "Vertex Label"},
		config.OpenAICompatibilityModel{Name: "upstream", Alias: "compat-alias", DisplayName: "Compat Label"},
	}
	wants := []string{"Codex Label", "Claude Label", "Gemini Label", "Vertex Label", "Compat Label"}
	models := buildConfigModels(entries, "fixture", "openai")
	for index, model := range models {
		if model.DisplayName != wants[index] || model.ID != entries[index].GetAlias() || model.UpstreamID != "upstream" {
			t.Fatal("display override changed routing or was ignored")
		}
	}
	for _, label := range []string{"", "  "} {
		models := buildConfigModels([]config.CodexModel{{Name: "upstream", Alias: "alias", DisplayName: label}}, "fixture", "openai")
		if models[0].DisplayName != "upstream" {
			t.Fatal("blank label changed default")
		}
	}
}

func TestOAuthModelDisplayLabelKeepsForkSource(t *testing.T) {
	for _, fork := range []bool{false, true} {
		source := []*ModelInfo{{ID: "upstream", DisplayName: "Original Label", ContextLength: 1000}}
		cfg := &config.Config{OAuthModelAlias: map[string][]config.OAuthModelAlias{"codex": {{Name: "upstream", Alias: "alias", DisplayName: " Alias Label ", Fork: fork}}}}
		out := applyOAuthModelAlias(cfg, "codex", "oauth", source)
		found := false
		for _, model := range out {
			if model.ID == "alias" {
				found = true
				if model.DisplayName != "Alias Label" || model.UpstreamID != "upstream" {
					t.Fatal("OAuth label override failed")
				}
			}
		}
		if !found || source[0].DisplayName != "Original Label" || source[0].ID != "upstream" {
			t.Fatal("alias mutated source model")
		}
		if fork && len(out) != 2 {
			t.Fatal("fork lost original model")
		}
	}
}

func TestOpenAICompatibilityRegistrationUsesDisplayLabel(t *testing.T) {
	const authID = "compat-display-fixture"
	s := &Service{cfg: &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat-label", Models: []config.OpenAICompatibilityModel{{Name: "upstream", Alias: "alias", DisplayName: "Friendly Label"}}}}}}
	a := &coreauth.Auth{ID: authID, Provider: "openai-compatibility", Attributes: map[string]string{"compat_name": "compat-label", "provider_key": "compat-label"}}
	s.registerModelsForAuth(a)
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	models := registry.GetGlobalRegistry().GetModelsForClient(authID)
	if len(models) != 1 || models[0].DisplayName != "Friendly Label" || models[0].ID != "alias" || models[0].UpstreamID != "upstream" {
		t.Fatal("compatibility registration lost label or routing identity")
	}
}
