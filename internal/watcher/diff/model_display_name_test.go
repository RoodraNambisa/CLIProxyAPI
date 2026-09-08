package diff

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestModelDisplayNameChangesAreLoggedWithoutValues(t *testing.T) {
	makeConfig := func(label string) *config.Config {
		return &config.Config{
			GeminiKey:           []config.GeminiKey{{APIKey: "private-key-marker", Models: []config.GeminiModel{{Name: "upstream", Alias: "alias", DisplayName: label}}}},
			InteractionsKey:     []config.GeminiKey{{Models: []config.GeminiModel{{Name: "upstream", Alias: "alias", DisplayName: label}}}},
			ClaudeKey:           []config.ClaudeKey{{Models: []config.ClaudeModel{{Name: "upstream", Alias: "alias", DisplayName: label}}}},
			CodexKey:            []config.CodexKey{{Models: []config.CodexModel{{Name: "upstream", Alias: "alias", DisplayName: label}}}},
			VertexCompatAPIKey:  []config.VertexCompatKey{{Models: []config.VertexCompatModel{{Name: "upstream", Alias: "alias", DisplayName: label}}}},
			OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat", Models: []config.OpenAICompatibilityModel{{Name: "upstream", Alias: "alias", DisplayName: label}}}},
			OAuthModelAlias:     map[string][]config.OAuthModelAlias{"codex": {{Name: "upstream", Alias: "alias", DisplayName: label}}},
		}
	}
	before := makeConfig("Private-label-marker")
	for _, next := range []string{"", "private-label-marker", "label with\nnewlines"} {
		after := makeConfig(next)
		for _, pair := range [][2]*config.Config{{before, after}, {after, before}} {
			details := strings.Join(BuildConfigChangeDetails(pair[0], pair[1]), "\n")
			for _, private := range []string{"private-key-marker", "Private-label-marker", "private-label-marker", "label with"} {
				if strings.Contains(details, private) {
					t.Fatal("display name change log exposed a field value")
				}
			}
			for _, marker := range []string{"gemini[0].models", "interactions[0].models", "claude[0].models", "codex[0].models", "vertex[0].models", "models updated", "oauth-model-alias[codex]"} {
				if !strings.Contains(details, marker) {
					t.Errorf("missing label change: %s", marker)
				}
			}
		}
		if ComputeGeminiModelsHash(before.GeminiKey[0].Models) != ComputeGeminiModelsHash(after.GeminiKey[0].Models) ||
			ComputeClaudeModelsHash(before.ClaudeKey[0].Models) != ComputeClaudeModelsHash(after.ClaudeKey[0].Models) ||
			ComputeCodexModelsHash(before.CodexKey[0].Models) != ComputeCodexModelsHash(after.CodexKey[0].Models) ||
			ComputeVertexCompatModelsHash(before.VertexCompatAPIKey[0].Models) != ComputeVertexCompatModelsHash(after.VertexCompatAPIKey[0].Models) ||
			ComputeOpenAICompatModelsHash(before.OpenAICompatibility[0].Models) != ComputeOpenAICompatModelsHash(after.OpenAICompatibility[0].Models) {
			t.Fatal("label change altered a credential routing hash")
		}
	}
	if details := BuildConfigChangeDetails(before, before); len(details) != 0 {
		t.Fatal("unchanged labels produced log entries")
	}
}
