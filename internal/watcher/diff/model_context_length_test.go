package diff

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestModelContextLengthChangesDoNotChangeCredentialRoutingHashes(t *testing.T) {
	makeConfig := func(limit int) *config.Config {
		return &config.Config{
			GeminiKey:           []config.GeminiKey{{APIKey: "private-key-marker", Models: []config.GeminiModel{{Name: "upstream", Alias: "local", MaxContextLength: limit}}}},
			InteractionsKey:     []config.GeminiKey{{Models: []config.GeminiModel{{Name: "upstream", Alias: "local", MaxContextLength: limit}}}},
			ClaudeKey:           []config.ClaudeKey{{Models: []config.ClaudeModel{{Name: "upstream", Alias: "local", MaxContextLength: limit}}}},
			CodexKey:            []config.CodexKey{{Models: []config.CodexModel{{Name: "upstream", Alias: "local", MaxContextLength: limit}}}},
			VertexCompatAPIKey:  []config.VertexCompatKey{{Models: []config.VertexCompatModel{{Name: "upstream", Alias: "local", MaxContextLength: limit}}}},
			OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat", Models: []config.OpenAICompatibilityModel{{Name: "upstream", Alias: "local", MaxContextLength: limit}}}},
		}
	}
	before := makeConfig(1024)
	for _, limit := range []int{0, 2048} {
		after := makeConfig(limit)
		for _, pair := range [][2]*config.Config{{before, after}, {after, before}} {
			details := strings.Join(BuildConfigChangeDetails(pair[0], pair[1]), "\n")
			for _, marker := range []string{"gemini[0].models", "interactions[0].models", "claude[0].models", "codex[0].models", "vertex[0].models", "models updated"} {
				if !strings.Contains(details, marker) {
					t.Errorf("missing context change marker %s", marker)
				}
			}
			if strings.Contains(details, "private-key-marker") || strings.Contains(details, "1024") || strings.Contains(details, "2048") {
				t.Fatal("context change log printed field values")
			}
		}
		if ComputeGeminiModelsHash(before.GeminiKey[0].Models) != ComputeGeminiModelsHash(after.GeminiKey[0].Models) ||
			ComputeClaudeModelsHash(before.ClaudeKey[0].Models) != ComputeClaudeModelsHash(after.ClaudeKey[0].Models) ||
			ComputeCodexModelsHash(before.CodexKey[0].Models) != ComputeCodexModelsHash(after.CodexKey[0].Models) ||
			ComputeVertexCompatModelsHash(before.VertexCompatAPIKey[0].Models) != ComputeVertexCompatModelsHash(after.VertexCompatAPIKey[0].Models) ||
			ComputeOpenAICompatModelsHash(before.OpenAICompatibility[0].Models) != ComputeOpenAICompatModelsHash(after.OpenAICompatibility[0].Models) {
			t.Fatal("declaration edit changed a credential routing hash")
		}
	}
	if len(BuildConfigChangeDetails(before, before)) != 0 {
		t.Fatal("unchanged context declarations produced changes")
	}
}
