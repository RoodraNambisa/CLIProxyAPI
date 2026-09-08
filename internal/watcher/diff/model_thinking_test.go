package diff

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestModelThinkingChangeDetailsDoNotChangeRoutingHashes(t *testing.T) {
	makeConfig := func(support *registry.ThinkingSupport) *config.Config {
		return &config.Config{
			GeminiKey:           []config.GeminiKey{{APIKey: "private-thinking-key", Models: []config.GeminiModel{{Name: "upstream", Alias: "local", Thinking: support}}}},
			InteractionsKey:     []config.GeminiKey{{Models: []config.GeminiModel{{Name: "upstream", Alias: "local", Thinking: support}}}},
			ClaudeKey:           []config.ClaudeKey{{Models: []config.ClaudeModel{{Name: "upstream", Alias: "local", Thinking: support}}}},
			CodexKey:            []config.CodexKey{{Models: []config.CodexModel{{Name: "upstream", Alias: "local", Thinking: support}}}},
			VertexCompatAPIKey:  []config.VertexCompatKey{{Models: []config.VertexCompatModel{{Name: "upstream", Alias: "local", Thinking: support}}}},
			OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat", Models: []config.OpenAICompatibilityModel{{Name: "upstream", Alias: "local", Thinking: support}}}},
		}
	}
	before := makeConfig(&registry.ThinkingSupport{Levels: []string{"low", "high"}})
	for _, support := range []*registry.ThinkingSupport{nil, {}, {Levels: []string{"high", "low"}}, {Min: 1024, Max: 32768}, {Levels: []string{"low", "high"}, DynamicAllowed: true}} {
		after := makeConfig(support)
		for _, pair := range [][2]*config.Config{{before, after}, {after, before}} {
			details := strings.Join(BuildConfigChangeDetails(pair[0], pair[1]), "\n")
			for _, marker := range []string{"gemini[0].models", "interactions[0].models", "claude[0].models", "codex[0].models", "vertex[0].models", "models updated"} {
				if !strings.Contains(details, marker) {
					t.Errorf("missing thinking change marker %s", marker)
				}
			}
			for _, private := range []string{"private-thinking-key", "32768", "1024", "xhigh"} {
				if strings.Contains(details, private) {
					t.Fatal("thinking change log exposed field values")
				}
			}
		}
		if ComputeGeminiModelsHash(before.GeminiKey[0].Models) != ComputeGeminiModelsHash(after.GeminiKey[0].Models) || ComputeClaudeModelsHash(before.ClaudeKey[0].Models) != ComputeClaudeModelsHash(after.ClaudeKey[0].Models) || ComputeCodexModelsHash(before.CodexKey[0].Models) != ComputeCodexModelsHash(after.CodexKey[0].Models) || ComputeVertexCompatModelsHash(before.VertexCompatAPIKey[0].Models) != ComputeVertexCompatModelsHash(after.VertexCompatAPIKey[0].Models) || ComputeOpenAICompatModelsHash(before.OpenAICompatibility[0].Models) != ComputeOpenAICompatModelsHash(after.OpenAICompatibility[0].Models) {
			t.Fatal("thinking edit changed credential routing hashes")
		}
	}
	if SummarizeCodexModels(before.CodexKey[0].Models).hash != SummarizeCodexModels(makeConfig(&registry.ThinkingSupport{Levels: []string{" LOW ", "high", "low"}}).CodexKey[0].Models).hash {
		t.Fatal("equivalent thinking declarations have different signatures")
	}
}
