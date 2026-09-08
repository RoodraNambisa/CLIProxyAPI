package config_test

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestPublicModelThinkingTypesDoNotRequireInternalImports(t *testing.T) {
	support := &config.ThinkingSupport{Min: 0, Max: config.MaxModelThinkingBudget, Levels: []string{"low", "high"}}
	cfg := &config.Config{
		GeminiKey:           []config.GeminiKey{{Models: []config.GeminiModel{{Thinking: support}}}},
		InteractionsKey:     []config.GeminiKey{{Models: []config.GeminiModel{{Thinking: support}}}},
		CodexKey:            []config.CodexKey{{Models: []config.CodexModel{{Thinking: support}}}},
		ClaudeKey:           []config.ClaudeKey{{Models: []config.ClaudeModel{{Thinking: support}}}},
		VertexCompatAPIKey:  []config.VertexCompatKey{{Models: []config.VertexCompatModel{{Thinking: support}}}},
		OpenAICompatibility: []config.OpenAICompatibility{{Models: []config.OpenAICompatibilityModel{{Thinking: support}}}},
	}
	if err := cfg.ValidateModelThinking(); err != nil {
		t.Fatal(err)
	}
	cfg.CodexKey[0].Models[0].Thinking = &config.ThinkingSupport{Min: -1}
	if err := cfg.ValidateModelThinking(); err == nil {
		t.Fatal("public configuration accepted an invalid reasoning declaration")
	}
}
