package synthesizer

import (
	"strconv"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCredentialWeightProjectionAcrossAPIKeyFamilies(t *testing.T) {
	zero := 0
	negative, maximum := -2, config.MaxCredentialWeight
	cfg := &config.Config{
		GeminiKey:           []config.GeminiKey{{APIKey: "gemini", Weight: &zero}},
		InteractionsKey:     []config.GeminiKey{{APIKey: "interactions", Weight: &zero}},
		ClaudeKey:           []config.ClaudeKey{{APIKey: "claude", Weight: &zero}},
		CodexKey:            []config.CodexKey{{APIKey: "codex", Weight: &zero}},
		VertexCompatAPIKey:  []config.VertexCompatKey{{APIKey: "vertex", BaseURL: "https://example.invalid", Weight: &zero}},
		OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat", BaseURL: "https://example.invalid", APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "compat", Weight: &zero}}}},
	}
	for _, weight := range []*int{nil, &zero, &negative, &maximum} {
		cfg.GeminiKey[0].Weight, cfg.InteractionsKey[0].Weight, cfg.ClaudeKey[0].Weight = weight, weight, weight
		cfg.CodexKey[0].Weight, cfg.VertexCompatAPIKey[0].Weight, cfg.OpenAICompatibility[0].APIKeyEntries[0].Weight = weight, weight, weight
		auths, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, Now: time.Now(), IDGenerator: NewStableIDGenerator()})
		if err != nil || len(auths) != 6 {
			t.Fatalf("credential projection failed: count %d, err %v", len(auths), err)
		}
		for _, a := range auths {
			value, present := a.Attributes[coreauth.AttributeWeight]
			if (weight == nil && present) || (weight != nil && (!present || value != strconv.Itoa(max(0, *weight)))) {
				t.Fatal("an API-key family lost weight or default inheritance")
			}
		}
	}
	invalid := config.MaxCredentialWeight + 1
	cfg.CodexKey[0].Weight = &invalid
	if auths, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, IDGenerator: NewStableIDGenerator()}); err == nil || len(auths) != 0 {
		t.Fatal("invalid config produced a partial credential set")
	}
}
