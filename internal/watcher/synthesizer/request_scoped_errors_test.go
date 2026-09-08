package synthesizer

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestRequestScopedErrorConfigProjectionAndClear(t *testing.T) {
	cfg := &config.Config{
		GeminiKey: []config.GeminiKey{{APIKey: "gemini"}}, InteractionsKey: []config.GeminiKey{{APIKey: "interactions"}},
		ClaudeKey: []config.ClaudeKey{{APIKey: "claude"}}, CodexKey: []config.CodexKey{{APIKey: "codex", BaseURL: "https://example.test"}},
		VertexCompatAPIKey: []config.VertexCompatKey{{APIKey: "vertex", BaseURL: "https://example.test"}},
		OpenAICompatibility: []config.OpenAICompatibility{
			{Name: "compat", BaseURL: "https://example.test", APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "compat"}}},
			{Name: "anonymous", BaseURL: "https://example.test"},
		},
	}
	rule := config.RequestScopedErrorRule{Status: 400, Match: []string{"fixture"}, MatchRegexr: []string{"^fixture$"}, Action: "stop"}
	manager := coreauth.NewManager(nil, nil, nil)
	var previousIDs []string
	for _, rules := range [][]config.RequestScopedErrorRule{nil, {rule}, nil} {
		cfg.GeminiKey[0].RequestScopedErrors, cfg.InteractionsKey[0].RequestScopedErrors = rules, rules
		cfg.ClaudeKey[0].RequestScopedErrors, cfg.CodexKey[0].RequestScopedErrors = rules, rules
		cfg.VertexCompatAPIKey[0].RequestScopedErrors = rules
		cfg.OpenAICompatibility[0].RequestScopedErrors, cfg.OpenAICompatibility[1].RequestScopedErrors = rules, rules
		auths, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, Now: time.Now(), IDGenerator: NewStableIDGenerator()})
		if err != nil || len(auths) != 7 {
			t.Fatalf("synthesis: count=%d err=%v", len(auths), err)
		}
		ids := make([]string, len(auths))
		for index, auth := range auths {
			ids[index] = auth.ID
			if previousIDs != nil && previousIDs[index] != auth.ID {
				t.Fatal("rule update changed credential identity")
			}
			got, exists := auth.Metadata["request_scoped_errors"]
			if exists != (rules != nil) {
				t.Fatal("rule projection or clearing was lost")
			}
			if exists {
				projected := got.([]config.RequestScopedErrorRule)
				projected[0].Match[0], projected[0].MatchRegexr[0] = "changed", "changed"
				if rule.Match[0] != "fixture" || rule.MatchRegexr[0] != "^fixture$" {
					t.Fatal("projection shared config slices")
				}
			}
			if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); err != nil {
				t.Fatal(err)
			}
			current, _ := manager.GetByID(auth.ID)
			if _, exists := current.Metadata["request_scoped_errors"]; exists != (rules != nil) {
				t.Fatal("manager retained cleared rules")
			}
		}
		previousIDs = ids
	}
	cfg.OpenAICompatibility[1].RequestScopedErrors = []config.RequestScopedErrorRule{{Status: 400, Match: []string{"fixture"}, Action: "invalid"}}
	if auths, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, IDGenerator: NewStableIDGenerator()}); err == nil || len(auths) != 0 {
		t.Fatal("invalid final entry emitted partial credential set")
	}
}
