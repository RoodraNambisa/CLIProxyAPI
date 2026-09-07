package synthesizer

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCredentialRequestRetryProjectionAndClearAcrossFamilies(t *testing.T) {
	cfg := &config.Config{
		GeminiKey: []config.GeminiKey{{APIKey: "gemini"}}, InteractionsKey: []config.GeminiKey{{APIKey: "interactions"}},
		ClaudeKey: []config.ClaudeKey{{APIKey: "claude"}}, CodexKey: []config.CodexKey{{APIKey: "codex", BaseURL: "https://example.test"}},
		VertexCompatAPIKey: []config.VertexCompatKey{{APIKey: "vertex", BaseURL: "https://example.test"}},
		OpenAICompatibility: []config.OpenAICompatibility{
			{Name: "compat", BaseURL: "https://example.test", APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "compat"}}},
			{Name: "anonymous", BaseURL: "https://example.test"},
		},
	}
	manager := coreauth.NewManager(nil, nil, nil)
	zero, negative, positive := 0, -3, 2
	var previousIDs []string
	for _, retry := range []*int{nil, &positive, &zero, &negative, nil} {
		cfg.GeminiKey[0].RequestRetry, cfg.InteractionsKey[0].RequestRetry, cfg.ClaudeKey[0].RequestRetry = retry, retry, retry
		cfg.CodexKey[0].RequestRetry, cfg.VertexCompatAPIKey[0].RequestRetry = retry, retry
		cfg.OpenAICompatibility[0].RequestRetry, cfg.OpenAICompatibility[1].RequestRetry = retry, retry
		auths, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, Now: time.Now(), IDGenerator: NewStableIDGenerator()})
		if err != nil || len(auths) != 7 {
			t.Fatalf("synthesis count=%d error=%v", len(auths), err)
		}
		ids := make([]string, len(auths))
		for index, auth := range auths {
			ids[index] = auth.ID
			if previousIDs != nil && previousIDs[index] != auth.ID {
				t.Fatal("retry update changed credential identity")
			}
			want := 0
			if retry != nil {
				want = max(0, *retry)
			}
			if got, set := auth.RequestRetryOverride(); set != (retry != nil) || got != want {
				t.Fatalf("%s: retry=%d set=%t expected=%d/%t", auth.Provider, got, set, want, retry != nil)
			}
			if previousIDs == nil {
				_, err = manager.Register(coreauth.WithSkipPersist(t.Context()), auth)
			} else {
				_, err = manager.Update(coreauth.WithSkipPersist(t.Context()), auth)
			}
			if err != nil {
				t.Fatal(err)
			}
			current, _ := manager.GetByID(auth.ID)
			if got, set := current.RequestRetryOverride(); set != (retry != nil) || got != want {
				t.Fatal("manager retained or changed the projected override")
			}
		}
		previousIDs = ids
		if retry != nil {
			original := *retry
			*retry = 9
			for _, auth := range auths {
				if got, _ := auth.RequestRetryOverride(); got != max(0, original) {
					t.Fatal("configuration pointer mutation changed a prepared credential")
				}
			}
			*retry = original
		}
	}
	large := int64(config.MaxCredentialRequestRetry) + 1
	if int64(int(large)) != large {
		return
	}
	invalid := int(large)
	cfg.OpenAICompatibility[1].RequestRetry = &invalid
	if auths, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, IDGenerator: NewStableIDGenerator()}); err == nil || len(auths) != 0 {
		t.Fatal("invalid final provider produced a partial credential set")
	}
}
