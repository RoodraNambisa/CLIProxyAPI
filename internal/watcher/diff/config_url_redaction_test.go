package diff

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestConfigDiffRedactsURLCredentialsAcrossProviders(t *testing.T) {
	setters := map[string]func(*config.Config, string){
		"gemini":       func(c *config.Config, address string) { c.GeminiKey = []config.GeminiKey{{BaseURL: address}} },
		"interactions": func(c *config.Config, address string) { c.InteractionsKey = []config.GeminiKey{{BaseURL: address}} },
		"claude":       func(c *config.Config, address string) { c.ClaudeKey = []config.ClaudeKey{{BaseURL: address}} },
		"codex":        func(c *config.Config, address string) { c.CodexKey = []config.CodexKey{{BaseURL: address}} },
		"vertex": func(c *config.Config, address string) {
			c.VertexCompatAPIKey = []config.VertexCompatKey{{BaseURL: address}}
		},
		"panel": func(c *config.Config, address string) { c.RemoteManagement.PanelGitHubRepository = address },
		"compat-label": func(c *config.Config, address string) {
			c.OpenAICompatibility = []config.OpenAICompatibility{{BaseURL: address}}
		},
	}
	for name, set := range setters {
		t.Run(name, func(t *testing.T) {
			oldConfig, newConfig := &config.Config{}, &config.Config{}
			set(oldConfig, "https://fixture-user:fixture-password@old.example/fixture-path?token=fixture-query#fixture-fragment")
			set(newConfig, "https://fixture-user:fixture-password@new.example/fixture-path?token=fixture-query#fixture-fragment")
			details := strings.Join(BuildConfigChangeDetails(oldConfig, newConfig), "\n")
			for _, secret := range []string{"fixture-user", "fixture-password", "fixture-path", "fixture-query", "fixture-fragment"} {
				if strings.Contains(details, secret) {
					t.Errorf("URL component %q was exposed", secret)
				}
			}
			if !strings.Contains(details, "old.example") || !strings.Contains(details, "new.example") {
				t.Error("safe URL hosts were lost")
			}
		})
	}
}

func TestCompatURLRedactionDoesNotChangeDiffIdentity(t *testing.T) {
	oldEntry := config.OpenAICompatibility{BaseURL: "https://example.test/old"}
	newEntry := config.OpenAICompatibility{BaseURL: "https://example.test/new"}
	oldKey, oldLabel := openAICompatKey(oldEntry, 0)
	newKey, newLabel := openAICompatKey(newEntry, 0)
	if oldKey == newKey || oldLabel != "https://example.test" || newLabel != oldLabel {
		t.Fatal("redaction must change labels without merging distinct provider identities")
	}
	if changes := DiffOpenAICompatibility([]config.OpenAICompatibility{oldEntry}, []config.OpenAICompatibility{newEntry}); len(changes) != 2 {
		t.Fatalf("provider remove/add changes = %d, want 2", len(changes))
	}
}
