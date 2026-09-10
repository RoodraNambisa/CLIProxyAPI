package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCodexInputTokenEstimateConfiguration(t *testing.T) {
	for _, value := range []string{"", "true", "false", "[]", "1", `"false"`} {
		t.Run(value, func(t *testing.T) {
			data := "codex:\n  future-field: retained\n"
			if value != "" {
				data += "  estimate-claude-input-tokens: " + value + "\n"
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			valid := value == "" || value == "true" || value == "false"
			if (err == nil) != valid {
				t.Fatalf("boolean validation failed: %v", err)
			}
			if !valid {
				return
			}
			if cfg.Codex.EstimateClaudeInputTokens != (value == "true") {
				t.Fatal("wrong default/value")
			}
			if err := SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatal(err)
			}
			reloaded, err := LoadConfig(path)
			if err != nil || reloaded.Codex.EstimateClaudeInputTokens != cfg.Codex.EstimateClaudeInputTokens {
				t.Fatal("save/reload changed the estimate setting")
			}
		})
	}
}
