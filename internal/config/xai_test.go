package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestXAIConfigDefaultsValidationAndClone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	for _, sample := range []struct {
		raw   string
		valid bool
	}{
		{"xai: {}", true},
		{"xai:\n  session-identity-pool-size: 0", false},
		{"xai:\n  session-identity-pool-size: 65", false},
		{"xai:\n  headers:\n    X-Test: ok\n    x-test: duplicate", false},
		{"xai:\n  request-defaults:\n    model: forbidden", false},
		{"xai:\n  request-defaults:\n    temperature: 3", false},
		{"xai:\n  request-defaults:\n    max_output_tokens: 123\n    stream_tool_calls: false\n    reasoning: {effort: high}", true},
	} {
		if err := os.WriteFile(path, []byte(sample.raw), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfig(path)
		if (err == nil) != sample.valid {
			t.Fatalf("valid=%v for %s: %v", sample.valid, sample.raw, err)
		}
		if err == nil && (cfg.XAI.IdentityEnabled() || cfg.XAI.PoolSize() != 4) {
			t.Fatal("new identity defaults are not opt-in")
		}
		if err == nil {
			copy, errCopy := Clone(cfg)
			if errCopy != nil || copy.XAI.PoolSize() != 4 {
				t.Fatal("configuration clone lost Grok defaults")
			}
		}
	}
}
