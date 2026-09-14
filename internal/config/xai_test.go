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
		{"xai:\n  default-base-url-mode: us-west-2", true},
		{"xai:\n  default-base-url-mode: unknown", false},
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

func TestNormalizeXAIBaseURL(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", ""}, {" https://API.X.AI:443/v1/ ", "https://api.x.ai/v1"},
		{"http://localhost:8317/v1", "http://localhost:8317/v1"},
		{"HTTPS://US-EAST-1.API.X.AI/v1", "https://us-east-1.api.x.ai/v1"},
	} {
		if got, err := NormalizeXAIBaseURL(tc.input); err != nil || got != tc.want {
			t.Fatalf("normalize %q = %q, %v", tc.input, got, err)
		}
	}
	for _, raw := range []string{"api.x.ai/v1", "ftp://api.x.ai/v1", "https://token@api.x.ai/v1", "https://api.x.ai/v1?key=secret", "https://api.x.ai/v1?", "https://api.x.ai/v1#frag", "https://api.x.ai/v1#", "https://api.x.ai/\\evil", "https://api.x.ai/v1\r\nX-Test: bad"} {
		if _, err := NormalizeXAIBaseURL(raw); err == nil {
			t.Fatalf("accepted invalid base URL %q", raw)
		}
	}
}
