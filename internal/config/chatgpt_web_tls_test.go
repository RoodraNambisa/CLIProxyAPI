package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestChatGPTWebTLSTrustAnchorsConfiguration(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		enabled bool
		invalid bool
	}{
		{"{}", true, false}, {"tls-trust-anchors-enabled: true", true, false},
		{"tls-trust-anchors-enabled: false", false, false}, {"tls-trust-anchors-enabled: null", true, false},
		{"tls-trust-anchors-enabled: invalid", false, true}, {"tls-trust-anchors-enabled: 1", false, true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			var cfg ChatGPTWebConfig
			err := yaml.Unmarshal([]byte(tc.raw), &cfg)
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid boolean accepted")
				}
				return
			}
			if err != nil || cfg.TrustAnchorsEnabled() != tc.enabled {
				t.Fatalf("configuration = %v, %v", cfg.TrustAnchorsEnabled(), err)
			}
			cloned, err := Clone(&Config{ChatGPTWeb: cfg})
			if err != nil || cloned.ChatGPTWeb.TrustAnchorsEnabled() != tc.enabled {
				t.Fatal("clone lost TLS setting")
			}
		})
	}
}
func TestChatGPTWebTLSTrustAnchorsPreservesFalse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("# retained\nchatgpt-web:\n  timezone: UTC\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	disabled := false
	cfg.ChatGPTWeb.TLSTrustAnchorsEnabled = &disabled
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "tls-trust-anchors-enabled: false") || !strings.Contains(string(body), "# retained") {
		t.Fatal("setting or comment lost")
	}
	loaded, err := LoadConfig(path)
	if err != nil || loaded.ChatGPTWeb.TrustAnchorsEnabled() {
		t.Fatal("false lost on reload")
	}
}
