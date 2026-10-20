package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexServiceTierConfigRoundTrip(t *testing.T) {
	for _, value := range []string{"", "null", "false", "true"} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			body := "port: 8317\ncodex:\n  future-field: keep\n"
			if value != "" {
				body += "  allow-client-service-tier: " + value + " # tier policy\n"
			}
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Codex.AllowClientServiceTier != (value == "true") {
				t.Fatal("unexpected service tier default")
			}
			for _, enabled := range []bool{true, false} {
				cfg.Codex.AllowClientServiceTier = enabled
				if err := SaveConfigPreserveComments(path, cfg); err != nil {
					t.Fatal(err)
				}
				reloaded, err := LoadConfig(path)
				if err != nil || reloaded.Codex.AllowClientServiceTier != enabled {
					t.Fatalf("service tier did not round trip: %v", err)
				}
				saved, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(saved), "future-field: keep") {
					t.Fatal("unknown config field was lost")
				}
				encoded, err := json.Marshal(reloaded.Codex)
				if err != nil {
					t.Fatal(err)
				}
				var object map[string]any
				if err := json.Unmarshal(encoded, &object); err != nil {
					t.Fatal(err)
				}
				if object["allow-client-service-tier"] != enabled {
					t.Fatal("management config JSON omitted the service tier setting")
				}
			}
		})
	}
}
