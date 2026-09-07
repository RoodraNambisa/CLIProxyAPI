package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigCodexStreamBootstrap(t *testing.T) {
	for _, value := range []string{"", "true", "false", "[]", "1", `"false"`} {
		t.Run(value, func(t *testing.T) {
			data := "codex: {}\n"
			if value != "" {
				data = "codex:\n  stream-bootstrap-buffering: " + value + "\n"
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			valid := value == "" || value == "true" || value == "false"
			if (err == nil) != valid {
				t.Fatalf("boolean validation: err=%v", err)
			}
			if valid && cfg.Codex.StreamBootstrapBuffering != (value == "true") {
				t.Fatal("wrong bootstrap default or value")
			}
			if valid {
				if err := SaveConfigPreserveComments(path, cfg); err != nil {
					t.Fatal(err)
				}
				reloaded, err := LoadConfig(path)
				if err != nil || reloaded.Codex.StreamBootstrapBuffering != cfg.Codex.StreamBootstrapBuffering {
					t.Fatal("save and reload changed the bootstrap policy")
				}
			}
		})
	}
}
