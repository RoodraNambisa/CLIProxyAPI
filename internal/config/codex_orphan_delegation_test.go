package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigCodexOrphanDelegation(t *testing.T) {
	for _, value := range []string{"", "true", "false", "[]", "1", `"false"`} {
		t.Run(value, func(t *testing.T) {
			data := "codex: {}\n"
			if value != "" {
				data = "codex:\n  orphan-delegation-compatibility: " + value + "\n"
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			valid := value == "" || value == "true" || value == "false"
			if (err == nil) != valid {
				t.Fatalf("boolean validation: err=%v", err)
			}
			if valid && cfg.Codex.OrphanDelegationCompatibility != (value == "true") {
				t.Fatal("wrong orphan delegation default or value")
			}
			if valid {
				if err := SaveConfigPreserveComments(path, cfg); err != nil {
					t.Fatal(err)
				}
				reloaded, err := LoadConfig(path)
				if err != nil || reloaded.Codex.OrphanDelegationCompatibility != cfg.Codex.OrphanDelegationCompatibility {
					t.Fatal("save and reload changed orphan delegation policy")
				}
			}
		})
	}
}
