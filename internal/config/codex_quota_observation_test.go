package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexQuotaObservationConfiguration(t *testing.T) {
	for _, value := range []string{"", "true", "false", "null", "[]", "1", `"false"`} {
		t.Run(value, func(t *testing.T) {
			data := "codex:\n  future-field: retained\n"
			if value != "" {
				data += "  observe-quota: " + value + "\n"
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			valid := value == "" || value == "true" || value == "false" || value == "null"
			if (err == nil) != valid {
				t.Fatalf("quota boolean validation failed: %v", err)
			}
			if !valid {
				return
			}
			if cfg.Codex.ObserveQuota != (value == "true") {
				t.Fatal("wrong quota observation default/value")
			}
			if err := SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatal(err)
			}
			reloaded, err := LoadConfig(path)
			if err != nil || reloaded.Codex.ObserveQuota != cfg.Codex.ObserveQuota {
				t.Fatal("save/reload changed the quota observation setting")
			}
			saved, err := os.ReadFile(path)
			if err != nil || !strings.Contains(string(saved), "future-field: retained") {
				t.Fatal("quota setting save removed an unknown field")
			}
			encoded, err := json.Marshal(reloaded.Codex)
			if err != nil || !strings.Contains(string(encoded), `"observe-quota":`+map[bool]string{true: "true", false: "false"}[cfg.Codex.ObserveQuota]) {
				t.Fatal("config JSON does not expose the quota setting")
			}
		})
	}
}
