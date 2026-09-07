package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSessionAffinityAcrossPrioritiesConfigValidationAndSave(t *testing.T) {
	for _, value := range []string{"", "true", "false", "null", "[]", "{}", "1", `"false"`} {
		t.Run(value, func(t *testing.T) {
			data := "routing:\n  future-setting: preserved\n"
			if value != "" {
				data += "  session-affinity-across-priorities: " + value + "\n"
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			valid := value == "" || value == "true" || value == "false" || value == "null"
			if (err == nil) != valid {
				t.Fatalf("boolean validation: %v", err)
			}
			if !valid {
				return
			}
			if cfg.Routing.SessionAffinityAcrossPriorities != (value == "true") || cfg.Routing.SessionAffinity {
				t.Fatal("default changed or dependent policy enabled general affinity")
			}
			for _, enabled := range []bool{true, false} {
				cfg.Routing.SessionAffinityAcrossPriorities = enabled
				if err := SaveConfigPreserveComments(path, cfg); err != nil {
					t.Fatal(err)
				}
				reloaded, err := LoadConfig(path)
				if err != nil || reloaded.Routing.SessionAffinityAcrossPriorities != enabled {
					t.Fatal("affinity policy did not survive save/reload")
				}
				raw, err := os.ReadFile(path)
				if err != nil || !strings.Contains(string(raw), "future-setting: preserved") {
					t.Fatal("unknown routing setting was lost")
				}
			}
		})
	}
	for _, value := range []string{"true", "false", "null", "1", `"true"`, "[]"} {
		var cfg Config
		err := json.Unmarshal([]byte(`{"routing":{"session-affinity-across-priorities":`+value+`}}`), &cfg)
		if (err == nil) != (value == "true" || value == "false" || value == "null") {
			t.Fatal("JSON boolean handling changed")
		}
	}
}
