package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSessionAffinityUseHistoryDefaultsAndRoundTrip(t *testing.T) {
	for _, value := range []string{"", "true", "false", "null"} {
		t.Run(value, func(t *testing.T) {
			body := "routing:\n  future-setting: kept\n"
			if value != "" {
				body += "  session-affinity-use-history: " + value + "\n"
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if err != nil || cfg.Routing.SessionAffinityHistoryEnabled() != (value != "false") || cfg.Routing.SessionAffinity {
				t.Fatal("history default or session affinity dependency changed", err)
			}
			for _, enabled := range []bool{false, true} {
				cfg.Routing.SessionAffinityUseHistory = &enabled
				if err := SaveConfigPreserveComments(path, cfg); err != nil {
					t.Fatal(err)
				}
				reloaded, err := LoadConfig(path)
				if err != nil || reloaded.Routing.SessionAffinityHistoryEnabled() != enabled {
					t.Fatal("explicit history policy was lost on save", err)
				}
				raw, err := os.ReadFile(path)
				if err != nil || !strings.Contains(string(raw), "future-setting: kept") {
					t.Fatal("save discarded unrelated routing fields")
				}
			}
		})
	}
	for _, value := range []string{"true", "false", "null", `"false"`, "1", "[]"} {
		var cfg Config
		err := json.Unmarshal([]byte(`{"routing":{"session-affinity-use-history":`+value+`}}`), &cfg)
		valid := value == "true" || value == "false" || value == "null"
		if (err == nil) != valid || (valid && cfg.Routing.SessionAffinityHistoryEnabled() != (value != "false")) {
			t.Fatalf("JSON policy=%s error=%v", value, err)
		}
	}
}

func TestSessionAffinityUseHistorySaveAddsDisabledRoutingParent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("port: 8317\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	off := false
	cfg.Routing.SessionAffinityUseHistory = &off
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadConfig(path)
	if err != nil || reloaded.Routing.SessionAffinityHistoryEnabled() {
		t.Fatal("new routing parent dropped the explicit disable", err)
	}
}
