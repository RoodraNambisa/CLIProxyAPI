package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexAlphaSearchConfigurationTypesAndDefaults(t *testing.T) {
	for _, tc := range []struct {
		value         string
		want, invalid bool
	}{
		{"", false, false}, {"true", true, false}, {"false", false, false}, {"null", false, false},
		{`"true"`, false, true}, {"yes", false, true}, {"1", false, true}, {"[]", false, true}, {"{}", false, true},
		{"!!bool yes", false, true}, {"!!bool invalid", false, true},
	} {
		for _, optional := range []bool{false, true} {
			path := filepath.Join(t.TempDir(), "config.yaml")
			data := "codex-api-key:\n  - api-key: fixture\n    base-url: https://example.invalid/v1\n"
			if tc.value != "" {
				data += "    alpha-search: " + tc.value + "\n"
			}
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfigOptional(path, optional)
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), "alpha-search") {
					t.Fatalf("invalid %q accepted with optional=%t", tc.value, optional)
				}
				continue
			}
			if err != nil || len(cfg.CodexKey) != 1 || cfg.CodexKey[0].AlphaSearch != tc.want {
				t.Fatalf("value %q lost its default or value: %v", tc.value, err)
			}
		}
	}
	for _, raw := range []string{`{"alpha-search":"true"}`, `{"alpha-search":1}`, `{"alpha-search":[]}`} {
		var key CodexKey
		if json.Unmarshal([]byte(raw), &key) == nil {
			t.Fatal("invalid JSON capability accepted")
		}
	}
}

func TestCodexAlphaSearchYAMLMergesAndSaveReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := "defaults: &key\n  api-key: fixture\n  base-url: https://example.invalid/v1\n  alpha-search: true\ncodex-api-key:\n  - <<: *key\n    future-field: keep\n"
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil || len(cfg.CodexKey) != 1 || !cfg.CodexKey[0].AlphaSearch {
		t.Fatalf("merged capability not loaded: %v", err)
	}
	for _, enabled := range []bool{true, false, true} {
		cfg.CodexKey[0].AlphaSearch = enabled
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		reloaded, err := LoadConfig(path)
		if err != nil || len(reloaded.CodexKey) != 1 || reloaded.CodexKey[0].AlphaSearch != enabled || reloaded.CodexKey[0].APIKey != "fixture" {
			t.Fatalf("saved capability not restored: %v", err)
		}
		saved, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(saved), "future-field: keep") {
			t.Fatal("save dropped an unrelated field")
		}
	}
	invalid := strings.Replace(data, "alpha-search: true", "alpha-search: []", 1)
	if err := os.WriteFile(path, []byte(invalid), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfigOptional(path, true); err == nil {
		t.Fatal("invalid inherited capability bypassed optional validation")
	}
	explicit := invalid + "    alpha-search: false\n"
	if err := os.WriteFile(path, []byte(explicit), 0o600); err != nil {
		t.Fatal(err)
	}
	if cfg, err := LoadConfig(path); err != nil || cfg.CodexKey[0].AlphaSearch {
		t.Fatalf("explicit override did not win over merge: %v", err)
	}
}
