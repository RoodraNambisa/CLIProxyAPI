package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAPIKeyNameNormalizationAndNameOnlyGroup(t *testing.T) {
	for _, name := range []string{"", "  ", "  工作 / Work 🔑  ", strings.Repeat("名", 100)} {
		groups, err := NormalizeAPIKeyGroups([]APIKeyGroup{{APIKey: "fixture", Name: name}}, []string{"fixture"})
		if err != nil {
			t.Fatal(err)
		}
		want := strings.TrimSpace(name)
		if want == "" {
			if len(groups) != 0 {
				t.Fatal("empty name retained an otherwise empty group")
			}
		} else if len(groups) != 1 || groups[0].Name != want || len(groups[0].Providers) != 0 {
			t.Fatal("display-only name was lost or changed access")
		}
	}
	for _, name := range []string{strings.Repeat("名", 101), "a\nb", "a\tb", "a\x00b", "a\u0085b", "\xff"} {
		if _, err := NormalizeAPIKeyGroups([]APIKeyGroup{{APIKey: "fixture", Name: name}}, []string{"fixture"}); err == nil {
			t.Fatal("invalid display name was accepted")
		}
	}
}

func TestAPIKeyNameYAMLSaveReloadAndClearInheritedName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	const original = "api-keys: [fixture]\nshared: &shared\n  name: 原名称\n  future-setting: preserved\napi-key-groups:\n  - <<: *shared\n    api-key: fixture\n    providers: [codex]\n    allowed-priorities: [1]\n    excluded-priorities: [2]\n"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{" 新名称 🔑 ", ""} {
		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		cfg.APIKeyGroups[0].Name = name
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		loaded, err := LoadConfig(path)
		if err != nil || loaded.APIKeyGroups[0].Name != strings.TrimSpace(name) || len(loaded.APIKeyGroups[0].AllowedPriorities) != 1 || len(loaded.APIKeyGroups[0].ExcludedPriorities) != 1 {
			t.Fatal("name round-trip changed the label or restrictions", err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := yaml.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		group := document["api-key-groups"].([]any)[0].(map[string]any)
		if group["future-setting"] != "preserved" {
			t.Fatal("name save removed unknown YAML fields")
		}
	}
}
