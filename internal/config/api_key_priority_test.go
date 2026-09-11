package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAPIKeyPriorityListsValidateJSONAndYAML(t *testing.T) {
	for _, raw := range []string{`[1.5]`, `["1"]`, `[true]`, `[null]`, `[9007199254740992]`, `[-9007199254740992]`, `[9223372036854775808]`, `{}`, `"1"`} {
		for _, decode := range []func([]byte, any) error{json.Unmarshal, yaml.Unmarshal} {
			var list APIKeyPriorityList
			if err := decode([]byte(raw), &list); err == nil {
				t.Fatalf("invalid priority list accepted: %s", raw)
			}
		}
	}
	for _, raw := range []string{`[]`, `null`, `[-9007199254740991, -1, 0, 1, 9007199254740991]`} {
		var fromJSON, fromYAML APIKeyPriorityList
		if err := json.Unmarshal([]byte(raw), &fromJSON); err != nil {
			t.Fatal(err)
		}
		if err := yaml.Unmarshal([]byte(raw), &fromYAML); err != nil || !reflect.DeepEqual(fromJSON, fromYAML) {
			t.Fatalf("valid list differs across protocols: %s, %v", raw, err)
		}
	}
}

func TestAPIKeyPriorityNormalizationPreservesPriorityOnlyGroups(t *testing.T) {
	groups, err := NormalizeAPIKeyGroups([]APIKeyGroup{{APIKey: "fixture", Providers: []string{"*"}, AllowedPriorities: []int{3, -1, 3, 0}, ExcludedPriorities: []int{3, 3}}}, []string{"fixture"})
	if err != nil || len(groups) != 1 || len(groups[0].Providers) != 0 || !reflect.DeepEqual(groups[0].AllowedPriorities, APIKeyPriorityList{-1, 0, 3}) || !reflect.DeepEqual(groups[0].ExcludedPriorities, APIKeyPriorityList{3}) {
		t.Fatalf("priority-only group lost its restriction: %#v, %v", groups, err)
	}
	var cfg Config
	if err := yaml.Unmarshal([]byte("tiers: &tiers [-1, 0]\napi-key-groups: [{api-key: fixture, allowed-priorities: *tiers}]"), &cfg); err != nil || !reflect.DeepEqual(cfg.APIKeyGroups[0].AllowedPriorities, APIKeyPriorityList{-1, 0}) {
		t.Fatal("YAML priority list alias was not preserved", err)
	}
}

func TestAPIKeyPrioritySaveClearsInheritedListsAndPreservesExtensions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	const body = "api-keys: [fixture]\ndefaults: &defaults\n  allowed-priorities: [1]\n  excluded-priorities: [2]\n  future-setting: preserved\n  id: extension-id\napi-key-groups:\n  - <<: *defaults\n    api-key: fixture\n    providers: [codex]\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.APIKeyGroups[0].AllowedPriorities = nil
	cfg.APIKeyGroups[0].ExcludedPriorities = nil
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil || len(loaded.APIKeyGroups[0].AllowedPriorities) != 0 || len(loaded.APIKeyGroups[0].ExcludedPriorities) != 0 {
		t.Fatal("cleared inherited restrictions returned after reload", err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Groups []map[string]any `yaml:"api-key-groups"`
	}
	if err := yaml.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	if document.Groups[0]["future-setting"] != "preserved" || document.Groups[0]["id"] != "extension-id" {
		t.Fatal("saving priority restrictions dropped unrelated YAML extensions")
	}
}
