package config

import (
	"encoding/json"
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
