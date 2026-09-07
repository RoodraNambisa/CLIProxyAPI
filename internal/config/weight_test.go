package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCredentialWeightYAMLTypesAliasesAndMergePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"missing", "codex-api-key:\n  - api-key: test\n", true},
		{"zero", "codex-api-key:\n  - api-key: test\n    weight: 0\n", true},
		{"negative", "codex-api-key:\n  - api-key: test\n    weight: -2\n", true},
		{"maximum", "codex-api-key:\n  - api-key: test\n    weight: 1000000\n", true},
		{"fraction", "codex-api-key:\n  - api-key: test\n    weight: 1.5\n", false},
		{"float syntax", "codex-api-key:\n  - api-key: test\n    weight: 1.0\n", false},
		{"string", "codex-api-key:\n  - api-key: test\n    weight: '1'\n", false},
		{"null", "codex-api-key:\n  - api-key: test\n    weight: null\n", false},
		{"overflow", "codex-api-key:\n  - api-key: test\n    weight: 9223372036854775808\n", false},
		{"large", "codex-api-key:\n  - api-key: test\n    weight: 1000001\n", false},
		{"alias scalar", "number: &number 1.5\ncodex-api-key:\n  - api-key: test\n    weight: *number\n", false},
		{"alias sequence", "keys: &keys [{api-key: test, weight: 1.5}]\ncodex-api-key: *keys\n", false},
		{"inherited", "base: &base {weight: 1.5}\ncodex-api-key:\n  - <<: *base\n    api-key: test\n", false},
		{"explicit wins", "base: &base {weight: 1.5}\ncodex-api-key:\n  - <<: *base\n    api-key: test\n    weight: 2\n", true},
		{"merge order", "first: &first {weight: 3}\nsecond: &second {weight: 1.5}\ncodex-api-key:\n  - <<: [*first,*second]\n    api-key: test\n", true},
		{"compat", "openai-compatibility:\n  - name: test\n    api-key-entries:\n      - api-key: test\n        weight: 1.5\n", false},
		{"compat provider alias", "providers: &providers [{name: test, api-key-entries: [{api-key: test, weight: 1.5}]}]\nopenai-compatibility: *providers\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateCredentialWeightYAML([]byte(tc.body)); (err == nil) != tc.valid {
				t.Fatalf("weight YAML validation=%v", err)
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(path); (err == nil) != tc.valid {
				t.Fatalf("config accepted invalid weight or rejected valid one: %v", err)
			}
		})
	}
}

func TestCredentialWeightRejectsExcessiveMergeDepth(t *testing.T) {
	var body strings.Builder
	body.WriteString("base0: &base0 {weight: 1.5}\n")
	for index := 1; index <= 260; index++ {
		fmt.Fprintf(&body, "base%d: &base%d {<<: *base%d}\n", index, index, index-1)
	}
	body.WriteString("codex-api-key:\n  - api-key: test\n    base-url: https://example.test\n    <<: *base260\n")
	if err := validateCredentialWeightYAML([]byte(body.String())); err == nil {
		t.Fatal("weight traversal silently skipped a deeply merged invalid field")
	}
}

func TestCredentialWeightSaveClearAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := "# retained comment\ncustom-extension: retained\ncodex-api-key:\n  - api-key: test\n    base-url: https://example.test\n    weight: 4\n"
	if err := os.WriteFile(path, []byte(initial), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	cfg.CodexKey[0].Weight = &zero
	cfg.GeminiKey = []GeminiKey{{APIKey: "test", Weight: &zero}}
	cfg.InteractionsKey = []GeminiKey{{APIKey: "test", Weight: &zero}}
	cfg.ClaudeKey = []ClaudeKey{{APIKey: "test", Weight: &zero}}
	cfg.VertexCompatAPIKey = []VertexCompatKey{{APIKey: "test", BaseURL: "https://example.test", Weight: &zero}}
	cfg.OpenAICompatibility = []OpenAICompatibility{{Name: "compat", BaseURL: "https://example.test", APIKeyEntries: []OpenAICompatibilityAPIKey{{APIKey: "test", Weight: &zero}}}}
	for _, clear := range []bool{false, true} {
		if clear {
			cfg.CodexKey[0].Weight, cfg.GeminiKey[0].Weight, cfg.InteractionsKey[0].Weight = nil, nil, nil
			cfg.ClaudeKey[0].Weight, cfg.VertexCompatAPIKey[0].Weight, cfg.OpenAICompatibility[0].APIKeyEntries[0].Weight = nil, nil, nil
		}
		if err = SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		reloaded, errLoad := LoadConfig(path)
		if errLoad != nil {
			t.Fatal(errLoad)
		}
		weights := []*int{reloaded.CodexKey[0].Weight, reloaded.GeminiKey[0].Weight, reloaded.InteractionsKey[0].Weight, reloaded.ClaudeKey[0].Weight, reloaded.VertexCompatAPIKey[0].Weight, reloaded.OpenAICompatibility[0].APIKeyEntries[0].Weight}
		for _, weight := range weights {
			if (clear && weight != nil) || (!clear && (weight == nil || *weight != 0)) {
				t.Fatal("saved weight did not preserve zero or cleared inheritance")
			}
		}
		saved, errRead := os.ReadFile(path)
		if errRead != nil || !strings.Contains(string(saved), "custom-extension: retained") || !strings.Contains(string(saved), "# retained comment") {
			t.Fatal("weight save dropped an unrelated extension or comment")
		}
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	invalid := MaxCredentialWeight + 1
	cfg.CodexKey[0].Weight = &invalid
	if err = SaveConfigPreserveComments(path, cfg); err == nil {
		t.Fatal("invalid weight was saved")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(before) {
		t.Fatal("rejected weight changed the saved file")
	}
}

func TestCredentialWeightZeroSurvivesYAMLRoundTrip(t *testing.T) {
	zero := 0
	cfg := Config{CodexKey: []CodexKey{{APIKey: "test", Weight: &zero}}, OpenAICompatibility: []OpenAICompatibility{{Name: "compat", APIKeyEntries: []OpenAICompatibilityAPIKey{{APIKey: "test", Weight: &zero}}}}}
	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var restored Config
	if err := yaml.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.CodexKey[0].Weight == nil || *restored.CodexKey[0].Weight != 0 || restored.OpenAICompatibility[0].APIKeyEntries[0].Weight == nil {
		t.Fatal("zero became default weight during serialization")
	}
	tooLarge := MaxCredentialWeight + 1
	for _, cfg := range []Config{
		{GeminiKey: []GeminiKey{{Weight: &tooLarge}}},
		{InteractionsKey: []GeminiKey{{Weight: &tooLarge}}},
		{ClaudeKey: []ClaudeKey{{Weight: &tooLarge}}},
		{CodexKey: []CodexKey{{Weight: &tooLarge}}},
		{VertexCompatAPIKey: []VertexCompatKey{{Weight: &tooLarge}}},
		{OpenAICompatibility: []OpenAICompatibility{{APIKeyEntries: []OpenAICompatibilityAPIKey{{Weight: &tooLarge}}}}},
	} {
		if err := cfg.ValidateCredentialWeights(); err == nil || !strings.Contains(err.Error(), "weight") {
			t.Fatal("API-key family bypassed weight validation")
		}
	}
}
