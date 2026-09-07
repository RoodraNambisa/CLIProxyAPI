package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCredentialYAMLKeepsExtensionsWhenClearingWeight(t *testing.T) {
	for name, body := range map[string]string{
		"root merge":     "defaults: &defaults {codex-api-key: [{api-key: test, base-url: 'https://example.test', weight: 4, future-option: {counter: 9007199254740993, values: [a, b]}}]}\n<<: *defaults\n",
		"direct":         "codex-api-key:\n  - api-key: test\n    base-url: https://example.test\n    weight: 4\n    future-option: {counter: 9007199254740993, values: [a, b]}\n",
		"mapping alias":  "entry: &entry {api-key: test, base-url: 'https://example.test', weight: 4, future-option: {counter: 9007199254740993, values: [a, b]}}\ncodex-api-key: [*entry]\n",
		"merged":         "entry: &entry {weight: 4, future-option: {counter: 9007199254740993, values: [a, b]}}\ncodex-api-key:\n  - <<: *entry\n    api-key: test\n    base-url: https://example.test\n",
		"sequence alias": "entries: &entries [{api-key: test, base-url: 'https://example.test', weight: 4, future-option: {counter: 9007199254740993, values: [a, b]}}]\ncodex-api-key: *entries\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.CodexKey[0].Weight = nil
			if err = SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatal(err)
			}
			saved, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var raw struct {
				Keys []struct {
					Weight *int `yaml:"weight"`
					Future struct {
						Counter uint64   `yaml:"counter"`
						Values  []string `yaml:"values"`
					} `yaml:"future-option"`
				} `yaml:"codex-api-key"`
			}
			if err = yaml.Unmarshal(saved, &raw); err != nil {
				t.Fatal(err)
			}
			if len(raw.Keys) != 1 || raw.Keys[0].Weight != nil || raw.Keys[0].Future.Counter != 9007199254740993 || strings.Join(raw.Keys[0].Future.Values, ",") != "a,b" {
				t.Fatal("credential extensions were lost or cleared weight was inherited again")
			}
		})
	}
}

func TestCredentialYAMLKeepsExtensionsWithTheirEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "openai-compatibility:\n  - name: compat\n    base-url: https://example.test\n    future-provider: retained\n    api-key-entries:\n      - api-key: first\n        future-key: first-option\n        weight: 2\n      - api-key: second\n        future-key: second-option\n        weight: 3\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	keys := cfg.OpenAICompatibility[0].APIKeyEntries
	cfg.OpenAICompatibility[0].APIKeyEntries = []OpenAICompatibilityAPIKey{keys[1], keys[0]}
	cfg.OpenAICompatibility[0].APIKeyEntries[0].Weight = nil
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Providers []struct {
			Extension string `yaml:"future-provider"`
			Keys      []struct {
				Key       string `yaml:"api-key"`
				Extension string `yaml:"future-key"`
				Weight    *int   `yaml:"weight"`
			} `yaml:"api-key-entries"`
		} `yaml:"openai-compatibility"`
	}
	if err = yaml.Unmarshal(saved, &raw); err != nil {
		t.Fatal(err)
	}
	if raw.Providers[0].Extension != "retained" || raw.Providers[0].Keys[0].Key != "second" || raw.Providers[0].Keys[0].Extension != "second-option" || raw.Providers[0].Keys[0].Weight != nil || raw.Providers[0].Keys[1].Extension != "first-option" {
		t.Fatal("reordering attached an extension to another credential or lost an inherited weight")
	}
}
