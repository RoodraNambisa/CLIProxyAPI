package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestModelCompatRejectsInvalidTypesAndAllowsDefault(t *testing.T) {
	for _, family := range retryConfigFamilies {
		for _, value := range []string{"true", "false", "null", "'true'", "1", "[]", "{}"} {
			t.Run(family+"/"+value, func(t *testing.T) {
				body := family + ": [{api-key: fixture, name: compat, base-url: https://example.test, models: [{name: upstream, alias: local, is-compat: " + value + "}]}]\n"
				path := filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(path, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
				_, err := LoadConfigOptional(path, true)
				valid := value == "true" || value == "false" || value == "null"
				if (err == nil) != valid {
					t.Fatalf("compat validity=%v want=%v", err == nil, valid)
				}
				if err != nil && !strings.Contains(err.Error(), "is-compat") {
					t.Fatal("compat error lost the field location")
				}
			})
		}
		for _, value := range []string{`"true"`, "1", "[]", "{}"} {
			var cfg Config
			if err := json.Unmarshal([]byte(`{"`+family+`":[{"models":[{"is-compat":`+value+`}]}]}`), &cfg); err == nil {
				t.Fatal("invalid JSON compatibility flag accepted")
			}
		}
	}
	for _, body := range []string{
		"flag: &flag 'true'\ncodex-api-key: [{models: [{is-compat: *flag}]}]\n",
		"model: &model {is-compat: 1}\ncodex-api-key: [{models: [{<<: *model}]}]\n",
		"defaults: &defaults {codex-api-key: [{models: [{is-compat: []}]}]}\n<<: *defaults\n",
	} {
		if err := validateModelCatalogFieldsYAML([]byte(body)); err == nil {
			t.Fatal("inherited invalid compatibility flag accepted")
		}
	}
	if err := validateModelCatalogFieldsYAML([]byte("model: &model {is-compat: 1}\ncodex-api-key: [{models: [{<<: *model, is-compat: false}]}]\n")); err != nil {
		t.Fatal(err)
	}
}

func TestModelCompatSaveReloadCloneAndClearPreserveExtensions(t *testing.T) {
	var body strings.Builder
	for _, family := range retryConfigFamilies {
		body.WriteString(family + ": [{api-key: fixture, name: compat, base-url: https://example.test, models: [{name: upstream, alias: local, is-compat: true, future-model: keep}]}]\n")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body.String()), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	get := func(c *Config) []bool {
		return []bool{c.GeminiKey[0].Models[0].GetIsCompat(), c.InteractionsKey[0].Models[0].GetIsCompat(), c.ClaudeKey[0].Models[0].GetIsCompat(), c.CodexKey[0].Models[0].GetIsCompat(), c.VertexCompatAPIKey[0].Models[0].GetIsCompat(), c.OpenAICompatibility[0].Models[0].GetIsCompat()}
	}
	snapshot, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []bool{true, false} {
		for _, got := range get(cfg) {
			if !got {
				t.Fatal("loaded compatibility flag was lost")
			}
		}
		if !want {
			cfg.GeminiKey[0].Models[0].IsCompat = false
			cfg.InteractionsKey[0].Models[0].IsCompat = false
			cfg.ClaudeKey[0].Models[0].IsCompat = false
			cfg.CodexKey[0].Models[0].IsCompat = false
			cfg.VertexCompatAPIKey[0].Models[0].IsCompat = false
			cfg.OpenAICompatibility[0].Models[0].IsCompat = false
		}
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		reloaded, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, got := range get(reloaded) {
			if got != want {
				t.Fatal("saved compatibility flag did not survive reload")
			}
		}
		for _, got := range get(snapshot) {
			if !got {
				t.Fatal("old config snapshot was changed")
			}
		}
		saved, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(saved), "future-model: keep") != 6 || (!want && strings.Contains(string(saved), "is-compat: true")) {
			t.Fatal("save removed unknown extensions or retained a cleared flag")
		}
	}
}
