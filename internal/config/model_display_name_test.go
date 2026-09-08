package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfiguredModelDisplayNamesSaveReloadAndClear(t *testing.T) {
	var body strings.Builder
	for _, family := range retryConfigFamilies {
		fmt.Fprintf(&body, "%s: [{api-key: test, name: compat, base-url: https://example.test, models: [{name: upstream, alias: local, display-name: Custom Label, future-model: keep}]}]\n", family)
	}
	body.WriteString("oauth-model-alias: {codex: [{name: upstream, alias: local, display-name: OAuth Label}]}\n")
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body.String()), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, model := range []interface{ GetDisplayName() string }{cfg.GeminiKey[0].Models[0], cfg.InteractionsKey[0].Models[0], cfg.ClaudeKey[0].Models[0], cfg.CodexKey[0].Models[0], cfg.VertexCompatAPIKey[0].Models[0], cfg.OpenAICompatibility[0].Models[0]} {
		if model.GetDisplayName() != "Custom Label" {
			t.Fatal("model label did not load")
		}
	}
	if cfg.OAuthModelAlias["codex"][0].DisplayName != "OAuth Label" {
		t.Fatal("OAuth sanitizer dropped label")
	}
	encoded, err := json.Marshal(cfg)
	if err != nil || !strings.Contains(string(encoded), `"display-name":"Custom Label"`) {
		t.Fatal("model JSON lost label")
	}
	for _, clear := range []bool{false, true} {
		if clear {
			cfg.GeminiKey[0].Models[0].DisplayName = ""
			cfg.InteractionsKey[0].Models[0].DisplayName = ""
			cfg.ClaudeKey[0].Models[0].DisplayName = ""
			cfg.CodexKey[0].Models[0].DisplayName = ""
			cfg.VertexCompatAPIKey[0].Models[0].DisplayName = ""
			cfg.OpenAICompatibility[0].Models[0].DisplayName = ""
			cfg.OAuthModelAlias["codex"][0].DisplayName = ""
		}
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		saved, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(saved), "future-model: keep") != 6 {
			t.Fatal("save dropped model extensions")
		}
		if clear && strings.Contains(string(saved), "display-name:") {
			t.Fatal("cleared label was retained")
		}
		if _, err := LoadConfig(path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConfiguredModelDisplayNameRejectsTypesAndAllowsInheritance(t *testing.T) {
	for _, family := range append(append([]string{}, retryConfigFamilies...), "oauth-model-alias") {
		for _, value := range []string{"123", "true", "[]", "{}", "null", "'  '", "'中文名称'"} {
			body := family + ": [{api-key: test, name: compat, base-url: https://example.test, models: &models [{name: upstream, alias: local, display-name: " + value + "}]}]\n"
			if family == "oauth-model-alias" {
				body = family + ": {codex: [{name: upstream, alias: local, display-name: " + value + "}]}\n"
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfigOptional(path, true)
			valid := value == "null" || strings.HasPrefix(value, "'")
			if (err == nil) != valid {
				t.Fatalf("%s value %s validation mismatch", family, value)
			}
		}
	}
}

func TestModelDisplayNameAliasesMergesAndJSONTypes(t *testing.T) {
	for _, body := range []string{
		"label: &label 123\ncodex-api-key: [{models: [{name: upstream, display-name: *label}]}]\n",
		"models: &models [{name: upstream, display-name: false}]\ncodex-api-key: [{models: *models}]\n",
		"defaults: &defaults {codex-api-key: [{models: [{display-name: 123}]}]}\n<<: *defaults\n",
		"defaults: &defaults {oauth-model-alias: {codex: [{display-name: false}]}}\n<<: *defaults\n",
	} {
		if err := validateModelCatalogFieldsYAML([]byte(body)); err == nil {
			t.Fatal("inherited invalid display name was accepted")
		}
	}
	validOverride := "model: &model {display-name: 123}\ncodex-api-key: [{models: [{<<: *model, name: upstream, display-name: valid}]}]\n"
	if err := validateModelCatalogFieldsYAML([]byte(validOverride)); err != nil {
		t.Fatal(err)
	}
	for _, family := range retryConfigFamilies {
		for _, value := range []string{"123", "false", "[]", "{}"} {
			var cfg Config
			body := `{"` + family + `":[{"models":[{"name":"upstream","display-name":` + value + `}]}]}`
			if err := json.Unmarshal([]byte(body), &cfg); err == nil {
				t.Fatal("invalid JSON display name was accepted")
			}
		}
	}
}
