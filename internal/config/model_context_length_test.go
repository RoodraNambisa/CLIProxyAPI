package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestModelContextLengthConfigValidation(t *testing.T) {
	for _, family := range retryConfigFamilies {
		for _, tc := range []struct {
			value string
			valid bool
		}{
			{"0", true}, {"1", true}, {"1048576", true}, {"2147483647", true}, {"null", true},
			{"-1", false}, {"2147483648", false}, {"9223372036854775808", false},
			{"1.5", false}, {"1.0", false}, {"'1048576'", false}, {"true", false}, {"[]", false}, {"{}", false},
		} {
			t.Run(family+"/"+tc.value, func(t *testing.T) {
				body := family + ": [{api-key: fixture, name: compat, base-url: https://example.test, models: [{name: upstream, alias: local, max-context-length: " + tc.value + "}]}]\n"
				path := filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(path, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
				_, err := LoadConfigOptional(path, true)
				if (err == nil) != tc.valid {
					t.Fatalf("validation success = %v, want %v", err == nil, tc.valid)
				}
				if err != nil && !strings.Contains(err.Error(), "max-context-length") {
					t.Fatal("error omitted field location")
				}
			})
		}
	}
}

func TestModelContextLengthSaveReloadAndClear(t *testing.T) {
	var body strings.Builder
	for _, family := range retryConfigFamilies {
		fmt.Fprintf(&body, "%s: [{api-key: fixture, name: compat, base-url: https://example.test, models: [{name: upstream, alias: local, max-context-length: 1048576, future-model: keep}]}]\n", family)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body.String()), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	getLimits := func(c *Config) []int {
		return []int{c.GeminiKey[0].Models[0].GetMaxContextLength(), c.InteractionsKey[0].Models[0].GetMaxContextLength(), c.ClaudeKey[0].Models[0].GetMaxContextLength(), c.CodexKey[0].Models[0].GetMaxContextLength(), c.VertexCompatAPIKey[0].Models[0].GetMaxContextLength(), c.OpenAICompatibility[0].Models[0].GetMaxContextLength()}
	}
	for _, want := range []int{1048576, 0} {
		if want == 0 {
			cfg.GeminiKey[0].Models[0].MaxContextLength = 0
			cfg.InteractionsKey[0].Models[0].MaxContextLength = 0
			cfg.ClaudeKey[0].Models[0].MaxContextLength = 0
			cfg.CodexKey[0].Models[0].MaxContextLength = 0
			cfg.VertexCompatAPIKey[0].Models[0].MaxContextLength = 0
			cfg.OpenAICompatibility[0].Models[0].MaxContextLength = 0
		}
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		reloaded, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, got := range getLimits(reloaded) {
			if got != want {
				t.Fatalf("context limit = %d, want %d", got, want)
			}
		}
		saved, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(saved), "future-model: keep") != 6 {
			t.Fatal("save removed model extensions")
		}
		if want == 0 && strings.Contains(string(saved), "max-context-length:") {
			t.Fatal("cleared limit retained in YAML")
		}
	}
}

func TestModelContextLengthJSONAndProgrammaticValidation(t *testing.T) {
	for _, family := range retryConfigFamilies {
		for _, value := range []string{"-1", "2147483648", "9223372036854775808", "1.5", "true", "[]", "{}", `"1048576"`} {
			var cfg Config
			body := `{"` + family + `":[{"models":[{"name":"upstream","max-context-length":` + value + `}]}]}`
			if err := json.Unmarshal([]byte(body), &cfg); err != nil {
				continue
			}
			if err := cfg.ValidateModelContextLengths(); err == nil {
				t.Fatalf("%s accepted invalid JSON range", family)
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := SaveConfigPreserveComments(path, &cfg); err == nil {
				t.Fatal("invalid programmatic config saved")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("invalid save wrote a file")
			}
		}
	}
	if err := (*Config)(nil).ValidateModelContextLengths(); err != nil {
		t.Fatal(err)
	}
	if err := (&Config{}).ValidateModelContextLengths(); err != nil {
		t.Fatal(err)
	}
}

func TestModelContextLengthYAMLMergePrecedence(t *testing.T) {
	for _, body := range []string{
		"limit: &limit -1\ncodex-api-key: [{models: [{max-context-length: *limit}]}]\n",
		"model: &model {max-context-length: 1.5}\ncodex-api-key: [{models: [{<<: *model}]}]\n",
		"models: &models [{max-context-length: -1}]\ncodex-api-key: [{models: *models}]\n",
		"defaults: &defaults {codex-api-key: [{models: [{max-context-length: -1}]}]}\n<<: *defaults\n",
	} {
		if err := validateModelCatalogFieldsYAML([]byte(body)); err == nil {
			t.Fatal("inherited invalid limit accepted")
		}
	}
	valid := "model: &model {max-context-length: -1}\ncodex-api-key: [{models: [{<<: *model, max-context-length: 0}]}]\n"
	if err := validateModelCatalogFieldsYAML([]byte(valid)); err != nil {
		t.Fatal(err)
	}
}
