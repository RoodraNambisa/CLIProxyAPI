package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestContainsDeprecatedGeminiCLIConfig(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want bool
	}{
		{name: "endpoint flag", yaml: "enable-gemini-cli-endpoint: true\n", want: true},
		{name: "mixed case endpoint flag", yaml: "Enable-Gemini-CLI-Endpoint: true\n", want: true},
		{name: "disabled endpoint flag", yaml: "enable-gemini-cli-endpoint: false\n", want: true},
		{name: "model alias", yaml: "oauth-model-alias:\n  gemini-cli:\n    - name: old\n      alias: new\n", want: true},
		{name: "empty model alias", yaml: "oauth-model-alias:\n  gemini-cli: []\n", want: true},
		{name: "excluded models", yaml: "oauth-excluded-models:\n  Gemini-CLI:\n    - model\n", want: true},
		{name: "empty excluded models", yaml: "oauth-excluded-models:\n  gemini-cli: []\n", want: true},
		{name: "merged config", yaml: "defaults: &defaults\n  enable-gemini-cli-endpoint: true\n<<: *defaults\n", want: true},
		{name: "merged alias overridden", yaml: "defaults: &defaults\n  oauth-model-alias:\n    gemini-cli:\n      - name: old\n        alias: new\n<<: *defaults\noauth-model-alias:\n  codex:\n    - name: old\n      alias: new\n", want: true},
		{name: "request payload key", yaml: "payload:\n  override:\n    - params:\n        enable-gemini-cli-endpoint: true\n", want: false},
		{name: "cyclic merge sequence", yaml: "defaults: &loop [*loop]\n<<: *loop\n", want: false},
		{name: "active gemini api key", yaml: "gemini-api-key:\n  - api-key: test\n", want: false},
		{name: "unrelated provider", yaml: "oauth-model-alias:\n  antigravity:\n    - name: old\n      alias: new\n", want: false},
		{name: "invalid yaml", yaml: "oauth-model-alias: [\n", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := containsDeprecatedGeminiCLIConfig([]byte(test.yaml)); got != test.want {
				t.Fatalf("containsDeprecatedGeminiCLIConfig() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestDeprecatedGeminiCLIEndpointIsIgnoredByConfigSerialization(t *testing.T) {
	var cfg Config
	if errUnmarshal := yaml.Unmarshal([]byte("enable-gemini-cli-endpoint: true\n"), &cfg); errUnmarshal != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", errUnmarshal)
	}
	if cfg.EnableGeminiCLIEndpoint {
		t.Fatal("deprecated endpoint flag was loaded into runtime config")
	}
	cfg.EnableGeminiCLIEndpoint = true
	yamlData, errYAML := yaml.Marshal(&cfg)
	if errYAML != nil {
		t.Fatalf("yaml.Marshal() error = %v", errYAML)
	}
	jsonData, errJSON := json.Marshal(&cfg)
	if errJSON != nil {
		t.Fatalf("json.Marshal() error = %v", errJSON)
	}
	if strings.Contains(string(yamlData), "enable-gemini-cli-endpoint") || strings.Contains(string(jsonData), "enable-gemini-cli-endpoint") {
		t.Fatalf("deprecated endpoint flag was serialized: yaml=%s json=%s", yamlData, jsonData)
	}
}

func TestDeprecatedGeminiCLIModelConfigurationIsDiscarded(t *testing.T) {
	cfg := Config{
		OAuthModelAlias: map[string][]OAuthModelAlias{
			"Gemini-CLI": {{Name: "old", Alias: "legacy"}},
			"codex":      {{Name: "gpt-old", Alias: "gpt-new"}},
		},
		OAuthExcludedModels: map[string][]string{
			"GEMINI-CLI": {"legacy-model"},
			"codex":      {"gpt-old"},
		},
	}
	cfg.SanitizeOAuthModelAlias()
	cfg.OAuthExcludedModels = NormalizeOAuthExcludedModels(cfg.OAuthExcludedModels)

	if _, exists := cfg.OAuthModelAlias["gemini-cli"]; exists {
		t.Fatal("deprecated Gemini CLI model aliases survived normalization")
	}
	if _, exists := cfg.OAuthExcludedModels["gemini-cli"]; exists {
		t.Fatal("deprecated Gemini CLI exclusions survived normalization")
	}
	if len(cfg.OAuthModelAlias["codex"]) != 1 || len(cfg.OAuthExcludedModels["codex"]) != 1 {
		t.Fatalf("supported provider configuration was removed: aliases=%#v exclusions=%#v", cfg.OAuthModelAlias, cfg.OAuthExcludedModels)
	}

	yamlData, errYAML := yaml.Marshal(&cfg)
	if errYAML != nil {
		t.Fatalf("yaml.Marshal() error = %v", errYAML)
	}
	jsonData, errJSON := json.Marshal(&cfg)
	if errJSON != nil {
		t.Fatalf("json.Marshal() error = %v", errJSON)
	}
	if strings.Contains(strings.ToLower(string(yamlData)), "gemini-cli") || strings.Contains(strings.ToLower(string(jsonData)), "gemini-cli") {
		t.Fatalf("deprecated Gemini CLI model configuration was serialized: yaml=%s json=%s", yamlData, jsonData)
	}
}

func TestRemoveDeprecatedGeminiCLIConfigRootFollowsModelMappingMerges(t *testing.T) {
	original := `alias-defaults: &alias-defaults
  Gemini-CLI:
    - name: old
      alias: legacy
excluded-defaults: &excluded-defaults
  GEMINI-CLI:
    - legacy-model
oauth-model-alias:
  <<: *alias-defaults
oauth-excluded-models:
  <<: *excluded-defaults
payload:
  override:
    - params:
        enable-gemini-cli-endpoint: true
`
	var document yaml.Node
	if errUnmarshal := yaml.Unmarshal([]byte(original), &document); errUnmarshal != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", errUnmarshal)
	}
	removeDeprecatedGeminiCLIConfigRoot(document.Content[0])
	saved, errMarshal := yaml.Marshal(&document)
	if errMarshal != nil {
		t.Fatalf("yaml.Marshal() error = %v", errMarshal)
	}
	if strings.Contains(strings.ToLower(string(saved)), "gemini-cli:") {
		t.Fatalf("merged retired Gemini CLI keys survived cleanup:\n%s", saved)
	}
	if !strings.Contains(string(saved), "enable-gemini-cli-endpoint: true") {
		t.Fatalf("payload parameter was removed during cleanup:\n%s", saved)
	}
}

func TestRemoveDeprecatedGeminiCLIConfigRootHandlesCyclicMergeSequence(t *testing.T) {
	var document yaml.Node
	if errUnmarshal := yaml.Unmarshal([]byte("defaults: &loop [*loop]\n<<: *loop\n"), &document); errUnmarshal != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", errUnmarshal)
	}
	removeDeprecatedGeminiCLIConfigRoot(document.Content[0])
}

func TestSaveConfigPreserveCommentsRemovesDeprecatedGeminiCLIKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	original := `# retained comment
port: 8317
Enable-Gemini-CLI-Endpoint: true
OAuth-Model-Alias:
  Gemini-CLI:
    - name: mixed-old
      alias: mixed-legacy
OAuth-Excluded-Models:
  Gemini-CLI:
    - mixed-legacy-model
deprecated-defaults: &deprecated-defaults
  enable-gemini-cli-endpoint: true
  oauth-model-alias:
    gemini-cli:
      - name: anchored-old
        alias: anchored-legacy
  oauth-excluded-models:
    GEMINI-CLI:
      - anchored-legacy-model
<<: *deprecated-defaults
oauth-model-alias:
  gemini-cli:
    - name: old
      alias: legacy
  codex:
    - name: gpt-old
      alias: gpt-new
oauth-excluded-models:
  gemini-cli:
    - legacy-model
  codex:
    - gpt-old
`
	if errWrite := os.WriteFile(path, []byte(original), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &Config{
		Port: 8317,
		OAuthModelAlias: map[string][]OAuthModelAlias{
			"codex":      {{Name: "gpt-old", Alias: "gpt-new"}},
			"gemini-cli": {{Name: "generated-old", Alias: "generated-legacy"}},
		},
		OAuthExcludedModels: map[string][]string{
			"codex":      {"gpt-old"},
			"gemini-cli": {"generated-legacy-model"},
		},
		Payload: PayloadConfig{
			Override: []PayloadRule{{
				Models: []PayloadModelRule{{Name: "gpt-*", Protocol: "responses"}},
				Params: map[string]any{"enable-gemini-cli-endpoint": true},
			}},
		},
	}
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved config: %v", errRead)
	}
	saved := string(data)
	if strings.Count(saved, "enable-gemini-cli-endpoint") != 1 || strings.Contains(saved, "Enable-Gemini-CLI-Endpoint") || strings.Contains(strings.ToLower(saved), "gemini-cli:") {
		t.Fatalf("deprecated Gemini CLI keys survived structured save:\n%s", saved)
	}
	for _, retained := range []string{"# retained comment", "port: 8317", "codex:", "gpt-old", "gpt-new", "enable-gemini-cli-endpoint: true"} {
		if !strings.Contains(saved, retained) {
			t.Fatalf("supported config %q was removed:\n%s", retained, saved)
		}
	}
}

func TestWarnDeprecatedGeminiCLIConfigOnce(t *testing.T) {
	var once sync.Once
	warnings := 0
	warn := func() { warnings++ }
	data := []byte("enable-gemini-cli-endpoint: true\n")
	warnDeprecatedGeminiCLIConfigOnce(data, &once, warn)
	warnDeprecatedGeminiCLIConfigOnce(data, &once, warn)
	if warnings != 1 {
		t.Fatalf("warnings = %d, want 1", warnings)
	}
}
