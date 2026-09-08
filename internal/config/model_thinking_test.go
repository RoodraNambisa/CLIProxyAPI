package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestNormalizeModelThinkingSupportPreservesSourceAndLevelOrder(t *testing.T) {
	raw := &registry.ThinkingSupport{Min: 1024, Max: 32768, Levels: []string{" HIGH ", "max", "HIGH", "none", "AUTO", ""}}
	normalized := NormalizeModelThinkingSupport(raw)
	if normalized == raw || !reflect.DeepEqual(normalized.Levels, []string{"high", "max", "none", "auto"}) || !normalized.ZeroAllowed || !normalized.DynamicAllowed || normalized.Min != 1024 || normalized.Max != 32768 {
		t.Fatal("thinking normalization changed budget or level order")
	}
	normalized.Levels[0] = "low"
	if raw.Levels[0] != " HIGH " || raw.ZeroAllowed || raw.DynamicAllowed {
		t.Fatal("normalization mutated saved thinking")
	}
	if NormalizeModelThinkingSupport(nil) != nil {
		t.Fatal("missing thinking gained an override")
	}
}

func TestModelThinkingRejectsInvalidYAMLBeforeOptionalFallback(t *testing.T) {
	for _, family := range retryConfigFamilies {
		for _, tc := range []struct {
			value string
			valid bool
		}{
			{"null", true}, {"{}", true}, {"{levels: []}", true}, {"{min: 0, max: 2147483647}", true},
			{"{min: 1024, max: 32768, zero-allowed: true, dynamic-allowed: false}", true},
			{"{levels: [none, minimal, low, medium, high, xhigh, max, auto, ' HIGH ']}", true},
			{"[]", false}, {"true", false}, {"{min: -1}", false}, {"{max: 2147483648}", false},
			{"{max: 9223372036854775808}", false}, {"{min: 10, max: 9}", false}, {"{min: 1.0, max: 3}", false},
			{"{min: '1', max: 3}", false}, {"{max: true}", false}, {"{levels: high}", false},
			{"{levels: [1]}", false}, {"{levels: [true]}", false}, {"{levels: [null]}", false},
			{"{levels: [unknown]}", false}, {"{levels: ['']}", false}, {"{zero-allowed: 'true'}", false},
			{"{dynamic-allowed: 1}", false},
		} {
			t.Run(family+"/"+tc.value, func(t *testing.T) {
				body := family + ": [{api-key: fixture, name: compat, base-url: https://example.test, models: [{name: upstream, alias: local, thinking: " + tc.value + "}]}]\n"
				path := filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(path, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
				_, err := LoadConfigOptional(path, true)
				if (err == nil) != tc.valid {
					t.Fatalf("thinking validity=%v want=%v: %v", err == nil, tc.valid, err)
				}
				if err != nil && !strings.Contains(err.Error(), "thinking") {
					t.Fatal("thinking error lost its field location")
				}
			})
		}
	}
}

func TestModelThinkingSaveReloadSnapshotAndClearPreserveExtensions(t *testing.T) {
	var body strings.Builder
	for _, family := range retryConfigFamilies {
		body.WriteString(family + ": [{api-key: fixture, name: compat, base-url: https://example.test, models: [{name: upstream, alias: local, thinking: {levels: [none, ' XHIGH ', max], zero-allowed: true}, future-model: keep}]}]\n")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body.String()), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	get := func(c *Config) []*registry.ThinkingSupport {
		return []*registry.ThinkingSupport{c.GeminiKey[0].Models[0].GetThinking(), c.InteractionsKey[0].Models[0].GetThinking(), c.ClaudeKey[0].Models[0].GetThinking(), c.CodexKey[0].Models[0].GetThinking(), c.VertexCompatAPIKey[0].Models[0].GetThinking(), c.OpenAICompatibility[0].Models[0].GetThinking()}
	}
	snapshot, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, support := range get(cfg) {
		if support == nil || len(support.Levels) != 3 || support.Levels[1] != " XHIGH " || !support.ZeroAllowed {
			t.Fatal("configured model thinking was dropped or normalized during loading")
		}
		support.Levels[1] = "high"
	}
	for _, support := range get(snapshot) {
		if support.Levels[1] != " XHIGH " {
			t.Fatal("config snapshot shares its reasoning levels")
		}
	}
	for _, clear := range []bool{false, true} {
		if clear {
			cfg.GeminiKey[0].Models[0].Thinking = nil
			cfg.InteractionsKey[0].Models[0].Thinking = nil
			cfg.ClaudeKey[0].Models[0].Thinking = nil
			cfg.CodexKey[0].Models[0].Thinking = nil
			cfg.VertexCompatAPIKey[0].Models[0].Thinking = nil
			cfg.OpenAICompatibility[0].Models[0].Thinking = nil
		}
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		reloaded, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, support := range get(reloaded) {
			if clear && support != nil {
				t.Fatal("cleared thinking override remained saved")
			}
			if !clear && (support == nil || support.Levels[1] != "high") {
				t.Fatal("saved thinking override was lost")
			}
		}
		saved, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(saved), "future-model: keep") != 6 {
			t.Fatal("saving thinking removed unknown model extensions")
		}
	}
}

func TestModelThinkingJSONAndProgrammaticValidation(t *testing.T) {
	for _, family := range retryConfigFamilies {
		for _, value := range []string{`{"min":-1}`, `{"max":2147483648}`, `{"max":1.0}`, `{"levels":[1]}`, `{"levels":["unknown"]}`, `{"zero_allowed":"true"}`, `{"min":9,"max":1}`} {
			var cfg Config
			if err := json.Unmarshal([]byte(`{"`+family+`":[{"models":[{"name":"upstream","thinking":`+value+`}]}]}`), &cfg); err != nil {
				continue
			}
			if err := cfg.ValidateModelThinking(); err == nil {
				t.Fatal("invalid JSON thinking declaration accepted")
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := SaveConfigPreserveComments(path, &cfg); err == nil {
				t.Fatal("invalid thinking declaration saved")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("invalid thinking save created a file")
			}
		}
	}
	if err := (*Config)(nil).ValidateModelThinking(); err != nil {
		t.Fatal(err)
	}
}

func TestModelThinkingYAMLMergeAndAliasValidation(t *testing.T) {
	for _, body := range []string{
		"capability: &cap {levels: [high, 1]}\ncodex-api-key: [{models: [{thinking: *cap}]}]\n",
		"capability: &cap {min: -1}\ncodex-api-key: [{models: [{thinking: {<<: *cap}}]}]\n",
		"model: &model {thinking: {max: 1.5}}\ncodex-api-key: [{models: [{<<: *model}]}]\n",
		"defaults: &defaults {codex-api-key: [{models: [{thinking: {levels: [unknown]}}]}]}\n<<: *defaults\n",
	} {
		if err := validateModelCatalogFieldsYAML([]byte(body)); err == nil {
			t.Fatal("inherited invalid thinking accepted")
		}
	}
	valid := "capability: &cap {min: -1}\ncodex-api-key: [{models: [{thinking: {<<: *cap, min: 0, levels: []}}]}]\n"
	if err := validateModelCatalogFieldsYAML([]byte(valid)); err != nil {
		t.Fatal(err)
	}
}
