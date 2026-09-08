package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRequestScopedErrorRuleStrictYAMLTypes(t *testing.T) {
	for _, body := range []string{
		"status: 400.5\nmatch: [x]\naction: stop",
		"status: '400'\nmatch: [x]\naction: stop",
		"status: 400\nmatch: [123]\naction: stop",
		"status: 400\nmatch-regexr: [true]\naction: stop",
		"status: 400\nmatch: x\naction: stop",
		"status: 400\nmatch: [x]\naction: 123",
		"status: 18446744073709551616\nmatch: [x]\naction: stop",
		"<<: {status: 400.5}\nmatch: [x]\naction: stop",
	} {
		var rule RequestScopedErrorRule
		if err := yaml.Unmarshal([]byte(body), &rule); err == nil {
			t.Fatal("invalid scalar type accepted")
		}
	}
	for _, body := range []string{
		"<<: {status: 400.5}\nstatus: 400\nmatch: [x]\naction: stop",
		"pattern: &p x\nstatus: 400\nmatch: [*p]\naction: stop",
		"status: 400\nmatch-regexr: ['^x$']\naction: stop",
	} {
		var rule RequestScopedErrorRule
		if err := yaml.Unmarshal([]byte(body), &rule); err != nil {
			t.Fatal(err)
		}
		if _, err := CompileRequestScopedErrors([]RequestScopedErrorRule{rule}); err != nil {
			t.Fatal(err)
		}
	}
	for _, body := range []string{`{"status":400.5}`, `{"status":"400"}`, `{"match":[123]}`, `{"action":false}`} {
		var rule RequestScopedErrorRule
		if err := json.Unmarshal([]byte(body), &rule); err == nil {
			t.Fatal("invalid JSON type accepted")
		}
	}
}

func TestRequestScopedErrorsConfigSaveAndClear(t *testing.T) {
	var source strings.Builder
	source.WriteString("# preserved\nfuture-global: kept\n")
	for _, family := range retryConfigFamilies {
		fmt.Fprintf(&source, "%s:\n  - api-key: test\n    name: compat\n    base-url: https://example.test\n    future-option: retained\n", family)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(source.String()), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	rule := RequestScopedErrorRule{Status: 400, Match: []string{" exact "}, MatchRegexr: []string{"^example$"}, Action: "stop"}
	for _, rules := range [][]RequestScopedErrorRule{{rule}, nil} {
		cfg.GeminiKey[0].RequestScopedErrors, cfg.InteractionsKey[0].RequestScopedErrors = rules, rules
		cfg.ClaudeKey[0].RequestScopedErrors, cfg.CodexKey[0].RequestScopedErrors = rules, rules
		cfg.VertexCompatAPIKey[0].RequestScopedErrors, cfg.OpenAICompatibility[0].RequestScopedErrors = rules, rules
		cfg.OAuthRequestScopedErrors = map[string][]RequestScopedErrorRule{"Codex": rules}
		if rules == nil {
			cfg.OAuthRequestScopedErrors = nil
		}
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		cfg, err = LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, got := range [][]RequestScopedErrorRule{cfg.GeminiKey[0].RequestScopedErrors, cfg.InteractionsKey[0].RequestScopedErrors, cfg.ClaudeKey[0].RequestScopedErrors, cfg.CodexKey[0].RequestScopedErrors, cfg.VertexCompatAPIKey[0].RequestScopedErrors, cfg.OpenAICompatibility[0].RequestScopedErrors, cfg.OAuthRequestScopedErrors["Codex"]} {
			if len(got) != len(rules) || (len(got) > 0 && got[0].Match[0] != " exact ") {
				t.Fatal("rule save, literal or clearing changed")
			}
		}
		saved, err := os.ReadFile(path)
		if err != nil || strings.Count(string(saved), "future-option: retained") != 6 || !bytes.Contains(saved, []byte("future-global: kept")) || !bytes.Contains(saved, []byte("# preserved")) {
			t.Fatal("saving rules lost unrelated YAML")
		}
	}
	before, _ := os.ReadFile(path)
	cfg.CodexKey[0].RequestScopedErrors = []RequestScopedErrorRule{{Status: 400, MatchRegexr: []string{"["}, Action: "stop"}}
	if err := SaveConfigPreserveComments(path, cfg); err == nil {
		t.Fatal("invalid regex saved")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("rejected configuration modified disk")
	}
}

func TestOAuthRequestScopedErrorsSnapshotAndProviderValidation(t *testing.T) {
	cfg := &Config{OAuthRequestScopedErrors: map[string][]RequestScopedErrorRule{" Codex ": {{Status: 400, Match: []string{"x"}, Action: "stop"}}}}
	policies, err := cfg.CompileOAuthRequestScopedErrors()
	if err != nil {
		t.Fatal(err)
	}
	cfg.OAuthRequestScopedErrors[" Codex "][0].Match[0] = "changed"
	if action, ok := policies["codex"].Match(400, "x"); !ok || action != RequestScopedActionStop {
		t.Fatal("published rules changed with saved data")
	}
	if _, ok := cfg.OAuthRequestScopedErrors["codex"]; ok {
		t.Fatal("validation rewrote saved provider spelling")
	}
	cfg.OAuthRequestScopedErrors["codex"] = nil
	if err := cfg.ValidateRequestScopedErrorRules(); err == nil {
		t.Fatal("ambiguous provider rules accepted")
	}
}

func TestOAuthRequestScopedErrorsClearDoesNotReinheritYAML(t *testing.T) {
	for _, suffix := range []string{"", "oauth-request-scoped-errors: {codex: [{status: 400, match: [override], action: stop}]}\n"} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		body := "base: &base {oauth-request-scoped-errors: {codex: [{status: 400, match: [original], action: stop}]}}\n<<: *base\n" + suffix
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		cfg.OAuthRequestScopedErrors = nil
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		loaded, err := LoadConfig(path)
		if err != nil || len(loaded.OAuthRequestScopedErrors) != 0 {
			t.Fatal("cleared OAuth rules reappeared from YAML merge")
		}
	}
}

func TestRequestScopedErrorRuleInvalidTypesRejectOptionalLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	bodies := []string{"oauth-request-scoped-errors: {codex: [{status: 400.5, match: [x], action: stop}]}\n", "oauth-request-scoped-errors: {codex: wrong}\n"}
	for _, family := range retryConfigFamilies {
		bodies = append(bodies, family+": [{request-scoped-errors: wrong}]\n")
	}
	for _, body := range bodies {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfigOptional(path, true); err == nil {
			t.Fatal("optional startup hid an invalid rule")
		}
	}
}
