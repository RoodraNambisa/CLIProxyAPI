package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var retryConfigFamilies = []string{"gemini-api-key", "interactions-api-key", "claude-api-key", "codex-api-key", "vertex-api-key", "openai-compatibility"}

func TestCredentialRequestRetryYAMLValidation(t *testing.T) {
	for _, family := range retryConfigFamilies {
		for _, tc := range []struct {
			name, field string
			valid       bool
		}{
			{"missing", "", true}, {"zero", "request-retry: 0", true}, {"negative", "request-retry: -2", true},
			{"maximum", "request-retry: 2147483647", true}, {"null", "request-retry: null", true},
			{"too large", "request-retry: 2147483648", false}, {"overflow", "request-retry: 9223372036854775808", false},
			{"fraction", "request-retry: 1.5", false}, {"float", "request-retry: 1.0", false}, {"string", "request-retry: '1'", false},
			{"boolean", "request-retry: true", false}, {"list", "request-retry: [1]", false}, {"duplicate", "request-retry: 1\nrequest-retry: 2", false},
		} {
			t.Run(family+"/"+tc.name, func(t *testing.T) {
				body := fmt.Sprintf("%s:\n  - api-key: key-material-marker\n    name: compat\n    base-url: https://example.test\n    %s\n", family, strings.ReplaceAll(tc.field, "\n", "\n    "))
				path := filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(path, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
				for _, optional := range []bool{false, true} {
					_, err := LoadConfigOptional(path, optional)
					if (err == nil) != tc.valid {
						t.Fatalf("valid=%t optional=%t error=%v", tc.valid, optional, err)
					}
					if err != nil && strings.Contains(err.Error(), "key-material-marker") {
						t.Fatal("invalid retry value exposed unrelated credential material")
					}
				}
			})
		}
	}
}

func TestCredentialRequestRetryYAMLAliasesAndMergePrecedence(t *testing.T) {
	for name, tc := range map[string]struct {
		body  string
		valid bool
	}{
		"scalar alias":   {"count: &count 1.5\ncodex-api-key: [{api-key: test, request-retry: *count}]", false},
		"sequence alias": {"keys: &keys [{api-key: test, request-retry: 1.5}]\ncodex-api-key: *keys", false},
		"inherited":      {"base: &base {request-retry: 1.5}\ncodex-api-key: [{<<: *base, api-key: test}]", false},
		"explicit wins":  {"base: &base {request-retry: 1.5}\ncodex-api-key: [{<<: *base, api-key: test, request-retry: 0}]", true},
		"null wins":      {"base: &base {request-retry: 1.5}\ncodex-api-key: [{<<: *base, api-key: test, request-retry: null}]", true},
		"root merge":     {"base: &base {codex-api-key: [{api-key: test, request-retry: 1.5}]}\n<<: *base", false},
		"compat alias":   {"entries: &entries [{name: compat, request-retry: 1.5}]\nopenai-compatibility: *entries", false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateCredentialRequestRetryYAML([]byte(tc.body)); (err == nil) != tc.valid {
				t.Fatalf("invalid alias handling: %v", err)
			}
		})
	}
}

func setAllConfigRetryOverrides(cfg *Config, retry *int) {
	cfg.GeminiKey[0].RequestRetry, cfg.InteractionsKey[0].RequestRetry = retry, retry
	cfg.ClaudeKey[0].RequestRetry, cfg.CodexKey[0].RequestRetry = retry, retry
	cfg.VertexCompatAPIKey[0].RequestRetry, cfg.OpenAICompatibility[0].RequestRetry = retry, retry
}

func TestCredentialRequestRetrySaveClearAndReload(t *testing.T) {
	var source strings.Builder
	source.WriteString("# preserved\nrequest-retry: 5\n")
	for _, family := range retryConfigFamilies {
		fmt.Fprintf(&source, "%s:\n  - api-key: test\n    name: compat\n    base-url: https://example.test\n    request-retry: 3\n    future-option: retained\n", family)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(source.String()), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	zero, negative := 0, -2
	for _, retry := range []*int{&negative, &zero, nil} {
		setAllConfigRetryOverrides(cfg, retry)
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		loaded, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, got := range []*int{loaded.GeminiKey[0].RequestRetry, loaded.InteractionsKey[0].RequestRetry, loaded.ClaudeKey[0].RequestRetry, loaded.CodexKey[0].RequestRetry, loaded.VertexCompatAPIKey[0].RequestRetry, loaded.OpenAICompatibility[0].RequestRetry} {
			if (retry == nil && got != nil) || (retry != nil && (got == nil || *got != *retry)) {
				t.Fatal("retry override lost zero, legacy negative, or inheritance")
			}
		}
		saved, err := os.ReadFile(path)
		if err != nil || strings.Count(string(saved), "future-option: retained") != 6 || !strings.Contains(string(saved), "# preserved") || loaded.RequestRetry != 5 {
			t.Fatalf("retry save changed unrelated settings: read-error=%v extensions=%d comment=%t global-retry=%d", err, strings.Count(string(saved), "future-option: retained"), strings.Contains(string(saved), "# preserved"), loaded.RequestRetry)
		}
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	large := int64(MaxCredentialRequestRetry) + 1
	if int64(int(large)) != large {
		return
	}
	invalid := int(large)
	setAllConfigRetryOverrides(cfg, &invalid)
	if err := SaveConfigPreserveComments(path, cfg); err == nil {
		t.Fatal("invalid retry override was persisted")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(before) {
		t.Fatal("invalid retry override modified the saved file")
	}
}

func TestCredentialRequestRetryClearDoesNotReinheritYAMLValue(t *testing.T) {
	for _, family := range []string{"codex-api-key", "openai-compatibility"} {
		t.Run(family, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			body := "base: &base {api-key: test, name: compat, base-url: 'https://example.test', request-retry: 4, future-option: kept}\n" + family + ": [*base]\n"
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			if family == "codex-api-key" {
				cfg.CodexKey[0].RequestRetry = nil
			} else {
				cfg.OpenAICompatibility[0].RequestRetry = nil
			}
			if err := SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatal(err)
			}
			loaded, err := LoadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			var got *int
			if family == "codex-api-key" {
				got = loaded.CodexKey[0].RequestRetry
			} else {
				got = loaded.OpenAICompatibility[0].RequestRetry
			}
			if got != nil {
				t.Fatal("cleared retry override reappeared from its YAML alias")
			}
		})
	}
}

func TestCredentialYAMLRejectsDuplicateKnownFieldsBeforeOptionalFallback(t *testing.T) {
	for _, body := range []string{
		"codex-api-key: [{api-key: test, base-url: 'https://example.test', weight: 1, weight: 2}]\n",
		"codex-api-key: []\ncodex-api-key: [{api-key: test, base-url: 'https://example.test'}]\n",
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfigOptional(path, true); err == nil {
			t.Fatal("duplicate credential fields silently loaded an empty config")
		}
	}
}
