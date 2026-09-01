package config

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeErrorResponseRewritesPreservesOrderAndEmptyBody(t *testing.T) {
	const largeInteger int64 = 9007199254740993
	firstBody := map[string]any{
		"error": map[string]any{
			"message": "context too large",
			"code":    "context_too_large",
			"trace":   largeInteger,
		},
	}
	emptyBody := map[string]any{}
	rules, errNormalize := NormalizeErrorResponseRewrites([]ErrorResponseRewriteRule{
		{
			Sources:            []string{" ChatGPT-Web ", "codex", "CHATGPT-WEB"},
			AuthPriorities:     []int{0, -1, 0},
			StatusCode:         429,
			MessageContains:    "  Maximum Context  ",
			ResponseStatusCode: 400,
			ResponseBody:       &firstBody,
		},
		{
			MessageContains: "upstream unavailable",
			ResponseBody:    &emptyBody,
		},
	})
	if errNormalize != nil {
		t.Fatalf("NormalizeErrorResponseRewrites() error = %v", errNormalize)
	}
	if len(rules) != 2 {
		t.Fatalf("rules length = %d, want 2", len(rules))
	}
	if rules[0].MessageContains != "Maximum Context" || rules[0].ResponseStatusCode != 400 {
		t.Fatalf("first rule = %#v", rules[0])
	}
	if got := strings.Join(rules[0].Sources, ","); got != "chatgpt-web,codex" {
		t.Fatalf("normalized sources = %q", got)
	}
	if len(rules[0].AuthPriorities) != 2 || rules[0].AuthPriorities[0] != 0 || rules[0].AuthPriorities[1] != -1 {
		t.Fatalf("normalized auth priorities = %v", rules[0].AuthPriorities)
	}
	if rules[1].ResponseBody == nil || len(*rules[1].ResponseBody) != 0 {
		t.Fatalf("empty response body = %#v, want explicit empty object", rules[1].ResponseBody)
	}
	encoded, errMarshal := json.Marshal(*rules[0].ResponseBody)
	if errMarshal != nil || !strings.Contains(string(encoded), `"trace":9007199254740993`) {
		t.Fatalf("large integer changed: body=%s error=%v", encoded, errMarshal)
	}

	firstBody["mutated"] = true
	if _, exists := (*rules[0].ResponseBody)["mutated"]; exists {
		t.Fatal("normalized response body aliases input map")
	}
}

func TestNormalizeErrorResponseRewritesRejectsInvalidRules(t *testing.T) {
	validBody := map[string]any{"error": map[string]any{"message": "rewritten"}}
	invalidBody := map[string]any{"value": math.Inf(1)}
	testCases := []struct {
		name string
		rule ErrorResponseRewriteRule
		want string
	}{
		{name: "missing matcher", rule: ErrorResponseRewriteRule{ResponseStatusCode: 400}, want: "requires status-code or message-contains"},
		{name: "invalid match status", rule: ErrorResponseRewriteRule{StatusCode: 99, ResponseStatusCode: 400}, want: "status-code must be between"},
		{name: "missing projection", rule: ErrorResponseRewriteRule{StatusCode: 429}, want: "requires response-status-code or response-body"},
		{name: "invalid response status", rule: ErrorResponseRewriteRule{StatusCode: 429, ResponseStatusCode: 399}, want: "response-status-code must be between"},
		{name: "invalid response body", rule: ErrorResponseRewriteRule{StatusCode: 429, ResponseBody: &invalidBody}, want: "response-body must be a JSON object"},
		{name: "nil response body map", rule: ErrorResponseRewriteRule{StatusCode: 429, ResponseBody: new(map[string]any)}, want: "response-body must be a JSON object"},
		{name: "invalid source", rule: ErrorResponseRewriteRule{Sources: []string{"chatgpt web"}, StatusCode: 429, ResponseStatusCode: 400}, want: "contains invalid provider ID"},
		{name: "valid body guard", rule: ErrorResponseRewriteRule{StatusCode: 429, ResponseBody: &validBody}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, errNormalize := NormalizeErrorResponseRewrites([]ErrorResponseRewriteRule{testCase.rule})
			if testCase.want == "" {
				if errNormalize != nil {
					t.Fatalf("NormalizeErrorResponseRewrites() error = %v", errNormalize)
				}
				return
			}
			if errNormalize == nil || !strings.Contains(errNormalize.Error(), testCase.want) {
				t.Fatalf("NormalizeErrorResponseRewrites() error = %v, want %q", errNormalize, testCase.want)
			}
		})
	}
}

func TestErrorResponseRewriteBodyNumbersSurviveConfigSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`error-response-rewrites:
  - status-code: 429
    response-body:
      retry_after: 30
      trace: 9007199254740993
`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}
	reloaded, errReload := LoadConfig(path)
	if errReload != nil {
		t.Fatalf("reload config: %v", errReload)
	}
	body, errMarshal := json.Marshal(*reloaded.ErrorResponseRewrites[0].ResponseBody)
	if errMarshal != nil {
		t.Fatalf("marshal body: %v", errMarshal)
	}
	if !strings.Contains(string(body), `"retry_after":30`) || !strings.Contains(string(body), `"trace":9007199254740993`) {
		t.Fatalf("numeric values changed after save: %s", body)
	}
}

func TestLoadConfigPreservesExplicitEmptyErrorRewriteBody(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`error-response-rewrites:
  - message-contains: "upstream unavailable"
    response-body: {}
`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	if len(cfg.ErrorResponseRewrites) != 1 || cfg.ErrorResponseRewrites[0].ResponseBody == nil || len(*cfg.ErrorResponseRewrites[0].ResponseBody) != 0 {
		t.Fatalf("loaded rules = %#v, want explicit empty response body", cfg.ErrorResponseRewrites)
	}
}

func TestSaveConfigPreserveCommentsRemovesDeletedErrorResponseRewrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`# keep this comment
debug: true
error-response-rewrites:
  - message-contains: "upstream unavailable"
    response-status-code: 400
`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	cfg.ErrorResponseRewrites = nil
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}

	saved, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved config: %v", errRead)
	}
	if strings.Contains(string(saved), "error-response-rewrites:") {
		t.Fatalf("deleted error response rewrites survived save:\n%s", saved)
	}
	if !strings.Contains(string(saved), "# keep this comment") {
		t.Fatalf("unrelated comment was not preserved:\n%s", saved)
	}
	reloaded, errReload := LoadConfig(path)
	if errReload != nil {
		t.Fatalf("reload config: %v", errReload)
	}
	if len(reloaded.ErrorResponseRewrites) != 0 {
		t.Fatalf("reloaded rules = %#v, want none", reloaded.ErrorResponseRewrites)
	}
	if errSave := SaveConfigPreserveComments(path, reloaded); errSave != nil {
		t.Fatalf("second SaveConfigPreserveComments() error = %v", errSave)
	}
	savedAgain, errReadAgain := os.ReadFile(path)
	if errReadAgain != nil {
		t.Fatalf("read config after second save: %v", errReadAgain)
	}
	if strings.Contains(string(savedAgain), "error-response-rewrites:") {
		t.Fatalf("deleted error response rewrites returned after reload and save:\n%s", savedAgain)
	}
}

func TestSaveConfigPreserveCommentsReplacesErrorResponseRewriteFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`error-response-rewrites:
  - status-code: 429
    message-contains: "old matcher"
    response-status-code: 400
    response-body:
      error: old body
`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	cfg.ErrorResponseRewrites = []ErrorResponseRewriteRule{{
		Sources:            []string{"ChatGPT-Web", "custom-provider"},
		AuthPriorities:     []int{0, -1},
		MessageContains:    "new matcher",
		ResponseStatusCode: 503,
	}}
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}

	reloaded, errReload := LoadConfig(path)
	if errReload != nil {
		t.Fatalf("reload config: %v", errReload)
	}
	if len(reloaded.ErrorResponseRewrites) != 1 {
		t.Fatalf("reloaded rules = %#v, want one", reloaded.ErrorResponseRewrites)
	}
	rule := reloaded.ErrorResponseRewrites[0]
	if rule.StatusCode != 0 || rule.MessageContains != "new matcher" || rule.ResponseStatusCode != 503 || rule.ResponseBody != nil ||
		strings.Join(rule.Sources, ",") != "chatgpt-web,custom-provider" || len(rule.AuthPriorities) != 2 || rule.AuthPriorities[1] != -1 {
		t.Fatalf("reloaded rule retained deleted fields: %#v", rule)
	}
}
