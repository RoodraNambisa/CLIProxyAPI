package auth

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRequestScopedErrorMetadataAliasesTypesAndDetachedRules(t *testing.T) {
	rules := []config.RequestScopedErrorRule{{Status: 400, Match: []string{"fixture"}, Action: "stop"}}
	for _, raw := range []any{rules, []any{map[string]any{"status": json.Number("400"), "match": []any{"fixture"}, "action": "stop"}}} {
		auth := &Auth{Metadata: map[string]any{"request-scoped-errors": raw}}
		compiled, err := compileAuthRequestScopedErrors(auth)
		if err != nil {
			t.Fatal(err)
		}
		if action, ok := compiled.Match(400, "fixture"); !ok || action != config.RequestScopedActionStop {
			t.Fatal("valid metadata did not match")
		}
		auth.Metadata["request_scoped_errors"] = nil
		compiled, err = compileAuthRequestScopedErrors(auth)
		if err != nil || compiled != nil {
			t.Fatal("explicit canonical null did not win")
		}
	}
	for _, raw := range []any{true, "rules", []any{map[string]any{"status": 400.5}}, []any{map[string]any{"status": 400, "match": []any{1}, "action": "stop"}}, []config.RequestScopedErrorRule{{Status: 400, MatchRegexr: []string{"private-pattern["}, Action: "stop"}}} {
		auth := &Auth{Metadata: map[string]any{"request_scoped_errors": rules, "request-scoped-errors": raw}}
		if err := ValidateAuthRequestScopedErrors(auth); err == nil || strings.Contains(err.Error(), "private-pattern") {
			t.Fatal("invalid shadowed alias accepted or pattern exposed")
		}
	}
	auth := &Auth{Metadata: map[string]any{"request_scoped_errors": rules}}
	compiled, err := compileAuthRequestScopedErrors(auth)
	if err != nil {
		t.Fatal(err)
	}
	rules[0].Match[0] = "changed"
	if action, ok := compiled.Match(400, "fixture"); !ok || action != config.RequestScopedActionStop {
		t.Fatal("compiled metadata changed with input")
	}
}

func TestRequestScopedErrorMetadataCloneAndRefreshPreserveUnknownFields(t *testing.T) {
	var raw any
	decoder := json.NewDecoder(strings.NewReader(`[{"status":400,"match":["fixture"],"action":"stop","future":{"large":9007199254740993}}]`))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		t.Fatal(err)
	}
	current := &Auth{Metadata: map[string]any{"request_scoped_errors": raw}}
	next := &Auth{Metadata: map[string]any{"request-scoped-errors": []any{}, "unrelated": "kept"}}
	carryForwardConfiguredRequestScopedErrors(current, next)
	if _, exists := next.Metadata["request-scoped-errors"]; exists || next.Metadata["unrelated"] != "kept" {
		t.Fatal("refresh learned absent alias or lost unrelated metadata")
	}
	cloned := next.Metadata["request_scoped_errors"].([]any)[0].(map[string]any)
	cloned["match"].([]any)[0] = "changed"
	original := current.Metadata["request_scoped_errors"].([]any)[0].(map[string]any)
	if original["match"].([]any)[0] != "fixture" || cloned["future"].(map[string]any)["large"] != json.Number("9007199254740993") {
		t.Fatal("clone shared mutable data or changed unknown fields")
	}
	if !requestScopedErrorConfigurationChanged(current, next) {
		t.Fatal("concurrent rule edit was missed")
	}
	typed := []config.RequestScopedErrorRule{{Match: []string{"literal"}, MatchRegexr: []string{"pattern"}}}
	copy := cloneRequestScopedRuleMetadata(typed).([]config.RequestScopedErrorRule)
	copy[0].Match[0], copy[0].MatchRegexr[0] = "new", "new"
	if typed[0].Match[0] != "literal" || typed[0].MatchRegexr[0] != "pattern" {
		t.Fatal("typed rules shared nested slices")
	}
}
