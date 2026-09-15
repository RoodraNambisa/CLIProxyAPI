package auth

import (
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestAuthModelExclusionDefaultPriorityMatchesZero(t *testing.T) {
	for _, provider := range []string{"xai", "codex", "claude"} {
		for _, tc := range []struct {
			name     string
			attrs    map[string]string
			metadata map[string]any
			want     bool
		}{
			{name: "missing", want: true},
			{name: "unrelated metadata", metadata: map[string]any{"type": provider}, want: true},
			{name: "empty attribute", attrs: map[string]string{"priority": " "}, want: true},
			{name: "explicit zero", attrs: map[string]string{"priority": "0"}, want: true},
			{name: "matching metadata", metadata: map[string]any{"priority": float64(3)}, want: true},
			{name: "different priority", attrs: map[string]string{"priority": "4"}},
			{name: "invalid priority", attrs: map[string]string{"priority": "invalid"}},
		} {
			t.Run(provider+"/"+tc.name, func(t *testing.T) {
				auth := &Auth{Provider: provider, Attributes: tc.attrs, Metadata: tc.metadata}
				rule := internalconfig.AuthModelExclusionRule{Models: []string{"-all", "+gpt-5.6-sol", "+gpt-6-astra"}, Priorities: []int{0, 3}}
				if got := AuthModelExclusionRuleMatches(rule, auth, provider); got != tc.want {
					t.Fatalf("matched=%t, want %t", got, tc.want)
				}
				rule.Providers = []string{"codex"}
				if got := AuthModelExclusionRuleMatches(rule, auth, provider); got != (tc.want && provider == "codex") {
					t.Fatal("provider condition was not preserved")
				}
			})
		}
	}
}

func TestAuthModelExclusionDefaultPriorityDoesNotAddKeyword(t *testing.T) {
	auth := &Auth{ID: "grok-credential", Provider: "xai"}
	rule := internalconfig.AuthModelExclusionRule{Models: []string{"-all"}, Priorities: []int{0}, KeywordContains: []string{"0"}}
	if AuthModelExclusionRuleMatches(rule, auth, "xai") {
		t.Fatal("default priority must not invent a non-secret keyword field")
	}
}
