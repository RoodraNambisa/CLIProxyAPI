package auth

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestImageGenerationPriorityScope(t *testing.T) {
	for _, tc := range []struct {
		name, priority, provider, keyword string
		scope                             []int
		disabled                          bool
		want                              bool
	}{
		{"only five matches", "5", "codex", "", []int{5}, true, true},
		{"three keeps generation", "3", "codex", "", []int{5}, true, false},
		{"only three matches", "3", "codex", "", []int{3}, true, true},
		{"five keeps generation", "5", "codex", "", []int{3}, true, false},
		{"both", "3", "codex", "", []int{3, 5}, true, true},
		{"legacy missing", "3", "codex", "", nil, true, true},
		{"legacy empty", "5", "codex", "", []int{}, true, true},
		{"disabled", "5", "codex", "", []int{5}, false, false},
		{"scope cannot expand parent", "7", "codex", "", []int{7}, true, false},
		{"another provider", "5", "claude", "", []int{5}, true, false},
		{"keyword match", "5", "codex", "team", []int{5}, true, true},
		{"keyword mismatch", "5", "codex", "trial", []int{5}, true, false},
		{"invalid priority", "invalid", "codex", "", []int{0}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := config.AuthModelExclusionRule{
				Models: []string{"excluded"}, Providers: []string{"codex"}, Priorities: []int{3, 5},
				DisableImageGeneration: tc.disabled, DisableImageGenerationPriorities: tc.scope,
			}
			if tc.keyword != "" {
				rule.KeywordContains = []string{tc.keyword}
			}
			a := &Auth{Provider: tc.provider, Label: "TEAM", Attributes: map[string]string{"priority": tc.priority}}
			cfg := &config.Config{AuthModelExclusions: []config.AuthModelExclusionRule{rule}}
			if got := AuthDisablesImageGeneration(cfg, a, a.Provider); got != tc.want {
				t.Fatalf("disabled = %t, want %t", got, tc.want)
			}
			if tc.priority == "3" && !AuthModelExclusionRuleMatches(rule, a, a.Provider) {
				t.Fatal("image scope changed the shared model exclusions")
			}
		})
	}
}

func TestImageGenerationPriorityScopeDefaultAndAdditive(t *testing.T) {
	a := &Auth{Provider: "codex"}
	cfg := &config.Config{AuthModelExclusions: []config.AuthModelExclusionRule{{
		Providers: []string{"codex"}, DisableImageGeneration: true, DisableImageGenerationPriorities: []int{0},
	}}}
	if !AuthDisablesImageGeneration(cfg, a, "codex") {
		t.Fatal("omitted priority must match zero")
	}
	a.Attributes = map[string]string{"priority": "3"}
	cfg.AuthModelExclusions[0].DisableImageGenerationPriorities = []int{5}
	cfg.AuthModelExclusions = append(cfg.AuthModelExclusions, config.AuthModelExclusionRule{Priorities: []int{3}, DisableImageGeneration: true})
	if !AuthDisablesImageGeneration(cfg, a, "codex") {
		t.Fatal("a narrower rule must not override another restriction")
	}
}

func BenchmarkImageGenerationPriorityScope(b *testing.B) {
	a := &Auth{Provider: "codex", Attributes: map[string]string{"priority": "5"}}
	cfg := &config.Config{AuthModelExclusions: []config.AuthModelExclusionRule{{
		Providers: []string{"codex"}, Priorities: []int{3, 5}, DisableImageGeneration: true,
		DisableImageGenerationPriorities: []int{5},
	}}}
	b.ReportAllocs()
	for b.Loop() {
		if !AuthDisablesImageGeneration(cfg, a, "codex") {
			b.Fatal("scope not matched")
		}
	}
}
