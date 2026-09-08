package config

import (
	"strings"
	"sync"
	"testing"
)

func TestRequestScopedErrorsOrderAlternativesAndSnapshot(t *testing.T) {
	source := []RequestScopedErrorRule{
		{Status: 400, Match: []string{"exact phrase"}, MatchRegexr: []string{`^prefix [0-9]+$`}, Action: " STOP "},
		{Status: 400, Match: []string{"phrase", "prefix"}, Action: "continue-and-cooldown"},
		{Status: 429, Match: []string{"rate"}, Action: "stop-and-cooldown"},
		{Status: 500, Match: []string{"temporary"}, Action: "continue"},
	}
	rules, err := CompileRequestScopedErrors(source)
	if err != nil {
		t.Fatal(err)
	}
	source[0].Match[0], source[0].MatchRegexr[0], source[0].Action = "changed", "changed", "continue"
	for _, tc := range []struct {
		status int
		body   string
		action RequestScopedErrorAction
	}{
		{400, "exact phrase", RequestScopedActionStop},
		{400, "prefix 123", RequestScopedActionStop},
		{400, "a phrase", RequestScopedActionContinueAndCooldown},
		{429, "rate", RequestScopedActionStopAndCooldown},
		{500, "temporary", RequestScopedActionContinue},
		{401, "exact phrase", ""}, {400, "EXACT PHRASE", ""}, {400, "", ""},
	} {
		action, matched := rules.Match(tc.status, tc.body)
		if action != tc.action || matched != (tc.action != "") {
			t.Fatalf("status %d: %s %t, want %s", tc.status, action, matched, tc.action)
		}
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				if action, ok := rules.Match(400, "prefix 123"); !ok || action != RequestScopedActionStop {
					t.Error("concurrent snapshot changed")
				}
			}
		})
	}
	wg.Wait()
}

func TestRequestScopedErrorsValidationAndEmptyDefaults(t *testing.T) {
	for _, source := range [][]RequestScopedErrorRule{nil, {}, {{}}, {{Match: []string{""}, MatchRegexr: []string{""}}}} {
		rules, err := CompileRequestScopedErrors(source)
		if err != nil || rules != nil {
			t.Fatal("empty rules enabled a policy")
		}
		if _, matched := rules.Match(400, "anything"); matched {
			t.Fatal("nil rules matched")
		}
	}
	for _, rule := range []RequestScopedErrorRule{
		{Status: -1, Match: []string{"x"}, Action: "stop"},
		{Status: 600, Match: []string{"x"}, Action: "stop"},
		{Status: 400, Match: []string{"x"}, Action: "invalid"},
		{Status: 400, Action: "stop"},
		{Status: 400, MatchRegexr: []string{"private-pattern["}, Action: "stop"},
	} {
		if rules, err := CompileRequestScopedErrors([]RequestScopedErrorRule{rule}); err == nil || rules != nil || strings.Contains(err.Error(), "private-pattern") {
			t.Fatal("invalid rules accepted or private pattern exposed")
		}
	}
	rules, err := CompileRequestScopedErrors([]RequestScopedErrorRule{{Status: 400, Match: []string{" x "}, Action: "stop"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rules.Match(400, "x"); ok {
		t.Fatal("literal whitespace was silently removed")
	}
	if _, ok := rules.Match(400, " x "); !ok {
		t.Fatal("literal whitespace was lost")
	}
	_, err = CompileRequestScopedErrors([]RequestScopedErrorRule{{Status: 400, MatchRegexr: []string{"", "["}, Action: "stop"}})
	if err == nil || !strings.Contains(err.Error(), "match-regexr[1]") {
		t.Fatal("validation lost the original pattern position")
	}
}
