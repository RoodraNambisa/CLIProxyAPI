package session

import (
	"fmt"
	"sync"
	"testing"
	"time"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func testHistory(texts ...string) History {
	json := `{"input":[`
	for index, text := range texts {
		if index > 0 {
			json += ","
		}
		json += fmt.Sprintf(`{"role":"user","content":%q}`, text)
	}
	return FingerprintHistory(sdktranslator.FormatCodex, []byte(json+`]}`))
}

func TestHistoryMatcherLongestPrefixAndAmbiguity(t *testing.T) {
	m := NewHistoryMatcher(time.Hour)
	first, branchA, branchB := testHistory("question"), testHistory("question", "branch-a"), testHistory("question", "branch-b")
	if m.Bind("caller/provider/model", first, "a") == 0 {
		t.Fatal("valid history was not bound")
	}
	if got, ok := m.Match("caller/provider/model", branchA); !ok || got.AuthID != "a" || got.PrefixLength != 1 {
		t.Fatal("continuation did not prefer its known prefix")
	}
	m.Bind("caller/provider/model", branchA, "a")
	m.Bind("caller/provider/model", branchB, "b")
	if _, ok := m.Match("caller/provider/model", testHistory("question", "new branch")); ok {
		t.Fatal("ambiguous credentials were chosen by recency")
	}
	if got, ok := m.Match("caller/provider/model", testHistory("question", "branch-b", "next")); !ok || got.AuthID != "b" || got.PrefixLength != 2 {
		t.Fatal("longer branch did not outrank the ambiguous root")
	}
	if _, ok := m.Match("another caller/provider/model", branchB); ok {
		t.Fatal("scope boundary was crossed")
	}
	m.InvalidateAuth("b")
	if got, ok := m.Match("caller/provider/model", branchB); !ok || got.AuthID != "a" {
		t.Fatal("retired preference survived invalidation")
	}
}

func TestHistoryMatcherBoundsTTLAndLRU(t *testing.T) {
	now := time.Unix(1000, 0)
	m := newHistoryMatcher(time.Minute, 2, 3, func() time.Time { return now })
	m.Bind("scope", testHistory("a"), "a")
	m.Bind("scope", testHistory("b"), "b")
	m.Match("scope", testHistory("a"))
	m.Bind("scope", testHistory("c"), "c")
	if _, ok := m.Match("scope", testHistory("b")); ok {
		t.Fatal("least-recent group survived capacity eviction")
	}
	m.Bind("scope", testHistory("a", "next", "last"), "a")
	if len(m.groups) != 1 || m.prefixCount != 3 || len(m.prefixes) != 3 {
		t.Fatal("prefix reference capacity was not enforced")
	}
	if m.Bind("scope", testHistory("too", "many", "history", "items"), "other") != 0 || len(m.groups) != 1 {
		t.Fatal("oversized binding evicted useful state")
	}
	now = now.Add(2 * time.Minute)
	if _, ok := m.Match("scope", testHistory("a", "next", "last")); ok {
		t.Fatal("expired history was still usable")
	}
	for range 128 {
		m.Match("scope", testHistory("unmatched"))
	}
	if len(m.groups) != 0 || len(m.prefixes) != 0 || m.lru.Len() != 0 || m.prefixCount != 0 {
		t.Fatal("expired index references were retained")
	}
}

func TestHistoryMatcherRejectsInvalidStateAndConcurrentChurn(t *testing.T) {
	var zero HistoryMatcher
	var absent *HistoryMatcher
	for _, m := range []*HistoryMatcher{absent, &zero, NewHistoryMatcher(0)} {
		if m.Bind("scope", History{}, "a") != 0 || m.Bind("", testHistory("user"), "a") != 0 {
			t.Fatal("invalid namespace or history was bound")
		}
		if _, ok := m.Match("scope", History{}); ok {
			t.Fatal("empty history matched")
		}
	}
	m := newHistoryMatcher(time.Minute, 8, 32, time.Now)
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Go(func() {
			for step := range 50 {
				namespace, auth := fmt.Sprint(worker), fmt.Sprint(worker%2)
				h := testHistory("question", fmt.Sprint(step))
				m.Bind(namespace, h, auth)
				m.Match(namespace, h)
				if step%7 == 0 {
					m.InvalidateAuth(auth)
				}
			}
		})
	}
	workers.Wait()
	if len(m.groups) > 8 || m.prefixCount > 32 || m.lru.Len() != len(m.groups) {
		t.Fatal("concurrent churn escaped index limits")
	}
}
