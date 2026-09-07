package session

import (
	"sync"
	"testing"
	"time"
)

func TestHistoryMatcherRollbackPreservesNewerBindings(t *testing.T) {
	m := NewHistoryMatcher(time.Hour)
	h := testHistory("question")
	m.Bind("scope", h, "original")
	rollback := m.BindWithRollback("scope", h, "temporary")
	rollback()
	rollback()
	if got, ok := m.Match("scope", h); !ok || got.AuthID != "original" {
		t.Fatal("rollback did not restore the valid prior preference")
	}
	rollback = m.BindWithRollback("scope", h, "temporary")
	m.Bind("scope", h, "newer")
	rollback()
	if got, ok := m.Match("scope", h); !ok || got.AuthID != "newer" {
		t.Fatal("old cleanup removed a newer preference")
	}
	rollback = m.BindWithRollback("other", h, "temporary")
	rollback()
	if _, ok := m.Match("other", h); ok {
		t.Fatal("a rolled-back first binding remained")
	}
	if m.BindWithRollback("scope", History{}, "invalid") != nil {
		t.Fatal("ineligible history returned a rollback")
	}
}

func TestHistoryMatcherNestedRollbackCannotRestoreAnAlreadyCanceledBinding(t *testing.T) {
	m := NewHistoryMatcher(time.Hour)
	h := testHistory("question")
	m.Bind("scope", h, "original")
	oldRollback := m.BindWithRollback("scope", h, "canceled")
	newRollback := m.BindWithRollback("scope", h, "newer")
	oldRollback()
	if got, ok := m.Match("scope", h); !ok || got.AuthID != "newer" {
		t.Fatal("older cleanup removed the newer preference")
	}
	newRollback()
	if got, ok := m.Match("scope", h); ok && got.AuthID == "canceled" {
		t.Fatal("nested rollback resurrected an already canceled preference")
	}
}

func TestHistoryMatcherRollbackDoesNotResurrectInvalidatedOrExpiredState(t *testing.T) {
	for _, expired := range []bool{false, true} {
		now := time.Unix(1000, 0)
		m := newHistoryMatcher(time.Minute, 2, 4, func() time.Time { return now })
		h := testHistory("question")
		m.Bind("scope", h, "original")
		now = now.Add(30 * time.Second)
		rollback := m.BindWithRollback("scope", h, "temporary")
		if expired {
			now = now.Add(31 * time.Second)
		} else {
			m.InvalidateAuth("original")
		}
		rollback()
		if _, ok := m.Match("scope", h); ok || len(m.groups) != 0 || m.prefixCount != 0 {
			t.Fatal("expired or retired prior state was resurrected")
		}
	}
}

func TestHistoryMatcherConcurrentRollbackAndEvictionStayBounded(t *testing.T) {
	m := newHistoryMatcher(time.Minute, 4, 8, time.Now)
	h := testHistory("question", "next")
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Go(func() {
			for step := range 30 {
				rollback := m.BindWithRollback(string(rune('a'+worker)), h, "credential")
				if step%5 == 0 {
					m.InvalidateAuth("credential")
				}
				rollback()
				m.Match(string(rune('a'+worker)), h)
			}
		})
	}
	workers.Wait()
	if len(m.groups) > 4 || m.prefixCount > 8 || m.lru.Len() != len(m.groups) {
		t.Fatal("rollback or eviction corrupted index capacity")
	}
}
