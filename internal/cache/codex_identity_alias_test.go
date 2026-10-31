package cache

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCodexAliasesBoundsAndExpiry(t *testing.T) {
	c := NewCodexIdentityAliases()
	now := time.Unix(100, 0)
	c.now = func() time.Time { return now }
	for i := 0; i < 110000; i++ {
		c.Put(CodexIdentityAlias{Scope: "a", Role: "conversation", Original: strings.Repeat("x", 700), Mapped: fmt.Sprint(i)})
	}
	count, size := 0, 0
	for i := range c.shards {
		count += len(c.shards[i].entries)
		size += c.shards[i].bytes
	}
	if count > CodexIdentityAliasLimit || size > CodexIdentityAliasBytes {
		t.Fatalf("unbounded: %d %d", count, size)
	}
	c.Put(CodexIdentityAlias{Scope: "a", Role: "turn", Original: strings.Repeat("x", 4096), Mapped: "too-big"})
	if _, ok := c.Get("turn", "too-big"); ok {
		t.Fatal("oversize cached")
	}
	now = now.Add(CodexIdentityAliasTTL)
	c.Prune()
	for i := range c.shards {
		if c.shards[i].entries != nil || c.shards[i].bytes != 0 {
			t.Fatal("expired references retained")
		}
	}
}
func TestCodexAliasConvergenceHasNoArbitraryInverse(t *testing.T) {
	c := NewCodexIdentityAliases()
	c.Put(CodexIdentityAlias{Scope: "a", Generation: "1", Role: "conversation", Original: "one", Mapped: "slot"})
	c.Put(CodexIdentityAlias{Scope: "a", Generation: "1", Role: "conversation", Original: "two", Mapped: "slot"})
	got, ok := c.Get("conversation", "slot")
	if !ok || !got.Converged || got.Original != "" {
		t.Fatalf("ambiguous inverse: %+v", got)
	}
}
