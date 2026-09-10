package auth

import (
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestAffinityResultSnapshotProtectsDelayedSelectionCommit(t *testing.T) {
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	original := core.Options{Headers: http.Header{"Session-Id": {"delayed-selection"}}, Metadata: map[string]any{"retained": "value"}}
	opts := manager.withSessionAffinityResultSnapshot(t.Context(), []string{"codex", "xai"}, "model(high)", original)
	snapshot := sessionBindingSnapshotFromOptions(opts, selector.cache)
	if snapshot == nil || snapshot.provider != "mixed" || snapshot.model != "model" {
		t.Fatal("incorrect selection namespace")
	}
	if len(original.Metadata) != 1 || opts.Metadata["retained"] != "value" {
		t.Fatal("caller metadata changed")
	}
	commit := selector.deferSelectionBinding(snapshot.key, "old", opts)
	selector.cache.Set(snapshot.key, "new")
	commit()
	rollback := selector.BindSessionWithRollback(t.Context(), "mixed", "model(low)", opts, "old")
	rollback()
	if got, ok := selector.cache.Get(snapshot.key); !ok || got != "new" {
		t.Fatal("old selection or result replaced new binding")
	}
}

func TestAffinityResultSnapshotTracksOwnCommitAndConditionalRollback(t *testing.T) {
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	opts := manager.withSessionAffinityResultSnapshot(t.Context(), []string{"codex"}, "model", core.Options{OriginalRequest: []byte(`{"messages":[{"role":"user","content":"test"}]}`)})
	snapshot := sessionBindingSnapshotFromOptions(opts, selector.cache)
	if snapshot == nil {
		t.Fatal("legacy history identity missing")
	}
	selector.deferSelectionBinding(snapshot.key, "selected", opts)()
	opts.OriginalRequest = nil
	rollback := selector.BindSessionWithRollback(t.Context(), "codex", "model", opts, "selected")
	if got, ok := selector.cache.Get(snapshot.key); !ok || got != "selected" {
		t.Fatal("own selection or released body lost binding")
	}
	selector.cache.Set(snapshot.key, "replacement")
	rollback()
	if got, _ := selector.cache.Get(snapshot.key); got != "replacement" {
		t.Fatal("rollback removed replacement")
	}
}

func TestAffinityResultSnapshotDoesNotEnableHistoryInference(t *testing.T) {
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &RoundRobinSelector{}, LCP: true})
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityLCP: true}})
	ctx := affinityCallerContext(t, "caller-a", "codex")
	opts := core.Options{Headers: make(http.Header), OriginalRequest: []byte(`{"input":[{"role":"user","content":"question"}]}`)}
	if updated := manager.withSessionAffinityResultSnapshot(ctx, []string{"codex"}, "model", opts); updated.Metadata != nil {
		t.Fatal("result snapshot created an implicit session binding in LCP mode")
	}
}

func TestAffinityResultSnapshotAllowsRebindAfterSameCredentialRenewal(t *testing.T) {
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	opts := core.Options{Headers: http.Header{"Session-Id": {"same-auth-renewal"}}}
	selector.BindSession(t.Context(), "codex", "model", opts, "a")
	first := manager.withSessionAffinityResultSnapshot(t.Context(), []string{"codex"}, "model", opts)
	second := manager.withSessionAffinityResultSnapshot(t.Context(), []string{"codex"}, "model", opts)
	selector.BindSession(t.Context(), "codex", "model", first, "a")
	selector.BindSession(t.Context(), "codex", "model", second, "b")
	if got := selector.cachedAuthID("codex", "model", opts); got != "b" {
		t.Fatal("same-credential TTL renewal blocked valid failover")
	}
	selector.BindSession(t.Context(), "codex", "model", opts, "a")
	selector.BindSession(t.Context(), "codex", "model", second, "b")
	if got := selector.cachedAuthID("codex", "model", opts); got != "a" {
		t.Fatal("old result ignored an intervening rebind")
	}
}
