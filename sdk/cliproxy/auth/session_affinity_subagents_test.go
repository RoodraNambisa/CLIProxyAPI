package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func subagentOptions(id, parent string, fork bool) core.Options {
	opts := core.Options{Headers: http.Header{"Session-Id": {id}}}
	if fork {
		opts.OriginalRequest = []byte(fmt.Sprintf(`{"forked_from_id":%q}`, parent))
	} else if parent != "" {
		opts.Headers.Set("X-Codex-Parent-Thread-Id", parent)
	}
	return opts
}

func TestSubagentAffinityExplicitInheritanceAndOwnBinding(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, fork := range []bool{false, true} {
			t.Run(fmt.Sprintf("enabled=%t/fork=%t", enabled, fork), func(t *testing.T) {
				s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Subagents: enabled})
				t.Cleanup(s.Stop)
				ctx := affinityCallerContext(t, "caller-a", "codex")
				parent, child := subagentOptions("root", "", false), subagentOptions("child", "root", fork)
				auths := []*Auth{{ID: "a", Provider: "codex"}, {ID: "b", Provider: "codex"}}
				s.BindSession(ctx, "codex", "gpt-5.4", parent, "b")
				picked, err := s.Pick(ctx, "codex", "gpt-5.4", child, auths)
				want := "a"
				if enabled {
					want = "b"
				}
				if err != nil || picked.ID != want {
					t.Fatalf("child pick=%v, %v; want %s", picked, err, want)
				}
				s.BindSession(ctx, "codex", "gpt-5.4", child, "a")
				picked, err = s.Pick(ctx, "codex", "gpt-5.4", child, auths)
				if err != nil || picked.ID != "a" {
					t.Fatal("parent replaced an established child binding")
				}
			})
		}
	}
}

func TestSubagentAffinityIsolationAndOptionalPriority(t *testing.T) {
	for _, boundary := range []string{"caller", "authorization", "provider", "model", "anonymous", "priority", "unavailable-parent"} {
		t.Run(boundary, func(t *testing.T) {
			strict := false
			s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Subagents: true, Failover: &strict})
			t.Cleanup(s.Stop)
			ctx := affinityCallerContext(t, "caller-a", "codex")
			s.BindSession(ctx, "codex", "gpt-5.4", subagentOptions("root", "", false), "b")
			childCtx := ctx
			provider, model := "codex", "gpt-5.4"
			auths := []*Auth{{ID: "a", Provider: "codex"}, {ID: "b", Provider: "codex"}}
			switch boundary {
			case "caller":
				childCtx = affinityCallerContext(t, "caller-b", "codex")
			case "authorization":
				childCtx = affinityCallerContext(t, "caller-a", "codex,xai")
			case "provider":
				provider = "xai"
			case "model":
				model = "gpt-5.5"
			case "anonymous":
				childCtx = t.Context()
			case "priority":
				auths[0].Attributes = map[string]string{"priority": "10"}
			case "unavailable-parent":
				auths[1].Disabled = true
			}
			picked, err := s.Pick(childCtx, provider, model, subagentOptions("child", "root", false), auths)
			if err != nil || picked.ID != "a" {
				t.Fatalf("inheritance crossed %s: %v, %v", boundary, picked, err)
			}
		})
	}
	for _, across := range []bool{false, true} {
		s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Subagents: true, AcrossPriorities: across})
		t.Cleanup(s.Stop)
		ctx := affinityCallerContext(t, "caller-a", "codex")
		s.BindSession(ctx, "codex", "", subagentOptions("root", "", false), "b")
		picked, err := s.Pick(ctx, "codex", "", subagentOptions("child", "root", false), []*Auth{{ID: "a", Attributes: map[string]string{"priority": "10"}}, {ID: "b"}})
		want := "a"
		if across {
			want = "b"
		}
		if err != nil || picked.ID != want {
			t.Fatal("cross-priority option was bypassed")
		}
	}
}

func TestSubagentAffinitySnapshotRetainsOrdinaryHistoryAndRollback(t *testing.T) {
	s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Subagents: true})
	t.Cleanup(s.Stop)
	ctx := affinityCallerContext(t, "caller-a", "codex")
	opts := core.Options{Headers: make(http.Header), OriginalRequest: []byte(`{"messages":[{"role":"user","content":"ordinary history"}]}`)}
	primary, fallback := extractMessageHashIDs(opts.OriginalRequest)
	frozen := withAffinityIdentity(ctx, core.Request{}, opts)
	captured := captureAffinityIdentity(ctx, core.Request{}, frozen)
	clear(opts.OriginalRequest)
	frozen.OriginalRequest = nil
	gotPrimary, gotFallback := s.sessionIDs(ctx, frozen)
	if gotPrimary != scopedAffinityID(captured.scope, primary) || gotFallback != scopedAffinityID(captured.scope, fallback) || gotPrimary == "" {
		t.Fatal("ordinary history or released snapshot changed")
	}
	s.BindSession(ctx, "codex", "", frozen, "a")
	rollback := s.BindSessionWithRollback(ctx, "codex", "", frozen, "b")
	rollback()
	if got := s.cachedAuthID("codex", "", frozen, context.Background()); got != "a" {
		t.Fatal("rollback failed to restore scoped binding")
	}
}

func TestAffinityCanceledPickCannotInheritOrBind(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Subagents: enabled})
		t.Cleanup(s.Stop)
		ctx := affinityCallerContext(t, "caller-a", "codex")
		s.BindSession(ctx, "codex", "", subagentOptions("root", "", false), "b")
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		child := subagentOptions("child", "root", false)
		picked, err := s.Pick(canceled, "codex", "", child, []*Auth{{ID: "a"}, {ID: "b"}})
		if picked != nil || !errors.Is(err, context.Canceled) {
			t.Fatal("canceled direct selection inherited or created a binding")
		}
		_, bind, err := s.pickWithPreparedFallbackDeferredBinding(canceled, "codex", "", child, []*Auth{{ID: "a"}, {ID: "b"}}, func() (*Auth, error) { t.Fatal("canceled fallback invoked"); return nil, nil })
		if bind != nil || !errors.Is(err, context.Canceled) {
			t.Fatal("canceled prepared selection was accepted")
		}
	}
}
