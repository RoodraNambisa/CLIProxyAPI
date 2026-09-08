package auth

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func lcpOptions(branch string, continuation bool) core.Options {
	body := fmt.Sprintf(`{"input":[{"role":"user","content":"root question"},{"role":"assistant","content":"root answer"},{"role":"user","content":%q}]}`, branch)
	if continuation {
		body = body[:len(body)-2] + `,{"role":"assistant","content":"branch answer"},{"role":"user","content":"next question"}]}`
	}
	return core.Options{Headers: make(http.Header), SourceFormat: sdktranslator.FormatCodex, OriginalRequest: []byte(body)}
}

func TestLCPAffinityDistinguishesBranchesBeyondLegacyMessageHash(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, LCP: enabled})
		t.Cleanup(s.Stop)
		ctx := affinityCallerContext(t, "caller-a", "codex")
		s.BindSession(ctx, "codex", "gpt-5.4", lcpOptions("branch-a", false), "a")
		s.BindSession(ctx, "codex", "gpt-5.4", lcpOptions("branch-b", false), "b")
		picked, err := s.Pick(ctx, "codex", "gpt-5.4", lcpOptions("branch-a", true), []*Auth{{ID: "a"}, {ID: "b"}})
		want := "b"
		if enabled {
			want = "a"
		}
		if err != nil || picked.ID != want {
			t.Fatalf("LCP=%t selected %v, %v; want %s", enabled, picked, err, want)
		}
		if !enabled && s.historyMatcher != nil {
			t.Fatal("disabled mode allocated a history index")
		}
	}
}

func TestLCPAffinityScopesExplicitIDsAndBindingLifecycle(t *testing.T) {
	s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, LCP: true})
	t.Cleanup(s.Stop)
	ctx := affinityCallerContext(t, "caller-a", "codex")
	opts := lcpOptions("branch", false)
	s.BindSession(ctx, "codex", "gpt-5.4", opts, "b")
	for _, boundary := range []struct {
		ctx             context.Context
		provider, model string
	}{
		{affinityCallerContext(t, "caller-b", "codex"), "codex", "gpt-5.4"},
		{affinityCallerContext(t, "caller-a", "codex,xai"), "codex", "gpt-5.4"},
		{ctx, "xai", "gpt-5.4"}, {ctx, "codex", "gpt-5.5"},
	} {
		picked, err := s.Pick(boundary.ctx, boundary.provider, boundary.model, opts, []*Auth{{ID: "a"}, {ID: "b"}})
		if err != nil || picked.ID != "a" {
			t.Fatal("history inference crossed caller, authorization, provider or model")
		}
	}
	if s.cachedStrictAuthID(ctx, "codex", "gpt-5.4", opts) != "" {
		t.Fatal("inferred preference became a strict identity lock")
	}
	explicit := lcpOptions("branch", false)
	explicit.Headers.Set("Session-Id", "separate-session")
	if preferred := s.historyPreferredAuth(ctx, "codex", "gpt-5.4", explicit); preferred != "" {
		t.Fatal("history replaced explicit session identity")
	}
	rollback := s.BindSessionWithRollback(ctx, "codex", "gpt-5.4", opts, "a")
	rollback()
	if preferred := s.historyPreferredAuth(ctx, "codex", "gpt-5.4(high)", opts); preferred != "b" {
		t.Fatal("rollback or reasoning-model canonicalization lost the preference")
	}
	s.InvalidateAuth("b")
	if preferred := s.historyPreferredAuth(ctx, "codex", "gpt-5.4", opts); preferred != "" {
		t.Fatal("retired history preference remained")
	}
	// Selection alone must not publish a successful history or survive as a binding.
	s.Pick(ctx, "codex", "gpt-5.4", opts, []*Auth{{ID: "a"}})
	if preferred := s.historyPreferredAuth(ctx, "codex", "gpt-5.4", opts); preferred != "" {
		t.Fatal("selection committed history before a successful response")
	}
}

func TestLCPAffinityRespectsPriorityAndPreparedCandidates(t *testing.T) {
	for _, across := range []bool{false, true} {
		s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, LCP: true, AcrossPriorities: across})
		t.Cleanup(s.Stop)
		ctx := affinityCallerContext(t, "caller-a", "codex")
		opts := lcpOptions("branch", false)
		s.BindSession(ctx, "codex", "", opts, "b")
		auths := []*Auth{{ID: "a", Attributes: map[string]string{"priority": "10"}}, {ID: "b"}}
		picked, err := s.Pick(ctx, "codex", "", opts, auths)
		want := "a"
		if across {
			want = "b"
		}
		if err != nil || picked.ID != want {
			t.Fatal("history preference bypassed priority policy")
		}
		picked, bind, err := s.pickWithPreparedFallbackDeferredBinding(ctx, "codex", "", opts, auths[:1], func() (*Auth, error) { return auths[0], nil })
		if err != nil || picked.ID != "a" || bind != nil {
			t.Fatal("history bypassed prepared availability or committed a selection")
		}
	}
}
