package auth

import (
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestResponseGuardAffinityScopesAndLateRebind(t *testing.T) {
	for _, mode := range []string{"none", "session", "credential"} {
		for _, rebind := range []bool{false, true} {
			t.Run(mode+map[bool]string{true: "/rebound", false: "/original"}[rebind], func(t *testing.T) {
				s := NewSessionAffinitySelector(&RoundRobinSelector{})
				t.Cleanup(s.Stop)
				m := NewManager(nil, s, nil)
				a, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "a", Provider: "codex"})
				if err != nil {
					t.Fatal(err)
				}
				opts := core.Options{Headers: http.Header{"Session-Id": {"current"}}}
				other := core.Options{Headers: http.Header{"Session-Id": {"other"}}}
				s.BindSession(t.Context(), "codex", "model", opts, a.ID)
				s.BindSession(t.Context(), "codex", "other-model", opts, a.ID)
				s.BindSession(t.Context(), "codex", "model", other, a.ID)
				opts = m.withSessionAffinityResultSnapshot(t.Context(), []string{"codex"}, "model", opts)
				clear := m.captureResponseGuardAffinity(t.Context(), a, "model", opts, mode)
				if rebind {
					s.BindSession(t.Context(), "codex", "model", core.Options{Headers: opts.Headers}, "replacement")
					s.BindSession(t.Context(), "codex", "model", core.Options{Headers: opts.Headers}, a.ID)
				}
				if clear != nil {
					clear()
				}
				current := s.cachedAuthID("codex", "model", opts)
				if (current == a.ID) != (mode == "none" || rebind) {
					t.Fatalf("current binding=%q mode=%s rebind=%v", current, mode, rebind)
				}
				for _, got := range []string{s.cachedAuthID("codex", "other-model", opts), s.cachedAuthID("codex", "model", other)} {
					if (got == a.ID) != (mode != "credential") {
						t.Fatalf("wrong removal scope: %q", got)
					}
				}
			})
		}
	}
}

func TestResponseGuardHistoryIsBoundedAndKeepsRewriteCounter(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetConfig(&config.Config{Codex: config.CodexConfig{ResponseGuard: config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("observe")}}}})
	a, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "a", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := core.WithResponseGuardRequest(t.Context())
	var oldest *core.ResponseGuardAttempt
	for i := 0; i < 30; i++ {
		opts := m.withResponseGuardAttempt(ctx, a, core.Request{Model: "m"}, core.Options{})
		attempt := opts.ResponseGuard
		if i == 0 {
			oldest = attempt
		}
		attempt.Start(config.CodexResponseGuardPolicy{}, "m", true, "sse", 200)
		attempt.Update(func(r *core.ResponseGuardRecord) {
			r.Outcome = "observed"
			r.Verdict = config.CodexResponseVerdict{Reasons: []string{"model_mismatch"}}
		})
		attempt.SetClientModel("public", true, 1)
	}
	oldest.Update(func(r *core.ResponseGuardRecord) { r.Outcome = "aborted" })
	stats := m.AuthResponseModelRewriteSummary(a, true)
	if len(stats.Recent) != 20 || stats.Total != 30 || stats.Blocked != 1 || stats.Observed != 29 {
		t.Fatalf("unbounded or duplicate counters: %+v", stats)
	}
	if stats.Recent[0].Validation.Attempt != 30 {
		t.Fatal("late evicted attempt replaced current history")
	}
}
