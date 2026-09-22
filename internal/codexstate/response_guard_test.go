package codexstate

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestResponseGuardAcceptanceIsSharedWithStateAndCookie(t *testing.T) {
	_, c, state := fixture()
	state.Enabled = true
	state.InvalidateOnModelMismatch = true
	state.InvalidateOnStateLengthMismatch = true
	cfg := config.CodexConfig{StateOverride: state, ResponseGuard: config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("enforce"), AllowedReturnedModels: new([]string{"accepted-alternative"}), LengthMode: new("allow"), Lengths: new([]int{312})}}}
	for _, strategy := range []string{"state", "cookie-only"} {
		t.Run(strategy, func(t *testing.T) {
			cfg.StateOverride.Strategy = strategy
			base := cfg.ManagedStateConfig()
			p, _, ok := base.PolicyFor(c.Scope())
			if !ok {
				t.Fatal("missing resource policy")
			}
			r := cookieResult(time.Now(), "candidate")
			r.State = strings.Repeat("x", 312)
			r.Model = "accepted-alternative"
			if reason := ValidateAcquisition(p, c.Model, r, time.Now()); reason != "" {
				t.Fatalf("accepted response rejected during acquisition: %s", reason)
			}
			if strategy == "cookie-only" {
				m := New()
				m.Sync(base, []Credential{c})
				installCookie(m, c, r, time.Now())
				used, _, _ := m.PickCookie(c, r.Cookies.Origin, time.Now(), p)
				if used.Header == "" {
					t.Fatal("candidate did not activate")
				}
				h := http.Header{"X-Codex-Turn-State": {r.State}}
				if m.ObserveCookie(c, used, p, h, r.Model, true) {
					t.Fatal("accepted alternative invalidated Cookie")
				}
				if !m.ObserveCookieEvidence(c, used, p, h, "unaccepted", false, true) {
					t.Fatal("definite early mismatch did not invalidate Cookie")
				}
				if m.CookieSnapshot(c.ID, time.Now()).Main != nil {
					t.Fatal("intercepted resource remained active")
				}
			}
			p.ReturnedLengthMode = "deny"
			p.Lengths = []int{292}
			if reason := ValidateAcquisition(p, c.Model, r, time.Now()); reason != "" {
				t.Fatalf("deny list changed acceptance: %s", reason)
			}
			r.Model = c.Model
			r.State = strings.Repeat("x", 292)
			if reason := ValidateAcquisition(p, c.Model, r, time.Now()); reason != "state_length_mismatch" {
				t.Fatalf("deny list ignored: %s", reason)
			}
		})
	}
}
