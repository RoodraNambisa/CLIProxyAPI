package auth

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCodexQuotaAutoDisableCreditsSecondaryCondition(t *testing.T) {
	for _, source := range []string{"http", "websocket"} {
		for _, tc := range []struct {
			name, used, has, unlimited, balance string
			minimum                             *float64
			disable                             bool
		}{
			{"sufficient", "99", "true", "false", "62500", quotaThreshold(1000), false},
			{"below", "99", "true", "false", "999.5", quotaThreshold(1000), true},
			{"equal", "99", "true", "false", "1000", quotaThreshold(1000), false},
			{"fraction equal", "99", "true", "false", "0.1", quotaThreshold(0.1), false},
			{"unlimited", "99", "false", "true", "0", quotaThreshold(1000), false},
			{"no usable credits", "99", "false", "false", "62500", nil, true},
			{"zero balance", "99", "true", "false", "0", nil, true},
			{"zero threshold exhausted", "99", "", "", "0", quotaThreshold(0), true},
			{"zero threshold available", "99", "true", "false", "1", quotaThreshold(0), false},
			{"healthy quota exhausted credits", "10", "false", "false", "0", quotaThreshold(1000), false},
			{"healthy quota below credits threshold", "10", "true", "false", "1", quotaThreshold(1000), false},
			{"quota equal", "90", "false", "false", "0", nil, false},
			{"missing credits", "99", "", "", "", quotaThreshold(1000), false},
			{"missing balance", "99", "true", "false", "", quotaThreshold(1000), false},
			{"available flag only", "99", "true", "", "", nil, false},
			{"unavailable flag only", "99", "false", "", "", nil, true},
			{"invalid balance", "99", "true", "false", "invalid", quotaThreshold(1000), false},
			{"nan", "99", "", "", "NaN", quotaThreshold(1000), false},
			{"infinite", "99", "", "", "+Inf", quotaThreshold(1000), false},
			{"negative", "99", "", "", "-1", quotaThreshold(1000), false},
			{"balance only below", "99", "", "", "20", quotaThreshold(1000), true},
		} {
			t.Run(source+"/"+tc.name, func(t *testing.T) {
				cfg := quotaDisableConfig()
				cfg.Codex.QuotaAutoDisable.Rules[0].Credits = config.CodexQuotaAutoDisableCredits{Enabled: true, MinimumBalance: tc.minimum}
				m := NewManager(nil, nil, nil)
				m.SetConfig(cfg)
				a, _ := m.Register(t.Context(), &Auth{ID: t.Name(), Provider: "codex"})
				headers := quotaHeaders("premium", "primary", "10080", tc.used)
				headers.Set("x-codex-credits-has-credits", tc.has)
				headers.Set("x-codex-credits-unlimited", tc.unlimited)
				headers.Set("x-codex-credits-balance", tc.balance)
				core.CodexQuotaObserverFromContext(m.withCodexQuotaObservation(t.Context()))(a.ID, a.RuntimeInstanceID(), source, headers)
				current, _ := m.GetByID(a.ID)
				if current.Disabled != tc.disable {
					t.Fatalf("disabled=%v, want %v", current.Disabled, tc.disable)
				}
				if tc.disable && !strings.Contains(CodexQuotaAutoDisableReason(current), "credits") {
					t.Fatal("disable reason omitted the credit condition")
				}
			})
		}
	}
}

func TestCodexQuotaAutoDisableCreditsNoHistoryOrStandaloneTrigger(t *testing.T) {
	cfg := quotaDisableConfig()
	cfg.Codex.QuotaAutoDisable.Rules[0].Credits = config.CodexQuotaAutoDisableCredits{Enabled: true, MinimumBalance: quotaThreshold(1000)}
	m := NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	a, _ := m.Register(t.Context(), &Auth{ID: "credit-history", Provider: "codex"})
	observer := core.CodexQuotaObserverFromContext(m.withCodexQuotaObservation(t.Context()))
	observer(a.ID, a.RuntimeInstanceID(), "http", http.Header{"X-Codex-Credits-Balance": {"0"}})
	observer(a.ID, a.RuntimeInstanceID(), "http", quotaHeaders("premium", "primary", "10080", "99"))
	observer(a.ID, a.RuntimeInstanceID(), "websocket", http.Header{"X-Codex-Credits-Balance": {"0"}})
	current, _ := m.GetByID(a.ID)
	if current.Disabled {
		t.Fatal("reused old credits or old main quota")
	}
	headers := quotaHeaders("codex_bengalfox", "primary", "10080", "99")
	headers.Set("X-Codex-Credits-Balance", "0")
	observer(a.ID, a.RuntimeInstanceID(), "websocket", headers)
	current, _ = m.GetByID(a.ID)
	if current.Disabled {
		t.Fatal("secondary condition promoted a Spark window to main quota")
	}
}

func TestCodexQuotaAutoDisableCreditsRuleScopeAndSnapshot(t *testing.T) {
	cfg := quotaDisableConfig()
	rule := &cfg.Codex.QuotaAutoDisable.Rules[0]
	rule.Credits = config.CodexQuotaAutoDisableCredits{Enabled: true, MinimumBalance: quotaThreshold(1000)}
	a := &Auth{Provider: "codex"}
	headers := quotaHeaders("premium", "primary", "10080", "99")
	headers.Set("X-Codex-Credits-Balance", "2000")
	observation := newCodexQuotaObservation(headers, "http", time.Now())
	pool := extractCodexQuotaPools(observation)[0]
	copy := cloneCodexQuotaAutoDisable(cfg.Codex.QuotaAutoDisable)
	*rule.Credits.MinimumBalance = 3000
	if *copy.Rules[0].Credits.MinimumBalance != 1000 || codexQuotaDisableMatchForAuth(copy, a, pool, observation.Signals) != nil {
		t.Fatal("credit threshold snapshot mutated")
	}
	copy.Rules = append(copy.Rules, config.CodexQuotaAutoDisableRule{WeeklyRemainingPercent: quotaThreshold(10)})
	match := codexQuotaDisableMatchForAuth(copy, a, pool, observation.Signals)
	if match == nil || match.rule != 2 || match.creditsReason != "" {
		t.Fatal("credit protection changed an independent legacy rule")
	}
}

func TestCodexQuotaAutoDisableCreditsRejectsOlderCreditEvidence(t *testing.T) {
	cfg := quotaDisableConfig()
	cfg.Codex.QuotaAutoDisable.Rules[0].Credits.Enabled = true
	m := NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	a, _ := m.Register(t.Context(), &Auth{ID: "credit-freshness", Provider: "codex"})
	headers := quotaHeaders("premium", "primary", "10080", "99")
	headers.Set("X-Codex-Credits-Balance", "0")
	old := newCodexQuotaObservation(headers, "http", time.Unix(10, 0))
	m.recordCodexQuotaObservation(a.ID, a.RuntimeInstanceID(), "http", headers, old.ObservedAt)
	m.recordCodexQuotaObservation(a.ID, a.RuntimeInstanceID(), "websocket", http.Header{"X-Codex-Credits-Balance": {"62500"}}, time.Unix(20, 0))
	m.applyCodexQuotaAutoDisable(t.Context(), cfg.Codex.QuotaAutoDisable, a.ID, a.RuntimeInstanceID(), old)
	current, _ := m.GetByID(a.ID)
	if current.Disabled {
		t.Fatal("older exhausted balance disabled a replenished credential")
	}
}
