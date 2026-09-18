package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func quotaThreshold(value float64) *float64 { return &value }

func quotaDisableConfig() *config.Config {
	return &config.Config{Codex: config.CodexConfig{ObserveQuota: true, QuotaAutoDisable: config.CodexQuotaAutoDisableConfig{
		Enabled: true, Rules: []config.CodexQuotaAutoDisableRule{{WeeklyRemainingPercent: quotaThreshold(10)}},
	}}}
}

func quotaHeaders(active, kind, minutes, used string) http.Header {
	headers := make(http.Header)
	headers.Set("x-codex-active-limit", active)
	headers.Set("x-codex-"+kind+"-window-minutes", minutes)
	headers.Set("x-codex-"+kind+"-used-percent", used)
	return headers
}

func TestCodexQuotaAutoDisableMainPoolAndWindowSemantics(t *testing.T) {
	for _, test := range []struct {
		name, active, kind, minutes, used string
		weekly, fiveHour                  *float64
		want                              bool
	}{
		{"weekly primary", "premium", "primary", "10080", "91", quotaThreshold(10), nil, true},
		{"weekly secondary", "codex", "secondary", "10080", "91", quotaThreshold(10), nil, true},
		{"five hour primary", "premium", "primary", "300", "96", nil, quotaThreshold(5), true},
		{"five hour secondary", "premium", "secondary", "300", "96", nil, quotaThreshold(5), true},
		{"weekly only ignores 5 hours", "premium", "primary", "300", "100", quotaThreshold(10), nil, false},
		{"five hour only ignores week", "premium", "primary", "10080", "100", nil, quotaThreshold(5), false},
		{"threshold equal", "premium", "primary", "10080", "90", quotaThreshold(10), nil, false},
		{"fraction equal", "premium", "primary", "10080", "89.9", quotaThreshold(10.1), nil, false},
		{"fraction below", "premium", "primary", "10080", "89.91", quotaThreshold(10.1), nil, true},
		{"zero threshold", "premium", "primary", "10080", "100", quotaThreshold(0), nil, false},
		{"no active limit", "", "primary", "10080", "100", quotaThreshold(1), nil, true},
		{"spark active", "codex_bengalfox", "primary", "10080", "100", quotaThreshold(10), nil, false},
		{"reserve active", "base-model-inference", "primary", "10080", "100", quotaThreshold(10), nil, false},
		{"unknown pool", "unknown", "primary", "10080", "100", quotaThreshold(10), nil, false},
		{"inactive window", "premium", "primary", "0", "100", quotaThreshold(10), nil, false},
		{"unknown window", "premium", "primary", "1440", "100", quotaThreshold(10), nil, false},
		{"missing duration", "premium", "primary", "", "100", quotaThreshold(10), nil, false},
		{"missing percent", "premium", "primary", "10080", "", quotaThreshold(10), nil, false},
		{"bad percent", "premium", "primary", "10080", "bad", quotaThreshold(10), nil, false},
		{"nan", "premium", "primary", "10080", "NaN", quotaThreshold(10), nil, false},
		{"infinity", "premium", "primary", "10080", "+Inf", quotaThreshold(10), nil, false},
		{"negative percent", "premium", "primary", "10080", "-1", quotaThreshold(100), nil, false},
		{"above 100", "premium", "primary", "10080", "101", quotaThreshold(10), nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			cfg := quotaDisableConfig()
			cfg.Codex.QuotaAutoDisable.Rules[0].WeeklyRemainingPercent = test.weekly
			cfg.Codex.QuotaAutoDisable.Rules[0].FiveHourRemainingPercent = test.fiveHour
			m.SetConfig(cfg)
			a, err := m.Register(t.Context(), &Auth{ID: "quota", Provider: "codex"})
			if err != nil {
				t.Fatal(err)
			}
			observer := core.CodexQuotaObserverFromContext(m.withCodexQuotaObservation(m.WithRoutingPolicySnapshot(t.Context())))
			headers := quotaHeaders(test.active, test.kind, test.minutes, test.used)
			// Additional depleted pools must not replace or contaminate the main window.
			headers.Set("x-codex-bengalfox-primary-used-percent", "100")
			headers.Set("x-codex-bengalfox-primary-window-minutes", "10080")
			headers.Set("x-base-model-inference-primary-used-percent", "100")
			headers.Set("x-base-model-inference-primary-window-minutes", "10080")
			observer(a.ID, a.RuntimeInstanceID(), "http", headers)
			current, _ := m.GetByID(a.ID)
			if current.Disabled != test.want {
				t.Fatalf("disabled=%t, want %t", current.Disabled, test.want)
			}
		})
	}
}

func TestCodexQuotaAutoDisableFilters(t *testing.T) {
	a := &Auth{Provider: "codex", Index: "short-id"}
	pool := extractCodexQuotaPools(newCodexQuotaObservation(quotaHeaders("premium", "primary", "10080", "99"), "http", time.Now()))[0]
	for _, test := range []struct {
		rule config.CodexQuotaAutoDisableRule
		want bool
	}{
		{config.CodexQuotaAutoDisableRule{}, true},
		{config.CodexQuotaAutoDisableRule{Providers: []string{"xai", "CODEX"}, AuthPriorities: []int{0, 3}, CredentialIDs: []string{"short-id"}}, true},
		{config.CodexQuotaAutoDisableRule{Providers: []string{"xai"}}, false},
		{config.CodexQuotaAutoDisableRule{AuthPriorities: []int{3}}, false},
		{config.CodexQuotaAutoDisableRule{CredentialIDs: []string{"other"}}, false},
	} {
		test.rule.WeeklyRemainingPercent = quotaThreshold(10)
		p := config.CodexQuotaAutoDisableConfig{Enabled: true, Rules: []config.CodexQuotaAutoDisableRule{test.rule}}
		if got := codexQuotaDisableMatchForAuth(p, a, pool) != nil; got != test.want {
			t.Fatalf("match=%t for %+v", got, test.rule)
		}
	}
	for _, provider := range []string{"xai", "chatgpt-web", "claude"} {
		a.Provider = provider
		if codexQuotaDisableMatchForAuth(quotaDisableConfig().Codex.QuotaAutoDisable, a, pool) != nil {
			t.Fatal("non-Codex credential disabled")
		}
	}
}

func TestCodexQuotaAutoDisablePersistsOnceAndPreservesActiveRequest(t *testing.T) {
	store := &quotaObservationStore{}
	m := NewManager(store, nil, nil)
	m.SetConfig(quotaDisableConfig())
	a, err := m.Register(t.Context(), &Auth{ID: "quota", Provider: "codex", Metadata: map[string]any{"type": "codex", "access_token": "fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	execCtx, release, active := a.BeginRuntimeExecution(t.Context())
	if !active {
		t.Fatal("execution not admitted")
	}
	defer release()
	writes := store.writes.Load()
	observer := core.CodexQuotaObserverFromContext(m.withCodexQuotaObservation(m.WithRoutingPolicySnapshot(execCtx)))
	var wg sync.WaitGroup
	for i := range 12 {
		wg.Go(func() {
			observer(a.ID, a.RuntimeInstanceID(), []string{"http", "websocket"}[i%2], quotaHeaders("premium", "primary", "10080", "99"))
		})
	}
	wg.Wait()
	current, _ := m.GetByID(a.ID)
	if !current.Disabled || current.Status != StatusDisabled || current.Metadata["disabled"] != true || CodexQuotaAutoDisableReason(current) == "" {
		t.Fatal("missing disabled state/reason")
	}
	if execCtx.Err() != nil || current.RuntimeInstanceID() != a.RuntimeInstanceID() || a.RuntimeInstanceRetired() {
		t.Fatal("auto-disable interrupted an admitted request")
	}
	if got := store.writes.Load() - writes; got != 1 {
		t.Fatalf("disable persisted %d times", got)
	}
	for _, success := range []bool{true, false} {
		m.markResult(t.Context(), Result{AuthID: a.ID, Provider: "codex", Model: "gpt-6-astra", Success: success}, a.RuntimeInstanceID(), nil, 0, nil, false, false)
		current, _ = m.GetByID(a.ID)
		if !current.Disabled || current.Status != StatusDisabled || CodexQuotaAutoDisableReason(current) != current.StatusMessage {
			t.Fatal("late result cleared disabled status/reason")
		}
	}
	if err := ApplyFileAuthProjection(current, FileAuthProjectionOptions{Now: time.Now()}); err != nil || current.StatusMessage == "" || !current.Disabled {
		t.Fatalf("reload lost disable reason: %v", err)
	}
}

func TestCodexQuotaAutoDisableSnapshotPrerequisitesAndReplacement(t *testing.T) {
	for _, observe := range []bool{false, true} {
		for _, enable := range []bool{false, true} {
			t.Run(fmt.Sprintf("observe=%t enable=%t", observe, enable), func(t *testing.T) {
				m := NewManager(nil, nil, nil)
				cfg := quotaDisableConfig()
				cfg.Codex.ObserveQuota = observe
				cfg.Codex.QuotaAutoDisable.Enabled = enable
				m.SetConfig(cfg)
				a, _ := m.Register(t.Context(), &Auth{ID: "quota", Provider: "codex"})
				ctx := m.WithRoutingPolicySnapshot(t.Context())
				// Freeze threshold values and preserve the policy through selector changes.
				*cfg.Codex.QuotaAutoDisable.Rules[0].WeeklyRemainingPercent = 0
				m.SetSelector(&RandomSelector{})
				if enable && *m.selectionPolicy().codexQuotaAutoDisable.Rules[0].WeeklyRemainingPercent != 10 {
					t.Fatal("selector reset policy or threshold aliases caller config")
				}
				m.SetConfig(&config.Config{})
				if observer := core.CodexQuotaObserverFromContext(m.withCodexQuotaObservation(ctx)); observer != nil {
					observer(a.ID, a.RuntimeInstanceID(), "websocket", quotaHeaders("premium", "secondary", "10080", "99"))
				}
				current, _ := m.GetByID(a.ID)
				if current.Disabled != (observe && enable) {
					t.Fatal("prerequisite or frozen policy violated")
				}
			})
		}
	}
	m := NewManager(nil, nil, nil)
	m.SetConfig(quotaDisableConfig())
	a, _ := m.Register(t.Context(), &Auth{ID: "quota", Provider: "codex"})
	observer := core.CodexQuotaObserverFromContext(m.withCodexQuotaObservation(t.Context()))
	observer(a.ID, a.RuntimeInstanceID(), "http", quotaHeaders("premium", "primary", "10080", "99"))
	replacement := a.CloneWithoutRuntimeInstance()
	replacement.Disabled = false
	replacement.Status = StatusActive
	updated, err := m.Update(t.Context(), replacement)
	if err != nil {
		t.Fatal(err)
	}
	observer(a.ID, a.RuntimeInstanceID(), "http", quotaHeaders("premium", "primary", "10080", "99"))
	current, _ := m.GetByID(a.ID)
	if current.Disabled || updated.RuntimeInstanceID() == a.RuntimeInstanceID() {
		t.Fatal("old response disabled a manually re-enabled credential")
	}
}

func TestCodexQuotaAutoDisableRejectsHistoryAndOlderMainPool(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetConfig(quotaDisableConfig())
	a, _ := m.Register(t.Context(), &Auth{ID: "quota", Provider: "codex"})
	old := newCodexQuotaObservation(quotaHeaders("premium", "primary", "10080", "99"), "http", time.Unix(10, 0))
	m.recordCodexQuotaObservation(a.ID, a.RuntimeInstanceID(), "http", quotaHeaders("premium", "primary", "10080", "20"), time.Unix(20, 0))
	m.applyCodexQuotaAutoDisable(context.Background(), m.selectionPolicy().codexQuotaAutoDisable, a.ID, a.RuntimeInstanceID(), old)
	current, _ := m.GetByID(a.ID)
	if current.Disabled {
		t.Fatal("older main pool disabled credential")
	}
	m.recordCodexQuotaObservation(a.ID, a.RuntimeInstanceID(), "http", quotaHeaders("premium", "primary", "10080", "99"), time.Unix(30, 0))
	observer := core.CodexQuotaObserverFromContext(m.withCodexQuotaObservation(t.Context()))
	observer(a.ID, a.RuntimeInstanceID(), "websocket", quotaHeaders("codex_bengalfox", "primary", "300", "99"))
	current, _ = m.GetByID(a.ID)
	if current.Disabled || !strings.EqualFold(current.Provider, "codex") {
		t.Fatal("stored main history was re-evaluated on Spark observation")
	}
}

func TestCodexQuotaAutoDisableCapturedFiveHourAndWeeklyHeaders(t *testing.T) {
	// Main-pool fields from a successful live request on 2026-09-18. No identity
	// or token fields are retained. The account has both 5-hour and weekly limits.
	headers := http.Header{
		"X-Codex-Active-Limit": {"premium"}, "X-Codex-Plan-Type": {"plus"},
		"X-Codex-Primary-Used-Percent": {"1"}, "X-Codex-Primary-Window-Minutes": {"300"},
		"X-Codex-Secondary-Used-Percent": {"0"}, "X-Codex-Secondary-Window-Minutes": {"10080"},
	}
	for _, test := range []struct {
		name             string
		weekly, fiveHour *float64
		want             bool
	}{
		{"weekly only", quotaThreshold(100), nil, false},
		{"5-hour below threshold", nil, quotaThreshold(100), true},
		{"5-hour at threshold", nil, quotaThreshold(99), false},
		{"either window", quotaThreshold(10), quotaThreshold(100), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			cfg := quotaDisableConfig()
			cfg.Codex.QuotaAutoDisable.Rules[0].WeeklyRemainingPercent = test.weekly
			cfg.Codex.QuotaAutoDisable.Rules[0].FiveHourRemainingPercent = test.fiveHour
			m.SetConfig(cfg)
			a, _ := m.Register(t.Context(), &Auth{ID: "captured-quota", Provider: "codex"})
			core.CodexQuotaObserverFromContext(m.withCodexQuotaObservation(t.Context()))(a.ID, a.RuntimeInstanceID(), "http", headers)
			current, _ := m.GetByID(a.ID)
			if current.Disabled != test.want {
				t.Fatalf("disabled=%t, want %t", current.Disabled, test.want)
			}
		})
	}
}

func TestCodexQuotaAutoDisableSurvivesConcurrentTokenRefresh(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetConfig(quotaDisableConfig())
	a, err := m.Register(t.Context(), &Auth{ID: "quota", Provider: "codex", Metadata: map[string]any{"type": "codex", "access_token": "old"}})
	if err != nil {
		t.Fatal(err)
	}
	baseline := a.Clone()
	refreshed := a.Clone()
	refreshed.Metadata["access_token"] = "refreshed"
	core.CodexQuotaObserverFromContext(m.withCodexQuotaObservation(t.Context()))(a.ID, a.RuntimeInstanceID(), "http", quotaHeaders("premium", "primary", "10080", "99"))
	updated, err := m.applyRefreshedAuth(t.Context(), a, baseline, refreshed, time.Time{})
	if err != nil || updated == nil {
		t.Fatalf("refresh failed: %v", err)
	}
	if !updated.Disabled || updated.Status != StatusDisabled || updated.StatusMessage == "" || updated.Metadata["access_token"] != "refreshed" {
		t.Fatal("refresh lost disabled state or refreshed token")
	}
	persisted, err := CanonicalMetadataBytes(updated)
	if err != nil || !strings.Contains(string(persisted), `"disabled":true`) {
		t.Fatal("refresh would persist an enabled credential")
	}
}

func TestCodexQuotaAutoDisableCapturedWeeklyOnlyHeaders(t *testing.T) {
	// A second live account reports the week in primary and an inactive secondary.
	headers := http.Header{
		"X-Codex-Active-Limit": {"premium"}, "X-Codex-Plan-Type": {"pro"},
		"X-Codex-Primary-Used-Percent": {"16"}, "X-Codex-Primary-Window-Minutes": {"10080"},
		"X-Codex-Secondary-Used-Percent": {"0"}, "X-Codex-Secondary-Window-Minutes": {"0"},
		"X-Codex-Secondary-Reset-After-Seconds": {"0"}, "X-Codex-Secondary-Reset-At": {""},
	}
	for _, test := range []struct {
		name             string
		weekly, fiveHour *float64
		want             bool
	}{
		{"weekly below threshold", quotaThreshold(85), nil, true},
		{"weekly at threshold", quotaThreshold(84), nil, false},
		{"5-hour absent", nil, quotaThreshold(100), false},
		{"combined with inactive secondary", quotaThreshold(10), quotaThreshold(100), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			cfg := quotaDisableConfig()
			cfg.Codex.QuotaAutoDisable.Rules[0].WeeklyRemainingPercent = test.weekly
			cfg.Codex.QuotaAutoDisable.Rules[0].FiveHourRemainingPercent = test.fiveHour
			m.SetConfig(cfg)
			a, _ := m.Register(t.Context(), &Auth{ID: "weekly-only", Provider: "codex"})
			core.CodexQuotaObserverFromContext(m.withCodexQuotaObservation(t.Context()))(a.ID, a.RuntimeInstanceID(), "http", headers)
			current, _ := m.GetByID(a.ID)
			if current.Disabled != test.want {
				t.Fatalf("disabled=%t, want %t", current.Disabled, test.want)
			}
		})
	}
}
