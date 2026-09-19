package codexstate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func rulePtr[T any](v T) *T { return &v }
func TestRulesKeepStateIsolatedAndOnlyRetireChangedPairs(t *testing.T) {
	m := New()
	a := Credential{ID: "a", Owner: "account-a", Instance: "a1", Model: "model", Priority: 4}
	b := a
	b.ID, b.Owner, b.Instance = "b", "account-b", "b1"
	cfg := config.CodexStateOverrideConfig{Enabled: true, Acquisition: "all", Rules: &[]config.CodexStateRule{
		{ID: "a-rule", Credentials: []string{"a"}, Settings: config.CodexStateRuleSettings{Lengths: rulePtr([]int{332}), RetrySeconds: rulePtr(2)}},
		{ID: "default-rule", Priorities: []int{4}, Settings: config.CodexStateRuleSettings{Lengths: rulePtr([]int{292}), RetrySeconds: rulePtr(5)}},
	}}
	m.Sync(cfg, []Credential{a, b})
	now := time.Now()
	finishFixture(m, a, Result{Completed: true, Model: "model", State: strings.Repeat("a", 332)}, nil, now)
	finishFixture(m, b, good(), nil, now)
	if state, _, _ := m.Pick(a, "", now); len(state) != 332 {
		t.Fatal("account A used another policy")
	}
	if state, _, _ := m.Pick(b, "", now); len(state) != 292 {
		t.Fatal("account B used another policy")
	}
	(*cfg.Rules)[0].Name = "renamed"
	m.Sync(cfg, []Credential{a, b})
	if state, _, _ := m.Pick(a, "", now); state == "" {
		t.Fatal("renaming cleared state")
	}
	(*cfg.Rules)[0].Settings.Lengths = rulePtr([]int{312})
	m.Sync(cfg, []Credential{a, b})
	if state, _, _ := m.Pick(a, "", now); state != "" {
		t.Fatal("changed validation retained old state")
	}
	if state, _, _ := m.Pick(b, "", now); state == "" {
		t.Fatal("unrelated pair was cleared")
	}
	for i := 0; i < 2; i++ {
		finishFixture(m, a, Result{}, errors.New("fixture"), now)
		finishFixture(m, b, Result{}, errors.New("fixture"), now)
	}
	if m.Snapshots("a", now)[0].NextAttempt.Sub(now) != 2*time.Second || m.Snapshots("b", now)[0].NextAttempt.Sub(now) != 5*time.Second {
		t.Fatal("per-rule fixed interval was lost")
	}
	(*cfg.Rules)[0].Action = "skip"
	m.Sync(cfg, []Credential{a, b})
	if len(m.Snapshots("a", now)) != 0 {
		t.Fatal("skip rule fell through to generic rule")
	}
}
func TestRulesHotReloadRejectsStaleWorkerWithoutCancelingOtherPair(t *testing.T) {
	m := New()
	a := Credential{ID: "a", Owner: "a", Instance: "a", Model: "model"}
	b := a
	b.ID, b.Owner, b.Instance = "b", "b", "b"
	cfg := config.CodexStateOverrideConfig{Enabled: true, Concurrency: 2, Acquisition: "all", Rules: &[]config.CodexStateRule{{ID: "a", Credentials: []string{"a"}}, {ID: "b", Credentials: []string{"b"}}}}
	m.Sync(cfg, []Credential{a, b})
	started := make(chan string, 2)
	release := make(chan struct{})
	m.Tick(t.Context(), time.Now(), func(ctx context.Context, c Credential, _ config.CodexStateOverrideConfig) (Result, error) {
		started <- c.ID
		<-release
		if c.ID == "b" && ctx.Err() != nil {
			t.Error("unrelated worker was canceled")
		}
		return good(), nil
	})
	<-started
	<-started
	(*cfg.Rules)[0].Settings.Lengths = rulePtr([]int{332})
	m.Sync(cfg, []Credential{a, b})
	close(release)
	m.Wait()
	if m.Snapshots("a", time.Now())[0].Acquired != 0 || m.Snapshots("b", time.Now())[0].Acquired != 1 {
		t.Fatal("stale acquisition overwrote new rule or valid worker was discarded")
	}
}

func TestReviewLegacyManualScopeRemoval(t *testing.T) {
	m := New()
	c := Credential{ID: "a", Owner: "a", Instance: "a", Model: "old", Route: "old"}
	cfg := config.CodexStateOverrideConfig{Enabled: true, Models: []string{"old"}, Acquisition: "manual"}
	m.Sync(cfg, nil, c)
	if ok, _ := m.QueueManual(c); !ok {
		t.Fatal("manual acquisition was not queued")
	}
	cfg.Models = []string{"new"}
	m.Sync(cfg, nil, c)
	if len(m.Snapshots(c.ID, time.Now())) != 0 {
		t.Fatal("out-of-scope manual entry survived model scope removal")
	}
}

func TestReviewManualRetryClearsFailureDeadlineOnPolicyEdit(t *testing.T) {
	m := New()
	c := Credential{ID: "a", Owner: "a", Instance: "a", Model: "model"}
	cfg := config.CodexStateOverrideConfig{Enabled: true, Acquisition: "manual", RetrySeconds: 60}
	m.Sync(cfg, nil, c)
	if ok, _ := m.QueueManual(c); !ok {
		t.Fatal("manual acquisition was not queued")
	}
	now := time.Now()
	finishFixture(m, c, Result{}, errors.New("fixture"), now)
	if ok, _ := m.QueueManual(c); !ok {
		t.Fatal("manual retry was not queued")
	}
	cfg.ProxyMode = "direct"
	m.Sync(cfg, nil, c)
	if s := m.Snapshots(c.ID, now)[0]; !s.NextAttempt.IsZero() || s.ConsecutiveFailures != 0 {
		t.Fatal("manual retry inherited an obsolete failure deadline")
	}
}
