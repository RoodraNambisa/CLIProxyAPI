package codexstate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestHiddenStateRoutesRecoverWithoutTraffic(t *testing.T) {
	m, c, cfg := fixture()
	c.Aliases = []string{"friendly", "team/friendly"}
	cfg.MissingPolicy, cfg.Acquisition = "hide", "active"
	m.Sync(cfg, []Credential{c})
	now := time.Now()
	check := func(want bool, at time.Time) {
		t.Helper()
		for _, model := range []string{c.Model, c.Route, "friendly", "team/friendly"} {
			if got := m.Availability().Available(c.ID, model, at); got != want {
				t.Fatalf("%s available=%v, want %v", model, got, want)
			}
		}
		if !m.Availability().Available("other", c.Model, at) || !m.Availability().Available(c.ID, "other", at) {
			t.Fatal("unrelated credential or model was hidden")
		}
	}
	check(false, now)
	calls := 0
	probe := func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		calls++
		return good(), nil
	}
	m.Tick(t.Context(), now, probe)
	m.Wait()
	if calls != 1 {
		t.Fatal("hidden active route never bootstrapped")
	}
	check(true, now)
	check(false, now.Add(2*time.Hour))
	m.Action(c.ID, c.Model, "pause")
	check(false, now)
	m.Action(c.ID, c.Model, "resume")
	check(true, now)
	value, _, version, _ := m.PickVersion(c, "", now)
	m.ObserveResponse(c, version, value, "other", 312)
	// Watchers are opt-in; clearing always removes route availability.
	m.Action(c.ID, c.Model, "clear")
	check(false, now)
	m.Tick(t.Context(), now, probe)
	m.Wait()
	check(true, time.Now())
	cfg.MissingPolicy = "error"
	m.Sync(cfg, []Credential{c})
	if len(m.Availability()) != 0 {
		t.Fatal("changing missing policy retained hidden routes")
	}
	cfg.MissingPolicy = "hide"
	m.Sync(cfg, []Credential{c})
	m.Sync(config.CodexStateOverrideConfig{}, nil)
	if len(m.Availability()) != 0 {
		t.Fatal("disabled management retained hidden routes")
	}
}

func TestHiddenStateFailureDetailsBudgetAndManualMode(t *testing.T) {
	m, c, cfg := fixture()
	cfg.MissingPolicy, cfg.Acquisition, cfg.MaxAttempts = "hide", "active", 2
	cfg.InvalidateOnModelMismatch, cfg.InvalidateOnStateLengthMismatch = true, true
	m.Sync(cfg, []Credential{c})
	now := time.Now()
	for _, result := range []Result{
		{State: strings.Repeat("x", 312), Model: c.Model, Status: 200, Completed: true},
		{State: strings.Repeat("x", 292), Model: "unexpected-model", Status: 200, Completed: true},
	} {
		finishFixture(m, c, result, nil, now)
		s := m.Snapshots(c.ID, now)[0]
		if !s.RoutingHidden || s.LastReturnedLength == nil || *s.LastReturnedLength != len(result.State) || s.LastReturnedModel != result.Model {
			t.Fatalf("missing diagnostic details: %+v", s)
		}
		*s.LastReturnedLength = 1
		if *m.Snapshots(c.ID, now)[0].LastReturnedLength != len(result.State) {
			t.Fatal("snapshot leaked mutable length pointer")
		}
	}
	m.Tick(t.Context(), now.Add(time.Hour), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		t.Error("hidden route bypassed exhausted failure budget")
		return good(), nil
	})
	m.Wait()
	finishFixture(m, c, good(), nil, now)
	value, _, version, _ := m.PickVersion(c, "", now)
	m.ObserveResponse(c, version, value, "unexpected-response-model", 332)
	s := m.Snapshots(c.ID, now)[0]
	if !s.RoutingHidden || s.InvalidationModel != "unexpected-response-model" || s.InvalidationLength == nil || *s.InvalidationLength != 332 {
		t.Fatalf("invalidation missing details: %+v", s)
	}
	finishFixture(m, c, Result{Status: 200, Completed: true}, nil, now)
	s = m.Snapshots(c.ID, now)[0]
	if s.LastReturnedLength == nil || *s.LastReturnedLength != 0 || s.LastReturnedModel != "" {
		t.Fatal("empty response retained stale diagnostic details")
	}
	cfg.Acquisition = "manual"
	m.Sync(cfg, []Credential{c})
	m.Action(c.ID, c.Model, "clear")
	m.Tick(t.Context(), now.Add(time.Hour), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		t.Error("hidden manual route started automatically")
		return good(), nil
	})
	m.Wait()
}
