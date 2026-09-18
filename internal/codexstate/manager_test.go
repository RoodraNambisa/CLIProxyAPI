package codexstate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func fixture() (*Manager, Credential, config.CodexStateOverrideConfig) {
	m := New()
	c := Credential{ID: "one", Owner: "account", Instance: "instance", Model: "model", Route: "alias"}
	cfg := config.CodexStateOverrideConfig{Enabled: true, Acquisition: "all", RetrySeconds: 1}
	m.Sync(cfg, []Credential{c})
	return m, c, cfg
}

func TestStateResponseInvalidationCriteria(t *testing.T) {
	for _, tc := range []struct {
		name, plan, model, reason    string
		length                       int
		byLength, byModel, unlimited bool
	}{
		{name: "disabled", model: "other", length: 312},
		{name: "length only ignores model", byLength: true, model: "other", length: 292},
		{name: "model only ignores length", byModel: true, model: "model", length: 312},
		{name: "missing metadata", byLength: true, byModel: true},
		{name: "valid metadata", byLength: true, byModel: true, model: "model", length: 292},
		{name: "bad length", byLength: true, length: 312, reason: "response_state_length_mismatch"},
		{name: "bad model", byModel: true, model: "other", reason: "response_model_mismatch"},
		{name: "both model", byLength: true, byModel: true, model: "other", length: 312, reason: "response_model_mismatch"},
		{name: "both length", byLength: true, byModel: true, model: "model", length: 312, reason: "response_state_length_mismatch"},
		{name: "team valid", plan: "team", byLength: true, length: 332},
		{name: "team invalid", plan: "team", byLength: true, length: 292, reason: "response_state_length_mismatch"},
		{name: "unlimited lengths", byLength: true, length: 312, unlimited: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, c, cfg := fixture()
			cfg.Acquisition = "manual"
			cfg.InvalidateOnStateLengthMismatch, cfg.InvalidateOnModelMismatch = tc.byLength, tc.byModel
			cfg.PlanLengths = []config.CodexStatePlanLengths{{PlanTypes: []string{"business"}, Lengths: []int{332}}}
			if tc.unlimited {
				cfg.Lengths = []int{}
			}
			c.Plan = tc.plan
			m.Sync(cfg, []Credential{c})
			result := good()
			if tc.plan == "team" {
				result.State = strings.Repeat("s", 332)
			}
			now := time.Now()
			finishFixture(m, c, result, nil, now)
			value, _, version, _ := m.PickVersion(c, "", now)
			if version == 0 {
				t.Fatal("missing acquired version")
			}
			if reason := m.ObserveResponse(c, version, value, tc.model, tc.length); reason != tc.reason {
				t.Fatalf("reason = %q, want %q", reason, tc.reason)
			}
			s := m.Snapshots(c.ID, now)[0]
			if tc.reason == "" {
				if s.Invalidations != 0 || s.Status != "valid" {
					t.Fatalf("unexpected invalidation: %+v", s)
				}
				return
			}
			if s.Invalidations != 1 || s.LastInvalidation != tc.reason || s.Status != "queued" || s.Length != 0 || s.Digest != "" || !s.ExpiresAt.IsZero() {
				t.Fatalf("invalidation not queued: %+v", s)
			}
			if value, _, _, _ := m.PickVersion(c, "", now); value != "" {
				t.Fatal("invalidated value still usable")
			}
			// Repeated responses using the same version cannot count or queue twice.
			m.ObserveResponse(c, version, value, tc.model, tc.length)
			m.Tick(t.Context(), now, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) { return result, nil })
			m.Wait()
			if s = m.Snapshots(c.ID, now)[0]; s.Acquired != 2 || s.Invalidations != 1 || s.Status != "valid" {
				t.Fatalf("replacement failed: %+v", s)
			}
		})
	}
}

func TestStateResponseOldVersionCannotInvalidateReplacement(t *testing.T) {
	m, c, cfg := fixture()
	cfg.InvalidateOnModelMismatch = true
	m.Sync(cfg, []Credential{c})
	now := time.Now()
	finishFixture(m, c, good(), nil, now)
	value, _, version, _ := m.PickVersion(c, "", now)
	finishFixture(m, c, good(), nil, now)
	newValue, _, newVersion, _ := m.PickVersion(c, "", now)
	if value != newValue || version == newVersion {
		t.Fatal("ABA fixture did not acquire identical values at distinct versions")
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { m.ObserveResponse(c, version, value, "other", 0) })
	}
	wg.Wait()
	if s := m.Snapshots(c.ID, now)[0]; s.Invalidations != 0 || s.Status != "valid" {
		t.Fatalf("old response removed new value: %+v", s)
	}
	// Disable/re-enable must not recycle generation numbers either.
	m.Sync(config.CodexStateOverrideConfig{}, nil)
	m.Sync(cfg, []Credential{c})
	finishFixture(m, c, good(), nil, now)
	m.ObserveResponse(c, newVersion, newValue, "other", 0)
	if s := m.Snapshots(c.ID, now)[0]; s.Invalidations != 0 || s.Status != "valid" {
		t.Fatal("old response survived re-enable version isolation")
	}
}

func TestStateResponseInvalidationPreservesFailureBudget(t *testing.T) {
	for _, failures := range []int{1, 3} {
		m, c, cfg := fixture()
		cfg.InvalidateOnStateLengthMismatch = true
		m.Sync(cfg, []Credential{c})
		now := time.Now()
		finishFixture(m, c, good(), nil, now)
		value, _, version, _ := m.PickVersion(c, "", now)
		for range failures {
			finishFixture(m, c, Result{}, errors.New("renewal failed"), now)
		}
		before := m.Snapshots(c.ID, now)[0]
		m.ObserveResponse(c, version, value, "", 312)
		after := m.Snapshots(c.ID, now)[0]
		if after.ConsecutiveFailures != failures || after.Exhausted != before.Exhausted || !after.NextAttempt.Equal(before.NextAttempt) {
			t.Fatalf("failure budget reset: %+v", after)
		}
		m.Tick(t.Context(), now, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
			t.Error("ignored backoff or exhausted failure budget")
			return good(), nil
		})
		m.Wait()
	}
}

func TestStateResponseInvalidationDoesNotDuplicateActiveRenewal(t *testing.T) {
	m, c, cfg := fixture()
	cfg.InvalidateOnModelMismatch = true
	m.Sync(cfg, []Credential{c})
	now := time.Now()
	finishFixture(m, c, good(), nil, now)
	value, _, version, _ := m.PickVersion(c, "", now)
	started, finish := make(chan struct{}), make(chan struct{})
	m.Action(c.ID, c.Model, "acquire")
	m.Tick(t.Context(), now, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		close(started)
		<-finish
		return good(), nil
	})
	<-started
	m.ObserveResponse(c, version, value, "other", 0)
	close(finish)
	m.Wait()
	if s := m.Snapshots(c.ID, now)[0]; s.Status != "valid" || s.Acquired != 2 || s.Attempts != 2 || s.Invalidations != 1 {
		t.Fatalf("renewal duplicated: %+v", s)
	}
}
func finishFixture(m *Manager, c Credential, result Result, err error, now time.Time) {
	m.mu.Lock()
	e := m.entries[key(c)]
	m.running++
	e.Attempts++
	version := m.version
	m.mu.Unlock()
	m.finish(key(c), e, version, nil, result, err, now)
}
func good() Result {
	return Result{State: strings.Repeat("s", 292), Model: "model", Completed: true, Status: 200, Tokens: 12}
}

func TestStateAcquisitionAndFailureLimit(t *testing.T) {
	m, c, _ := fixture()
	now := time.Now()
	for range 3 {
		finishFixture(m, c, Result{Status: 503}, errors.New("failed"), now)
	}
	s := m.Snapshots(c.ID, now)[0]
	if !s.Exhausted || s.ConsecutiveFailures != 3 || s.Status != "exhausted" {
		t.Fatalf("missing failure limit: %+v", s)
	}
	m.Tick(t.Context(), now.Add(24*time.Hour), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		t.Error("exhausted acquisition retried")
		return good(), nil
	})
	if !m.Action(c.ID, c.Model, "acquire") {
		t.Fatal("manual retry unavailable")
	}
	finishFixture(m, c, good(), nil, now)
	state, policy, eligible := m.Pick(c, "client", now)
	if !eligible || policy != "" || len(state) != 292 {
		t.Fatal("valid state unavailable")
	}
	s = m.Snapshots(c.ID, now)[0]
	if s.Exhausted || s.ConsecutiveFailures != 0 || s.CurrentUses != 1 || s.Acquired != 1 || s.Attempts != 4 {
		t.Fatalf("invalid counters: %+v", s)
	}
	encoded, _ := json.Marshal(s)
	if strings.Contains(string(encoded), state) {
		t.Fatal("state leaked in snapshot")
	}
	finishFixture(m, c, Result{State: "bad", Model: c.Model, Completed: true}, nil, now.Add(time.Minute))
	if value, _, _ := m.Pick(c, "", now.Add(2*time.Minute)); value != state {
		t.Fatal("failed renewal replaced unexpired state")
	}
	if value, policy, _ := m.Pick(c, "", now.Add(time.Hour)); value != "" || policy != "continue" {
		t.Fatal("expired state remained usable")
	}
}

func TestStateScopeAndValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Result)
		reason string
	}{
		{"incomplete", func(r *Result) { r.Completed = false }, "response_not_completed"},
		{"length", func(r *Result) { r.State = strings.Repeat("x", 312) }, "state_length_mismatch"},
		{"model", func(r *Result) { r.Model = "other" }, "response_model_mismatch"},
		{"missing", func(r *Result) { r.State = "" }, "invalid_or_missing_state"},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, c, _ := fixture()
			result := good()
			test.mutate(&result)
			finishFixture(m, c, result, nil, time.Now())
			s := m.Snapshots(c.ID, time.Now())[0]
			if s.Acquired != 0 || s.LastError != test.reason {
				t.Fatalf("accepted invalid result %+v", s)
			}
		})
	}
	m, c, cfg := fixture()
	now := time.Now()
	finishFixture(m, c, good(), nil, now)
	for _, other := range []Credential{{ID: c.ID, Owner: "different", Instance: c.Instance, Model: c.Model}, {ID: c.ID, Owner: c.Owner, Instance: c.Instance, Model: "different"}} {
		if state, _, ok := m.Pick(other, "", now); ok || state != "" {
			t.Fatal("state crossed account or model")
		}
	}
	cfg.Mode = "missing"
	m.Sync(cfg, []Credential{c})
	if value, policy, ok := m.Pick(c, "explicit-client-state", now); !ok || value != "" || policy != "" {
		t.Fatal("fill-only replaced explicit state")
	}
	m.Sync(config.CodexStateOverrideConfig{}, nil)
	if len(m.Snapshots(c.ID, now)) != 0 {
		t.Fatal("disabled runtime retained entries")
	}
}

func TestStateTasksDeduplicatedAndCanceled(t *testing.T) {
	m, c, cfg := fixture()
	started := make(chan struct{})
	stopped := make(chan struct{})
	probe := func(ctx context.Context, _ Credential, _ config.CodexStateOverrideConfig) (Result, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return good(), nil
	}
	m.Tick(t.Context(), time.Now(), probe)
	<-started
	m.Tick(t.Context(), time.Now(), probe)
	if s := m.Snapshots(c.ID, time.Now())[0]; s.Attempts != 1 {
		t.Fatal("duplicate acquisition")
	}
	cfg.Enabled = false
	m.Sync(cfg, nil)
	<-stopped
	if len(m.Snapshots(c.ID, time.Now())) != 0 {
		t.Fatal("obsolete task survived disable")
	}
}

func TestProxyPlaceholderIsFixedWithinTask(t *testing.T) {
	value, err := ExpandProxy("http://u-{12}-{12}:secret@proxy.example:9999")
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(strings.Split(value, ":secret")[0], "-")
	if len(parts) != 3 || parts[1] != parts[2] || len(parts[1]) != 12 {
		t.Fatal("unstable placeholder")
	}
}

func TestStateSurvivesTokenRefreshWithoutAcceptingObsoleteAcquisition(t *testing.T) {
	m, c, cfg := fixture()
	now := time.Now()
	finishFixture(m, c, good(), nil, now)
	started := make(chan struct{})
	release := make(chan struct{})
	m.Action(c.ID, c.Model, "acquire")
	m.Tick(t.Context(), now, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		close(started)
		<-release
		r := good()
		r.State = strings.Repeat("b", 292)
		return r, nil
	})
	<-started
	refreshed := c
	refreshed.Instance = "new-token-instance"
	m.Sync(cfg, []Credential{refreshed})
	close(release)
	m.Wait()
	if state, _, ok := m.Pick(refreshed, "", now); !ok || state != good().State {
		t.Fatal("refresh discarded state or accepted an obsolete acquisition")
	}
	if _, _, ok := m.Pick(c, "", now); ok {
		t.Fatal("retired runtime instance can still use state")
	}
	refreshed.Owner = "another-account"
	m.Sync(cfg, []Credential{refreshed})
	if state, policy, ok := m.Pick(refreshed, "", now); !ok || state != "" || policy != "continue" {
		t.Fatal("account replacement retained the previous account state")
	}
}

func TestStatePlanChangesInvalidateOldLengths(t *testing.T) {
	m, c, cfg := fixture()
	c.Plan = "pro"
	cfg.PlanLengths = []config.CodexStatePlanLengths{{PlanTypes: []string{"business"}, Lengths: []int{332}}}
	m.Sync(cfg, []Credential{c})
	now := time.Now()
	finishFixture(m, c, good(), nil, now)
	changed := c
	changed.Plan = "team"
	if state, _, _ := m.Pick(changed, "", now); state != "" {
		t.Fatal("new plan used old state before synchronization")
	}
	m.Sync(cfg, []Credential{changed})
	if m.Snapshots(c.ID, now)[0].Length != 0 {
		t.Fatal("plan change retained the old state")
	}
	finishFixture(m, changed, good(), nil, now)
	if m.Snapshots(c.ID, now)[0].LastError != "state_length_mismatch" {
		t.Fatal("old plan length was accepted")
	}
	result := good()
	result.State = strings.Repeat("b", 332)
	finishFixture(m, changed, result, nil, now)
	if value, _, ok := m.Pick(changed, "", now); !ok || len(value) != 332 {
		t.Fatal("new plan length was not accepted")
	}
}
