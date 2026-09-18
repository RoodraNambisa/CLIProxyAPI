package codexstate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
