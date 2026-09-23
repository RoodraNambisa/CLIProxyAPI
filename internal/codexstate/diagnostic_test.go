package codexstate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestDiagnosticAcquisitionIgnoresAutomaticScopeWithoutRenewal(t *testing.T) {
	for _, rules := range []*[]config.CodexStateRule{nil, {}, {{ID: "other", Priorities: []int{4}, Models: []string{"other"}}}, {{Action: "skip"}}} {
		m := NewDiagnostic()
		c := Credential{ID: "test", Owner: "owner", Instance: "one", Model: "model", Route: "model"}
		cfg := config.CodexStateOverrideConfig{Enabled: false, Rules: rules, Priorities: []int{4}, Models: []string{"other"}, ExcludedCredentials: []string{c.ID}, Acquisition: "all", TTLMinutes: 1, Prompt: "test prompt", ProxyMode: "direct"}
		m.Sync(cfg, nil, c)
		if ok, _ := m.QueueManual(c); !ok {
			t.Fatal("explicit test blocked by automatic scope")
		}
		m.Sync(cfg, nil, c)
		calls := 0
		probe := func(_ context.Context, _ Credential, policy config.CodexStateOverrideConfig) (Result, error) {
			calls++
			if policy.ProxyMode != "direct" || policy.Prompt != "test prompt" || policy.TTLMinutes != 1 {
				t.Error("diagnostic lost common settings")
			}
			return good(), nil
		}
		m.Tick(t.Context(), time.Now(), probe)
		m.Wait()
		if value, _, _ := m.Pick(c, "", time.Now()); value != good().State {
			t.Fatal("could not reuse diagnostic State")
		}
		m.Sync(cfg, nil, c)
		m.Tick(t.Context(), time.Now().Add(2*time.Hour), probe)
		m.Wait()
		if calls != 1 || !m.Snapshots(c.ID, time.Now())[0].ManualOnly {
			t.Fatal("one-shot test automatically renewed")
		}
		if ok, baseline := m.QueueManual(c); !ok || baseline != 1 {
			t.Fatal("could not explicitly acquire another value")
		}
		m.Tick(t.Context(), time.Now(), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
			return Result{}, errors.New("upstream failure")
		})
		m.Wait()
		m.Tick(t.Context(), time.Now().Add(2*time.Hour), probe)
		m.Wait()
		if calls != 1 {
			t.Fatal("failed one-shot test automatically retried")
		}
		c.Instance = "replacement"
		m.Sync(cfg, nil, c)
		if len(m.Snapshots(c.ID, time.Now())) != 0 {
			t.Fatal("credential replacement retained diagnostic State")
		}
	}
}

func TestDiagnosticRuleValidationAndHotUpdate(t *testing.T) {
	lengths, prompt := []int{332}, "rule prompt"
	c := Credential{ID: "test", Owner: "owner", Instance: "one", Model: "model", Aliases: []string{"alias"}}
	cfg := config.CodexStateOverrideConfig{Rules: &[]config.CodexStateRule{{ID: "match", Models: []string{"alias"}, Settings: config.CodexStateRuleSettings{Lengths: &lengths, Prompt: &prompt}}}}
	m := NewDiagnostic()
	m.Sync(cfg, nil, c)
	m.QueueManual(c)
	m.Tick(t.Context(), time.Now(), func(_ context.Context, _ Credential, policy config.CodexStateOverrideConfig) (Result, error) {
		if policy.Prompt != prompt || len(policy.Lengths) != 1 || policy.Lengths[0] != 332 {
			t.Error("matching rule ignored")
		}
		return good(), nil
	})
	m.Wait()
	if snapshot := m.Snapshots(c.ID, time.Now())[0]; snapshot.LastError != "state_length_mismatch" || snapshot.RuleID != "match" {
		t.Fatalf("scope bypass also bypassed validation: %+v", snapshot)
	}
	m.QueueManual(c)
	m.Tick(t.Context(), time.Now(), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		result := good()
		result.State = strings.Repeat("r", 332)
		return result, nil
	})
	m.Wait()
	if value, _, _ := m.Pick(c, "", time.Now()); value == "" {
		t.Fatal("valid diagnostic result not reusable")
	}
	newLengths := []int{292}
	(*cfg.Rules)[0].Settings.Lengths = &newLengths
	policy, _ := DiagnosticPolicy(cfg, c)
	if value, _, _, _ := m.PickVersionForPolicy(c, "", time.Now(), policy); value != "" {
		t.Fatal("request reused old validation before background sync")
	}
	m.Sync(cfg, nil, c)
	if snapshot := m.Snapshots(c.ID, time.Now())[0]; snapshot.Length != 0 || snapshot.Status == "queued" {
		t.Fatal("hot update reused or automatically reacquired diagnostic State")
	}
}

func TestDiagnosticRetirementCancelsWithoutRequeueAndSharesCapacity(t *testing.T) {
	m := NewDiagnostic()
	c := Credential{ID: "test", Owner: "owner", Instance: "one", Model: "model"}
	cfg := config.CodexStateOverrideConfig{Concurrency: 1}
	m.Sync(cfg, nil, c)
	m.QueueManual(c)
	started := make(chan struct{})
	probe := func(ctx context.Context, _ Credential, _ config.CodexStateOverrideConfig) (Result, error) {
		close(started)
		<-ctx.Done()
		return good(), nil
	}
	m.TickWithCapacity(t.Context(), time.Now(), probe, 0)
	if m.Running() != 0 {
		t.Fatal("diagnostic exceeded shared capacity")
	}
	m.TickWithCapacity(t.Context(), time.Now(), probe, 1)
	<-started
	m.Sync(cfg, nil)
	m.Wait()
	if m.Running() != 0 || len(m.Snapshots(c.ID, time.Now())) != 0 {
		t.Fatal("retired credential kept an acquisition or result")
	}
}

func TestAutoCookieDiscardKeepsStateAndCancelsCookieJobs(t *testing.T) {
	m := NewDiagnostic()
	c := Credential{ID: "test", Owner: "owner", Instance: "one", Model: "model"}
	cookie := c
	cookie.Model = "cookie-model"
	m.Sync(config.CodexStateOverrideConfig{Concurrency: 2}, nil, c)
	m.QueueManual(c, "state")
	m.Tick(t.Context(), time.Now(), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		return good(), nil
	})
	m.Wait()
	if ok, _ := m.QueueManualStrategy(cookie, "cookie-only"); !ok {
		t.Fatal("could not queue Cookie diagnostic")
	}
	started := make(chan struct{})
	m.Tick(t.Context(), time.Now(), func(ctx context.Context, _ Credential, _ config.CodexStateOverrideConfig) (Result, error) {
		close(started)
		<-ctx.Done()
		return good(), nil
	})
	if m.Running() != 1 {
		t.Fatal("Cookie diagnostic did not start")
	}
	<-started
	m.DiscardCookieOnly()
	m.Wait()
	if snapshots := m.Snapshots(c.ID, time.Now()); len(snapshots) != 1 || snapshots[0].Model != c.Model {
		t.Fatal("discard removed State or allowed a late Cookie result", snapshots)
	}
	if value, _, _ := m.Pick(c, "", time.Now()); value != good().State {
		t.Fatal("State resource lost during passive Cookie activation")
	}
}
