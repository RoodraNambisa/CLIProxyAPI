package codexstate

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRetryRoundsWaitAndBoundTotalAttempts(t *testing.T) {
	for _, interval := range []int{30, 60} {
		m, c, cfg := fixture()
		cfg.MaxAttempts, cfg.MaxRetryRounds, cfg.RetryRoundIntervalMinutes = 2, 2, interval
		m.Sync(cfg, []Credential{c})
		var calls atomic.Int32
		probe := func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
			calls.Add(1)
			return Result{}, errors.New("fixture")
		}
		now := time.Now()
		for round := 0; round <= 2; round++ {
			for attempt := 0; attempt < 2; attempt++ {
				before := calls.Load()
				if before > 0 {
					s := m.Snapshots(c.ID, now)[0]
					m.Tick(t.Context(), s.NextAttempt.Add(-time.Nanosecond), probe)
					m.Wait()
					if calls.Load() != before {
						t.Fatal("retry ran before its interval")
					}
					now = s.NextAttempt
				}
				m.Tick(t.Context(), now, probe)
				m.Wait()
				s := m.Snapshots(c.ID, time.Now())[0]
				if calls.Load() != before+1 || s.RetryRoundsUsed != round || s.ConsecutiveFailures != attempt+1 {
					t.Fatalf("incorrect round counters: calls=%d snapshot=%+v", calls.Load(), s)
				}
				if attempt == 0 && s.NextAttempt.Sub(m.entries[key(c)].lastFailure) != time.Duration(cfg.RetrySeconds)*time.Second {
					t.Fatal("within-round retry stopped using fixed seconds")
				}
			}
			s := m.Snapshots(c.ID, time.Now())[0]
			if round < 2 {
				if s.Exhausted || !s.RoundWaiting || s.Status != "retry_wait" || s.NextAttempt.Sub(m.entries[key(c)].lastFailure) != time.Duration(interval)*time.Minute {
					t.Fatalf("missing round cooldown: %+v", s)
				}
			} else if !s.Exhausted || s.RoundWaiting || !s.NextAttempt.IsZero() {
				t.Fatalf("final round did not stop: %+v", s)
			}
		}
		m.Tick(t.Context(), time.Now().Add(48*time.Hour), probe)
		m.Wait()
		if calls.Load() != 6 {
			t.Fatal("exhausted budget started more rounds")
		}
	}
}

func TestRetryRoundsPreserveCacheAndResetOnSuccessOrManualAcquire(t *testing.T) {
	m, c, cfg := fixture()
	cfg.MaxAttempts = 1
	cfg.MaxRetryRounds = 2
	cfg.TTLMinutes = 120
	cfg.Acquisition = "all"
	m.Sync(cfg, []Credential{c})
	now := time.Now()
	finishFixture(m, c, good(), nil, now)
	finishFixture(m, c, Result{}, errors.New("renewal failed"), now.Add(116*time.Minute))
	s := m.Snapshots(c.ID, now.Add(116*time.Minute))[0]
	if !s.RoundWaiting || s.Status != "valid" {
		t.Fatalf("old cache was lost during cooldown: %+v", s)
	}
	if v, _, _ := m.Pick(c, "", now.Add(117*time.Minute)); v == "" {
		t.Fatal("usable State removed on failure")
	}
	m.Action(c.ID, c.Model, "acquire")
	s = m.Snapshots(c.ID, now)[0]
	if s.RoundWaiting || s.RetryRoundsUsed != 0 || s.ConsecutiveFailures != 0 || !s.NextAttempt.IsZero() {
		t.Fatal("manual acquire did not reset cycle")
	}
	m.Tick(t.Context(), time.Now(), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) { return good(), nil })
	m.Wait()
	if s = m.Snapshots(c.ID, time.Now())[0]; s.RoundWaiting || s.RetryRoundsUsed != 0 || s.LastError != "" {
		t.Fatal("success did not reset cycle")
	}
}

func TestRetryRoundsResumeInactivePairsButRespectPauseAndManualMode(t *testing.T) {
	m, c, cfg := fixture()
	cfg.Acquisition = "active"
	cfg.ActiveMinutes = 1
	cfg.MaxAttempts = 1
	cfg.MaxRetryRounds = 1
	cfg.RetryRoundIntervalMinutes = 60
	m.Sync(cfg, []Credential{c})
	m.Pick(c, "", time.Now())
	finishFixture(m, c, Result{}, errors.New("failed"), time.Now())
	due := m.Snapshots(c.ID, time.Now())[0].NextAttempt
	var calls int
	probe := func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		calls++
		return good(), nil
	}
	m.Action(c.ID, c.Model, "pause")
	m.Tick(t.Context(), due, probe)
	m.Wait()
	if calls != 0 {
		t.Fatal("paused cooldown restarted")
	}
	m.Action(c.ID, c.Model, "resume")
	m.Tick(t.Context(), due, probe)
	m.Wait()
	if calls != 1 {
		t.Fatal("active window expiry blocked scheduled recovery")
	}
	for _, diagnostic := range []bool{false, true} {
		manager := New()
		cfg.Acquisition = "manual"
		if diagnostic {
			manager = NewDiagnostic()
			cfg.Acquisition = "all"
			manager.Sync(cfg, nil, c)
			manager.QueueManual(c)
		} else {
			manager.Sync(cfg, []Credential{c})
			manager.Action(c.ID, c.Model, "acquire")
		}
		manager.Tick(t.Context(), time.Now(), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
			return Result{}, errors.New("once")
		})
		manager.Wait()
		if manager.Snapshots(c.ID, time.Now())[0].RoundWaiting {
			t.Fatal("manual task scheduled rounds")
		}
		manager.Tick(t.Context(), time.Now().Add(24*time.Hour), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
			t.Error("manual task automatically retried")
			return good(), nil
		})
		manager.Wait()
	}
}

func TestRetryRoundHotReloadRecomputesFromFailureWithoutResettingBudget(t *testing.T) {
	m, c, cfg := fixture()
	cfg.MaxAttempts = 1
	cfg.MaxRetryRounds = 2
	cfg.RetryRoundIntervalMinutes = 30
	m.Sync(cfg, []Credential{c})
	now := time.Now()
	finishFixture(m, c, Result{}, errors.New("fixture"), now)
	for range 3 {
		m.Sync(cfg, []Credential{c})
	}
	if s := m.Snapshots(c.ID, now)[0]; !s.NextAttempt.Equal(now.Add(30 * time.Minute)) {
		t.Fatal("sync moved the cooldown")
	}
	cfg.RetryRoundIntervalMinutes = 60
	m.Sync(cfg, []Credential{c})
	if s := m.Snapshots(c.ID, now)[0]; !s.NextAttempt.Equal(now.Add(time.Hour)) || s.RetryRoundsUsed != 0 {
		t.Fatal("interval update consumed a round or used wrong origin")
	}
	m.Tick(t.Context(), now.Add(time.Hour), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		return Result{}, errors.New("fixture")
	})
	m.Wait()
	cfg.MaxRetryRounds = 0
	m.Sync(cfg, []Credential{c})
	s := m.Snapshots(c.ID, time.Now())[0]
	if !s.Exhausted || s.RoundWaiting || s.RetryRoundsUsed != 1 {
		t.Fatal("lowered budget started over")
	}
	cfg.MaxRetryRounds = 2
	m.Sync(cfg, []Credential{c})
	if s = m.Snapshots(c.ID, time.Now())[0]; !s.RoundWaiting || s.RetryRoundsUsed != 1 {
		t.Fatal("restored budget lost prior round usage")
	}
	m.Sync(cfg, nil)
	m.Tick(t.Context(), now.Add(24*time.Hour), func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		t.Error("retired pair retried")
		return good(), nil
	})
	m.Wait()
}

func TestRetryRoundWaitDoesNotConsumeConcurrency(t *testing.T) {
	m, c, cfg := fixture()
	cfg.MaxAttempts = 1
	cfg.MaxRetryRounds = 1
	other := c
	other.ID = "other"
	other.Owner = "other"
	m.Sync(cfg, []Credential{c, other})
	finishFixture(m, c, Result{}, errors.New("failed"), time.Now())
	m.Tick(t.Context(), time.Now(), func(_ context.Context, selected Credential, _ config.CodexStateOverrideConfig) (Result, error) {
		if selected.ID != other.ID {
			t.Error("cooldown took the slot")
		}
		return good(), nil
	})
	m.Wait()
	if m.Snapshots(other.ID, time.Now())[0].Acquired != 1 || m.Snapshots(c.ID, time.Now())[0].RetryRoundsUsed != 0 {
		t.Fatal("waiting pair blocked another model")
	}
}

func TestRetryRoundDeadlineAndInvalidationPreserveValidCacheBudget(t *testing.T) {
	m, c, cfg := fixture()
	cfg.MaxAttempts = 1
	cfg.MaxRetryRounds = 1
	cfg.RetryRoundIntervalMinutes = 30
	cfg.TTLMinutes = 120
	cfg.InvalidateOnModelMismatch = true
	m.Sync(cfg, []Credential{c})
	now := time.Now()
	finishFixture(m, c, good(), nil, now)
	finishFixture(m, c, Result{}, errors.New("manual renewal failed"), now)
	s := m.Snapshots(c.ID, now)[0]
	due := s.NextAttempt
	var calls int
	m.Tick(t.Context(), due, func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error) {
		calls++
		return Result{}, errors.New("failed")
	})
	m.Wait()
	if calls != 1 {
		t.Fatal("unexpired old State delayed the scheduled recovery round")
	}
	value, _, version, _ := m.PickVersion(c, "", now)
	m.ObserveResponse(c, version, value, "other", 0)
	if s = m.Snapshots(c.ID, now)[0]; !s.Exhausted || s.RoundWaiting || s.Status == "queued" || s.RetryRoundsUsed != 1 {
		t.Fatal("business invalidation replenished an exhausted round budget")
	}
}
