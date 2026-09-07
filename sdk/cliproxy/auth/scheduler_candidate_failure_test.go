package auth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestSchedulerStoredFailuresRespectProviderPriorityAndPin(t *testing.T) {
	for _, selector := range []Selector{&RoundRobinSelector{}, &FillFirstSelector{}, &RandomSelector{}, &WeightedRoundRobinSelector{}} {
		for _, mixed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%T/mixed=%t", selector, mixed), func(t *testing.T) {
				now := time.Now()
				high := weightedTestAuth(schedulerTestID(t, "high"), 1)
				low := weightedTestAuth(schedulerTestID(t, "low"), 1)
				excluded := weightedTestAuth(schedulerTestID(t, "excluded"), 1)
				unrelated := weightedTestAuth(schedulerTestID(t, "unrelated"), 1)
				high.Attributes["priority"] = "10"
				providers := []string{"codex"}
				if mixed {
					low.Provider = "openai"
					providers = append(providers, "openai")
				}
				unrelated.Provider = "unrelated-provider"
				for i, credential := range []*Auth{high, low, excluded, unrelated} {
					registerSchedulerModels(t, credential.Provider, "tracked-model", credential.ID)
					credential.Unavailable = true
					credential.CooldownScope = cooldownScopeAuth
					credential.NextRetryAfter = now.Add(time.Minute)
					credential.Quota.Exceeded = true
					credential.UpdatedAt = now.Add(time.Duration(i) * time.Hour)
					credential.LastError = &Error{Code: credential.ID, HTTPStatus: 429}
				}
				low.ModelStates = map[string]*ModelState{"tracked-model": {LastError: &Error{Code: "low-model", HTTPStatus: 503, Diagnostic: &ErrorDiagnostic{Stage: "fixture"}}, UpdatedAt: now.Add(-time.Hour)}}
				scheduler := newSchedulerForTest(selector, high, low, excluded, unrelated)
				allowed := func(auth *Auth) bool { return auth.ID != excluded.ID }
				opts := core.Options{Metadata: map[string]any{core.SelectionAttemptMetadataKey: 1}}
				picked, _, errPick := scheduler.pickMixed(t.Context(), providers, "tracked-model(high)", opts, nil, allowed)
				failure := StoredAuthFailureOf(errPick)
				if picked != nil || failure == nil || failure.Code != "low-model" {
					t.Fatal("scheduler lost scoped model failure")
				}
				var cooldown *modelCooldownError
				if !errors.As(errPick, &cooldown) || core.IsUpstreamAttemptError(errPick) {
					t.Fatal("stored failure changed the current error")
				}
				opts.Metadata[core.PinnedAuthMetadataKey] = high.ID
				_, _, pinErr := scheduler.pickMixed(t.Context(), providers, "tracked-model", opts, nil, allowed)
				if failure := StoredAuthFailureOf(pinErr); failure == nil || failure.Code != high.ID {
					t.Fatal("pinned auth borrowed another candidate failure")
				}
				delete(opts.Metadata, core.PinnedAuthMetadataKey)
				_, _, filteredErr := scheduler.pickMixed(t.Context(), providers, "tracked-model", opts, nil, func(auth *Auth) bool { return auth.ID == high.ID })
				if StoredAuthFailureOf(filteredErr) != nil {
					t.Fatal("a filtered lower tier borrowed higher-priority history")
				}
				canceled, cancel := context.WithCancel(t.Context())
				cancel()
				_, _, canceledErr := scheduler.pickMixed(canceled, providers, "tracked-model", opts, nil, allowed)
				if !errors.Is(canceledErr, context.Canceled) || StoredAuthFailureOf(canceledErr) != nil {
					t.Fatal("cancellation was replaced by history")
				}
				updated := low.Clone()
				updated.ModelStates["tracked-model"].LastError.Diagnostic.Stage = "changed"
				scheduler.upsertAuth(updated)
				if StoredAuthFailureOf(errPick).Diagnostic.Stage != "fixture" {
					t.Fatal("scheduler updates mutated a published failure")
				}
			})
		}
	}
}

func TestSchedulerCandidateFailureScopeExcludesReadyAndDisabled(t *testing.T) {
	makeEntry := func(id string, state scheduledState, priority int, websocket bool) *scheduledAuth {
		auth := &Auth{ID: id, LastError: &Error{Code: id}}
		return &scheduledAuth{auth: auth, meta: &scheduledAuthMeta{auth: auth, priority: priority, websocketEnabled: websocket}, state: state}
	}
	shard := &modelScheduler{modelKey: "model", entries: map[string]*scheduledAuth{
		"ready":    makeEntry("ready", scheduledStateReady, 0, true),
		"disabled": makeEntry("disabled", scheduledStateDisabled, 0, true),
		"http":     makeEntry("http", scheduledStateCooldown, 0, false),
		"ws":       makeEntry("ws", scheduledStateBlocked, 0, true),
		"higher":   makeEntry("higher", scheduledStateCooldown, 10, true),
	}}
	if failure := shard.candidateFailuresLocked(true, []int{0}, nil).latest(); failure == nil || failure.Code != "ws" {
		t.Fatal("transport or priority scope was ignored")
	}
	if failure := shard.candidateFailuresLocked(false, []int{}, nil).latest(); failure != nil {
		t.Fatal("empty tier scope widened to every tier")
	}
	if failure := shard.candidateFailuresLocked(false, nil, func(entry *scheduledAuth) bool { return entry.auth.ID == "ready" || entry.auth.ID == "disabled" }).latest(); failure != nil {
		t.Fatal("ready or disabled auth supplied history")
	}
	var absent *modelScheduler
	if absent.candidateFailuresLocked(false, nil, nil).latest() != nil {
		t.Fatal("nil shard supplied history")
	}
}

func TestSchedulerStoredFailuresFollowWinningAvailabilityBlocker(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		for _, mode := range []string{"rpm", "request", "both"} {
			for _, cooldownWins := range []bool{false, true} {
				t.Run(fmt.Sprintf("mixed=%t/%s/cooldown=%t", mixed, mode, cooldownWins), func(t *testing.T) {
					fixed := time.Now().UTC().Add(time.Hour).Truncate(5 * time.Minute).Add(10 * time.Second)
					a := &Auth{ID: schedulerTestID(t, "a"), Provider: "codex", LastError: &Error{Code: "stale-ready", HTTPStatus: 401}}
					c := &Auth{ID: schedulerTestID(t, "c"), Provider: "codex", Unavailable: true, Quota: QuotaState{Exceeded: true}, LastError: &Error{Code: "cooling", HTTPStatus: 429}}
					c.NextRetryAfter = fixed.Add(5 * time.Minute)
					if cooldownWins {
						c.NextRetryAfter = fixed.Add(10 * time.Second)
					}
					pool := []*Auth{a, c}
					providers := []string{"codex"}
					if mixed {
						c.Provider = "openai"
						providers = append(providers, "openai")
					}
					cfg := internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1}
					if mode != "rpm" {
						cfg.PerAuthRequestLimit = 1
						cfg.PerAuthRequestWindowMinutes = 5
					}
					if mode == "both" {
						a.Metadata = map[string]any{"plan_type": "plus"}
						disabled := 0
						cfg.PriorityOverrides = []internalconfig.RoutingPriorityOverride{{Priority: 0, SubscriptionOverrides: []internalconfig.RoutingSubscriptionOverride{{PlanTypes: []string{"plus"}, PerAuthRequestLimit: &disabled}}}}
						pool = append(pool, &Auth{ID: schedulerTestID(t, "b"), Provider: "codex"})
					}
					scheduler := newSchedulerForTest(&FillFirstSelector{}, pool...)
					scheduler.setRoutingConfig(cfg)
					scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
					scheduler.requestLimiter.now = func() time.Time { return fixed }
					for range len(pool) - 1 {
						picked, _, err := scheduler.pickMixed(t.Context(), providers, "", core.Options{}, nil)
						if err != nil || picked == nil {
							t.Fatal("could not consume local request allowance")
						}
					}
					_, _, err := scheduler.pickMixed(t.Context(), providers, "", core.Options{}, nil)
					if isModelCooldownError(err) != cooldownWins {
						t.Fatal("availability winner changed")
					}
					failure := StoredAuthFailureOf(err)
					if cooldownWins {
						if failure == nil || failure.Code != "cooling" {
							t.Fatal("winning cooldown lost its failure")
						}
					} else if failure != nil {
						t.Fatal("local capacity failure borrowed stored history")
					}
				})
			}
		}
	}
}

func TestSchedulerStoredFailureSnapshotsDuringUpdates(t *testing.T) {
	original := weightedTestAuth(schedulerTestID(t, "concurrent"), 1)
	original.Unavailable = true
	original.CooldownScope = cooldownScopeAuth
	original.NextRetryAfter = time.Now().Add(time.Minute)
	original.Quota.Exceeded = true
	original.LastError = &Error{Code: "original", HTTPStatus: 429}
	scheduler := newSchedulerForTest(&RoundRobinSelector{}, original)
	var workers sync.WaitGroup
	workers.Go(func() {
		for i := range 100 {
			updated := original.Clone()
			updated.LastError.Code = fmt.Sprint(i)
			scheduler.upsertAuth(updated)
		}
	})
	for range 4 {
		workers.Go(func() {
			for range 100 {
				_, err := scheduler.pickSingle(t.Context(), "codex", "", core.Options{}, nil)
				failure := StoredAuthFailureOf(err)
				if failure == nil || failure.HTTPStatus != 429 {
					t.Error("concurrent selection lost stored failure")
					return
				}
				failure.Code = "caller-owned"
			}
		})
	}
	workers.Wait()
}
