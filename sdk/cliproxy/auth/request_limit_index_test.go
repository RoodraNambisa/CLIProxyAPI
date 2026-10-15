package auth

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestRequestLimitIndexAvoidsRepeatedSaturatedScan(t *testing.T) {
	view, limiter, policy := benchmarkLimitedReadyView(10000, 0)
	if entry, block := view.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter); entry != nil || !block.limited() {
		t.Fatal("expected request limit")
	}
	before := SelectionMetricsSnapshot().CandidateChecks
	for range 100 {
		if entry, block := view.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter); entry != nil || !block.limited() {
			t.Fatal("lost limit diagnostics")
		}
	}
	if got := SelectionMetricsSnapshot().CandidateChecks - before; got != 0 {
		t.Fatalf("rescanned %d known limited candidates", got)
	}
	if len(view.limited.expiry) != 10000 || len(view.limited.available) != 0 {
		t.Fatal("index not bounded by credential membership")
	}
}

func TestSaturatedRequestLimitRecoveryLookupSkipsReadyPool(t *testing.T) {
	now := time.Now()
	shard := &modelScheduler{entries: make(map[string]*scheduledAuth)}
	for i := range 10000 {
		id := fmt.Sprint("ready-", i)
		shard.entries[id] = &scheduledAuth{auth: &Auth{ID: id}, meta: &scheduledAuthMeta{}, state: scheduledStateReady}
	}
	for i := range 3 {
		id := fmt.Sprint("blocked-", i)
		shard.entries[id] = &scheduledAuth{auth: &Auth{ID: id}, meta: &scheduledAuthMeta{}, state: scheduledStateCooldown, nextRetryAt: now.Add(time.Duration(i+1) * time.Minute)}
	}
	shard.rebuildIndexesLocked()
	checks := 0
	next, ok := shard.blockedEarliestAtPriorityLocked(false, 0, func(entry *scheduledAuth) bool {
		checks++
		return entry.auth.ID == "blocked-1"
	})
	if !ok || !next.Equal(now.Add(2*time.Minute)) || checks != 2 {
		t.Fatalf("recovery lookup: next %v, found %v, checks %d", next, ok, checks)
	}
}

func TestRequestLimitIndexMixedSchedulerAndHotUpdate(t *testing.T) {
	scheduler := newSchedulerForTest(&RandomSelector{},
		&Auth{ID: "a", Provider: "alpha"}, &Auth{ID: "b", Provider: "beta"},
	)
	fixed := time.Now().Truncate(time.Minute).Add(time.Second)
	scheduler.requestLimiter.now = func() time.Time { return fixed }
	routing := internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 1}
	scheduler.setRoutingConfig(routing)
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			if picked, _, err := scheduler.pickMixed(t.Context(), []string{"alpha", "beta"}, "", core.Options{}, nil); picked != nil && err == nil {
				accepted.Add(1)
			} else if !isAuthRequestLimitedError(err) {
				t.Errorf("unexpected selection error: %v", err)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 2 {
		t.Fatalf("accepted %d, want two total slots", accepted.Load())
	}
	before := SelectionMetricsSnapshot().CandidateChecks
	for range 10 {
		if _, _, err := scheduler.pickMixed(t.Context(), []string{"alpha", "beta"}, "", core.Options{}, nil); !isAuthRequestLimitedError(err) {
			t.Fatalf("lost request-limit result: %v", err)
		}
	}
	if SelectionMetricsSnapshot().CandidateChecks != before {
		t.Fatal("mixed scheduler rescanned exhausted candidates")
	}
	scheduler.setRoutingConfig(routing)
	if _, _, err := scheduler.pickMixed(t.Context(), []string{"alpha", "beta"}, "", core.Options{}, nil); !isAuthRequestLimitedError(err) {
		t.Fatal("same config reset consumed quota")
	}
	routing.PerAuthRequestLimit = 2
	scheduler.setRoutingConfig(routing)
	for range 4 {
		if _, _, err := scheduler.pickMixed(t.Context(), []string{"alpha", "beta"}, "", core.Options{}, nil); err != nil {
			t.Fatalf("new generation stayed excluded: %v", err)
		}
	}
}

func TestRequestLimitIndexMixedRandomWeightsCredentialsNotProviders(t *testing.T) {
	a := buildReadyView([]*scheduledAuth{{auth: &Auth{ID: "a"}}})
	b := buildReadyView([]*scheduledAuth{{auth: &Auth{ID: "b"}}, {auth: &Auth{ID: "c"}}, {auth: &Auth{ID: "d"}}})
	views := []mixedRandomReadyView{{provider: "alpha", view: &a}, {provider: "beta", view: &b}}
	limiter := newAuthRequestWindowLimiter()
	counts := map[string]int{}
	for range 8000 {
		picked, provider, _ := pickMixedRandomWithRequestLimits(views, nil, func(*Auth) authRequestLimitPolicy { return authRequestLimitPolicy{} }, limiter)
		if picked == nil || (provider == "alpha") != (picked.ID == "a") {
			t.Fatal("lost candidate ownership")
		}
		counts[picked.ID]++
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if counts[id] < 1500 || counts[id] > 2500 {
			t.Fatalf("biased mixed random selection: %v", counts)
		}
	}
}

func TestRequestLimitIndexRestoresReleasedReservationAcrossViews(t *testing.T) {
	now := time.Now()
	limiter := newAuthRequestWindowLimiter()
	limiter.now = func() time.Time { return now }
	auth := &Auth{ID: "chosen"}
	first := buildReadyView([]*scheduledAuth{{auth: auth}})
	second := buildReadyView([]*scheduledAuth{{auth: auth}})
	slot := &core.AuthRequestSlot{}
	policy := func(*Auth) authRequestLimitPolicy {
		return authRequestLimitPolicy{limit: 1, windowMinutes: 1, requestSlot: slot}
	}
	if picked, _ := first.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter); picked == nil {
		t.Fatal("first reservation failed")
	}
	if picked, block := second.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter); picked != nil || !block.limited() {
		t.Fatal("reservation oversubscribed")
	}
	slot.Release()
	if picked, _ := second.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter); picked == nil {
		t.Fatal("released reservation remained excluded")
	}
	slot.Release()
	if picked, _ := first.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter); picked == nil {
		t.Fatal("original view missed release")
	}
	slot.Release()
}

func TestRequestLimitIndexDoesNotPublishStaleBlockAfterReleaseNotification(t *testing.T) {
	for _, change := range []string{"release", "reset", "remove"} {
		t.Run(change, func(t *testing.T) {
			now := time.Now()
			limiter := newAuthRequestWindowLimiter()
			policy := authRequestLimitPolicy{limit: 1, windowMinutes: 1}
			reservation, ok, _ := limiter.reserveAt("chosen", policy, now)
			if !ok {
				t.Fatal("reservation failed")
			}
			_, acquired, oldBlock := limiter.reserveAt("chosen", policy, now)
			if acquired || !oldBlock.limited() {
				t.Fatal("expected exhausted result")
			}
			switch change {
			case "release":
				reservation.Release()
			case "reset":
				limiter.reset(1)
			case "remove":
				limiter.remove("chosen")
			}
			view := buildReadyView([]*scheduledAuth{{auth: &Auth{ID: "chosen"}}})
			index := view.requestLimitIndex(limiter, now)
			// The availability notification is consumed before the old result
			// reaches the view. It must not strand a newly available credential.
			index.exclude(view.flat[0], oldBlock, now)
			if len(index.excluded) != 0 || len(index.available) != 1 {
				t.Fatal("published stale request-limit exclusion")
			}
			if picked, _ := view.pickWithRequestLimit(schedulerStrategyRandom, nil, func(*Auth) authRequestLimitPolicy { return policy }, limiter); picked == nil {
				t.Fatal("lost released capacity")
			}
		})
	}
}

func TestRequestLimitIndexExpiryResetAndMissedChanges(t *testing.T) {
	for _, transition := range []string{"expiry", "reset", "remove", "journal_overflow"} {
		t.Run(transition, func(t *testing.T) {
			now := time.Now().Truncate(time.Minute).Add(time.Second)
			limiter := newAuthRequestWindowLimiter()
			limiter.now = func() time.Time { return now }
			view := buildReadyView([]*scheduledAuth{{auth: &Auth{ID: "chosen"}}})
			policy := func(*Auth) authRequestLimitPolicy { return authRequestLimitPolicy{limit: 1, windowMinutes: 1} }
			view.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter)
			if picked, _ := view.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter); picked != nil {
				t.Fatal("limit not applied")
			}
			switch transition {
			case "expiry":
				now = now.Add(time.Minute)
			case "reset":
				limiter.reset(2)
			case "remove":
				limiter.remove("chosen")
			case "journal_overflow":
				limiter.remove("chosen")
				for i := 0; i < requestLimitChangeHistory+1; i++ {
					limiter.remove(fmt.Sprint("unrelated-", i))
				}
			}
			if picked, _ := view.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter); picked == nil {
				t.Fatal("eligible credential never restored")
			}
		})
	}
}

func TestRequestLimitIndexDoesNotCacheRequestPredicates(t *testing.T) {
	view, limiter, policy := benchmarkLimitedReadyView(100, 10)
	deny := func(*scheduledAuth) bool { return false }
	if entry, _ := view.pickWithRequestLimit(schedulerStrategyRandom, deny, policy, limiter); entry != nil {
		t.Fatal("predicate ignored")
	}
	allowed := func(e *scheduledAuth) bool { return e.auth.ID == "candidate-3" }
	for range 5 {
		entry, _ := view.pickWithRequestLimit(schedulerStrategyRandom, allowed, policy, limiter)
		if entry == nil || entry.auth.ID != "candidate-3" {
			t.Fatal("request-local exclusion leaked")
		}
	}
}

func TestRequestLimitIndexKeepsUniformRandomSelection(t *testing.T) {
	view, limiter, policy := benchmarkLimitedReadyView(60, 10)
	counts := map[string]int{}
	for range 6000 {
		entry, _ := view.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter)
		if entry == nil {
			t.Fatal("ready credential missing")
		}
		counts[entry.auth.ID]++
	}
	if len(counts) != 6 {
		t.Fatalf("selected %d credentials", len(counts))
	}
	for id, n := range counts {
		if n < 700 || n > 1300 {
			t.Fatalf("random distribution is biased for %s: %d", id, n)
		}
	}
}

func TestRequestLimitIndexUpdatesChangedCredentialPolicy(t *testing.T) {
	now := time.Now()
	limiter := newAuthRequestWindowLimiter()
	shard := &modelScheduler{entries: map[string]*scheduledAuth{}, readyByPriority: map[int]*readyBucket{}}
	auth := &Auth{ID: "chosen", Provider: "test"}
	meta := buildScheduledAuthMetaWithSupportedModels(auth, nil)
	shard.upsertEntryLocked(meta, now)
	view := &shard.readyByPriority[0].all
	policy := func(*Auth) authRequestLimitPolicy { return authRequestLimitPolicy{limit: 1, windowMinutes: 1} }
	view.pickWithRequestLimit(schedulerStrategyRandom, nil, policy, limiter)
	updated := auth.Clone()
	updated.Metadata = map[string]any{"plan_type": "new-plan"}
	shard.upsertEntryLocked(buildScheduledAuthMetaWithSupportedModels(updated, nil), now)
	unlimited := func(*Auth) authRequestLimitPolicy { return authRequestLimitPolicy{windowMinutes: 1} }
	if picked, _ := view.pickWithRequestLimit(schedulerStrategyRandom, nil, unlimited, limiter); picked == nil {
		t.Fatal("changed plan kept old limit exclusion")
	}
}
