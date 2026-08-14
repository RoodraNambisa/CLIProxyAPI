package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestAuthRequestWindowLimiterUsesUTCAlignedWindow(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 4, 30, 0, time.FixedZone("test", 8*60*60))
	limiter := newAuthRequestWindowLimiter()
	policy := authRequestLimitPolicy{limit: 1, windowMinutes: 5}

	if acquired, _ := limiter.tryAcquireAt("auth", policy, fixed); !acquired {
		t.Fatal("first acquisition = false, want true")
	}
	if acquired, block := limiter.tryAcquireAt("auth", policy, fixed); acquired {
		t.Fatal("second acquisition = true, want false")
	} else if block.resetIn != 30*time.Second {
		t.Fatalf("resetIn = %v, want 30s", block.resetIn)
	}
	if acquired, _ := limiter.tryAcquireAt("auth", policy, fixed.Add(30*time.Second)); !acquired {
		t.Fatal("next fixed window acquisition = false, want true")
	}
}

func TestAuthRequestWindowLimiterEnforcesConcurrentCap(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 4, 30, 0, time.UTC)
	limiter := newAuthRequestWindowLimiter()
	policy := authRequestLimitPolicy{limit: 10, windowMinutes: 5}

	var wg sync.WaitGroup
	var mu sync.Mutex
	acquired := 0
	for index := 0; index < 100; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := limiter.tryAcquireAt("auth", policy, fixed); ok {
				mu.Lock()
				acquired++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if acquired != policy.limit {
		t.Fatalf("acquired = %d, want %d", acquired, policy.limit)
	}
}

func TestAuthRequestWindowLimiterReservationReleaseRestoresCapacity(t *testing.T) {
	fixed := time.Date(2026, 8, 14, 12, 4, 30, 0, time.UTC)
	limiter := newAuthRequestWindowLimiter()
	policy := authRequestLimitPolicy{limit: 1, windowMinutes: 5}
	reservation, acquired, _ := limiter.reserveAt("auth", policy, fixed)
	if !acquired || reservation == nil || !reservation.Reserved() || !reservation.Consumed() {
		t.Fatalf("reservation = (%v, %v), want consumed pending reservation", reservation, acquired)
	}
	if _, acquiredSecond, block := limiter.reserveAt("auth", policy, fixed); acquiredSecond || !block.limited() {
		t.Fatalf("second reservation = (%v, %#v), want request limit", acquiredSecond, block)
	}
	if !reservation.Release() || reservation.Release() {
		t.Fatal("Release must succeed exactly once")
	}
	if _, acquiredAfterRelease, _ := limiter.reserveAt("auth", policy, fixed); !acquiredAfterRelease {
		t.Fatal("reservation after release = false, want true")
	}
}

func TestAuthRequestWindowLimiterReservationEnforcesConcurrentCap(t *testing.T) {
	fixed := time.Date(2026, 8, 14, 12, 4, 30, 0, time.UTC)
	limiter := newAuthRequestWindowLimiter()
	policy := authRequestLimitPolicy{limit: 1, windowMinutes: 5}
	var acquired atomic.Int32
	reservations := make(chan *authRequestReservation, 100)
	var wg sync.WaitGroup
	for index := 0; index < 100; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reservation, ok, _ := limiter.reserveAt("auth", policy, fixed)
			if ok {
				acquired.Add(1)
				reservations <- reservation
			}
		}()
	}
	wg.Wait()
	close(reservations)
	if got := acquired.Load(); got != 1 {
		t.Fatalf("concurrent reservations = %d, want 1", got)
	}
	for reservation := range reservations {
		reservation.Release()
	}
}

func TestAuthRequestWindowLimiterCommittedReservationCannotRelease(t *testing.T) {
	fixed := time.Date(2026, 8, 14, 12, 4, 30, 0, time.UTC)
	limiter := newAuthRequestWindowLimiter()
	policy := authRequestLimitPolicy{limit: 1, windowMinutes: 5}
	reservation, acquired, _ := limiter.reserveAt("auth", policy, fixed)
	if !acquired || !reservation.Commit() || reservation.Commit() {
		t.Fatal("Commit must succeed exactly once")
	}
	if reservation.Release() {
		t.Fatal("Release after Commit = true, want false")
	}
	if _, acquiredSecond, block := limiter.reserveAt("auth", policy, fixed); acquiredSecond || !block.limited() {
		t.Fatalf("second reservation = (%v, %#v), want committed request limit", acquiredSecond, block)
	}
}

func TestAuthRequestWindowLimiterOldReservationCannotDecrementReplacement(t *testing.T) {
	fixed := time.Date(2026, 8, 14, 12, 4, 30, 0, time.UTC)
	policy := authRequestLimitPolicy{limit: 1, windowMinutes: 5}
	tests := []struct {
		name    string
		replace func(*authRequestWindowLimiter)
		now     time.Time
		policy  authRequestLimitPolicy
	}{
		{name: "window rollover", now: fixed.Add(30 * time.Second), policy: policy},
		{name: "remove and readd", replace: func(limiter *authRequestWindowLimiter) { limiter.remove("auth") }, now: fixed, policy: policy},
		{name: "generation reset", replace: func(limiter *authRequestWindowLimiter) { limiter.reset(2) }, now: fixed, policy: authRequestLimitPolicy{limit: 1, windowMinutes: 5, generation: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limiter := newAuthRequestWindowLimiter()
			oldReservation, acquired, _ := limiter.reserveAt("auth", policy, fixed)
			if !acquired {
				t.Fatal("old reservation = false, want true")
			}
			if test.replace != nil {
				test.replace(limiter)
			}
			newReservation, acquiredNew, _ := limiter.reserveAt("auth", test.policy, test.now)
			if !acquiredNew || !newReservation.Commit() {
				t.Fatal("replacement reservation must commit")
			}
			if !oldReservation.Release() {
				t.Fatal("old reservation release must settle its own state")
			}
			if _, acquiredAfterOldRelease, block := limiter.reserveAt("auth", test.policy, test.now); acquiredAfterOldRelease || !block.limited() {
				t.Fatalf("reservation after old release = (%v, %#v), want replacement count preserved", acquiredAfterOldRelease, block)
			}
		})
	}
}

func TestAuthRequestWindowLimiterNoOpReservationDoesNotConsumeCapacity(t *testing.T) {
	limiter := newAuthRequestWindowLimiter()
	reservation, acquired, _ := limiter.reserveAt("auth", authRequestLimitPolicy{limit: 0, windowMinutes: 5}, time.Now())
	if !acquired || reservation == nil || reservation.Consumed() {
		t.Fatalf("no-op reservation = (%v, %v), want non-consuming reservation", reservation, acquired)
	}
	if !reservation.Commit() || reservation.Commit() {
		t.Fatal("no-op Commit must remain idempotent")
	}
}

func TestAuthRequestSlotReplacingLimitedReservationWithNoOpReleasesCapacity(t *testing.T) {
	fixed := time.Date(2026, 8, 14, 12, 4, 30, 0, time.UTC)
	limiter := newAuthRequestWindowLimiter()
	limitedPolicy := authRequestLimitPolicy{limit: 1, windowMinutes: 5}
	limited, acquired, _ := limiter.reserveAt("limited", limitedPolicy, fixed)
	if !acquired {
		t.Fatal("limited reservation = false, want true")
	}
	slot := &cliproxyexecutor.AuthRequestSlot{}
	slot.Bind(limited)
	noOp, acquiredNoOp, _ := limiter.reserveAt("unlimited", authRequestLimitPolicy{limit: 0, windowMinutes: 5}, fixed)
	if !acquiredNoOp {
		t.Fatal("no-op reservation = false, want true")
	}
	slot.Bind(noOp)
	if _, acquiredAfterReplacement, _ := limiter.reserveAt("limited", limitedPolicy, fixed); !acquiredAfterReplacement {
		t.Fatal("limited capacity remained reserved after no-op replacement")
	}
}

func TestAuthRequestWindowLimiterRemoveClearsDeletedAuth(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 4, 30, 0, time.UTC)
	limiter := newAuthRequestWindowLimiter()
	policy := authRequestLimitPolicy{limit: 1, windowMinutes: 5}
	if acquired, _ := limiter.tryAcquireAt("auth", policy, fixed); !acquired {
		t.Fatal("first acquisition = false, want true")
	}
	limiter.remove("auth")
	if acquired, _ := limiter.tryAcquireAt("auth", policy, fixed); !acquired {
		t.Fatal("acquisition after removal = false, want true")
	}
}

func TestManagerPerAuthRequestLimitDeleteAndReaddClearsCount(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }

	register := func() {
		t.Helper()
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: "reused", Provider: "test"}); errRegister != nil {
			t.Fatalf("register auth: %v", errRegister)
		}
	}
	register()
	if selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil); errPick != nil || selected == nil {
		t.Fatalf("first pick = (%v, %v), want auth", selected, errPick)
	}
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), "reused"); errDelete != nil {
		t.Fatalf("delete auth: %v", errDelete)
	}
	register()
	if selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil); errPick != nil || selected == nil {
		t.Fatalf("pick after re-add = (%v, %v), want cleared request count", selected, errPick)
	}
}

func TestAuthRequestWindowLimiterHandlesMaximumWindow(t *testing.T) {
	limiter := newAuthRequestWindowLimiter()
	policy := normalizeAuthRequestLimitPolicy(authRequestLimitPolicy{limit: 1, windowMinutes: int(^uint(0) >> 1)})
	now := time.Now()
	if acquired, _ := limiter.tryAcquireAt("auth", policy, now); !acquired {
		t.Fatal("maximum-window acquisition = false, want true")
	}
	if acquired, block := limiter.tryAcquireAt("auth", policy, now); acquired {
		t.Fatal("second maximum-window acquisition = true, want false")
	} else if maxReset := time.Duration(policy.windowMinutes) * time.Minute; block.resetIn <= 0 || block.resetIn > maxReset {
		t.Fatalf("maximum-window resetIn = %v, want a positive representable duration", block.resetIn)
	}
}

func TestAuthRequestWindowLimiterRejectsStalePolicyWithoutClearingCurrentCount(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	limiter := newAuthRequestWindowLimiter()
	oldPolicy := authRequestLimitPolicy{limit: 1, windowMinutes: 5, generation: 1}
	limiter.reset(2)
	currentPolicy := authRequestLimitPolicy{limit: 1, windowMinutes: 10, generation: 2}
	if acquired, _ := limiter.tryAcquireAt("auth", currentPolicy, fixed); !acquired {
		t.Fatal("current-policy acquisition = false, want true")
	}
	if acquired, block := limiter.tryAcquireAt("auth", oldPolicy, fixed); acquired || !block.stalePolicy {
		t.Fatalf("stale-policy acquisition = (%v, %#v), want stale rejection", acquired, block)
	}
	if acquired, block := limiter.tryAcquireAt("auth", currentPolicy, fixed); acquired || !block.limited() {
		t.Fatalf("second current-policy acquisition = (%v, %#v), want request limit", acquired, block)
	}
}

func TestAuthRequestWindowLimiterRejectsStaleDisabledPolicy(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	limiter := newAuthRequestWindowLimiter()
	oldDisabledPolicy := authRequestLimitPolicy{limit: 0, windowMinutes: 5, generation: 1}
	limiter.reset(2)
	currentPolicy := authRequestLimitPolicy{limit: 1, windowMinutes: 5, generation: 2}
	if acquired, _ := limiter.tryAcquireAt("auth", currentPolicy, fixed); !acquired {
		t.Fatal("current-policy acquisition = false, want true")
	}
	if acquired, block := limiter.tryAcquireAt("auth", oldDisabledPolicy, fixed); acquired || !block.stalePolicy {
		t.Fatalf("stale disabled-policy acquisition = (%v, %#v), want stale rejection", acquired, block)
	}
	if acquired, block := limiter.tryAcquireAt("auth", currentPolicy, fixed); acquired || !block.limited() {
		t.Fatalf("second current-policy acquisition = (%v, %#v), want preserved request limit", acquired, block)
	}
}

func TestAuthRequestLimitPolicyForRoutingAuthAppliesProviderPlanInheritance(t *testing.T) {
	priorityLimit := 10
	priorityWindow := 5
	proLimit := 3
	plusWindow := 15
	teamLimit := 4
	routing := internalconfig.RoutingConfig{
		PerAuthRequestLimit:         100,
		PerAuthRequestWindowMinutes: 60,
		PriorityOverrides: []internalconfig.RoutingPriorityOverride{{
			Priority:                    0,
			PerAuthRequestLimit:         &priorityLimit,
			PerAuthRequestWindowMinutes: &priorityWindow,
			SubscriptionOverrides: []internalconfig.RoutingSubscriptionOverride{
				{Providers: []string{"codex"}, PlanTypes: []string{"pro"}, PerAuthRequestLimit: &proLimit},
				{PlanTypes: []string{"plus"}, PerAuthRequestWindowMinutes: &plusWindow},
				{PlanTypes: []string{"team"}, PerAuthRequestLimit: &teamLimit},
			},
		}},
	}

	tests := []struct {
		name       string
		auth       *Auth
		wantLimit  int
		wantWindow int
	}{
		{
			name:       "provider-specific pro",
			auth:       &Auth{Provider: "codex", Attributes: map[string]string{"plan_type": "Pro"}},
			wantLimit:  3,
			wantWindow: 5,
		},
		{
			name:       "same plan outside provider scope",
			auth:       &Auth{Provider: "chatgpt-web", Metadata: map[string]any{"plan_type": "pro"}},
			wantLimit:  10,
			wantWindow: 5,
		},
		{
			name:       "all-provider plus inherits limit",
			auth:       &Auth{Provider: "chatgpt-web", Metadata: map[string]any{"plan_type": "ChatGPTPlusPlan"}},
			wantLimit:  10,
			wantWindow: 15,
		},
		{
			name:       "unknown plan falls back",
			auth:       &Auth{Provider: "codex"},
			wantLimit:  10,
			wantWindow: 5,
		},
		{
			name:       "business wrapper uses existing team profile",
			auth:       &Auth{Provider: "chatgpt-web", Metadata: map[string]any{"plan_type": "ChatGPTBusinessPlan"}},
			wantLimit:  4,
			wantWindow: 5,
		},
		{
			name:       "codex self serve business uses team profile",
			auth:       &Auth{Provider: "codex", Attributes: map[string]string{"plan_type": "Self_serve_business_usage_based"}},
			wantLimit:  4,
			wantWindow: 5,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := authRequestLimitPolicyForRoutingAuth(routing, test.auth)
			if policy.limit != test.wantLimit || policy.windowMinutes != test.wantWindow {
				t.Fatalf("policy = %+v, want limit=%d window=%d", policy, test.wantLimit, test.wantWindow)
			}
		})
	}
}

func TestSchedulerPerAuthRequestLimitUsesSubscriptionWithinPriority(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	proLimit := 1
	plusLimit := 2
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{
		Strategy:                    "fill-first",
		PerAuthRequestWindowMinutes: 5,
		PriorityOverrides: []internalconfig.RoutingPriorityOverride{{
			Priority: 0,
			SubscriptionOverrides: []internalconfig.RoutingSubscriptionOverride{
				{PlanTypes: []string{"pro"}, PerAuthRequestLimit: &proLimit},
				{PlanTypes: []string{"plus"}, PerAuthRequestLimit: &plusLimit},
			},
		}},
	}})
	fixed := time.Date(2026, 7, 29, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
	for _, auth := range []*Auth{
		{ID: "a-pro", Provider: "test", Attributes: map[string]string{"plan_type": "pro"}},
		{ID: "b-plus", Provider: "test", Metadata: map[string]any{"plan_type": "plus"}},
	} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatalf("register %s: %v", auth.ID, errRegister)
		}
	}

	wantIDs := []string{"a-pro", "b-plus", "b-plus"}
	for index, wantID := range wantIDs {
		selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil || selected == nil || selected.ID != wantID {
			t.Fatalf("pick %d = (%v, %v), want %s", index, selected, errPick, wantID)
		}
	}
	if selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil); selected != nil || !isAuthRequestLimitedError(errPick) {
		t.Fatalf("final pick = (%v, %T %v), want auth_request_limited", selected, errPick, errPick)
	}
}

func TestSchedulerPerAuthRequestLimitUsesSubscriptionAcrossStrategies(t *testing.T) {
	for _, strategy := range []string{"round-robin", "random"} {
		t.Run(strategy, func(t *testing.T) {
			manager := NewManager(nil, &RoundRobinSelector{}, nil)
			manager.RegisterExecutor(schedulerTestExecutor{})
			proLimit := 1
			plusLimit := 2
			manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{
				Strategy:                    strategy,
				PerAuthRequestWindowMinutes: 5,
				PriorityOverrides: []internalconfig.RoutingPriorityOverride{{
					Priority: 0,
					SubscriptionOverrides: []internalconfig.RoutingSubscriptionOverride{
						{PlanTypes: []string{"pro"}, PerAuthRequestLimit: &proLimit},
						{PlanTypes: []string{"plus"}, PerAuthRequestLimit: &plusLimit},
					},
				}},
			}})
			fixed := time.Date(2026, 7, 29, 12, 0, 10, 0, time.UTC)
			manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
			for _, auth := range []*Auth{
				{ID: "a-pro", Provider: "test", Attributes: map[string]string{"plan_type": "pro"}},
				{ID: "b-plus", Provider: "test", Metadata: map[string]any{"plan_type": "plus"}},
			} {
				if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
					t.Fatalf("register %s: %v", auth.ID, errRegister)
				}
			}

			counts := make(map[string]int)
			for index := 0; index < proLimit+plusLimit; index++ {
				selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil)
				if errPick != nil || selected == nil {
					t.Fatalf("pick %d = (%v, %v), want auth", index, selected, errPick)
				}
				counts[selected.ID]++
			}
			if counts["a-pro"] != proLimit || counts["b-plus"] != plusLimit {
				t.Fatalf("counts = %#v, want pro=%d plus=%d", counts, proLimit, plusLimit)
			}
			if selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil); selected != nil || !isAuthRequestLimitedError(errPick) {
				t.Fatalf("final pick = (%v, %T %v), want auth_request_limited", selected, errPick, errPick)
			}
		})
	}
}

func TestLegacySelectorPerAuthRequestLimitUsesSubscription(t *testing.T) {
	manager := NewManager(nil, &trackingSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	proLimit := 1
	plusLimit := 2
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{
		PerAuthRequestWindowMinutes: 5,
		PriorityOverrides: []internalconfig.RoutingPriorityOverride{{
			Priority: 0,
			SubscriptionOverrides: []internalconfig.RoutingSubscriptionOverride{
				{PlanTypes: []string{"pro"}, PerAuthRequestLimit: &proLimit},
				{PlanTypes: []string{"plus"}, PerAuthRequestLimit: &plusLimit},
			},
		}},
	}})
	fixed := time.Date(2026, 7, 29, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
	for _, auth := range []*Auth{
		{ID: "a-pro", Provider: "test", Attributes: map[string]string{"plan_type": "pro"}},
		{ID: "b-plus", Provider: "test", Metadata: map[string]any{"plan_type": "plus"}},
	} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatalf("register %s: %v", auth.ID, errRegister)
		}
	}

	for index, wantID := range []string{"b-plus", "b-plus", "a-pro"} {
		selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil || selected == nil || selected.ID != wantID {
			t.Fatalf("pick %d = (%v, %v), want %s", index, selected, errPick, wantID)
		}
	}
	if selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil); selected != nil || !isAuthRequestLimitedError(errPick) {
		t.Fatalf("final pick = (%v, %T %v), want auth_request_limited", selected, errPick, errPick)
	}
}

func TestSchedulerSubscriptionExplicitZeroUsesLegacyFillFirstRPM(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	disabled := 0
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{
		Strategy:                    "fill-first",
		FillFirstPerAuthRPM:         1,
		PerAuthRequestLimit:         1,
		PerAuthRequestWindowMinutes: 5,
		PriorityOverrides: []internalconfig.RoutingPriorityOverride{{
			Priority: 0,
			SubscriptionOverrides: []internalconfig.RoutingSubscriptionOverride{{
				PlanTypes:           []string{"plus"},
				PerAuthRequestLimit: &disabled,
			}},
		}},
	}})
	fixed := time.Date(2026, 7, 29, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
	manager.scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	for _, auth := range []*Auth{
		{ID: "a-plus", Provider: "test", Metadata: map[string]any{"plan_type": "plus"}},
		{ID: "b-plus", Provider: "test", Metadata: map[string]any{"plan_type": "plus"}},
	} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatalf("register %s: %v", auth.ID, errRegister)
		}
	}

	for index, wantID := range []string{"a-plus", "b-plus"} {
		selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil || selected == nil || selected.ID != wantID {
			t.Fatalf("pick %d = (%v, %v), want %s", index, selected, errPick, wantID)
		}
	}
}

func TestEarlierAvailabilityBlockerUsesEarliestReset(t *testing.T) {
	later := newAuthRequestLimitedError(authRequestLimitBlock{limit: 1, windowMinutes: 5, resetIn: 2 * time.Minute})
	earliest := newModelCooldownError("model", "antigravity", 30*time.Second)
	rpm := newAuthRPMLimitedError(time.Minute)

	got := earlierAvailabilityBlocker(nil, later)
	got = earlierAvailabilityBlocker(got, earliest)
	got = earlierAvailabilityBlocker(got, rpm)
	if got != earliest {
		t.Fatalf("earliest blocker = %T %v, want cooldown blocker", got, got)
	}
}

func TestEarlierAvailabilityBlockerUsesUpstreamRetryAfter(t *testing.T) {
	requestLimit := newAuthRequestLimitedError(authRequestLimitBlock{limit: 1, windowMinutes: 5, resetIn: time.Minute})
	retryAfterErr := &retryAfterStatusError{
		status:     http.StatusTooManyRequests,
		message:    "retry shortly",
		retryAfter: 10 * time.Second,
	}
	if got := earlierAvailabilityBlocker(requestLimit, retryAfterErr); got != retryAfterErr {
		t.Fatalf("earliest blocker = %T %v, want upstream Retry-After error", got, got)
	}
}

func TestPreferAuthRequestLimitErrorUsesEarliestLegacyLimiter(t *testing.T) {
	requestBlock := authRequestLimitBlock{limit: 1, windowMinutes: 5, resetIn: 30 * time.Second}
	rpmLater := newAuthRPMLimitedError(time.Minute)
	if got := preferAuthRequestLimitError(rpmLater, requestBlock); !isAuthRequestLimitedError(got) {
		t.Fatalf("preferred error = %T %v, want earlier generic request limit", got, got)
	}
	rpmEarlier := newAuthRPMLimitedError(10 * time.Second)
	if got := preferAuthRequestLimitError(rpmEarlier, requestBlock); got != rpmEarlier {
		t.Fatalf("preferred error = %T %v, want earlier RPM limit", got, got)
	}
}

func TestManagerLegacyStrictSessionIgnoresUnboundAuthRequestLimit(t *testing.T) {
	for _, testCase := range []struct {
		name string
		pick func(*Manager, cliproxyexecutor.Options) error
	}{
		{
			name: "single",
			pick: func(manager *Manager, opts cliproxyexecutor.Options) error {
				_, _, errPick := manager.pickNextLegacy(t.Context(), "test", "", opts, nil)
				return errPick
			},
		},
		{
			name: "mixed",
			pick: func(manager *Manager, opts cliproxyexecutor.Options) error {
				_, _, _, errPick := manager.pickNextMixedLegacy(t.Context(), []string{"test"}, "", opts, nil, nil)
				return errPick
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			failover := false
			selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, Failover: &failover})
			manager := NewManager(nil, selector, nil)
			manager.RegisterExecutor(schedulerTestExecutor{})
			manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 1}})
			fixed := time.Date(2026, 7, 18, 12, 0, 50, 0, time.UTC)
			manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
			for _, auth := range []*Auth{
				{ID: "bound", Provider: "test", Unavailable: true, CooldownScope: cooldownScopeAuth, NextRetryAfter: fixed.Add(time.Minute)},
				{ID: "unbound", Provider: "test"},
			} {
				if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
					t.Fatalf("register %s: %v", auth.ID, errRegister)
				}
			}
			policy := manager.routingAuthRequestLimitPolicyForPriority(0)
			if acquired, _ := manager.authRequestLimiter().tryAcquireAt("unbound", policy, fixed); !acquired {
				t.Fatal("failed to consume unbound auth request quota")
			}
			if acquired, _ := manager.authRequestLimiter().tryAcquireAt("bound", policy, fixed); !acquired {
				t.Fatal("failed to consume bound auth request quota")
			}
			opts := cliproxyexecutor.Options{Headers: http.Header{"Session-Id": {"strict-limited-session"}}}
			selector.BindSession(t.Context(), "test", "", opts, "bound")

			errPick := testCase.pick(manager, opts)
			if isAuthRequestLimitedError(errPick) {
				t.Fatalf("strict session error = %T %v, must not use unbound auth request limit", errPick, errPick)
			}
			var authErr *Error
			if !errors.As(errPick, &authErr) || authErr == nil || authErr.Code != "session_bound_auth_unavailable" {
				t.Fatalf("strict session error = %T %v, want session_bound_auth_unavailable", errPick, errPick)
			}
		})
	}
}

func TestSchedulerPerAuthRequestLimitSupportsAllStrategies(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		selector Selector
	}{
		{name: "round-robin", selector: &RoundRobinSelector{}},
		{name: "random", selector: &RandomSelector{}},
		{name: "fill-first", selector: &FillFirstSelector{}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixed := time.Date(2026, 7, 18, 12, 1, 10, 0, time.UTC)
			scheduler := newSchedulerForTest(testCase.selector,
				&Auth{ID: "a", Provider: "gemini"},
				&Auth{ID: "b", Provider: "gemini"},
			)
			scheduler.requestLimiter.now = func() time.Time { return fixed }
			scheduler.setRoutingConfig(internalconfig.RoutingConfig{
				PerAuthRequestLimit:         1,
				PerAuthRequestWindowMinutes: 5,
			})

			seen := make(map[string]struct{})
			for index := 0; index < 2; index++ {
				picked, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
				if errPick != nil || picked == nil {
					t.Fatalf("pick #%d = (%v, %v), want auth", index, picked, errPick)
				}
				seen[picked.ID] = struct{}{}
			}
			if len(seen) != 2 {
				t.Fatalf("selected IDs = %v, want both credentials", seen)
			}
			_, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
			limitErr, ok := errPick.(*authRequestLimitedError)
			if !ok {
				t.Fatalf("third pick error = %T %v, want *authRequestLimitedError", errPick, errPick)
			}
			if got := limitErr.Headers().Get("Retry-After"); got != "230" {
				t.Fatalf("Retry-After = %q, want 230", got)
			}
			var body map[string]map[string]any
			if errJSON := json.Unmarshal([]byte(limitErr.Error()), &body); errJSON != nil {
				t.Fatalf("decode error body: %v", errJSON)
			}
			if body["error"]["code"] != "auth_request_limited" || body["error"]["limit"] != float64(1) || body["error"]["window_minutes"] != float64(5) {
				t.Fatalf("error body = %#v", body)
			}
		})
	}
}

func TestSchedulerPerAuthRequestLimitFallsThroughPriorityAndHonorsOverride(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	lowerLimit := 2
	lowerWindow := 10
	scheduler := newSchedulerForTest(&FillFirstSelector{},
		&Auth{ID: "high", Provider: "gemini", Attributes: map[string]string{"priority": "1"}},
		&Auth{ID: "low", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
	)
	scheduler.requestLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{
		PerAuthRequestLimit:         1,
		PerAuthRequestWindowMinutes: 5,
		PriorityOverrides: []internalconfig.RoutingPriorityOverride{
			{Priority: 0, PerAuthRequestLimit: &lowerLimit, PerAuthRequestWindowMinutes: &lowerWindow},
		},
	})

	for index, wantID := range []string{"high", "low", "low"} {
		picked, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil || picked == nil || picked.ID != wantID {
			t.Fatalf("pick #%d = (%v, %v), want %s", index, picked, errPick, wantID)
		}
	}
}

func TestSchedulerPerAuthRequestLimitPreservesFillFirstGroups(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(&FillFirstSelector{Range: 2},
		&Auth{ID: "a", Provider: "gemini"},
		&Auth{ID: "b", Provider: "gemini"},
		&Auth{ID: "c", Provider: "gemini"},
	)
	scheduler.requestLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{
		FillFirstRange:              2,
		PerAuthRequestLimit:         1,
		PerAuthRequestWindowMinutes: 5,
	})

	firstGroup := make(map[string]struct{})
	for index := 0; index < 2; index++ {
		picked, errPick := scheduler.pickSingle(t.Context(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil || picked == nil {
			t.Fatalf("first-group pick #%d = (%v, %v)", index, picked, errPick)
		}
		firstGroup[picked.ID] = struct{}{}
	}
	if _, hasA := firstGroup["a"]; !hasA {
		t.Fatalf("first group = %v, missing a", firstGroup)
	}
	if _, hasB := firstGroup["b"]; !hasB {
		t.Fatalf("first group = %v, missing b", firstGroup)
	}
	picked, errPick := scheduler.pickSingle(t.Context(), "gemini", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil || picked == nil || picked.ID != "c" {
		t.Fatalf("second-group pick = (%v, %v), want c", picked, errPick)
	}
}

func TestSchedulerPerAuthRequestLimitExplicitZeroKeepsLegacyRPM(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	disabled := 0
	scheduler := newSchedulerForTest(&FillFirstSelector{},
		&Auth{ID: "a", Provider: "gemini"},
		&Auth{ID: "b", Provider: "gemini"},
	)
	scheduler.requestLimiter.now = func() time.Time { return fixed }
	scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{
		FillFirstPerAuthRPM: 1,
		PerAuthRequestLimit: 3,
		PriorityOverrides: []internalconfig.RoutingPriorityOverride{
			{Priority: 0, PerAuthRequestLimit: &disabled},
		},
	})

	for index, wantID := range []string{"a", "b"} {
		picked, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil || picked == nil || picked.ID != wantID {
			t.Fatalf("pick #%d = (%v, %v), want %s", index, picked, errPick, wantID)
		}
	}
}

func TestSchedulerPerAuthRequestLimitResetsOnlyWhenConfigChanges(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(&RoundRobinSelector{}, &Auth{ID: "a", Provider: "gemini"})
	scheduler.requestLimiter.now = func() time.Time { return fixed }
	routing := internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}
	scheduler.setRoutingConfig(routing)
	if _, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil); errPick != nil {
		t.Fatalf("first pick error = %v", errPick)
	}
	scheduler.setRoutingConfig(routing)
	if _, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil); !isAuthRequestLimitedError(errPick) {
		t.Fatalf("same-config pick error = %T %v, want request limit", errPick, errPick)
	}
	routing.PerAuthRequestWindowMinutes = 10
	scheduler.setRoutingConfig(routing)
	if _, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil); errPick != nil {
		t.Fatalf("changed-config pick error = %v", errPick)
	}
}

func TestSchedulerPerAuthRequestLimitMixedAndWebsocketPaths(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(&RoundRobinSelector{},
		&Auth{ID: "codex-ws", Provider: "codex", Attributes: map[string]string{"websockets": "true"}},
		&Auth{ID: "codex-http", Provider: "codex"},
		&Auth{ID: "gemini", Provider: "gemini"},
	)
	scheduler.requestLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5})
	wsCtx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	first, errFirst := scheduler.pickSingle(wsCtx, "codex", "", cliproxyexecutor.Options{}, nil)
	if errFirst != nil || first == nil || first.ID != "codex-ws" {
		t.Fatalf("websocket first pick = (%v, %v), want codex-ws", first, errFirst)
	}
	second, errSecond := scheduler.pickSingle(wsCtx, "codex", "", cliproxyexecutor.Options{}, nil)
	if errSecond != nil || second == nil || second.ID != "codex-http" {
		t.Fatalf("websocket fallback pick = (%v, %v), want codex-http", second, errSecond)
	}
	mixed, provider, errMixed := scheduler.pickMixed(context.Background(), []string{"codex", "gemini"}, "", cliproxyexecutor.Options{}, nil)
	if errMixed != nil || mixed == nil || mixed.ID != "gemini" || provider != "gemini" {
		t.Fatalf("mixed pick = (%v, %q, %v), want gemini", mixed, provider, errMixed)
	}
}

func TestSchedulerPerAuthRequestLimitMixedRoundRobinAdvancesPastActualSelection(t *testing.T) {
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(&RoundRobinSelector{},
		&Auth{ID: "a1", Provider: "alpha"},
		&Auth{ID: "a2", Provider: "alpha"},
		&Auth{ID: "b1", Provider: "beta"},
	)
	scheduler.requestLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5})
	policy := authRequestLimitPolicy{limit: 1, windowMinutes: 5}
	if acquired, _ := scheduler.requestLimiter.tryAcquireAt("a1", policy, fixed); !acquired {
		t.Fatal("failed to consume a1 quota")
	}

	first, firstProvider, errFirst := scheduler.pickMixed(t.Context(), []string{"alpha", "beta"}, "", cliproxyexecutor.Options{}, nil)
	if errFirst != nil || first == nil || first.ID != "a2" || firstProvider != "alpha" {
		t.Fatalf("first mixed pick = (%v, %q, %v), want a2/alpha", first, firstProvider, errFirst)
	}
	second, secondProvider, errSecond := scheduler.pickMixed(t.Context(), []string{"alpha", "beta"}, "", cliproxyexecutor.Options{}, nil)
	if errSecond != nil || second == nil || second.ID != "b1" || secondProvider != "beta" {
		t.Fatalf("second mixed pick = (%v, %q, %v), want b1/beta", second, secondProvider, errSecond)
	}
}

func TestManagerPerAuthRequestLimitSessionAffinityFailover(t *testing.T) {
	for _, failover := range []bool{false, true} {
		t.Run(map[bool]string{false: "strict", true: "failover"}[failover], func(t *testing.T) {
			selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, Failover: &failover})
			manager := NewManager(nil, selector, nil)
			manager.RegisterExecutor(schedulerTestExecutor{})
			for _, authID := range []string{"a", "b"} {
				if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: authID, Provider: "test"}); errRegister != nil {
					t.Fatalf("register %s: %v", authID, errRegister)
				}
			}
			manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
			fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
			manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
			opts := cliproxyexecutor.Options{Headers: http.Header{"Session-Id": {"request-limit-session"}}}

			first, _, errFirst := manager.pickNext(t.Context(), "test", "", opts, nil)
			if errFirst != nil || first == nil || first.ID != "a" {
				t.Fatalf("first pick = (%v, %v), want a", first, errFirst)
			}
			selector.BindSession(t.Context(), "test", "", opts, first.ID)
			second, _, errSecond := manager.pickNext(t.Context(), "test", "", opts, nil)
			if failover {
				if errSecond != nil || second == nil || second.ID != "b" {
					t.Fatalf("failover pick = (%v, %v), want b", second, errSecond)
				}
			} else if !isAuthRequestLimitedError(errSecond) || second != nil {
				t.Fatalf("strict pick = (%v, %T %v), want request limit", second, errSecond, errSecond)
			}
		})
	}
}

func TestManagerPerAuthRequestLimitExplicitZeroKeepsLegacyRPMLegacyPath(t *testing.T) {
	disabled := 0
	selector := NewSessionAffinitySelector(&FillFirstSelector{})
	manager := NewManager(nil, selector, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	for _, authID := range []string{"a", "b"} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: authID, Provider: "test"}); errRegister != nil {
			t.Fatalf("register %s: %v", authID, errRegister)
		}
	}
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{
		FillFirstPerAuthRPM: 1,
		PerAuthRequestLimit: 3,
		PriorityOverrides: []internalconfig.RoutingPriorityOverride{
			{Priority: 0, PerAuthRequestLimit: &disabled},
		},
	}})
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
	manager.scheduler.fillFirstLimiter.now = func() time.Time { return fixed }

	for index, wantID := range []string{"a", "b"} {
		selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil || selected == nil || selected.ID != wantID {
			t.Fatalf("pick #%d = (%v, %v), want %s", index, selected, errPick, wantID)
		}
	}
	selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil)
	if selected != nil || !isAuthRPMLimitedError(errPick) {
		t.Fatalf("third pick = (%v, %T %v), want legacy RPM limit", selected, errPick, errPick)
	}
}

type requestLimitCallbackSelector struct {
	selected *Auth
	onPick   func(*Auth)
	seen     []string
}

type requestLimitStalePolicySelector struct {
	calls  int
	onPick func(int, *Auth)
}

func (s *requestLimitStalePolicySelector) Pick(_ context.Context, _ string, _ string, _ cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	s.calls++
	wantID := map[int]string{1: "b", 2: "c", 3: "b"}[s.calls]
	for _, auth := range auths {
		if auth != nil && auth.ID == wantID {
			if s.onPick != nil {
				s.onPick(s.calls, auth)
			}
			return auth, nil
		}
	}
	return nil, nil
}

func (s *requestLimitCallbackSelector) Pick(_ context.Context, _ string, _ string, _ cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	s.seen = s.seen[:0]
	for _, auth := range auths {
		if auth != nil {
			s.seen = append(s.seen, auth.ID)
		}
	}
	selected := s.selected
	if selected == nil && len(auths) > 0 {
		selected = auths[0]
	}
	if s.onPick != nil && selected != nil {
		s.onPick(selected)
	}
	return selected, nil
}

func TestManagerPerAuthRequestLimitFiltersFullAuthBeforeCustomSelector(t *testing.T) {
	selector := &requestLimitCallbackSelector{}
	manager := NewManager(nil, selector, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	for _, authID := range []string{"a", "b"} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: authID, Provider: "test"}); errRegister != nil {
			t.Fatalf("register %s: %v", authID, errRegister)
		}
	}
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
	policy := manager.routingAuthRequestLimitPolicyForPriority(0)
	if acquired, _ := manager.scheduler.requestLimiter.tryAcquireAt("a", policy, fixed); !acquired {
		t.Fatal("failed to consume auth a quota")
	}

	selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil || selected == nil || selected.ID != "b" {
		t.Fatalf("pick = (%v, %v), want b", selected, errPick)
	}
	if len(selector.seen) != 1 || selector.seen[0] != "b" {
		t.Fatalf("custom selector candidates = %v, want [b]", selector.seen)
	}
}

func TestManagerLegacyStaleGenerationClearsDynamicExclusions(t *testing.T) {
	for _, testCase := range []struct {
		name string
		pick func(*Manager) (*Auth, error)
	}{
		{
			name: "single",
			pick: func(manager *Manager) (*Auth, error) {
				selected, _, errPick := manager.pickNextLegacy(t.Context(), "test", "", cliproxyexecutor.Options{}, nil)
				return selected, errPick
			},
		},
		{
			name: "mixed",
			pick: func(manager *Manager) (*Auth, error) {
				selected, _, _, errPick := manager.pickNextMixedLegacy(t.Context(), []string{"test"}, "", cliproxyexecutor.Options{}, nil, nil)
				return selected, errPick
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			selector := &requestLimitStalePolicySelector{}
			manager := NewManager(nil, selector, nil)
			manager.RegisterExecutor(schedulerTestExecutor{})
			manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
			fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
			manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
			for _, authID := range []string{"a", "b", "c"} {
				if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: authID, Provider: "test"}); errRegister != nil {
					t.Fatalf("register %s: %v", authID, errRegister)
				}
			}
			oldPolicy := manager.routingAuthRequestLimitPolicyForPriority(0)
			if acquired, _ := manager.authRequestLimiter().tryAcquireAt("a", oldPolicy, fixed); !acquired {
				t.Fatal("failed to consume auth a quota")
			}
			newGeneration := oldPolicy.generation + 1
			selector.onPick = func(call int, selected *Auth) {
				switch call {
				case 1:
					if selected.ID != "b" {
						t.Fatalf("first selector auth = %s, want b", selected.ID)
					}
					if acquired, _ := manager.authRequestLimiter().tryAcquireAt("b", oldPolicy, fixed); !acquired {
						t.Fatal("failed to create concurrent auth b limit")
					}
				case 2:
					manager.authRequestLimiter().reset(newGeneration)
				case 3:
					manager.scheduler.mu.Lock()
					manager.scheduler.requestLimitGeneration = newGeneration
					manager.scheduler.mu.Unlock()
				}
			}

			selected, errPick := testCase.pick(manager)
			if errPick != nil || selected == nil || selected.ID != "b" {
				t.Fatalf("selection after stale generation = (%v, %v), want restored auth b", selected, errPick)
			}
			if selector.calls != 3 {
				t.Fatalf("selector calls = %d, want 3", selector.calls)
			}
		})
	}
}

func TestManagerPerAuthRequestLimitRejectsCustomSelectorOutsideCandidates(t *testing.T) {
	selector := &requestLimitCallbackSelector{selected: &Auth{ID: "rogue", Provider: "test"}}
	manager := NewManager(nil, selector, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: "a", Provider: "test"}); errRegister != nil {
		t.Fatalf("register a: %v", errRegister)
	}
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})

	selected, _, errPick := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil)
	if selected != nil {
		t.Fatalf("selected = %v, want nil", selected)
	}
	var authErr *Error
	if !errors.As(errPick, &authErr) || authErr.Code != "auth_not_found" {
		t.Fatalf("pick error = %T %v, want auth_not_found", errPick, errPick)
	}
}

func TestManagerPerAuthRequestLimitCanonicalizesCustomSelectorResult(t *testing.T) {
	disabled := 0
	selector := &requestLimitCallbackSelector{selected: &Auth{ID: "a", Provider: "other", Attributes: map[string]string{"priority": "-1"}}}
	manager := NewManager(nil, selector, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: "a", Provider: "test"}); errRegister != nil {
		t.Fatalf("register a: %v", errRegister)
	}
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{
		PerAuthRequestLimit: 1,
		PriorityOverrides: []internalconfig.RoutingPriorityOverride{
			{Priority: -1, PerAuthRequestLimit: &disabled},
		},
	}})
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }

	selected, _, errFirst := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil)
	if errFirst != nil || selected == nil || selected.Provider != "test" || authPriority(selected) != 0 {
		t.Fatalf("first pick = (%+v, %v), want canonical test auth", selected, errFirst)
	}
	if selected, _, errSecond := manager.pickNext(t.Context(), "test", "", cliproxyexecutor.Options{}, nil); selected != nil || !isAuthRequestLimitedError(errSecond) {
		t.Fatalf("second pick = (%v, %T %v), want request limit", selected, errSecond, errSecond)
	}
}

func TestManagerPerAuthRequestLimitDoesNotPublishFailedSessionSelection(t *testing.T) {
	failover := true
	fallback := &requestLimitCallbackSelector{}
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: fallback, Failover: &failover})
	manager := NewManager(nil, selector, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	for _, authID := range []string{"a", "b"} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: authID, Provider: "test"}); errRegister != nil {
			t.Fatalf("register %s: %v", authID, errRegister)
		}
	}
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
	policy := manager.routingAuthRequestLimitPolicyForPriority(0)
	if acquired, _ := manager.scheduler.requestLimiter.tryAcquireAt("a", policy, fixed); !acquired {
		t.Fatal("failed to consume bound auth quota")
	}
	opts := cliproxyexecutor.Options{Headers: http.Header{"Session-Id": {"rollback-session"}}}
	selector.BindSession(t.Context(), "test", "", opts, "a")
	fallback.onPick = func(auth *Auth) {
		manager.scheduler.requestLimiter.tryAcquireAt(auth.ID, policy, fixed)
	}

	selected, _, errPick := manager.pickNext(t.Context(), "test", "", opts, nil)
	if selected != nil || !isAuthRequestLimitedError(errPick) {
		t.Fatalf("pick = (%v, %T %v), want request limit", selected, errPick, errPick)
	}
	if bound := selector.cachedAuthID("test", "", opts); bound != "a" {
		t.Fatalf("session binding = %q, want existing binding a", bound)
	}
}

func TestPreferAuthRequestLimitErrorUsesEarliestRecovery(t *testing.T) {
	block := newAuthRequestLimitBlock(authRequestLimitPolicy{limit: 1, windowMinutes: 5}, time.Minute)
	shortCooldown := newModelCooldownError("model", "provider", 30*time.Second)
	if got := preferAuthRequestLimitError(shortCooldown, block); got != shortCooldown {
		t.Fatalf("short cooldown result = %T %v, want original cooldown", got, got)
	}
	longCooldown := newModelCooldownError("model", "provider", 2*time.Minute)
	if got := preferAuthRequestLimitError(longCooldown, block); !isAuthRequestLimitedError(got) {
		t.Fatalf("long cooldown result = %T %v, want request limit", got, got)
	}
}

type requestLimitOperationExecutor struct {
	schedulerTestExecutor
	mu    sync.Mutex
	calls []string
}

type requestLimitImmediateChatGPTWebExecutor struct {
	requestLimitOperationExecutor
}

func (*requestLimitImmediateChatGPTWebExecutor) Identifier() string { return "chatgpt-web" }

type requestLimitReservationTestError struct{}

func (requestLimitReservationTestError) Error() string        { return "request limit reservation test error" }
func (requestLimitReservationTestError) SkipAuthResult() bool { return true }

type requestLimitChatGPTWebReservationExecutor struct {
	schedulerTestExecutor
	mu               sync.RWMutex
	commit           bool
	prepareErr       error
	prepareCalls     atomic.Int32
	calls            atomic.Int32
	missingPrepared  atomic.Int32
	preparedByMethod sync.Map
}

func (*requestLimitChatGPTWebReservationExecutor) Identifier() string { return "chatgpt-web" }

func (*requestLimitChatGPTWebReservationExecutor) DeferAuthRequestCommitUntilUpstream() bool {
	return true
}

func (e *requestLimitChatGPTWebReservationExecutor) PrepareProviderRequest(_ context.Context, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options, operation cliproxyexecutor.RequestOperation) (any, error) {
	e.prepareCalls.Add(1)
	e.mu.RLock()
	errPrepare := e.prepareErr
	e.mu.RUnlock()
	if errPrepare != nil {
		return nil, errPrepare
	}
	prepared := &struct {
		operation cliproxyexecutor.RequestOperation
	}{operation: operation}
	e.preparedByMethod.Store(operation, prepared)
	return prepared, nil
}

func (e *requestLimitChatGPTWebReservationExecutor) setPrepareError(errPrepare error) {
	e.mu.Lock()
	e.prepareErr = errPrepare
	e.mu.Unlock()
}

func (e *requestLimitChatGPTWebReservationExecutor) finish(opts cliproxyexecutor.Options, operation cliproxyexecutor.RequestOperation) error {
	e.calls.Add(1)
	prepared, ok := cliproxyexecutor.ProviderPreparedRequest(opts, "chatgpt-web")
	expected, expectedOK := e.preparedByMethod.Load(operation)
	if !ok || !expectedOK || prepared != expected {
		e.missingPrepared.Add(1)
	}
	e.mu.RLock()
	commit := e.commit
	e.mu.RUnlock()
	if commit && opts.AuthRequestSlot != nil {
		opts.AuthRequestSlot.Commit()
	}
	return requestLimitReservationTestError{}
}

func (e *requestLimitChatGPTWebReservationExecutor) Execute(_ context.Context, _ *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, e.finish(opts, cliproxyexecutor.RequestOperationExecute)
}

func (e *requestLimitChatGPTWebReservationExecutor) CountTokens(_ context.Context, _ *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, e.finish(opts, cliproxyexecutor.RequestOperationCount)
}

func (e *requestLimitChatGPTWebReservationExecutor) ExecuteStream(_ context.Context, _ *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, e.finish(opts, cliproxyexecutor.RequestOperationStream)
}

func newRequestLimitChatGPTWebReservationManager(t *testing.T, executor *requestLimitChatGPTWebReservationExecutor) *Manager {
	t.Helper()
	const model = "request-limit-chatgpt-web-reservation"
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(executor)
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
	manager.SetRetryConfig(0, 0, 0)
	manager.scheduler.requestLimiter.now = func() time.Time {
		return time.Date(2026, 8, 14, 12, 0, 10, 0, time.UTC)
	}
	auth := &Auth{
		ID:       "chatgpt-web-auth",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "test-access-token",
			"lifecycle_state": LifecycleStateActive,
		},
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	return manager
}

func TestManagerChatGPTWebRequestReservationReleasesBeforeUpstreamCommit(t *testing.T) {
	executor := &requestLimitChatGPTWebReservationExecutor{}
	manager := newRequestLimitChatGPTWebReservationManager(t, executor)
	request := cliproxyexecutor.Request{Model: "request-limit-chatgpt-web-reservation"}

	for attempt := 0; attempt < 2; attempt++ {
		if _, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, request, cliproxyexecutor.Options{}); errExecute == nil {
			t.Fatalf("Execute() attempt %d error = nil, want test error", attempt+1)
		}
	}
	if calls := executor.calls.Load(); calls != 2 {
		t.Fatalf("upstream attempts = %d, want 2 after uncommitted reservations were released", calls)
	}
	if got := manager.RequestExecutionMetrics(); got != (cliproxyexecutor.RequestExecutionMetricsSnapshot{
		AuthSlotReserved:        2,
		AuthSlotReleased:        2,
		SelectedButNotCommitted: 2,
	}) {
		t.Fatalf("execution metrics = %+v", got)
	}
}

func TestManagerChatGPTWebRequestReservationPersistsAfterUpstreamCommit(t *testing.T) {
	executor := &requestLimitChatGPTWebReservationExecutor{commit: true}
	manager := newRequestLimitChatGPTWebReservationManager(t, executor)
	request := cliproxyexecutor.Request{Model: "request-limit-chatgpt-web-reservation"}

	if _, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, request, cliproxyexecutor.Options{}); errExecute == nil {
		t.Fatal("first Execute() error = nil, want test error")
	}
	if _, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, request, cliproxyexecutor.Options{}); !isAuthRequestLimitedError(errExecute) {
		t.Fatalf("second Execute() error = %T %v, want auth_request_limited", errExecute, errExecute)
	}
	if calls := executor.calls.Load(); calls != 1 {
		t.Fatalf("upstream attempts = %d, want committed first attempt only", calls)
	}
	if got := manager.RequestExecutionMetrics(); got != (cliproxyexecutor.RequestExecutionMetricsSnapshot{
		AuthSlotReserved:   1,
		UpstreamCommitted:  1,
		AuthRequestLimited: 1,
	}) {
		t.Fatalf("execution metrics = %+v", got)
	}
}

func TestManagerChatGPTWebPreflightRejectsBeforeSelectionAndRequestLimit(t *testing.T) {
	executor := &requestLimitChatGPTWebReservationExecutor{}
	executor.setPrepareError(errors.New("deterministic preflight rejection"))
	manager := newRequestLimitChatGPTWebReservationManager(t, executor)
	request := cliproxyexecutor.Request{Model: "request-limit-chatgpt-web-reservation"}
	var selected atomic.Int32
	opts := cliproxyexecutor.Options{Metadata: map[string]any{
		cliproxyexecutor.SelectedAuthCallbackMetadataKey: func(string) { selected.Add(1) },
	}}

	for attempt := 0; attempt < 100; attempt++ {
		if _, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, request, opts); errExecute == nil {
			t.Fatalf("Execute() attempt %d error = nil, want preflight rejection", attempt+1)
		}
	}
	if got := executor.prepareCalls.Load(); got != 100 {
		t.Fatalf("preflight calls = %d, want 100", got)
	}
	if got := selected.Load(); got != 0 {
		t.Fatalf("selected credentials = %d, want 0", got)
	}
	if got := executor.calls.Load(); got != 0 {
		t.Fatalf("upstream attempts = %d, want 0", got)
	}
	if got := manager.RequestExecutionMetrics(); got != (cliproxyexecutor.RequestExecutionMetricsSnapshot{PreflightRejected: 100}) {
		t.Fatalf("metrics after deterministic rejections = %+v", got)
	}

	executor.setPrepareError(nil)
	executor.mu.Lock()
	executor.commit = true
	executor.mu.Unlock()
	if _, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, request, opts); errExecute == nil {
		t.Fatal("first valid Execute() error = nil, want test error")
	}
	if _, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, request, opts); !isAuthRequestLimitedError(errExecute) {
		t.Fatalf("second valid Execute() error = %T %v, want auth_request_limited", errExecute, errExecute)
	}
	if got := executor.calls.Load(); got != 1 {
		t.Fatalf("upstream attempts after valid request = %d, want 1", got)
	}
	if got := executor.missingPrepared.Load(); got != 0 {
		t.Fatalf("missing prepared requests = %d, want 0", got)
	}
	if got := manager.RequestExecutionMetrics(); got != (cliproxyexecutor.RequestExecutionMetricsSnapshot{
		PreflightRejected:  100,
		AuthSlotReserved:   1,
		UpstreamCommitted:  1,
		AuthRequestLimited: 1,
	}) {
		t.Fatalf("metrics after valid request and limit = %+v", got)
	}
}

func TestManagerChatGPTWebPreflightAllowsOtherProviderFallback(t *testing.T) {
	const model = "request-limit-preflight-provider-fallback"
	webExecutor := &requestLimitChatGPTWebReservationExecutor{}
	webExecutor.setPrepareError(cliproxyexecutor.NewProviderIncompatibleRequestPreparationError(errors.New("web-only deterministic rejection")))
	fallbackExecutor := &requestLimitOperationExecutor{}
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(webExecutor)
	manager.RegisterExecutor(fallbackExecutor)
	manager.SetRetryConfig(0, 0, 0)
	for _, auth := range []*Auth{
		{ID: "web-auth", Provider: "chatgpt-web", Status: StatusActive},
		{ID: "fallback-auth", Provider: "test", Status: StatusActive},
	} {
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, errRegister)
		}
	}

	if _, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web", "test"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if got := webExecutor.calls.Load(); got != 0 {
		t.Fatalf("web upstream attempts = %d, want 0", got)
	}
	if got := fallbackExecutor.callIDs(); len(got) != 1 || got[0] != "fallback-auth" {
		t.Fatalf("fallback calls = %v, want [fallback-auth]", got)
	}
}

func TestManagerGlobalPreflightErrorDoesNotFallbackOrSelectCredential(t *testing.T) {
	const model = "request-limit-preflight-global-invalid"
	webExecutor := &requestLimitChatGPTWebReservationExecutor{}
	webExecutor.setPrepareError(cliproxyexecutor.NewGlobalProviderRequestPreparationError(errors.New("malformed JSON payload")))
	fallbackExecutor := &requestLimitOperationExecutor{}
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(webExecutor)
	manager.RegisterExecutor(fallbackExecutor)
	manager.SetRetryConfig(0, 0, 0)
	for _, auth := range []*Auth{
		{ID: "web-auth", Provider: "chatgpt-web", Status: StatusActive},
		{ID: "fallback-auth", Provider: "test", Status: StatusActive},
	} {
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, errRegister)
		}
	}
	var selected atomic.Int32
	opts := cliproxyexecutor.Options{Metadata: map[string]any{
		cliproxyexecutor.SelectedAuthCallbackMetadataKey: func(string) { selected.Add(1) },
	}}

	_, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web", "test"}, cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"broken":`),
	}, opts)
	if errExecute == nil || !strings.Contains(errExecute.Error(), "malformed JSON payload") {
		t.Fatalf("Execute() error = %v, want malformed JSON preflight error", errExecute)
	}
	if got := selected.Load(); got != 0 {
		t.Fatalf("selected credentials = %d, want 0", got)
	}
	if got := webExecutor.calls.Load(); got != 0 {
		t.Fatalf("web upstream attempts = %d, want 0", got)
	}
	if got := fallbackExecutor.callIDs(); len(got) != 0 {
		t.Fatalf("fallback calls = %v, want none", got)
	}
	if got := manager.RequestExecutionMetrics(); got != (cliproxyexecutor.RequestExecutionMetricsSnapshot{PreflightRejected: 1}) {
		t.Fatalf("execution metrics = %+v", got)
	}
}

func TestManagerChatGPTWebPreparedRequestPropagatesAcrossOperations(t *testing.T) {
	for _, testCase := range []struct {
		name string
		run  func(*Manager) error
	}{
		{name: "execute", run: func(manager *Manager) error {
			_, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: "request-limit-chatgpt-web-reservation"}, cliproxyexecutor.Options{})
			return errExecute
		}},
		{name: "count", run: func(manager *Manager) error {
			_, errCount := manager.ExecuteCount(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: "request-limit-chatgpt-web-reservation"}, cliproxyexecutor.Options{})
			return errCount
		}},
		{name: "stream", run: func(manager *Manager) error {
			_, errStream := manager.ExecuteStream(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: "request-limit-chatgpt-web-reservation"}, cliproxyexecutor.Options{})
			return errStream
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			executor := &requestLimitChatGPTWebReservationExecutor{}
			manager := newRequestLimitChatGPTWebReservationManager(t, executor)
			if errRun := testCase.run(manager); errRun == nil {
				t.Fatal("operation error = nil, want test error")
			}
			if got := executor.missingPrepared.Load(); got != 0 {
				t.Fatalf("missing prepared requests = %d, want 0", got)
			}
		})
	}
}

func TestManagerChatGPTWebExecutorWithoutDeferredCommitCapabilityConsumesRequestSlot(t *testing.T) {
	for _, testCase := range []struct {
		name string
		run  func(*Manager) error
	}{
		{name: "execute", run: func(manager *Manager) error {
			_, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: "request-limit-chatgpt-web-immediate"}, cliproxyexecutor.Options{})
			return errExecute
		}},
		{name: "count", run: func(manager *Manager) error {
			_, errCount := manager.ExecuteCount(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: "request-limit-chatgpt-web-immediate"}, cliproxyexecutor.Options{})
			return errCount
		}},
		{name: "stream", run: func(manager *Manager) error {
			result, errStream := manager.ExecuteStream(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: "request-limit-chatgpt-web-immediate"}, cliproxyexecutor.Options{})
			if errStream != nil {
				return errStream
			}
			for range result.Chunks {
			}
			return nil
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			executor := &requestLimitImmediateChatGPTWebExecutor{}
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.RegisterExecutor(executor)
			manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
			manager.SetRetryConfig(0, 0, 0)
			manager.scheduler.requestLimiter.now = func() time.Time {
				return time.Date(2026, 8, 14, 12, 0, 10, 0, time.UTC)
			}
			auth := &Auth{
				ID:       "chatgpt-web-immediate-auth",
				Provider: "chatgpt-web",
				Status:   StatusActive,
				Metadata: map[string]any{
					"access_token":    "test-access-token",
					"lifecycle_state": LifecycleStateActive,
				},
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "request-limit-chatgpt-web-immediate"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
			if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}

			if errRun := testCase.run(manager); errRun != nil {
				t.Fatalf("first operation error = %v", errRun)
			}
			if errRun := testCase.run(manager); !isAuthRequestLimitedError(errRun) {
				t.Fatalf("second operation error = %T %v, want auth_request_limited", errRun, errRun)
			}
			if calls := executor.callIDs(); len(calls) != 1 || calls[0] != auth.ID {
				t.Fatalf("executor calls = %v, want [%s]", calls, auth.ID)
			}
			if got := manager.RequestExecutionMetrics(); got != (cliproxyexecutor.RequestExecutionMetricsSnapshot{
				AuthSlotReserved:   1,
				UpstreamCommitted:  1,
				AuthRequestLimited: 1,
			}) {
				t.Fatalf("execution metrics = %+v", got)
			}
		})
	}
}

func TestAuthRequestSlotUpdatesExecutionDiagnosticsAtSelectionAndCommit(t *testing.T) {
	diagnostics := &cliproxyexecutor.RequestExecutionDiagnostics{}
	slot := &cliproxyexecutor.AuthRequestSlot{}
	slot.SetDiagnostics(diagnostics)
	reservation := &authRequestReservation{noOp: false}

	slot.Bind(reservation)
	selected := diagnostics.Snapshot()
	if !selected.CredentialSelected || selected.UpstreamCommitted || selected.AuthRequestSlotConsumed {
		t.Fatalf("after Bind diagnostics = %+v", selected)
	}
	if !slot.Commit() {
		t.Fatal("Commit() = false, want true")
	}
	committed := diagnostics.Snapshot()
	if !committed.CredentialSelected || !committed.UpstreamCommitted || !committed.AuthRequestSlotConsumed {
		t.Fatalf("after Commit diagnostics = %+v", committed)
	}
}

func TestAuthRequestSlotTracksExecutionMetricsIdempotently(t *testing.T) {
	metrics := &cliproxyexecutor.RequestExecutionMetrics{}
	slot := &cliproxyexecutor.AuthRequestSlot{}
	slot.SetMetrics(metrics)

	released := &authRequestReservation{}
	slot.Bind(released)
	if !slot.Release() {
		t.Fatal("Release() = false, want true")
	}
	if slot.Release() {
		t.Fatal("second Release() = true, want false")
	}

	committed := &authRequestReservation{}
	slot.Bind(committed)
	if !slot.Commit() {
		t.Fatal("Commit() = false, want true")
	}
	if slot.Commit() {
		t.Fatal("second Commit() = true, want false")
	}
	if slot.Release() {
		t.Fatal("Release() after Commit() = true, want false")
	}

	noOp := &authRequestReservation{noOp: true}
	slot.Bind(noOp)
	if !slot.Release() {
		t.Fatal("no-op Release() = false, want true")
	}

	if got := metrics.Snapshot(); got != (cliproxyexecutor.RequestExecutionMetricsSnapshot{
		AuthSlotReserved:        2,
		AuthSlotReleased:        1,
		UpstreamCommitted:       1,
		SelectedButNotCommitted: 2,
	}) {
		t.Fatalf("metrics = %+v", got)
	}
}

func TestManagerAcquireAdditionalAuthRequestReplacesPendingReservation(t *testing.T) {
	fixed := time.Date(2026, 8, 14, 12, 0, 10, 0, time.UTC)
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
	auth := &Auth{ID: "chatgpt-web-auth", Provider: "chatgpt-web", Status: StatusActive}
	slot := &cliproxyexecutor.AuthRequestSlot{}
	policy := manager.routingAuthRequestLimitPolicyForAuth(auth)
	policy.requestSlot = slot
	if acquired, block := manager.authRequestLimiter().tryAcquireAt(auth.ID, policy, fixed); !acquired {
		t.Fatalf("initial reservation failed: %#v", block)
	}
	if errAcquire := manager.acquireAdditionalAuthRequest(auth, &requestLimitChatGPTWebReservationExecutor{}, slot); errAcquire != nil {
		t.Fatalf("acquireAdditionalAuthRequest() error = %v", errAcquire)
	}
	if !slot.Reserved() {
		t.Fatal("replacement reservation is not pending")
	}
	if !slot.Release() {
		t.Fatal("replacement reservation release = false, want true")
	}
}

func (e *requestLimitOperationExecutor) record(auth *Auth) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, auth.ID)
}

func (e *requestLimitOperationExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.record(auth)
	return cliproxyexecutor.Response{}, nil
}

func (e *requestLimitOperationExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.record(auth)
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("ok")}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *requestLimitOperationExecutor) CountTokens(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.record(auth)
	return cliproxyexecutor.Response{}, nil
}

func (e *requestLimitOperationExecutor) callIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.calls...)
}

type requestLimitRetryAfterRecoveryExecutor struct {
	schedulerTestExecutor
	mu         sync.Mutex
	calls      int
	firstErr   error
	retryAfter time.Duration
}

func (e *requestLimitRetryAfterRecoveryExecutor) nextError() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	if e.calls != 1 {
		return nil
	}
	if e.firstErr != nil {
		return e.firstErr
	}
	return &retryAfterStatusError{
		status:     http.StatusTooManyRequests,
		message:    "short upstream cooldown",
		retryAfter: e.retryAfter,
	}
}

func (e *requestLimitRetryAfterRecoveryExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, e.nextError()
}

func (e *requestLimitRetryAfterRecoveryExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, e.nextError()
}

func (e *requestLimitRetryAfterRecoveryExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if err := e.nextError(); err != nil {
		return nil, err
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("ok")}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *requestLimitRetryAfterRecoveryExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

type requestLimitRoundRetryError struct{}

func (requestLimitRoundRetryError) Error() string   { return "retry next request round" }
func (requestLimitRoundRetryError) StatusCode() int { return http.StatusBadGateway }

func TestManagerPerAuthRequestLimitPreservesRetryableErrorWhenFailedAuthHasQuota(t *testing.T) {
	testCases := []struct {
		name   string
		invoke func(*Manager) error
	}{
		{
			name: "execute",
			invoke: func(manager *Manager) error {
				_, err := manager.Execute(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				return err
			},
		},
		{
			name: "count",
			invoke: func(manager *Manager) error {
				_, err := manager.ExecuteCount(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				return err
			},
		},
		{
			name: "stream",
			invoke: func(manager *Manager) error {
				result, err := manager.ExecuteStream(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				if err == nil && result != nil {
					for range result.Chunks {
					}
				}
				return err
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			executor := &requestLimitRetryAfterRecoveryExecutor{firstErr: requestLimitRoundRetryError{}}
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.RegisterExecutor(executor)
			for _, authID := range []string{"a", "b"} {
				if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: authID, Provider: "test", Metadata: map[string]any{"disable_cooling": true}}); errRegister != nil {
					t.Fatalf("register %s: %v", authID, errRegister)
				}
			}
			manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 2, PerAuthRequestWindowMinutes: 5}})
			manager.SetRetryConfig(1, 0, 0)
			fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
			manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
			policy := manager.routingAuthRequestLimitPolicyForPriority(0)
			for index := 0; index < policy.limit; index++ {
				if acquired, _ := manager.authRequestLimiter().tryAcquireAt("b", policy, fixed); !acquired {
					t.Fatalf("consume b quota #%d = false, want true", index+1)
				}
			}

			if errInvoke := testCase.invoke(manager); errInvoke != nil {
				t.Fatalf("invocation error = %T %v, want retry through next request round", errInvoke, errInvoke)
			}
			if calls := executor.callCount(); calls != 2 {
				t.Fatalf("upstream calls = %d, want failure plus one request-round retry", calls)
			}
		})
	}
}

func TestManagerPerAuthRequestLimitPrefersEarlierTriedAuthRecovery(t *testing.T) {
	testCases := []struct {
		name   string
		invoke func(*Manager) error
	}{
		{
			name: "execute",
			invoke: func(manager *Manager) error {
				_, err := manager.Execute(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				return err
			},
		},
		{
			name: "count",
			invoke: func(manager *Manager) error {
				_, err := manager.ExecuteCount(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				return err
			},
		},
		{
			name: "stream",
			invoke: func(manager *Manager) error {
				result, err := manager.ExecuteStream(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				if err == nil && result != nil {
					for range result.Chunks {
					}
				}
				return err
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			executor := &requestLimitRetryAfterRecoveryExecutor{retryAfter: 10 * time.Millisecond}
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.RegisterExecutor(executor)
			for _, authID := range []string{"a", "b"} {
				if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: authID, Provider: "test"}); errRegister != nil {
					t.Fatalf("register %s: %v", authID, errRegister)
				}
			}
			manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 2, PerAuthRequestWindowMinutes: 5}})
			manager.SetRetryConfig(1, 100*time.Millisecond, 0)
			fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
			manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
			policy := manager.routingAuthRequestLimitPolicyForPriority(0)
			for index := 0; index < policy.limit; index++ {
				if acquired, _ := manager.authRequestLimiter().tryAcquireAt("b", policy, fixed); !acquired {
					t.Fatalf("consume b quota #%d = false, want true", index+1)
				}
			}

			if errInvoke := testCase.invoke(manager); errInvoke != nil {
				t.Fatalf("invocation error = %T %v, want recovery through earlier upstream Retry-After", errInvoke, errInvoke)
			}
			if calls := executor.callCount(); calls != 2 {
				t.Fatalf("upstream calls = %d, want failure plus one recovered retry", calls)
			}
		})
	}
}

func TestManagerPerAuthRequestLimitDoesNotWaitForExhaustedFailedAuth(t *testing.T) {
	testCases := []struct {
		name   string
		invoke func(*Manager) error
	}{
		{
			name: "execute",
			invoke: func(manager *Manager) error {
				_, err := manager.Execute(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				return err
			},
		},
		{
			name: "count",
			invoke: func(manager *Manager) error {
				_, err := manager.ExecuteCount(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				return err
			},
		},
		{
			name: "stream",
			invoke: func(manager *Manager) error {
				_, err := manager.ExecuteStream(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				return err
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			executor := &requestLimitRetryAfterRecoveryExecutor{retryAfter: 500 * time.Millisecond}
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.RegisterExecutor(executor)
			for _, authID := range []string{"a", "b"} {
				if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: authID, Provider: "test"}); errRegister != nil {
					t.Fatalf("register %s: %v", authID, errRegister)
				}
			}
			manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
			manager.SetRetryConfig(1, time.Second, 0)
			fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
			manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
			policy := manager.routingAuthRequestLimitPolicyForPriority(0)
			if acquired, _ := manager.authRequestLimiter().tryAcquireAt("b", policy, fixed); !acquired {
				t.Fatal("consume b quota = false, want true")
			}

			started := time.Now()
			errInvoke := testCase.invoke(manager)
			if !isAuthRequestLimitedError(errInvoke) {
				t.Fatalf("invocation error = %T %v, want request limit", errInvoke, errInvoke)
			}
			if elapsed := time.Since(started); elapsed >= executor.retryAfter/2 {
				t.Fatalf("invocation waited %v for exhausted auth Retry-After %v", elapsed, executor.retryAfter)
			}
			if calls := executor.callCount(); calls != 1 {
				t.Fatalf("upstream calls = %d, want no wasted request retry", calls)
			}
		})
	}
}

func TestManagerPerAuthRequestLimitStrictSessionCoversExecutionEntrypoints(t *testing.T) {
	testCases := []struct {
		name   string
		invoke func(*Manager, cliproxyexecutor.Options) error
	}{
		{
			name: "execute",
			invoke: func(manager *Manager, opts cliproxyexecutor.Options) error {
				_, err := manager.Execute(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, opts)
				return err
			},
		},
		{
			name: "count",
			invoke: func(manager *Manager, opts cliproxyexecutor.Options) error {
				_, err := manager.ExecuteCount(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, opts)
				return err
			},
		},
		{
			name: "stream",
			invoke: func(manager *Manager, opts cliproxyexecutor.Options) error {
				result, err := manager.ExecuteStream(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, opts)
				if err == nil && result != nil {
					for range result.Chunks {
					}
				}
				return err
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			failover := false
			selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, Failover: &failover})
			executor := &requestLimitOperationExecutor{}
			manager := NewManager(nil, selector, nil)
			manager.RegisterExecutor(executor)
			for _, authID := range []string{"a", "b"} {
				if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: authID, Provider: "test"}); errRegister != nil {
					t.Fatalf("register %s: %v", authID, errRegister)
				}
			}
			manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
			fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
			manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
			opts := cliproxyexecutor.Options{Headers: http.Header{"Session-Id": {"strict-operation-session"}}}

			if errFirst := testCase.invoke(manager, opts); errFirst != nil {
				t.Fatalf("first invocation error = %v", errFirst)
			}
			if errSecond := testCase.invoke(manager, opts); !isAuthRequestLimitedError(errSecond) {
				t.Fatalf("second invocation error = %T %v, want request limit", errSecond, errSecond)
			}
			if calls := executor.callIDs(); len(calls) != 1 || calls[0] != "a" {
				t.Fatalf("upstream calls = %v, want [a]", calls)
			}
		})
	}
}

func TestManagerAntigravityCreditsUsesGenericRequestLimit(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
	entries := []creditsCandidateEntry{{auth: &Auth{ID: "a", Provider: "antigravity"}, provider: "antigravity", eligible: true}}

	if selected, errPick := manager.pickAntigravityCreditsAtPriority(t.Context(), cliproxyexecutor.Options{}, 0, entries, nil); errPick != nil || selected == nil {
		t.Fatalf("first credits pick = (%v, %v), want auth", selected, errPick)
	}
	if selected, errPick := manager.pickAntigravityCreditsAtPriority(t.Context(), cliproxyexecutor.Options{}, 0, entries, nil); selected != nil || !isAuthRequestLimitedError(errPick) {
		t.Fatalf("second credits pick = (%v, %T %v), want request limit", selected, errPick, errPick)
	}
}

type requestLimitAntigravityExecutor struct {
	schedulerTestExecutor
	mu          sync.Mutex
	executeCall int
	streamCall  int
}

func (e *requestLimitAntigravityExecutor) Identifier() string { return "antigravity" }

func (e *requestLimitAntigravityExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	e.executeCall++
	e.mu.Unlock()
	return cliproxyexecutor.Response{}, nil
}

func (e *requestLimitAntigravityExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.mu.Lock()
	e.streamCall++
	e.mu.Unlock()
	chunks := make(chan cliproxyexecutor.StreamChunk)
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *requestLimitAntigravityExecutor) calls() (int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.executeCall, e.streamCall
}

func TestManagerPerAuthRequestLimitDoesNotTriggerAntigravityCreditsFallback(t *testing.T) {
	const model = "claude-request-limit"
	for _, testCase := range []struct {
		name   string
		invoke func(*Manager) error
	}{
		{
			name: "execute",
			invoke: func(manager *Manager) error {
				_, errExecute := manager.Execute(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return errExecute
			},
		},
		{
			name: "stream",
			invoke: func(manager *Manager) error {
				_, errExecute := manager.ExecuteStream(t.Context(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return errExecute
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			executor := &requestLimitAntigravityExecutor{}
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.RegisterExecutor(executor)
			manager.SetConfig(&internalconfig.Config{
				Routing:       internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5},
				QuotaExceeded: internalconfig.QuotaExceeded{AntigravityCredits: true},
			})
			fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
			manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
			for _, auth := range []*Auth{
				{ID: "active", Provider: "antigravity"},
				{
					ID:             "quota-cooldown",
					Provider:       "antigravity",
					Unavailable:    true,
					CooldownScope:  cooldownScopeAuth,
					NextRetryAfter: time.Now().Add(time.Hour),
					Quota:          QuotaState{Exceeded: true},
				},
			} {
				registry.GetGlobalRegistry().RegisterClient(auth.ID, "antigravity", []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
				if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
					t.Fatalf("register %s: %v", auth.ID, errRegister)
				}
			}
			policy := manager.routingAuthRequestLimitPolicyForPriority(0)
			if acquired, _ := manager.authRequestLimiter().tryAcquireAt("active", policy, fixed); !acquired {
				t.Fatal("failed to consume active auth request quota")
			}

			if errExecute := testCase.invoke(manager); !isAuthRequestLimitedError(errExecute) {
				t.Fatalf("execution error = %T %v, want request limit", errExecute, errExecute)
			}
			if executeCalls, streamCalls := executor.calls(); executeCalls != 0 || streamCalls != 0 {
				t.Fatalf("upstream calls = execute:%d stream:%d, want none", executeCalls, streamCalls)
			}
		})
	}
}

type requestLimitRetryError struct{}

func (requestLimitRetryError) Error() string        { return "retry another auth" }
func (requestLimitRetryError) StatusCode() int      { return http.StatusBadGateway }
func (requestLimitRetryError) SkipAuthResult() bool { return true }
func (requestLimitRetryError) RetryOtherAuth() bool { return true }

func enableManagerRequestLimitForTest(t *testing.T, manager *Manager, limit int) time.Time {
	t.Helper()
	cfg := &internalconfig.Config{}
	if current := manager.currentConfig(); current != nil {
		clone := *current
		cfg = &clone
	}
	cfg.Routing.PerAuthRequestLimit = limit
	cfg.Routing.PerAuthRequestWindowMinutes = 5
	manager.SetConfig(cfg)
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }
	return fixed
}

func TestManagerPerAuthRequestLimitCountsSameAuthModelPoolAttempts(t *testing.T) {
	const alias = "request-limited-model-pool"
	models := []internalconfig.OpenAICompatibilityModel{
		{Name: "first-upstream", Alias: alias},
		{Name: "second-upstream", Alias: alias},
	}

	t.Run("execute", func(t *testing.T) {
		executor := &openAICompatPoolExecutor{
			id:            "pool",
			executeErrors: map[string]error{"first-upstream": requestLimitRetryError{}},
		}
		manager := newOpenAICompatPoolTestManager(t, alias, models, executor)
		enableManagerRequestLimitForTest(t, manager, 1)

		_, errExecute := manager.Execute(t.Context(), []string{"pool"}, cliproxyexecutor.Request{Model: alias}, cliproxyexecutor.Options{})
		if !isAuthRequestLimitedError(errExecute) {
			t.Fatalf("Execute() error = %T %v, want auth_request_limited", errExecute, errExecute)
		}
		if got := executor.ExecuteModels(); len(got) != 1 || got[0] != "first-upstream" {
			t.Fatalf("Execute() models = %v, want only first-upstream", got)
		}
	})

	t.Run("count", func(t *testing.T) {
		executor := &openAICompatPoolExecutor{
			id:          "pool",
			countErrors: map[string]error{"first-upstream": requestLimitRetryError{}},
		}
		manager := newOpenAICompatPoolTestManager(t, alias, models, executor)
		enableManagerRequestLimitForTest(t, manager, 1)

		_, errCount := manager.ExecuteCount(t.Context(), []string{"pool"}, cliproxyexecutor.Request{Model: alias}, cliproxyexecutor.Options{})
		if !isAuthRequestLimitedError(errCount) {
			t.Fatalf("ExecuteCount() error = %T %v, want auth_request_limited", errCount, errCount)
		}
		if got := executor.CountModels(); len(got) != 1 || got[0] != "first-upstream" {
			t.Fatalf("ExecuteCount() models = %v, want only first-upstream", got)
		}
	})

	t.Run("stream", func(t *testing.T) {
		executor := &openAICompatPoolExecutor{
			id:                "pool",
			streamFirstErrors: map[string]error{"first-upstream": requestLimitRetryError{}},
		}
		manager := newOpenAICompatPoolTestManager(t, alias, models, executor)
		enableManagerRequestLimitForTest(t, manager, 1)

		_, errStream := manager.ExecuteStream(t.Context(), []string{"pool"}, cliproxyexecutor.Request{Model: alias}, cliproxyexecutor.Options{})
		if !isAuthRequestLimitedError(errStream) {
			t.Fatalf("ExecuteStream() error = %T %v, want auth_request_limited", errStream, errStream)
		}
		if got := executor.StreamModels(); len(got) != 1 || got[0] != "first-upstream" {
			t.Fatalf("ExecuteStream() models = %v, want only first-upstream", got)
		}
	})
}

func TestManagerPerAuthRequestLimitCountsUnauthorizedRefreshRetry(t *testing.T) {
	tests := []struct {
		name   string
		invoke func(context.Context, *Manager, string) error
		calls  func(*antigravityUnauthorizedRefreshExecutor) []string
	}{
		{
			name: "execute",
			invoke: func(ctx context.Context, manager *Manager, model string) error {
				_, errExecute := manager.Execute(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return errExecute
			},
			calls: func(executor *antigravityUnauthorizedRefreshExecutor) []string { return executor.executeCalls },
		},
		{
			name: "count",
			invoke: func(ctx context.Context, manager *Manager, model string) error {
				_, errCount := manager.ExecuteCount(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return errCount
			},
			calls: func(executor *antigravityUnauthorizedRefreshExecutor) []string { return executor.countCalls },
		},
		{
			name: "stream",
			invoke: func(ctx context.Context, manager *Manager, model string) error {
				_, errStream := manager.ExecuteStream(ctx, []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				return errStream
			},
			calls: func(executor *antigravityUnauthorizedRefreshExecutor) []string { return executor.streamCalls },
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			manager, executor, primary, backup, model := newAntigravityUnauthorizedFixture(t, false)
			fixed := enableManagerRequestLimitForTest(t, manager, 1)
			policy := manager.routingAuthRequestLimitPolicyForPriority(authPriority(backup))
			if acquired, _ := manager.authRequestLimiter().tryAcquireAt(backup.ID, policy, fixed); !acquired {
				t.Fatal("failed to consume backup request quota")
			}

			errInvoke := testCase.invoke(t.Context(), manager, model)
			if !isAuthRequestLimitedError(errInvoke) {
				t.Fatalf("request error = %T %v, want auth_request_limited", errInvoke, errInvoke)
			}
			executor.mu.Lock()
			calls := append([]string(nil), testCase.calls(executor)...)
			refreshCalls := executor.refreshCalls
			executor.mu.Unlock()
			if len(calls) != 1 || calls[0] != primary.ID {
				t.Fatalf("upstream calls = %v, want one primary attempt", calls)
			}
			if refreshCalls != 1 {
				t.Fatalf("refresh calls = %d, want 1", refreshCalls)
			}
		})
	}
}

type requestLimitRetryExecutor struct {
	schedulerTestExecutor
	mu    sync.Mutex
	calls []string
}

func (e *requestLimitRetryExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	e.calls = append(e.calls, auth.ID)
	e.mu.Unlock()
	if auth.ID == "a" {
		return cliproxyexecutor.Response{}, requestLimitRetryError{}
	}
	return cliproxyexecutor.Response{}, nil
}

func (e *requestLimitRetryExecutor) callIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.calls...)
}

func TestManagerPerAuthRequestLimitCountsRetryAttempts(t *testing.T) {
	executor := &requestLimitRetryExecutor{}
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(executor)
	for _, authID := range []string{"a", "b"} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: authID, Provider: "test"}); errRegister != nil {
			t.Fatalf("register %s: %v", authID, errRegister)
		}
	}
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
	fixed := time.Date(2026, 7, 18, 12, 0, 10, 0, time.UTC)
	manager.scheduler.requestLimiter.now = func() time.Time { return fixed }

	if _, errExecute := manager.Execute(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}); errExecute != nil {
		t.Fatalf("first Execute() error = %v", errExecute)
	}
	if got := executor.callIDs(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("first request calls = %v, want [a b]", got)
	}
	if _, errExecute := manager.Execute(t.Context(), []string{"test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}); !isAuthRequestLimitedError(errExecute) {
		t.Fatalf("second Execute() error = %T %v, want request limit", errExecute, errExecute)
	}
	if got := executor.callIDs(); len(got) != 2 {
		t.Fatalf("second request invoked upstream: calls=%v", got)
	}
}
