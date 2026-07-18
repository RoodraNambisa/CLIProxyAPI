package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
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
