package auth

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestWeightedRequestSnapshotSurvivesStrategyReloadOnEverySelectionPath(t *testing.T) {
	for _, path := range []string{"single", "mixed", "legacy-single", "legacy-mixed"} {
		t.Run(path, func(t *testing.T) {
			m := NewManager(nil, &WeightedRoundRobinSelector{}, nil)
			m.RegisterExecutor(schedulerTestExecutor{})
			m.RegisterExecutor(weightedNamedExecutor{provider: "other"})
			for _, credential := range []*Auth{weightedTestAuth("zero", 0), weightedTestAuth("positive", 1)} {
				credential.Provider = "test"
				if credential.ID == "positive" && (path == "mixed" || path == "legacy-mixed") {
					credential.Provider = "other"
				}
				if credential.ID == "zero" {
					credential.Attributes["priority"] = "10"
				}
				if _, err := m.Register(WithSkipPersist(t.Context()), credential); err != nil {
					t.Fatal(err)
				}
			}
			old := m.WithRoutingPolicySnapshot(t.Context())
			m.SetSelector(&RoundRobinSelector{})
			m.SetConfig(&config.Config{Routing: config.RoutingConfig{PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 0, Strategy: "fill-first"}}}})
			pick := func(ctx context.Context) (*Auth, error) {
				switch path {
				case "single":
					a, _, err := m.pickNext(ctx, "test", "", core.Options{}, nil)
					return a, err
				case "mixed":
					a, _, _, err := m.pickNextMixed(ctx, []string{"test", "other"}, "", core.Options{}, nil)
					return a, err
				case "legacy-single":
					a, _, err := m.pickNextLegacy(ctx, "test", "", core.Options{}, nil)
					return a, err
				default:
					a, _, _, err := m.pickNextMixedLegacy(ctx, []string{"test", "other"}, "", core.Options{}, nil, nil)
					return a, err
				}
			}
			for _, tc := range []struct {
				ctx  context.Context
				want string
			}{{old, "positive"}, {m.WithRoutingPolicySnapshot(t.Context()), "zero"}} {
				a, err := pick(tc.ctx)
				if err != nil || a == nil || a.ID != tc.want {
					t.Fatalf("selection did not follow request snapshot: want %s, err %v", tc.want, err)
				}
			}
			positive, _ := m.GetByID("positive")
			positive.Disabled = true
			if _, err := m.Update(WithSkipPersist(t.Context()), positive); err != nil {
				t.Fatal(err)
			}
			if a, err := pick(old); a != nil || err == nil {
				t.Fatal("request snapshot froze credential availability")
			}
		})
	}
}

type weightedNamedExecutor struct {
	schedulerTestExecutor
	provider string
}

func (e weightedNamedExecutor) Identifier() string { return e.provider }

func TestWeightedBoundZeroRespectsStrategyAndFailover(t *testing.T) {
	for _, weighted := range []bool{false, true} {
		for _, failover := range []bool{false, true} {
			t.Run(fmt.Sprintf("weighted=%v/failover=%v", weighted, failover), func(t *testing.T) {
				var fallback Selector = &RoundRobinSelector{}
				if weighted {
					fallback = &WeightedRoundRobinSelector{}
				}
				selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: fallback, Failover: &failover})
				t.Cleanup(selector.Stop)
				m := NewManager(nil, selector, nil)
				m.RegisterExecutor(schedulerTestExecutor{})
				for _, credential := range []*Auth{weightedTestAuth("zero", 0), weightedTestAuth("positive", 1)} {
					credential.Provider = "test"
					if _, err := m.Register(WithSkipPersist(t.Context()), credential); err != nil {
						t.Fatal(err)
					}
				}
				opts := core.Options{Headers: http.Header{"Session-Id": {"weighted-bound-session"}}}
				selector.cache.Set("test::codex:weighted-bound-session::", "zero")
				picked, _, err := m.pickNext(m.WithRoutingPolicySnapshot(t.Context()), "test", "", opts, nil)
				if weighted && !failover {
					authErr, ok := err.(*Error)
					if picked != nil || !ok || authErr.Code != "session_bound_auth_unavailable" {
						t.Fatalf("strict binding selected disabled-weight credential: %v", err)
					}
					return
				}
				want := "zero"
				if weighted {
					want = "positive"
				}
				if err != nil || picked == nil || picked.ID != want {
					t.Fatalf("binding selection did not follow strategy: want %s, err %v", want, err)
				}
			})
		}
	}
}

func TestWeightedLegacyReservationCommitsExactlyOnce(t *testing.T) {
	for _, affinity := range []bool{false, true} {
		weighted := &WeightedRoundRobinSelector{}
		var selector Selector = weighted
		if affinity {
			session := NewSessionAffinitySelector(weighted)
			t.Cleanup(session.Stop)
			selector = session
		}
		m := NewManager(nil, selector, nil)
		m.SetConfig(&config.Config{Routing: config.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 1}})
		pool := []*Auth{weightedTestAuth("a", 1), weightedTestAuth("b", 1)}
		fixed := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
		policy := m.routingAuthRequestLimitPolicyForAuth(pool[0])
		if acquired, _ := m.authRequestLimiter().tryAcquireAt("a", policy, fixed); !acquired {
			t.Fatal("failed to simulate another request taking the first slot")
		}
		blocked := authRequestLimitBlock{}
		reservation := weightedRequestReservation{manager: m, now: fixed, blocked: &blocked, rejected: make(map[string]struct{})}
		picked, err := m.pickAvailableAuthWithPriorityPolicy(t.Context(), "codex", "", core.Options{}, pool, reservation.acquire)
		if err != nil || picked.ID != "b" || !reservation.acquire(picked) {
			t.Fatal("weighted selection did not reserve the available credential exactly once")
		}
		if !reflect.DeepEqual(weighted.states["codex:"].current, map[string]int64{"b": 0}) {
			t.Fatal("a failed reservation advanced weighted credits")
		}
		if available, _ := m.authRequestLimiter().availableAt("b", policy, fixed); available {
			t.Fatal("selection failed to consume its slot")
		}
	}
}

func TestWeightedRequestSnapshotConcurrentReload(t *testing.T) {
	m := NewManager(nil, &WeightedRoundRobinSelector{}, nil)
	m.RegisterExecutor(schedulerTestExecutor{})
	for index, weight := range []int{0, 1, 1} {
		a := weightedTestAuth(fmt.Sprint(index), weight)
		a.Provider = "test"
		if _, err := m.Register(WithSkipPersist(t.Context()), a); err != nil {
			t.Fatal(err)
		}
	}
	ctx := m.WithRoutingPolicySnapshot(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 50 {
			m.SetSelector(&RoundRobinSelector{})
			m.SetConfig(&config.Config{Routing: config.RoutingConfig{PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 0, Strategy: "round-robin"}}}})
		}
	})
	for range 50 {
		a, _, err := m.pickNext(ctx, "test", "", core.Options{}, nil)
		if err != nil || a == nil || a.ID == "0" {
			t.Errorf("concurrent reload changed the in-flight strategy: %v", err)
			break
		}
	}
	wg.Wait()
}
