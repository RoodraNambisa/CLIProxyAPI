package auth

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestLCPPreferencePreservesCapacityAcrossStrategies(t *testing.T) {
	for _, strategy := range []string{"round-robin", "fill-first", "random", "weighted-round-robin"} {
		for _, across := range []bool{false, true} {
			for _, mixed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/across=%t/mixed=%t", strategy, across, mixed), func(t *testing.T) {
					m, previous := newAcrossPriorityFixture(t, strategy, across, false)
					failover := false
					s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: previous.fallback, LCP: true, AcrossPriorities: across, Failover: &failover})
					t.Cleanup(s.Stop)
					m.SetSelector(s)
					m.SetConfig(&config.Config{Routing: config.RoutingConfig{Strategy: strategy, PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}})
					providers := []string{"test"}
					if mixed {
						m.RegisterExecutor(acrossPrioritySecondExecutor{})
						if _, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "second", Provider: "second", Attributes: map[string]string{"priority": "-1"}}); err != nil {
							t.Fatal(err)
						}
						providers = append(providers, "second")
					}
					ctx := affinityCallerContext(t, "caller-a", "test,second")
					opts := lcpOptions("branch", false)
					s.BindSession(ctx, affinityProviderKey(providers), "", opts, "high")
					pick := func(ctx context.Context, opts core.Options) (*Auth, error) {
						if mixed {
							auth, _, _, err := m.pickNextMixed(ctx, providers, "", opts, nil)
							return auth, err
						}
						auth, _, err := m.pickNext(ctx, "test", "", opts, nil)
						return auth, err
					}
					if auth, err := pick(ctx, opts); err != nil || auth == nil || auth.ID != "high" {
						t.Fatalf("first: %v %v", auth, err)
					}
					if auth, err := pick(ctx, opts); err != nil || auth == nil || auth.ID != "low" {
						t.Fatalf("capacity fallback: %v %v", auth, err)
					}
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					if auth, err := pick(cancelled, opts); auth != nil || !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation: %v %v", auth, err)
					}
					for _, operation := range []string{"execute", "count", "stream"} {
						var err error
						switch operation {
						case "execute":
							_, err = m.Execute(cancelled, providers, core.Request{}, opts)
						case "count":
							_, err = m.ExecuteCount(cancelled, providers, core.Request{}, opts)
						case "stream":
							_, err = m.ExecuteStream(cancelled, providers, core.Request{}, opts)
						}
						if !errors.Is(err, context.Canceled) {
							t.Fatalf("%s cancellation: %v", operation, err)
						}
					}
					if preferred := s.historyPreferredAuth(ctx, affinityProviderKey(providers), "", opts); preferred != "high" {
						t.Fatal("selection or cancellation published unsuccessful history")
					}
				})
			}
		}
	}
}

func TestLCPHistoryCannotRestoreADeletedCredentialInstance(t *testing.T) {
	s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{LCP: true})
	t.Cleanup(s.Stop)
	m := NewManager(nil, s, nil)
	old, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "history-instance", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := affinityCallerContext(t, "caller-a", "codex")
	opts := lcpOptions("branch", false)
	s.BindSession(ctx, "codex", "", opts, old.ID)
	if err := m.Delete(WithSkipPersist(ctx), old.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Register(WithSkipPersist(ctx), &Auth{ID: old.ID, Provider: "codex"}); err != nil {
		t.Fatal(err)
	}
	m.bindSessionAffinity(ctx, []string{"codex"}, "", opts, old)
	if preferred := s.historyPreferredAuth(ctx, "codex", "", opts); preferred != "" {
		t.Fatal("old instance restored deleted history")
	}
	current, _ := m.GetByID(old.ID)
	m.bindSessionAffinity(ctx, []string{"codex"}, "", opts, current)
	if preferred := s.historyPreferredAuth(ctx, "codex", "", opts); preferred != old.ID {
		t.Fatal("current instance could not commit successful history")
	}
}
