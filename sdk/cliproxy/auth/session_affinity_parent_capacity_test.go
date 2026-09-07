package auth

import (
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestParentAffinityCapacityIsNotAnEstablishedStrictChildBinding(t *testing.T) {
	for _, rpm := range []bool{false, true} {
		for _, across := range []bool{false, true} {
			for _, mixed := range []bool{false, true} {
				t.Run(fmt.Sprintf("rpm=%t/across=%t/mixed=%t", rpm, across, mixed), func(t *testing.T) {
					failover := false
					s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{Range: 2}, Subagents: true, AcrossPriorities: across, Failover: &failover})
					t.Cleanup(s.Stop)
					m := NewManager(nil, s, nil)
					m.RegisterExecutor(schedulerTestExecutor{})
					routing := config.RoutingConfig{Strategy: "fill-first", FillFirstRange: 2, PerAuthRequestLimit: 2, PerAuthRequestWindowMinutes: 5}
					if rpm {
						routing.PerAuthRequestLimit = 0
						routing.FillFirstPerAuthRPM = 2
					}
					m.SetConfig(&config.Config{Routing: routing})
					for _, id := range []string{"a", "b"} {
						if _, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: id, Provider: "test"}); err != nil {
							t.Fatal(err)
						}
					}
					providerKey := "test"
					if mixed {
						providerKey = "mixed"
						m.RegisterExecutor(acrossPrioritySecondExecutor{})
						if _, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "c", Provider: "second"}); err != nil {
							t.Fatal(err)
						}
					}
					ctx := affinityCallerContext(t, "caller-a", "test,second")
					pick := func(opts core.Options) (*Auth, error) {
						if mixed {
							auth, _, _, err := m.pickNextMixed(ctx, []string{"test", "second"}, "", opts, nil)
							return auth, err
						}
						auth, _, err := m.pickNext(ctx, "test", "", opts, nil)
						return auth, err
					}
					parent := subagentOptions("root", "", false)
					parent.Metadata = map[string]any{core.PinnedAuthMetadataKey: "b"}
					s.BindSession(ctx, providerKey, "", parent, "b")
					for range 2 {
						if auth, err := pick(parent); err != nil || auth == nil || auth.ID != "b" {
							t.Fatalf("parent preparation: %v %v", auth, err)
						}
					}
					child := subagentOptions("child", "root", false)
					auth, err := pick(child)
					if err != nil || auth == nil || auth.ID != "a" {
						t.Fatalf("limited parent blocked an unbound child: %v %v", auth, err)
					}
					// The existing legacy RPM policy without cross-priority affinity
					// deliberately rotates rather than enforcing a strict cached auth.
					if !rpm || across {
						s.BindSession(ctx, providerKey, "", child, "b")
						if auth, err = pick(child); err == nil || auth != nil {
							t.Fatal("established strict child binding bypassed capacity")
						}
					}
				})
			}
		}
	}
}
