package auth

import (
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestStrictAffinityRetryPolicySurvivesReload(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, initialStrict := range []bool{false, true} {
			t.Run(mode+"/"+map[bool]string{false: "failover", true: "strict"}[initialStrict], func(t *testing.T) {
				manager := NewManager(nil, nil, nil)
				manager.SetRetryConfig(1, 0, 0)
				install := func(strict bool) {
					failover := !strict
					selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Failover: &failover})
					t.Cleanup(selector.Stop)
					manager.SetConfigAndSelector(&config.Config{NoCooldownStatusCodes: []int{500}, Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityFailover: &failover}}, selector)
				}
				install(initialStrict)
				captured := manager.WithRoutingPolicySnapshot(t.Context())
				install(!initialStrict)
				errs := map[string]error{"a": &Error{HTTPStatus: 500, Message: "fixture failure"}}
				executor := &authFallbackExecutor{id: "claude", executeErrors: errs, countErrors: errs, streamFirstErrors: errs}
				manager.RegisterExecutor(executor)
				registerFallbackAuthForModel(t, manager, &Auth{ID: "a", Provider: "claude"}, "affinity-snapshot-model")
				opts := core.Options{Headers: http.Header{"Session-Id": {"fixture-session"}}}
				if err := runCredentialRetryOperation(captured, manager, mode, core.Request{Model: "affinity-snapshot-model"}, opts); err == nil {
					t.Fatal("missing fixture error")
				}
				want := 2
				if initialStrict {
					want = 1
				}
				if calls := len(executor.ExecuteCalls()) + len(executor.CountCalls()) + len(executor.StreamCalls()); calls != want {
					t.Fatalf("in-flight calls=%d, want %d", calls, want)
				}
				if err := runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "affinity-snapshot-model"}, opts); err == nil {
					t.Fatal("missing new request fixture error")
				}
				if calls := len(executor.ExecuteCalls()) + len(executor.CountCalls()) + len(executor.StreamCalls()); calls != 3 {
					t.Fatalf("new request did not adopt updated policy: total calls=%d", calls)
				}
			})
		}
	}
}
