package auth

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestPriorityRetryLimitKeepsRequestSnapshot(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		t.Run(mode, func(t *testing.T) {
			manager := NewManager(nil, nil, nil)
			limit := 2
			makeConfig := func() *config.Config {
				return &config.Config{NoCooldownStatusCodes: []int{500}, Routing: config.RoutingConfig{PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 0, MaxRetryCredentials: &limit}}}}
			}
			manager.SetConfig(makeConfig())
			errs := map[string]error{}
			executor := &retryRoundHookExecutor{authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: errs, countErrors: errs, streamFirstErrors: errs}, hook: func() {
				limit = 1
				manager.SetConfig(makeConfig())
			}}
			manager.RegisterExecutor(executor)
			for _, id := range []string{"a", "b", "c"} {
				errs[id] = &Error{HTTPStatus: 500, Message: "synthetic failure"}
				registerFallbackAuthForModel(t, manager, &Auth{ID: id, Provider: "claude"}, "retry-priority-model")
			}
			for requestIndex, want := range []int{2, 3} {
				err := runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "retry-priority-model"}, core.Options{})
				if err == nil {
					t.Fatal("missing upstream failure")
				}
				if got := len(executor.ExecuteCalls()) + len(executor.CountCalls()) + len(executor.StreamCalls()); got != want {
					t.Fatalf("request %d: total calls=%d want=%d", requestIndex, got, want)
				}
			}
		})
	}
}

func TestPriorityRetryLimitSnapshotKeepsZeroInheritanceAndFirstRule(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	zero, negative, two := 0, -1, 2
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{PriorityOverrides: []config.RoutingPriorityOverride{
		{Priority: 3, MaxRetryCredentials: &zero},
		{Priority: 3, MaxRetryCredentials: &two},
		{Priority: 4, MaxRetryCredentials: &negative},
		{Priority: 6},
	}}})
	ctx := manager.WithRoutingPolicySnapshot(t.Context())
	manager.SetConfig(&config.Config{})
	for priority, want := range map[int]int{3: 0, 4: 0, 5: 7, 6: 7} {
		if got := manager.maxRetryCredentialsForPriority(priority, 7, ctx); got != want {
			t.Fatalf("priority=%d limit=%d want=%d", priority, got, want)
		}
		if got := manager.maxRetryCredentialsForPriority(priority, 7); got != 7 {
			t.Fatal("new request retained a removed override")
		}
	}
}
