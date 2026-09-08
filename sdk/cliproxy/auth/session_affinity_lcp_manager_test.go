package auth

import (
	"context"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type historySnapshotExecutor struct{ *affinitySnapshotExecutor }

func (e *historySnapshotExecutor) PrepareProviderRequest(_ context.Context, _ core.Request, opts core.Options, _ core.RequestOperation) (any, error) {
	captured, ok := opts.Metadata[affinityIdentityMetadataKey].(affinityRequestIdentity)
	if !ok || captured.history == nil || !captured.history.Usable() {
		return nil, fmt.Errorf("history missing before provider preparation")
	}
	return nil, nil
}

func TestLCPManagerCapturesHistoryBeforePreparationAndRelease(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		t.Run(mode, func(t *testing.T) {
			s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, LCP: true})
			t.Cleanup(s.Stop)
			m := NewManager(nil, s, nil)
			m.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true}})
			e := &historySnapshotExecutor{&affinitySnapshotExecutor{&authFallbackExecutor{id: "claude"}}}
			m.RegisterExecutor(e)
			const model = "lcp-manager-model"
			for _, id := range []string{"a", "b"} {
				registerFallbackAuthForModel(t, m, &Auth{ID: id, Provider: "claude"}, model)
			}
			ctx := affinityCallerContext(t, "caller-a", "claude")
			for _, branch := range []string{"a", "b"} {
				opts := lcpOptions(branch, false)
				opts.Metadata = map[string]any{core.PinnedAuthMetadataKey: branch}
				if err := runCredentialRetryOperation(ctx, m, mode, core.Request{Model: model}, opts); err != nil {
					t.Fatal(err)
				}
			}
			child := lcpOptions("a", true)
			body := child.OriginalRequest
			child.OriginalRequest = nil
			ctrl := core.NewRequestBodyReleaseController(int64(len(body)), nil)
			child.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: ctrl}
			if err := runCredentialRetryOperation(ctx, m, mode, core.Request{Model: model, Payload: body}, child); err != nil {
				t.Fatal(err)
			}
			calls := append(append(e.ExecuteCalls(), e.CountCalls()...), e.StreamCalls()...)
			if len(calls) != 3 || calls[0] != "a" || calls[1] != "b" || calls[2] != "a" || !ctrl.Released() {
				t.Fatalf("branch continuation=%v released=%t", calls, ctrl.Released())
			}
			if preferred := s.historyPreferredAuth(ctx, "claude", model, lcpOptions("a", true)); preferred != "a" {
				t.Fatal("successful released history was not committed")
			}
			if _, exists := child.Metadata[affinityIdentityMetadataKey]; exists {
				t.Fatal("caller metadata was mutated")
			}
		})
	}
}

func TestLCPManagerPreservesFailoverBudgetAndFailureNeutrality(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, failover := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failover=%t", mode, failover), func(t *testing.T) {
				s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, LCP: true, Failover: &failover})
				t.Cleanup(s.Stop)
				m := NewManager(nil, s, nil)
				m.SetConfig(&config.Config{NoCooldownStatusCodes: []int{500}, Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityFailover: &failover}})
				errs := map[string]error{"a": &Error{HTTPStatus: 500, Message: "fixture a"}, "b": &Error{HTTPStatus: 500, Message: "fixture b"}}
				e := &historySnapshotExecutor{&affinitySnapshotExecutor{&authFallbackExecutor{id: "claude", executeErrors: errs, countErrors: errs, streamFirstErrors: errs}}}
				m.RegisterExecutor(e)
				const model = "lcp-failure-model"
				for _, id := range []string{"a", "b"} {
					registerFallbackAuthForModel(t, m, &Auth{ID: id, Provider: "claude"}, model)
				}
				ctx := affinityCallerContext(t, "caller-a", "claude")
				original := lcpOptions("branch", false)
				s.BindSession(ctx, "claude", model, original, "b")
				request := lcpOptions("branch", true)
				if err := runCredentialRetryOperation(ctx, m, mode, core.Request{Model: model}, request); err == nil {
					t.Fatal("missing fixture failure")
				}
				calls := append(append(e.ExecuteCalls(), e.CountCalls()...), e.StreamCalls()...)
				want := 1
				if failover {
					want = 2
				}
				if len(calls) != want || calls[0] != "b" {
					t.Fatalf("failure attempts=%v want=%d", calls, want)
				}
				if preferred := s.historyPreferredAuth(ctx, "claude", model, request); preferred != "b" {
					t.Fatal("failed attempt changed successful history preference")
				}
			})
		}
	}
}
