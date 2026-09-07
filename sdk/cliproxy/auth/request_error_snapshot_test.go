package auth

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type errorRuleReloadPreparer struct {
	*authFallbackExecutor
	manager *Manager
	next    *config.Config
}

func (e *errorRuleReloadPreparer) PrepareProviderRequest(context.Context, core.Request, core.Options, core.RequestOperation) (any, error) {
	e.manager.SetConfig(e.next)
	return nil, nil
}

func TestRequestErrorRulesRemainStableAcrossCredentialSwitches(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, initialStops := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/initial-stop=%t", mode, initialStops), func(t *testing.T) {
				rule := []config.NonRetryableErrorRule{{StatusCode: 500, Code: "fixture_denial"}}
				initial := &config.Config{NoCooldownStatusCodes: []int{500}, NonRetryableErrors: []config.NonRetryableErrorRule{}}
				next := &config.Config{NoCooldownStatusCodes: []int{500}, NonRetryableErrors: []config.NonRetryableErrorRule{}}
				if initialStops {
					initial.NonRetryableErrors = rule
				} else {
					next.NonRetryableErrors = rule
				}
				manager := NewManager(nil, &FillFirstSelector{}, nil)
				manager.SetConfig(initial)
				manager.SetRetryConfig(1, 0, 0)
				failures := map[string]error{}
				for _, id := range []string{"a", "b"} {
					failures[id] = &Error{HTTPStatus: 500, Message: `{"error":{"code":"fixture_denial"}}`}
				}
				executor := &retryRoundHookExecutor{authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: failures, countErrors: failures, streamFirstErrors: failures}, hook: func() { manager.SetConfig(next) }}
				manager.RegisterExecutor(executor)
				for _, id := range []string{"a", "b"} {
					registerFallbackAuthForModel(t, manager, &Auth{ID: id, Provider: "claude"}, "switch-error-rules")
				}
				wantFirst := 4
				if initialStops {
					wantFirst = 1
				}
				for index, want := range []int{wantFirst, 5} {
					if err := runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "switch-error-rules"}, core.Options{}); err == nil {
						t.Fatal("synthetic request succeeded")
					}
					if calls := len(executor.ExecuteCalls()) + len(executor.CountCalls()) + len(executor.StreamCalls()); calls != want {
						t.Fatalf("request %d: calls=%d want=%d", index, calls, want)
					}
				}
			})
		}
	}
}

func TestRequestErrorRuleSnapshotOwnershipDefaultsAndEmptyRules(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	initial := &config.Config{NonRetryableErrors: []config.NonRetryableErrorRule{{StatusCode: 500, Code: "fixture_denial"}}}
	manager.SetConfig(initial)
	ctx := manager.WithRoutingPolicySnapshot(t.Context())
	captured := manager.SnapshotRequestErrorRetryPolicy(ctx)
	initial.NonRetryableErrors[0].Code = "changed"
	fault := &Error{HTTPStatus: 500, Message: `{"error":{"code":"fixture_denial"}}`}
	if captured(fault) || manager.SnapshotRequestErrorRetryPolicy(ctx)(fault) {
		t.Fatal("caller mutation changed the copied rules")
	}
	manager.SetConfig(&config.Config{NonRetryableErrors: []config.NonRetryableErrorRule{}})
	if manager.SnapshotRequestErrorRetryPolicy(ctx)(fault) || !manager.SnapshotRequestErrorRetryPolicy()(fault) {
		t.Fatal("reload changed in-flight rules or failed to affect a new request")
	}
	if manager.WithRoutingPolicySnapshot(ctx) != ctx {
		t.Fatal("nested request entry replaced the snapshot")
	}
	other := NewManager(nil, nil, nil)
	other.SetConfig(&config.Config{NonRetryableErrors: []config.NonRetryableErrorRule{}})
	if len(other.requestNonRetryableErrorRules(other.WithRoutingPolicySnapshot(ctx))) != 0 {
		t.Fatal("another manager inherited these rules")
	}
	manager.SetConfig(nil)
	defaults := manager.WithRoutingPolicySnapshot(t.Context())
	if !reflect.DeepEqual(manager.requestNonRetryableErrorRules(defaults), config.DefaultNonRetryableErrorRules()) {
		t.Fatal("default rules changed")
	}
	manager.SetConfig(&config.Config{NonRetryableErrors: []config.NonRetryableErrorRule{}})
	if len(manager.requestNonRetryableErrorRules(manager.WithRoutingPolicySnapshot(t.Context()))) != 0 {
		t.Fatal("explicit empty rules became defaults")
	}
	if !reflect.DeepEqual(manager.requestNonRetryableErrorRules(defaults), config.DefaultNonRetryableErrorRules()) {
		t.Fatal("default snapshot adopted the empty list")
	}
}

func TestRequestErrorRulesCapturedBeforeProviderPreparation(t *testing.T) {
	for _, initialStops := range []bool{false, true} {
		for _, mode := range []string{"execute", "count", "stream"} {
			t.Run(fmt.Sprintf("initial-stop=%t/%s", initialStops, mode), func(t *testing.T) {
				rules := []config.NonRetryableErrorRule{{StatusCode: 500, Code: "fixture_denial"}}
				initial := &config.Config{NoCooldownStatusCodes: []int{500}, NonRetryableErrors: []config.NonRetryableErrorRule{}}
				next := &config.Config{NoCooldownStatusCodes: []int{500}, NonRetryableErrors: []config.NonRetryableErrorRule{}}
				if initialStops {
					initial.NonRetryableErrors = rules
				} else {
					next.NonRetryableErrors = rules
				}
				manager := NewManager(nil, nil, nil)
				manager.SetConfig(initial)
				manager.SetRetryConfig(1, 0, 0)
				failures := map[string]error{"a": &Error{HTTPStatus: 500, Message: `{"error":{"code":"fixture_denial","message":"fixture"}}`}}
				executor := &errorRuleReloadPreparer{manager: manager, next: next, authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: failures, countErrors: failures, streamFirstErrors: failures}}
				manager.RegisterExecutor(executor)
				registerFallbackAuthForModel(t, manager, &Auth{ID: "a", Provider: "claude"}, "error-rules-snapshot")
				wantFirst := 2
				if initialStops {
					wantFirst = 1
				}
				for requestIndex, wantCalls := range []int{wantFirst, 3} {
					if err := runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "error-rules-snapshot"}, core.Options{}); err == nil {
						t.Fatal("synthetic request succeeded")
					}
					if calls := len(executor.ExecuteCalls()) + len(executor.CountCalls()) + len(executor.StreamCalls()); calls != wantCalls {
						t.Fatalf("request %d: calls=%d want=%d", requestIndex, calls, wantCalls)
					}
				}
			})
		}
	}
}
