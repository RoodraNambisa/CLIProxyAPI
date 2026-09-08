package auth

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestRequestScopedActionsExecuteAndCountAttempts(t *testing.T) {
	for _, mode := range []string{"execute", "count"} {
		for _, status := range []int{400, 500} {
			for _, action := range []string{"stop", "stop-and-cooldown", "continue", "continue-and-cooldown"} {
				t.Run(fmt.Sprintf("%s/%d/%s", mode, status, action), func(t *testing.T) {
					m := NewManager(nil, &FillFirstSelector{}, nil)
					m.SetRetryConfig(2, 0, 0)
					failure := &Error{HTTPStatus: status, Code: "custom_upstream", Message: "fixture"}
					e := &authFallbackExecutor{id: "claude", executeErrors: map[string]error{"a": failure}, countErrors: map[string]error{"a": failure}}
					m.RegisterExecutor(e)
					const model = "scoped-actions-model"
					for _, id := range []string{"a", "b"} {
						a := &Auth{ID: id, Provider: "claude"}
						if id == "a" {
							a.Metadata = map[string]any{"request_scoped_errors": []config.RequestScopedErrorRule{{Status: status, Match: []string{"fixture"}, Action: action}}}
						}
						registerFallbackAuthForModel(t, m, a, model)
					}
					err := runCredentialRetryOperation(t.Context(), m, mode, core.Request{Model: model}, core.Options{})
					stop := action == "stop" || action == "stop-and-cooldown"
					calls := append(e.ExecuteCalls(), e.CountCalls()...)
					want := 2
					if stop {
						want = 1
					}
					if len(calls) != want || calls[0] != "a" || (stop && !errors.Is(err, failure)) || (!stop && err != nil) {
						t.Fatalf("calls=%v error=%v", calls, err)
					}
					a, _ := m.GetByID("a")
					cooled := false
					if state := a.ModelStates[model]; state != nil {
						cooled = state.NextRetryAfter.After(time.Now())
					}
					if cooled != (action == "stop-and-cooldown" || action == "continue-and-cooldown") {
						t.Fatal("execution did not apply requested cooldown behavior")
					}
				})
			}
		}
	}
}

type requestScopedReloadPreparer struct {
	*authFallbackExecutor
	manager *Manager
	next    *config.Config
}

func (e *requestScopedReloadPreparer) PrepareProviderRequest(context.Context, core.Request, core.Options, core.RequestOperation) (any, error) {
	e.manager.SetConfig(e.next)
	return nil, nil
}

func TestRequestScopedActionsKeepProviderSnapshotAndRetryBudget(t *testing.T) {
	for _, mode := range []string{"execute", "count"} {
		m := NewManager(nil, &FillFirstSelector{}, nil)
		m.SetRetryConfig(1, 0, 0)
		makeConfig := func(action string) *config.Config {
			return &config.Config{OAuthRequestScopedErrors: map[string][]config.RequestScopedErrorRule{"claude": {{Status: 500, Match: []string{"fixture"}, Action: action}}}}
		}
		m.SetConfig(makeConfig("continue"))
		failure := &Error{HTTPStatus: 500, Message: "fixture"}
		e := &requestScopedReloadPreparer{authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: map[string]error{"a": failure, "b": failure}, countErrors: map[string]error{"a": failure, "b": failure}}, manager: m, next: makeConfig("stop")}
		m.RegisterExecutor(e)
		const model = "scoped-snapshot-model"
		for _, id := range []string{"a", "b"} {
			registerFallbackAuthForModel(t, m, &Auth{ID: id, Provider: "claude"}, model)
		}
		for _, expected := range []int{4, 5} {
			err := runCredentialRetryOperation(t.Context(), m, mode, core.Request{Model: model}, core.Options{})
			if !errors.Is(err, failure) || len(e.ExecuteCalls())+len(e.CountCalls()) != expected {
				t.Fatalf("%s calls=%d want=%d err=%v", mode, len(e.ExecuteCalls())+len(e.CountCalls()), expected, err)
			}
		}
	}
}

func TestRequestScopedActionsCannotRetryOrCoolPolicyRefusal(t *testing.T) {
	for _, mode := range []string{"execute", "count"} {
		m := NewManager(nil, &FillFirstSelector{}, nil)
		m.SetRetryConfig(2, 0, 0)
		failure := &Error{HTTPStatus: 403, Code: "misalignment_policy_violation", Message: "fixture"}
		e := &authFallbackExecutor{id: "claude", executeErrors: map[string]error{"a": failure}, countErrors: map[string]error{"a": failure}}
		m.RegisterExecutor(e)
		const model = "scoped-policy-model"
		for _, id := range []string{"a", "b"} {
			a := &Auth{ID: id, Provider: "claude", Metadata: map[string]any{"request_scoped_errors": []config.RequestScopedErrorRule{{Status: 403, Match: []string{"fixture"}, Action: "continue-and-cooldown"}}}}
			registerFallbackAuthForModel(t, m, a, model)
		}
		err := runCredentialRetryOperation(t.Context(), m, mode, core.Request{Model: model}, core.Options{})
		if !errors.Is(err, failure) || len(e.ExecuteCalls())+len(e.CountCalls()) != 1 {
			t.Fatal("rule retried a policy refusal")
		}
		a, _ := m.GetByID("a")
		if a.Unavailable || !a.NextRetryAfter.IsZero() || len(a.ModelStates) != 0 {
			t.Fatal("rule cooled a policy refusal")
		}
	}
}
