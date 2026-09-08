package auth

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type requestScopedStreamExecutor struct {
	*requestScopedRecoveryExecutor
	direct bool
}

type requestScopedResultHook struct {
	NoopHook
	failures atomic.Int32
}

func (h *requestScopedResultHook) OnResult(_ context.Context, result Result) {
	if !result.Success {
		h.failures.Add(1)
	}
}

func TestRequestScopedStreamActionsDoNotRepeatUnauthorizedResults(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, action := range []string{"stop", "stop-and-cooldown", "continue", "continue-and-cooldown"} {
			t.Run(fmt.Sprintf("direct=%t/action=%s", direct, action), func(t *testing.T) {
				hook := &requestScopedResultHook{}
				m := NewManager(nil, &FillFirstSelector{}, hook)
				failure := &Error{HTTPStatus: 401, Message: "fixture"}
				e := &requestScopedStreamExecutor{requestScopedRecoveryExecutor: &requestScopedRecoveryExecutor{authFallbackExecutor: &authFallbackExecutor{id: "antigravity", streamFirstErrors: map[string]error{"a": failure}}}, direct: direct}
				m.RegisterExecutor(e)
				const model = "scoped-deferred-model"
				for _, id := range []string{"a", "b"} {
					registerFallbackAuthForModel(t, m, &Auth{ID: id, Provider: "antigravity", Metadata: map[string]any{"refresh_token": "fixture", "request_scoped_errors": []config.RequestScopedErrorRule{{Status: 401, Match: []string{"fixture"}, Action: action}}}}, model)
				}
				stream, err := m.ExecuteStream(t.Context(), []string{"antigravity"}, core.Request{Model: model}, core.Options{})
				if stream != nil {
					for chunk := range stream.Chunks {
						if chunk.Err != nil {
							err = chunk.Err
						}
					}
					if stream.Headers.Get("X-Auth") == "" {
						t.Fatal("stream headers were lost")
					}
				}
				stop := action == "stop" || action == "stop-and-cooldown"
				wantCalls := 2
				if stop {
					wantCalls = 1
				}
				if len(e.StreamCalls()) != wantCalls || e.recoveries.Load() != 0 || hook.failures.Load() != 1 || (stop && !errors.Is(err, failure)) || (!stop && err != nil) {
					t.Fatalf("calls=%v recoveries=%d failures=%d error=%v", e.StreamCalls(), e.recoveries.Load(), hook.failures.Load(), err)
				}
				a, _ := m.GetByID("a")
				if action == "continue" || action == "stop" {
					if a.Unavailable || len(a.ModelStates) != 0 {
						t.Fatal("deferred outer result overrode no-cooldown action")
					}
				}
			})
		}
	}
}

func (e *requestScopedStreamExecutor) ExecuteStream(ctx context.Context, a *Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	if e.before != nil {
		e.before()
	}
	result, err := e.authFallbackExecutor.ExecuteStream(ctx, a, req, opts)
	if e.direct {
		if failure := e.streamFirstErrors[a.ID]; failure != nil {
			for range result.Chunks {
			}
			return nil, failure
		}
	}
	return result, err
}

func TestRequestScopedStreamActionsBeforeOutput(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, status := range []int{400, 500} {
			for _, action := range []string{"stop", "stop-and-cooldown", "continue", "continue-and-cooldown"} {
				t.Run(fmt.Sprintf("direct=%t/status=%d/action=%s", direct, status, action), func(t *testing.T) {
					m := NewManager(nil, &FillFirstSelector{}, nil)
					m.SetRetryConfig(2, 0, 0)
					failure := &Error{HTTPStatus: status, Code: "upstream_fixture", Message: "fixture"}
					e := &requestScopedStreamExecutor{requestScopedRecoveryExecutor: &requestScopedRecoveryExecutor{authFallbackExecutor: &authFallbackExecutor{id: "claude", streamFirstErrors: map[string]error{"a": failure}}}, direct: direct}
					m.RegisterExecutor(e)
					const model = "scoped-stream-model"
					for _, id := range []string{"a", "b"} {
						a := &Auth{ID: id, Provider: "claude", Metadata: map[string]any{"request_scoped_errors": []config.RequestScopedErrorRule{{Status: status, Match: []string{"fixture"}, Action: action}}}}
						registerFallbackAuthForModel(t, m, a, model)
					}
					err := runCredentialRetryOperation(t.Context(), m, "stream", core.Request{Model: model}, core.Options{})
					stop := action == "stop" || action == "stop-and-cooldown"
					wantCalls := 2
					if stop {
						wantCalls = 1
					}
					if len(e.StreamCalls()) != wantCalls || (stop && !errors.Is(err, failure)) || (!stop && err != nil) {
						t.Fatalf("calls=%v err=%v", e.StreamCalls(), err)
					}
					a, _ := m.GetByID("a")
					cooled := false
					if state := a.ModelStates[model]; state != nil {
						cooled = state.NextRetryAfter.After(time.Now())
					}
					if cooled != (action == "stop-and-cooldown" || action == "continue-and-cooldown") {
						t.Fatal("stream rule did not apply cooldown behavior")
					}
				})
			}
		}
	}
}
