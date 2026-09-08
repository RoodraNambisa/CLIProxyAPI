package auth

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type requestScopedRecoveryExecutor struct {
	*authFallbackExecutor
	before     func()
	recoveries atomic.Int32
}

func (e *requestScopedRecoveryExecutor) Execute(ctx context.Context, a *Auth, req core.Request, opts core.Options) (core.Response, error) {
	if e.before != nil {
		e.before()
	}
	return e.authFallbackExecutor.Execute(ctx, a, req, opts)
}
func (e *requestScopedRecoveryExecutor) CountTokens(ctx context.Context, a *Auth, req core.Request, opts core.Options) (core.Response, error) {
	if e.before != nil {
		e.before()
	}
	return e.authFallbackExecutor.CountTokens(ctx, a, req, opts)
}
func (e *requestScopedRecoveryExecutor) ShouldRecoverUnauthorized(_ *Auth, err error) bool {
	return isUnauthorizedError(err)
}
func (e *requestScopedRecoveryExecutor) RecoverUnauthorized(_ context.Context, a *Auth) (*Auth, error) {
	e.recoveries.Add(1)
	return a.Clone(), nil
}

func TestRequestScopedActionsControlUnauthorizedRecovery(t *testing.T) {
	for _, mode := range []string{"execute", "count"} {
		for _, action := range []string{"", "stop", "stop-and-cooldown", "continue", "continue-and-cooldown"} {
			t.Run(mode+"/"+action, func(t *testing.T) {
				m := NewManager(nil, &FillFirstSelector{}, nil)
				failure := &Error{HTTPStatus: 401, Message: "fixture"}
				e := &requestScopedRecoveryExecutor{authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: map[string]error{"a": failure}, countErrors: map[string]error{"a": failure}}}
				m.RegisterExecutor(e)
				const model = "scoped-recovery-model"
				for _, id := range []string{"a", "b"} {
					a := &Auth{ID: id, Provider: "claude"}
					if action != "" {
						a.Metadata = map[string]any{"request_scoped_errors": []config.RequestScopedErrorRule{{Status: 401, Match: []string{"fixture"}, Action: action}}}
					}
					registerFallbackAuthForModel(t, m, a, model)
				}
				err := runCredentialRetryOperation(t.Context(), m, mode, core.Request{Model: model}, core.Options{})
				wantCalls, wantRecoveries := 2, int32(0)
				stop := action == "stop" || action == "stop-and-cooldown"
				if stop {
					wantCalls = 1
				}
				if action == "" {
					wantCalls, wantRecoveries = 3, 1
				}
				if len(e.ExecuteCalls())+len(e.CountCalls()) != wantCalls || e.recoveries.Load() != wantRecoveries || (stop && !errors.Is(err, failure)) || (!stop && err != nil) {
					t.Fatalf("calls=%d recoveries=%d err=%v", len(e.ExecuteCalls())+len(e.CountCalls()), e.recoveries.Load(), err)
				}
			})
		}
	}
}

func TestRequestScopedContinueCannotReplayReleasedRequest(t *testing.T) {
	for _, mode := range []string{"execute", "count"} {
		for _, action := range []string{"continue", "continue-and-cooldown"} {
			t.Run(fmt.Sprintf("%s/%s", mode, action), func(t *testing.T) {
				ctrl := core.NewRequestBodyReleaseController(1, nil)
				m := NewManager(nil, &FillFirstSelector{}, nil)
				m.SetRetryConfig(3, 0, 0)
				failure := &Error{HTTPStatus: 500, Message: "fixture"}
				e := &requestScopedRecoveryExecutor{authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: map[string]error{"a": failure}, countErrors: map[string]error{"a": failure}}, before: func() { ctrl.Release() }}
				m.RegisterExecutor(e)
				const model = "scoped-release-model"
				for _, id := range []string{"a", "b"} {
					registerFallbackAuthForModel(t, m, &Auth{ID: id, Provider: "claude", Metadata: map[string]any{"request_scoped_errors": []config.RequestScopedErrorRule{{Status: 500, Match: []string{"fixture"}, Action: action}}}}, model)
				}
				opts := core.Options{Metadata: map[string]any{core.BodyReleaseControllerMetadataKey: ctrl}}
				err := runCredentialRetryOperation(t.Context(), m, mode, core.Request{Model: model}, opts)
				if !errors.Is(err, failure) || len(e.ExecuteCalls())+len(e.CountCalls()) != 1 || !ctrl.Released() {
					t.Fatal("continue action bypassed body release")
				}
			})
		}
	}
}
