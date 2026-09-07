package auth

import (
	"context"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type affinitySnapshotExecutor struct{ *authFallbackExecutor }

func (e *affinitySnapshotExecutor) PrepareProviderRequest(_ context.Context, _ core.Request, opts core.Options, _ core.RequestOperation) (any, error) {
	if _, ok := opts.Metadata[affinityIdentityMetadataKey].(affinityRequestIdentity); !ok {
		return nil, fmt.Errorf("missing affinity identity before provider preparation")
	}
	return nil, nil
}

func (e *affinitySnapshotExecutor) Execute(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	if ctrl := core.RequestBodyReleaseControllerFromOptions(opts); ctrl != nil {
		ctrl.Release()
	}
	return e.authFallbackExecutor.Execute(ctx, auth, req, opts)
}

func (e *affinitySnapshotExecutor) CountTokens(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	if ctrl := core.RequestBodyReleaseControllerFromOptions(opts); ctrl != nil {
		ctrl.Release()
	}
	return e.authFallbackExecutor.CountTokens(ctx, auth, req, opts)
}

func (e *affinitySnapshotExecutor) ExecuteStream(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	if ctrl := core.RequestBodyReleaseControllerFromOptions(opts); ctrl != nil {
		ctrl.Release()
	}
	return e.authFallbackExecutor.ExecuteStream(ctx, auth, req, opts)
}

func TestSubagentManagerCapturesIdentityBeforePreparationAndRelease(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		t.Run(mode, func(t *testing.T) {
			s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Subagents: true})
			t.Cleanup(s.Stop)
			m := NewManager(nil, s, nil)
			m.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true}})
			e := &affinitySnapshotExecutor{&authFallbackExecutor{id: "claude"}}
			m.RegisterExecutor(e)
			const model = "subagent-manager-model"
			for _, id := range []string{"a", "b"} {
				registerFallbackAuthForModel(t, m, &Auth{ID: id, Provider: "claude"}, model)
			}
			ctx := affinityCallerContext(t, "caller-a", "claude")
			parent := subagentOptions("root", "", false)
			parent.Metadata = map[string]any{core.PinnedAuthMetadataKey: "b"}
			if err := runCredentialRetryOperation(ctx, m, mode, core.Request{Model: model}, parent); err != nil {
				t.Fatal(err)
			}
			body := []byte(`{"client_metadata":{"session_id":"child","thread_id":"child","x-codex-parent-thread-id":"root"},"input":[{"role":"user","content":"hello"}]}`)
			ctrl := core.NewRequestBodyReleaseController(int64(len(body)), nil)
			child := core.Options{Metadata: map[string]any{core.BodyReleaseControllerMetadataKey: ctrl}}
			if err := runCredentialRetryOperation(ctx, m, mode, core.Request{Model: model, Payload: body}, child); err != nil {
				t.Fatal(err)
			}
			calls := append(append(e.ExecuteCalls(), e.CountCalls()...), e.StreamCalls()...)
			if len(calls) != 2 || calls[0] != "b" || calls[1] != "b" || !ctrl.Released() {
				t.Fatalf("inheritance after body release: calls=%v released=%t", calls, ctrl.Released())
			}
			check := core.Options{OriginalRequest: body}
			primary, _ := s.sessionIDs(ctx, check)
			if bound, ok := s.cache.Get("claude::" + primary + "::" + model); !ok || bound != "b" {
				t.Fatal("successful child binding was lost after body release")
			}
			if _, ok := child.Metadata[affinityIdentityMetadataKey]; ok {
				t.Fatal("Manager mutated caller Options")
			}
		})
	}
}

func TestSubagentManagerFailedAttemptsDoNotCommitChildBinding(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		t.Run(mode, func(t *testing.T) {
			s := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Subagents: true})
			t.Cleanup(s.Stop)
			m := NewManager(nil, s, nil)
			m.SetConfig(&config.Config{NoCooldownStatusCodes: []int{500}, Routing: config.RoutingConfig{SessionAffinity: true}})
			errs := map[string]error{"a": &Error{HTTPStatus: 500, Message: "fixture a"}, "b": &Error{HTTPStatus: 500, Message: "fixture b"}}
			e := &affinitySnapshotExecutor{&authFallbackExecutor{id: "claude", executeErrors: errs, countErrors: errs, streamFirstErrors: errs}}
			m.RegisterExecutor(e)
			const model = "subagent-manager-failure"
			for _, id := range []string{"a", "b"} {
				registerFallbackAuthForModel(t, m, &Auth{ID: id, Provider: "claude"}, model)
			}
			ctx := affinityCallerContext(t, "caller-a", "claude")
			s.BindSession(ctx, "claude", model, subagentOptions("root", "", false), "b")
			child := subagentOptions("child", "root", false)
			if err := runCredentialRetryOperation(ctx, m, mode, core.Request{Model: model}, child); err == nil {
				t.Fatal("missing fixture failure")
			}
			calls := append(append(e.ExecuteCalls(), e.CountCalls()...), e.StreamCalls()...)
			if len(calls) != 2 || calls[0] != "b" || calls[1] != "a" {
				t.Fatalf("attempt sequence=%v", calls)
			}
			primary, _ := s.sessionIDs(ctx, child)
			if _, ok := s.cache.Get("claude::" + primary + "::" + model); ok {
				t.Fatal("failed attempts committed child binding")
			}
		})
	}
}
