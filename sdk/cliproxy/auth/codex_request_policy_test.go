package auth

import (
	"context"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type multiAgentSnapshotExecutor struct {
	*authFallbackExecutor
	manager *Manager
	next    *config.Config
	seen    []bool
}

func (e *multiAgentSnapshotExecutor) PrepareProviderRequest(context.Context, core.Request, core.Options, core.RequestOperation) (any, error) {
	e.manager.SetConfig(e.next)
	return nil, nil
}
func (e *multiAgentSnapshotExecutor) recordPolicy(ctx context.Context) {
	value, ok := CodexMultiAgentV2RequestSetting(ctx)
	if !ok {
		panic("test executor did not receive request policy")
	}
	e.seen = append(e.seen, value)
}
func (e *multiAgentSnapshotExecutor) Execute(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	e.recordPolicy(ctx)
	return e.authFallbackExecutor.Execute(ctx, auth, req, opts)
}
func (e *multiAgentSnapshotExecutor) CountTokens(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	e.recordPolicy(ctx)
	return e.authFallbackExecutor.CountTokens(ctx, auth, req, opts)
}
func (e *multiAgentSnapshotExecutor) ExecuteStream(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	e.recordPolicy(ctx)
	return e.authFallbackExecutor.ExecuteStream(ctx, auth, req, opts)
}

func TestCodexMultiAgentSettingSurvivesPreparationRetryAndCredentialSwitch(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", mode, enabled), func(t *testing.T) {
				initial := &config.Config{NoCooldownStatusCodes: []int{500}, Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}}
				next := &config.Config{NoCooldownStatusCodes: []int{500}, Codex: config.CodexConfig{OptimizeMultiAgentV2: !enabled}}
				manager := NewManager(nil, &FillFirstSelector{}, nil)
				manager.SetConfig(initial)
				manager.SetRetryConfig(1, 0, 0)
				failures := map[string]error{"a": &Error{HTTPStatus: 500, Message: "fixture"}, "b": &Error{HTTPStatus: 500, Message: "fixture"}}
				executor := &multiAgentSnapshotExecutor{manager: manager, next: next, authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: failures, countErrors: failures, streamFirstErrors: failures}}
				manager.RegisterExecutor(executor)
				for _, id := range []string{"a", "b"} {
					registerFallbackAuthForModel(t, manager, &Auth{ID: id, Provider: "claude"}, "multi-agent-snapshot")
				}
				for _, want := range []bool{enabled, !enabled} {
					executor.seen = nil
					if err := runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "multi-agent-snapshot"}, core.Options{}); err == nil {
						t.Fatal("synthetic request succeeded")
					}
					if len(executor.seen) != 4 {
						t.Fatalf("attempts=%d, want four within the existing budget", len(executor.seen))
					}
					for _, got := range executor.seen {
						if got != want {
							t.Fatal("in-flight policy changed during preparation or retry")
						}
					}
				}
			})
		}
	}
}

func TestCodexMultiAgentSettingDefaultsAndManagerOwnership(t *testing.T) {
	if _, ok := CodexMultiAgentV2RequestSetting(nil); ok {
		t.Fatal("nil context fabricated a request snapshot")
	}
	if _, ok := CodexMultiAgentV2RequestSetting(t.Context()); ok {
		t.Fatal("plain context fabricated a request snapshot")
	}
	manager := NewManager(nil, nil, nil)
	defaults := manager.WithRoutingPolicySnapshot(nil)
	if value, ok := CodexMultiAgentV2RequestSetting(defaults); !ok || value {
		t.Fatal("missing config did not capture disabled default")
	}
	cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}}
	manager.SetConfig(cfg)
	ctx := manager.WithRoutingPolicySnapshot(t.Context())
	cfg.Codex.OptimizeMultiAgentV2 = false
	if value, ok := CodexMultiAgentV2RequestSetting(ctx); !ok || !value {
		t.Fatal("caller config mutation altered request")
	}
	if manager.WithRoutingPolicySnapshot(ctx) != ctx {
		t.Fatal("nested entry replaced the snapshot")
	}
	other := NewManager(nil, nil, nil)
	if value, ok := CodexMultiAgentV2RequestSetting(other.WithRoutingPolicySnapshot(ctx)); !ok || value {
		t.Fatal("another manager inherited the first manager's policy")
	}
}
