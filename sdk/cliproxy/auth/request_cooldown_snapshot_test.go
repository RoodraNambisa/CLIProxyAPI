package auth

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type cooldownPendingStreamExecutor struct {
	schedulerProviderTestExecutor
	chunks <-chan core.StreamChunk
}

func (e *cooldownPendingStreamExecutor) ExecuteStream(context.Context, *Auth, core.Request, core.Options) (*core.StreamResult, error) {
	return &core.StreamResult{Chunks: e.chunks}, nil
}

func TestRequestCooldownRulesSurviveDeferredStreamError(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprint(skip), func(t *testing.T) {
			initial, next := &config.Config{}, &config.Config{}
			if skip {
				initial.NoCooldownStatusCodes = []int{500}
			} else {
				next.NoCooldownStatusCodes = []int{500}
			}
			manager := NewManager(nil, nil, nil)
			manager.SetConfig(initial)
			chunks := make(chan core.StreamChunk, 2)
			closeChunks := sync.OnceFunc(func() { close(chunks) })
			defer closeChunks()
			chunks <- core.StreamChunk{Payload: []byte("data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")}
			executor := &cooldownPendingStreamExecutor{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "claude"}, chunks: chunks}
			manager.RegisterExecutor(executor)
			registerFallbackAuthForModel(t, manager, &Auth{ID: "a", Provider: "claude"}, "deferred-cooldown")
			stream, err := manager.ExecuteStream(t.Context(), []string{"claude"}, core.Request{Model: "deferred-cooldown"}, core.Options{})
			if err != nil || stream == nil {
				t.Fatal("stream did not start")
			}
			first := <-stream.Chunks
			if len(first.Payload) == 0 || first.Err != nil {
				t.Fatal("stream did not deliver its first output")
			}
			manager.SetConfig(next)
			chunks <- core.StreamChunk{Err: &Error{HTTPStatus: 500, Message: "late fixture failure"}}
			closeChunks()
			sawFailure := false
			for chunk := range stream.Chunks {
				sawFailure = sawFailure || chunk.Err != nil
			}
			if !sawFailure {
				t.Fatal("stream did not forward its terminal error")
			}
			current, _ := manager.GetByID("a")
			blocked, _, _ := isAuthBlockedForModel(current, "deferred-cooldown", time.Now())
			if blocked == skip {
				t.Fatal("deferred stream error adopted reloaded cooling rules")
			}
		})
	}
}

func TestRequestCooldownRuleSnapshotOwnershipAndFreshConfiguration(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	cfg := &config.Config{NoCooldownStatusCodes: []int{500}, FixedErrorCooldowns: []config.FixedErrorCooldownRule{{StatusCode: 500, CooldownSeconds: 30, Scope: "model"}}}
	manager.SetConfig(cfg)
	captured := manager.WithRoutingPolicySnapshot(t.Context())
	cfg.NoCooldownStatusCodes[0] = 401
	cfg.FixedErrorCooldowns[0].CooldownSeconds = 120
	err := &Error{HTTPStatus: 500, Message: "fixture"}
	fixed, ok := manager.fixedErrorCooldownForResult(err, captured)
	if !manager.cooldownSkippedForStatus(500, captured) || !ok || fixed.cooldown != 30*time.Second {
		t.Fatal("copied cooldown rules share the original slices")
	}
	manager.SetConfig(&config.Config{})
	fresh := manager.WithRoutingPolicySnapshot(t.Context())
	if manager.cooldownSkippedForStatus(500, fresh) {
		t.Fatal("fresh request retained the old status exclusion")
	}
	if _, ok := manager.fixedErrorCooldownForResult(err, fresh); ok {
		t.Fatal("fresh request retained the old fixed rule")
	}
	other := NewManager(nil, nil, nil)
	if other.cooldownSkippedForStatus(500, other.WithRoutingPolicySnapshot(captured)) {
		t.Fatal("another manager inherited cooling rules")
	}
	if _, ok := other.fixedErrorCooldownForResult(err, other.WithRoutingPolicySnapshot(captured)); ok {
		t.Fatal("another manager inherited fixed rules")
	}
	if _, ok := manager.fixedErrorCooldownForResult(err, captured); !ok {
		t.Fatal("reload cleared the in-flight rule")
	}
}

func TestRequestCooldownRulesCapturedBeforeProviderPreparation(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, scenario := range []struct {
			name          string
			initial, next *config.Config
			cooling       bool
			scope         string
			duration      time.Duration
		}{
			{"keep skip", &config.Config{NoCooldownStatusCodes: []int{500}}, &config.Config{}, false, "", 0},
			{"keep default cooling", &config.Config{}, &config.Config{NoCooldownStatusCodes: []int{500}}, true, "model", 0},
			{"keep model rule", &config.Config{NoCooldownStatusCodes: []int{500}, FixedErrorCooldowns: []config.FixedErrorCooldownRule{{StatusCode: 500, CooldownSeconds: 30, Scope: "model"}}}, &config.Config{FixedErrorCooldowns: []config.FixedErrorCooldownRule{{StatusCode: 500, CooldownSeconds: 120, Scope: "auth"}}}, true, "model", 30 * time.Second},
			{"keep auth rule", &config.Config{FixedErrorCooldowns: []config.FixedErrorCooldownRule{{StatusCode: 500, CooldownSeconds: 30, Scope: "auth"}}}, &config.Config{FixedErrorCooldowns: []config.FixedErrorCooldownRule{{StatusCode: 500, CooldownSeconds: 120, Scope: "model"}}}, true, "auth", 30 * time.Second},
		} {
			t.Run(fmt.Sprintf("%s/%s", mode, scenario.name), func(t *testing.T) {
				manager := NewManager(nil, nil, nil)
				manager.SetConfig(scenario.initial)
				manager.SetRetryConfig(0, 0, 0)
				failures := map[string]error{"a": &Error{HTTPStatus: 500, Message: "fixture cooldown failure"}}
				executor := &errorRuleReloadPreparer{manager: manager, next: scenario.next, authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: failures, countErrors: failures, streamFirstErrors: failures}}
				manager.RegisterExecutor(executor)
				registerFallbackAuthForModel(t, manager, &Auth{ID: "a", Provider: "claude"}, "cooldown-rules-snapshot")
				started := time.Now()
				if err := runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "cooldown-rules-snapshot"}, core.Options{}); err == nil {
					t.Fatal("synthetic request succeeded")
				}
				current, _ := manager.GetByID("a")
				blocked, _, retryAt := isAuthBlockedForModel(current, "cooldown-rules-snapshot", time.Now())
				if blocked != scenario.cooling {
					t.Fatal("in-flight request adopted the reloaded cooling policy")
				}
				if scenario.cooling && current.CooldownScope != scenario.scope {
					t.Fatalf("scope=%q want=%q", current.CooldownScope, scenario.scope)
				}
				if scenario.duration > 0 && (retryAt.Before(started.Add(scenario.duration)) || retryAt.After(time.Now().Add(scenario.duration))) {
					t.Fatal("in-flight request adopted the reloaded duration")
				}
			})
		}
	}
}
