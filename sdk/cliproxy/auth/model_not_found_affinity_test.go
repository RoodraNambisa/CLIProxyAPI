package auth

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func runCodexAvailabilityOperation(ctx context.Context, manager *Manager, operation string, req core.Request, opts core.Options) error {
	switch operation {
	case "count":
		_, err := manager.ExecuteCount(ctx, []string{"codex"}, req, opts)
		return err
	case "stream":
		stream, err := manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
		if stream != nil {
			for chunk := range stream.Chunks {
				if chunk.Err != nil {
					err = chunk.Err
				}
			}
		}
		return err
	default:
		_, err := manager.Execute(ctx, []string{"codex"}, req, opts)
		return err
	}
}

func TestModelNotFoundReleasesOnlyFailedSessionBinding(t *testing.T) {
	for _, operation := range []string{"execute", "count", "stream"} {
		for _, failover := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/failover=%t", operation, failover), func(t *testing.T) {
				selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, Failover: &failover})
				t.Cleanup(selector.Stop)
				manager := NewManager(nil, selector, nil)
				manager.SetConfigAndSelector(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityFailover: &failover}}, selector)
				manager.SetRetryConfig(0, 0, 1)
				failures := map[string]error{"binding-a": &Error{HTTPStatus: 404, Message: modelNotFoundFixture}}
				exec := &authFallbackExecutor{id: "codex", executeErrors: failures, countErrors: failures, streamFirstErrors: failures}
				manager.RegisterExecutor(exec)
				for _, id := range []string{"binding-a", "binding-b"} {
					registerFallbackAuthForModel(t, manager, &Auth{ID: id, Provider: "codex"}, "gpt-5.5")
				}
				opts := core.Options{Headers: http.Header{"X-Session-Id": {"local-binding-fixture"}}}
				selector.BindSession(t.Context(), "codex", "gpt-5.5", opts, "binding-a")
				selector.BindSession(t.Context(), "codex", "gpt-5.4", opts, "binding-a")
				if err := runCodexAvailabilityOperation(t.Context(), manager, operation, core.Request{Model: "gpt-5.5"}, opts); err == nil {
					t.Fatal("missing bounded model failure")
				}
				want := "binding-a"
				if failover {
					want = ""
				}
				if got := selector.cachedAuthID("codex", "gpt-5.5", opts); got != want {
					t.Fatalf("failed binding = %q, want %q", got, want)
				}
				if got := selector.cachedAuthID("codex", "gpt-5.4", opts); got != "binding-a" {
					t.Fatal("model failure removed another model's binding")
				}
				if calls := len(exec.ExecuteCalls()) + len(exec.CountCalls()) + len(exec.StreamCalls()); calls != 1 {
					t.Fatalf("binding release expanded the attempt budget: %d calls", calls)
				}
			})
		}
	}
}

type modelNotFoundBindingExecutor struct {
	schedulerProviderTestExecutor
	started chan struct{}
	release chan struct{}
}

func (e *modelNotFoundBindingExecutor) Execute(ctx context.Context, _ *Auth, _ core.Request, _ core.Options) (core.Response, error) {
	close(e.started)
	select {
	case <-ctx.Done():
		return core.Response{}, ctx.Err()
	case <-e.release:
		return core.Response{}, &Error{HTTPStatus: 404, Message: modelNotFoundFixture}
	}
}

func (e *modelNotFoundBindingExecutor) CountTokens(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	return e.Execute(ctx, auth, req, opts)
}

func (e *modelNotFoundBindingExecutor) ExecuteStream(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	chunks := make(chan core.StreamChunk, 2)
	chunks <- core.StreamChunk{Payload: []byte(`data: {"type":"response.output_text.delta","delta":"fixture"}`)}
	go func() {
		defer close(chunks)
		_, err := e.Execute(ctx, auth, req, opts)
		chunks <- core.StreamChunk{Err: err}
	}()
	return &core.StreamResult{Chunks: chunks}, nil
}

func TestModelNotFoundLateFailurePreservesNewBinding(t *testing.T) {
	for _, operation := range []string{"execute", "count", "stream"} {
		for _, rebind := range []string{"different_auth", "same_auth_after_rebind", "ttl_only"} {
			t.Run(operation+"/"+rebind, func(t *testing.T) {
				selector := NewSessionAffinitySelector(&FillFirstSelector{})
				t.Cleanup(selector.Stop)
				manager := NewManager(nil, selector, nil)
				manager.SetRetryConfig(0, 0, 1)
				exec := &modelNotFoundBindingExecutor{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"}, started: make(chan struct{}), release: make(chan struct{})}
				manager.RegisterExecutor(exec)
				var once sync.Once
				release := func() { once.Do(func() { close(exec.release) }) }
				t.Cleanup(release)
				registerFallbackAuthForModel(t, manager, &Auth{ID: "late-a", Provider: "codex"}, "gpt-5.5")
				opts := core.Options{Headers: http.Header{"X-Session-Id": {"late-failure-fixture"}}}
				selector.BindSession(t.Context(), "codex", "gpt-5.5", opts, "late-a")
				done := make(chan error, 1)
				go func() {
					done <- runCodexAvailabilityOperation(t.Context(), manager, operation, core.Request{Model: "gpt-5.5"}, opts)
				}()
				select {
				case <-exec.started:
				case <-time.After(5 * time.Second):
					t.Fatal("request did not start")
				}
				want := "late-b"
				if rebind != "ttl_only" {
					selector.BindSession(t.Context(), "codex", "gpt-5.5", opts, "late-b")
				}
				if rebind != "different_auth" {
					selector.BindSession(t.Context(), "codex", "gpt-5.5", opts, "late-a")
					want = "late-a"
				}
				if rebind == "ttl_only" {
					want = ""
				}
				release()
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("missing model failure")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("request did not finish")
				}
				if got := selector.cachedAuthID("codex", "gpt-5.5", opts); got != want {
					t.Fatalf("binding after late failure = %q, want %q", got, want)
				}
			})
		}
	}
}

func TestModelNotFoundReleaseRespectsRequestAndInstanceSnapshots(t *testing.T) {
	for _, scenario := range []string{"retired", "hot_reload_strict", "hot_reload_failover", "compact", "policy"} {
		t.Run(scenario, func(t *testing.T) {
			failover := scenario != "hot_reload_strict"
			selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Failover: &failover})
			t.Cleanup(selector.Stop)
			manager := NewManager(nil, selector, nil)
			auth, err := manager.Register(t.Context(), &Auth{ID: "snapshot-a", Provider: "codex"})
			if err != nil {
				t.Fatal(err)
			}
			ctx := manager.WithRoutingPolicySnapshot(t.Context())
			opts := core.Options{Headers: http.Header{"X-Session-Id": {"snapshot-failure-fixture"}}}
			selector.BindSession(ctx, "codex", "gpt-5.5", opts, auth.ID)
			opts = manager.withSessionAffinityResultSnapshot(ctx, []string{"codex"}, "gpt-5.5", opts)
			result := resultForAuth(auth, "codex", "gpt-5.5", false)
			result.Error = &Error{HTTPStatus: 404, Message: modelNotFoundFixture}
			want := auth.ID
			switch scenario {
			case "retired":
				if err := manager.Delete(t.Context(), auth.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := manager.Register(t.Context(), &Auth{ID: auth.ID, Provider: "codex"}); err != nil {
					t.Fatal(err)
				}
				selector.BindSession(ctx, "codex", "gpt-5.5", core.Options{Headers: opts.Headers}, auth.ID)
			case "hot_reload_strict", "hot_reload_failover":
				newFailover := !failover
				next := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Failover: &newFailover})
				t.Cleanup(next.Stop)
				manager.SetConfigAndSelector(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityFailover: &newFailover}}, next)
				if failover {
					want = ""
				}
			case "compact":
				result.availabilityNeutral = true
			case "policy":
				result.Error.Message = `{"error":{"code":"misalignment_policy_violation","type":"invalid_request_error"}}`
			}
			manager.markExecutionResult(ctx, result, opts)
			if got := selector.cachedAuthID("codex", "gpt-5.5", opts); got != want {
				t.Fatalf("binding after %s = %q, want %q", scenario, got, want)
			}
		})
	}
}
