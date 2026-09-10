package auth

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

type affinityResultOrderExecutor struct {
	schedulerProviderTestExecutor
	started chan string
	release chan struct{}
	once    sync.Once
}

func (e *affinityResultOrderExecutor) Execute(ctx context.Context, auth *Auth, req core.Request, _ core.Options) (core.Response, error) {
	if gjson.GetBytes(req.Payload, "input").String() == "slow" {
		e.once.Do(func() { e.started <- auth.ID })
		select {
		case <-ctx.Done():
			return core.Response{}, ctx.Err()
		case <-e.release:
		}
	} else if auth.ID == "order-a" {
		return core.Response{}, &Error{HTTPStatus: http.StatusServiceUnavailable, Message: "temporary overload"}
	}
	return core.Response{Payload: []byte(auth.ID)}, nil
}

func (e *affinityResultOrderExecutor) CountTokens(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	return e.Execute(ctx, auth, req, opts)
}

func (e *affinityResultOrderExecutor) ExecuteStream(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	response, err := e.Execute(ctx, auth, req, opts)
	if err != nil {
		return nil, err
	}
	chunks := make(chan core.StreamChunk, 1)
	chunks <- core.StreamChunk{Payload: response.Payload}
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func TestSessionAffinityEarlierResultPreservesReboundCredential(t *testing.T) {
	for _, operation := range []string{"execute", "count", "stream"} {
		t.Run(operation, func(t *testing.T) {
			selector := NewSessionAffinitySelector(&RoundRobinSelector{})
			t.Cleanup(selector.Stop)
			manager := NewManager(nil, selector, nil)
			manager.SetConfig(&config.Config{NoCooldownStatusCodes: []int{http.StatusServiceUnavailable}, Routing: config.RoutingConfig{SessionAffinity: true}})
			manager.SetRetryConfig(0, 0, 2)
			exec := &affinityResultOrderExecutor{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"}, started: make(chan string, 1), release: make(chan struct{})}
			manager.RegisterExecutor(exec)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(exec.release) }) }
			t.Cleanup(release)
			for _, id := range []string{"order-a", "order-b"} {
				if _, err := manager.Register(t.Context(), &Auth{ID: id, Provider: "codex", Status: StatusActive}); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "order-model"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			}
			opts := core.Options{Headers: http.Header{"X-Session-Id": {"result-order"}}}
			run := func(input string) error {
				req := core.Request{Model: "order-model", Payload: []byte(fmt.Sprintf(`{"input":%q}`, input))}
				if operation == "count" {
					_, err := manager.ExecuteCount(t.Context(), []string{"codex"}, req, opts)
					return err
				}
				if operation == "stream" {
					result, err := manager.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
					if err != nil {
						return err
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							return chunk.Err
						}
					}
					return nil
				}
				_, err := manager.Execute(t.Context(), []string{"codex"}, req, opts)
				return err
			}
			done := make(chan error, 1)
			go func() { done <- run("slow") }()
			select {
			case id := <-exec.started:
				if id != "order-a" {
					t.Fatalf("first auth=%s", id)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("earlier request not started")
			}
			if err := run("fast"); err != nil {
				t.Fatal(err)
			}
			if got := selector.cachedAuthID("codex", "order-model", opts); got != "order-b" {
				t.Fatalf("newer request binding=%s, want order-b", got)
			}
			release()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("earlier request not finished")
			}
			if got := selector.cachedAuthID("codex", "order-model", opts); got != "order-b" {
				t.Fatalf("earlier result replaced newer binding with %s", got)
			}
		})
	}
}
