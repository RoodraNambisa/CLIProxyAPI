package auth

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type usageStreamModeExecutor struct {
	streamResultTestExecutor
	modes chan bool
}

func (e *usageStreamModeExecutor) Execute(ctx context.Context, _ *Auth, _ core.Request, _ core.Options) (core.Response, error) {
	e.modes <- usage.StreamFromContext(ctx)
	return core.Response{Payload: []byte(`{}`)}, nil
}

func (e *usageStreamModeExecutor) ExecuteStream(ctx context.Context, _ *Auth, _ core.Request, _ core.Options) (*core.StreamResult, error) {
	e.modes <- usage.StreamFromContext(ctx)
	return successfulStreamResult(`data: {"type":"response.completed"}`), nil
}

func (e *usageStreamModeExecutor) CountTokens(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	return e.Execute(ctx, auth, req, opts)
}

func TestManagerPinsUsageStreamModeBeforeExecution(t *testing.T) {
	for _, operation := range []string{"execute", "count", "stream"} {
		for _, pinned := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("%s/pinned=%d", operation, pinned), func(t *testing.T) {
				executor := &usageStreamModeExecutor{streamResultTestExecutor: streamResultTestExecutor{id: "usage-mode-fixture"}, modes: make(chan bool, 1)}
				manager := NewManager(nil, nil, nil)
				manager.RegisterExecutor(executor)
				auth := &Auth{ID: uuid.NewString(), Provider: executor.Identifier(), Status: StatusActive}
				if _, err := manager.Register(WithSkipPersist(t.Context()), auth); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "fixture"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
				ctx := t.Context()
				want := operation == "stream"
				if pinned >= 0 {
					want = pinned == 1
					ctx = usage.WithStream(ctx, want)
				}
				req, opts := core.Request{Model: "fixture", Payload: []byte(`{}`)}, core.Options{}
				var err error
				switch operation {
				case "execute":
					_, err = manager.Execute(ctx, []string{auth.Provider}, req, opts)
				case "count":
					_, err = manager.ExecuteCount(ctx, []string{auth.Provider}, req, opts)
				case "stream":
					var result *core.StreamResult
					result, err = manager.ExecuteStream(ctx, []string{auth.Provider}, req, opts)
					if err == nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								t.Fatal(chunk.Err)
							}
						}
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := <-executor.modes; got != want {
					t.Fatalf("logical response mode=%t, want %t", got, want)
				}
			})
		}
	}
}
