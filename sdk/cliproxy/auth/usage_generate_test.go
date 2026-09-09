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

type usageGenerateExecutor struct {
	streamResultTestExecutor
	values chan bool
}

func (e *usageGenerateExecutor) Execute(ctx context.Context, _ *Auth, _ core.Request, _ core.Options) (core.Response, error) {
	e.values <- usage.GenerateFromContext(ctx)
	return core.Response{Payload: []byte(`{}`)}, nil
}

func (e *usageGenerateExecutor) ExecuteStream(ctx context.Context, _ *Auth, _ core.Request, _ core.Options) (*core.StreamResult, error) {
	e.values <- usage.GenerateFromContext(ctx)
	return successfulStreamResult(`data: {"type":"response.completed"}`), nil
}

func (e *usageGenerateExecutor) CountTokens(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	return e.Execute(ctx, auth, req, opts)
}

func TestManagerPinsGenerateMetadataForAllOperations(t *testing.T) {
	for _, operation := range []string{"execute", "count", "stream"} {
		for _, value := range []any{nil, false, true, "false", 0} {
			for _, inherited := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/value=%v/inherited=%t", operation, value, inherited), func(t *testing.T) {
					executor := &usageGenerateExecutor{streamResultTestExecutor: streamResultTestExecutor{id: "usage-generate-fixture"}, values: make(chan bool, 1)}
					manager := NewManager(nil, nil, nil)
					manager.RegisterExecutor(executor)
					auth := &Auth{ID: uuid.NewString(), Provider: executor.Identifier(), Status: StatusActive}
					if _, err := manager.Register(WithSkipPersist(t.Context()), auth); err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "fixture"}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
					ctx := usage.WithGenerate(t.Context(), inherited)
					req := core.Request{Model: "fixture", Payload: []byte(`{}`)}
					opts := core.Options{Metadata: map[string]any{core.GenerateMetadataKey: value}}
					want := inherited
					if explicit, ok := value.(bool); ok {
						want = explicit
					}
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
					if got := <-executor.values; got != want || usage.GenerateFromContext(ctx) != inherited {
						t.Fatal("generate metadata lost its precedence or mutated the caller context")
					}
				})
			}
		}
	}
}

func TestGenerateSnapshotSurvivesMetadataReplacement(t *testing.T) {
	opts := core.Options{Metadata: map[string]any{core.GenerateMetadataKey: false}}
	ctx := contextWithGenerateMetadata(t.Context(), opts)
	opts.Metadata[core.GenerateMetadataKey] = true
	if usage.GenerateFromContext(ctx) || contextWithGenerateMetadata(ctx, core.Options{}) != ctx || contextWithGenerateMetadata(nil, core.Options{}) != nil {
		t.Fatal("missing options or a later metadata edit replaced the request snapshot")
	}
}
