package handlers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type generateMetadataExecutor struct {
	failOnceStreamExecutor
	t       *testing.T
	want    bool
	checked atomic.Int32
}

func (e *generateMetadataExecutor) Execute(ctx context.Context, _ *auth.Auth, _ core.Request, opts core.Options) (core.Response, error) {
	e.checked.Add(1)
	if value, ok := opts.Metadata[core.GenerateMetadataKey].(bool); !ok || value != e.want || usage.GenerateFromContext(ctx) != e.want {
		e.t.Error("API generation intent was lost before execution")
	}
	return core.Response{Payload: []byte(`{}`)}, nil
}

func (e *generateMetadataExecutor) CountTokens(ctx context.Context, auth *auth.Auth, req core.Request, opts core.Options) (core.Response, error) {
	return e.Execute(ctx, auth, req, opts)
}

func (e *generateMetadataExecutor) ExecuteStream(ctx context.Context, auth *auth.Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	_, _ = e.Execute(ctx, auth, req, opts)
	chunks := make(chan core.StreamChunk, 1)
	chunks <- core.StreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"output\":[]}}\n\n")}
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func TestAPIGenerateMetadataAcrossExecutionEntrypoints(t *testing.T) {
	for _, operation := range []string{"execute", "override", "count", "stream"} {
		t.Run(operation, func(t *testing.T) {
			executor := &generateMetadataExecutor{t: t}
			manager := auth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			credential := &auth.Auth{ID: uuid.NewString(), Provider: "codex", Status: auth.StatusActive}
			if _, err := manager.Register(auth.WithSkipPersist(t.Context()), credential); err != nil {
				t.Fatal(err)
			}
			model := "generate-" + uuid.NewString()
			registry.GetGlobalRegistry().RegisterClient(credential.ID, "codex", []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credential.ID) })
			handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
			for _, payload := range []string{`{"generate":false}`, `{}`, `{"generate":true}`, `{"generate":"false"}`, `{"generate":null}`, `{"generate":0}`, `{"metadata":{"generate":false}}`, `{"generate":false}`} {
				executor.want = payload != `{"generate":false}`
				executor.checked.Store(0)
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				var errMsg *interfaces.ErrorMessage
				switch operation {
				case "execute":
					_, _, errMsg = handler.ExecuteWithAuthManager(ctx, "openai-response", model, []byte(payload), "")
				case "override":
					_, _, errMsg = handler.ExecuteWithProvidersAndExecutionModel(ctx, []string{"codex"}, "openai-response", model, model, []byte(payload), "")
				case "count":
					_, _, errMsg = handler.ExecuteCountWithAuthManager(ctx, "openai-response", model, []byte(payload), "")
				case "stream":
					data, _, errs := handler.ExecuteStreamWithAuthManager(ctx, "openai-response", model, []byte(payload), "")
					for data != nil || errs != nil {
						select {
						case _, ok := <-data:
							if !ok {
								data = nil
							}
						case failure, ok := <-errs:
							if !ok {
								errs = nil
							} else if failure != nil {
								errMsg = failure
							}
						case <-ctx.Done():
							cancel()
							t.Fatal("stream did not complete")
						}
					}
				}
				cancel()
				if errMsg != nil || executor.checked.Load() != 1 {
					t.Fatalf("entrypoint failed or changed attempts: error=%v attempts=%d", errMsg, executor.checked.Load())
				}
			}
		})
	}
	setGenerateMetadata(nil, []byte(`{"generate":false}`))
}
