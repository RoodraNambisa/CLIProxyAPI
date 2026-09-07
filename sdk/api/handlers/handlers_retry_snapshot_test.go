package handlers

import (
	"context"
	"net/http"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type retryReloadStreamExecutor struct {
	failOnceStreamExecutor
	manager *coreauth.Manager
}

func (e *retryReloadStreamExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	coreexecutor.MarkUpstreamAttempt(ctx)
	e.mu.Lock()
	e.calls++
	first := e.calls == 1
	e.mu.Unlock()
	failure := &coreauth.Error{HTTPStatus: http.StatusServiceUnavailable, Message: "synthetic upstream failure"}
	if !first {
		return nil, failure
	}
	e.manager.SetRetryConfig(2, 0, 0)
	chunks := make(chan coreexecutor.StreamChunk, 2)
	chunks <- coreexecutor.StreamChunk{Payload: []byte("event: pending\nid: 1\n\n")}
	chunks <- coreexecutor.StreamChunk{Err: failure}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestHandlerBootstrapRetryKeepsRequestRetrySettings(t *testing.T) {
	for _, format := range []string{"openai", "openai-response"} {
		t.Run(format, func(t *testing.T) {
			manager := coreauth.NewManager(nil, nil, nil)
			manager.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{503}})
			executor := &retryReloadStreamExecutor{manager: manager}
			manager.RegisterExecutor(executor)
			credential := &coreauth.Auth{ID: "retry-snapshot", Provider: "codex", Status: coreauth.StatusActive}
			if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), credential); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(credential.ID, "codex", []*registry.ModelInfo{{ID: "retry-snapshot-model"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credential.ID) })
			for index, bootstrapRetries := range []int{1, 0} {
				handler := NewBaseAPIHandlers(&config.SDKConfig{Streaming: config.StreamingConfig{BootstrapRetries: bootstrapRetries}}, manager)
				data, _, errs := handler.ExecuteStreamWithAuthManager(t.Context(), format, "retry-snapshot-model", []byte(`{"input":[]}`), "")
				receivedError := false
				for data != nil || errs != nil {
					select {
					case _, ok := <-data:
						if !ok {
							data = nil
						}
					case msg, ok := <-errs:
						if !ok {
							errs = nil
						} else if msg != nil {
							receivedError = true
							if msg.StatusCode != 503 {
								t.Errorf("unexpected failure status: %d", msg.StatusCode)
							}
						}
					}
				}
				if !receivedError {
					t.Fatal("missing synthetic upstream error")
				}
				want := []int{2, 5}[index]
				executor.mu.Lock()
				calls := executor.calls
				executor.mu.Unlock()
				if calls != want {
					t.Fatalf("request %d: total calls=%d want=%d", index, calls, want)
				}
			}
		})
	}
}
