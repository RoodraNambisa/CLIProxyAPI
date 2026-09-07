package handlers

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type routingReloadStreamExecutor struct {
	failOnceStreamExecutor
	manager *coreauth.Manager
	authIDs []string
}

func (e *routingReloadStreamExecutor) ExecuteStream(ctx context.Context, credential *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	coreexecutor.MarkUpstreamAttempt(ctx)
	e.mu.Lock()
	e.authIDs = append(e.authIDs, credential.ID)
	first := len(e.authIDs) == 1
	e.mu.Unlock()
	chunks := make(chan coreexecutor.StreamChunk, 2)
	if first {
		e.manager.SetSelector(&coreauth.RoundRobinSelector{})
		e.manager.SetConfig(&internalconfig.Config{})
		chunks <- coreexecutor.StreamChunk{Payload: []byte("event: pending\nid: 1\n\n")}
		chunks <- coreexecutor.StreamChunk{Err: &coreauth.Error{HTTPStatus: http.StatusServiceUnavailable, Message: "temporary overload"}}
	} else {
		chunks <- coreexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n")}
	}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestHandlerBootstrapRetryKeepsRoutingSnapshot(t *testing.T) {
	for _, format := range []string{"openai", "openai-response"} {
		t.Run(format, func(t *testing.T) {
			manager := coreauth.NewManager(nil, &coreauth.WeightedRoundRobinSelector{}, nil)
			executor := &routingReloadStreamExecutor{manager: manager}
			manager.RegisterExecutor(executor)
			for _, entry := range []struct{ id, weight, priority string }{{"first", "1", "5"}, {"retry", "1", "0"}, {"new-request", "0", "10"}} {
				credential := &coreauth.Auth{ID: entry.id, Provider: "codex", Status: coreauth.StatusActive, Attributes: map[string]string{"weight": entry.weight, "priority": entry.priority}}
				if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), credential); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(entry.id, "codex", []*registry.ModelInfo{{ID: "snapshot-bootstrap-model"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(entry.id) })
			}
			handler := NewBaseAPIHandlers(&config.SDKConfig{Streaming: config.StreamingConfig{BootstrapRetries: 1}}, manager)
			for range 2 {
				data, _, errs := handler.ExecuteStreamWithAuthManager(t.Context(), format, "snapshot-bootstrap-model", []byte(`{"input":[]}`), "")
				size := 0
				for data != nil || errs != nil {
					select {
					case chunk, ok := <-data:
						if !ok {
							data = nil
						} else {
							size += len(chunk)
						}
					case msg, ok := <-errs:
						if !ok {
							errs = nil
						} else if msg != nil {
							t.Errorf("unexpected streaming error: %v", msg.Error)
						}
					}
				}
				if size == 0 {
					t.Error("successful response produced no output")
				}
			}
			executor.mu.Lock()
			defer executor.mu.Unlock()
			if !reflect.DeepEqual(executor.authIDs, []string{"first", "retry", "new-request"}) {
				t.Fatalf("routing changed within a bootstrap retry: %v", executor.authIDs)
			}
		})
	}
}
