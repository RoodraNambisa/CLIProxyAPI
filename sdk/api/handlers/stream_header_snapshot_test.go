package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestStreamHeadersAreFinalWhenReturnedAfterBootstrapRetry(t *testing.T) {
	for _, protocol := range []string{"openai", "openai-response", "claude", "gemini"} {
		t.Run(protocol, func(t *testing.T) {
			for round := range 16 {
				ctx, cancel := context.WithCancel(t.Context())
				executor := &failOnceStreamExecutor{controlFirst: true}
				manager := coreauth.NewManager(nil, nil, nil)
				manager.RegisterExecutor(executor)
				model := fmt.Sprintf("header-snapshot-%s-%d", protocol, round)
				for index := range 2 {
					auth := &coreauth.Auth{ID: fmt.Sprintf("%s-%d", model, index), Provider: "codex", Status: coreauth.StatusActive}
					if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
						t.Fatal(errRegister)
					}
					registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
				}
				handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
					PassthroughHeaders: true,
					Streaming:          sdkconfig.StreamingConfig{BootstrapRetries: 1},
				}, manager)
				data, headers, failures := handler.ExecuteStreamWithAuthManager(ctx, protocol, model, []byte(`{"input":"fixture"}`), "")
				// Callers may inspect or copy response headers before reading any data.
				initial := headers.Clone()
				var body strings.Builder
				for chunk := range data {
					body.Write(chunk)
				}
				for failure := range failures {
					if failure != nil {
						cancel()
						t.Fatalf("stream failure: %v", failure.Error)
					}
				}
				cancel()
				if initial.Get("X-Upstream-Attempt") != "2" || headers.Get("X-Upstream-Attempt") != "2" {
					t.Fatalf("round %d: returned/final attempt = %q/%q; want 2/2", round, initial.Get("X-Upstream-Attempt"), headers.Get("X-Upstream-Attempt"))
				}
				if executor.Calls() != 2 || body.String() != "ok" {
					t.Fatalf("calls/body = %d/%q; want 2/ok", executor.Calls(), body.String())
				}
			}
		})
	}
}

type headerBoundaryStreamExecutor struct {
	failOnceStreamExecutor
	first  coreexecutor.StreamChunk
	opened chan struct{}
}

func (e *headerBoundaryStreamExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- e.first
	close(e.opened)
	go func() {
		<-ctx.Done()
		close(chunks)
	}()
	return &coreexecutor.StreamResult{Headers: http.Header{"X-Boundary": {"fixture"}}, Chunks: chunks}, nil
}

func TestStreamHeaderPublicationAllowsCommitAndCancellationWithoutPayload(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed-%t", committed), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			executor := &headerBoundaryStreamExecutor{opened: make(chan struct{}), first: coreexecutor.StreamChunk{Payload: []byte("event: pending\n\n")}}
			if committed {
				executor.first = coreexecutor.BootstrapCommitStreamChunk()
			}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			auth := &coreauth.Auth{ID: "header-boundary", Provider: "codex", Status: coreauth.StatusActive}
			if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
				t.Fatal(errRegister)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "header-boundary"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
			handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{PassthroughHeaders: true}, manager)
			returned, finished := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(finished)
				data, _, failures := handler.ExecuteStreamWithAuthManager(ctx, "openai-response", "header-boundary", []byte(`{}`), "")
				close(returned)
				if data != nil {
					for range data {
					}
				}
				for range failures {
				}
			}()
			<-executor.opened
			if committed {
				select {
				case <-returned:
				case <-time.After(3 * time.Second):
					t.Fatal("committed stream did not return headers before any payload")
				}
			}
			cancel()
			select {
			case <-finished:
			case <-time.After(3 * time.Second):
				t.Fatal("cancellation left header publication or stream consumption blocked")
			}
		})
	}
}
