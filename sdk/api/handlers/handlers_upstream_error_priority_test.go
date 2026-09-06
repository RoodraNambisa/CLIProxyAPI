package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type observedPreambleExecutor struct{ policyPreambleExecutor }

func (e *observedPreambleExecutor) ExecuteStream(ctx context.Context, credential *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	coreexecutor.MarkUpstreamAttempt(ctx)
	return e.policyPreambleExecutor.ExecuteStream(ctx, credential, req, opts)
}

func TestHandlerPreservesObservedFailureWhenBootstrapRetryHasNoCredential(t *testing.T) {
	for _, retries := range []int{0, 1} {
		t.Run(fmt.Sprintf("retries=%d", retries), func(t *testing.T) {
			fault := &coreauth.Error{HTTPStatus: http.StatusServiceUnavailable, Message: "upstream unavailable"}
			exec := &observedPreambleExecutor{policyPreambleExecutor{fault: fault, preamble: "event: pending\nid: 1\n\n"}}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(exec)
			const id = "observed-bootstrap-only"
			if _, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, Provider: "codex", Status: coreauth.StatusActive}); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "observed-bootstrap-model"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			h := NewBaseAPIHandlers(&config.SDKConfig{Streaming: config.StreamingConfig{BootstrapRetries: retries}}, manager)
			data, _, errs := h.ExecuteStreamWithAuthManager(t.Context(), "openai-response", "observed-bootstrap-model", []byte(`{"input":[]}`), "")
			errorCount := 0
			for data != nil || errs != nil {
				select {
				case _, ok := <-data:
					if !ok {
						data = nil
					}
				case msg, ok := <-errs:
					if !ok {
						errs = nil
						continue
					}
					errorCount++
					if msg == nil {
						t.Fatal("missing error message")
					}
					if msg.StatusCode != 503 || !errors.Is(msg.Error, fault) {
						t.Errorf("bootstrap retry replaced observed failure: status=%d type=%T cause=%v observed=%t", msg.StatusCode, msg.Error, errors.Is(msg.Error, fault), coreexecutor.IsUpstreamAttemptError(msg.Error))
					}
				}
			}
			if errorCount != 1 || exec.Calls() != 1 {
				t.Fatalf("unexpected error or upstream call count: errors=%d calls=%d", errorCount, exec.Calls())
			}
		})
	}
}
