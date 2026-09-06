package handlers

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type policyPreambleExecutor struct {
	failOnceStreamExecutor
	fault    error
	preamble string
}

func (e *policyPreambleExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	chunks := make(chan coreexecutor.StreamChunk, 2)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(e.preamble)}
	chunks <- coreexecutor.StreamChunk{Err: e.fault}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestHandlerPolicyRefusalAfterPreambleNeverRetries(t *testing.T) {
	for _, code := range []string{"misalignment_policy_violation", "cyber_policy", "content_policy_violation"} {
		for _, format := range []string{"openai", "openai-response"} {
			t.Run(code+"/"+format, func(t *testing.T) {
				fault := &coreauth.Error{HTTPStatus: http.StatusForbidden, Message: `{"error":{"code":"` + code + `"}}`}
				exec := &policyPreambleExecutor{fault: fault, preamble: "event: pending\nid: 1\nretry: 1000\n\n"}
				manager := coreauth.NewManager(nil, nil, nil)
				manager.RegisterExecutor(exec)
				for _, id := range []string{"policy-handler-one", "policy-handler-two"} {
					if _, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, Provider: "codex", Status: coreauth.StatusActive}); err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "policy-model"}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				}
				h := NewBaseAPIHandlers(&config.SDKConfig{Streaming: config.StreamingConfig{BootstrapRetries: 2}}, manager)
				data, _, errs := h.ExecuteStreamWithAuthManager(t.Context(), format, "policy-model", []byte(`{"model":"policy-model","input":[]}`), "")
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
						if msg == nil || !errors.Is(msg.Error, fault) {
							t.Error("handler replaced policy refusal")
						}
					}
				}
				if exec.Calls() != 1 {
					t.Fatalf("logical request made %d attempts for a policy refusal", exec.Calls())
				}
				for _, auth := range manager.List() {
					if auth.Unavailable || len(auth.ModelStates) > 0 {
						t.Error("policy refusal changed credential health")
					}
				}
			})
		}
	}
}
