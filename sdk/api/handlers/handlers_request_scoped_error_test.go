package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type requestScopedHandlerExecutor struct {
	failOnceStreamExecutor
	preamble string
	fault    error
	release  bool
}

func (e *requestScopedHandlerExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()
	if e.release {
		if ctrl := coreexecutor.RequestBodyReleaseControllerFromOptions(opts); ctrl != nil {
			ctrl.Release()
		}
	}
	chunks := make(chan coreexecutor.StreamChunk, 2)
	if call == 1 {
		if e.preamble != "" {
			chunks <- coreexecutor.StreamChunk{Payload: []byte(e.preamble)}
		}
		chunks <- coreexecutor.StreamChunk{Err: e.fault}
	} else {
		chunks <- coreexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n")}
	}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks, Headers: http.Header{"X-Fixture-Attempt": {fmt.Sprint(call)}}}, nil
}

func TestRequestScopedActionsAcrossHandlerBootstrap(t *testing.T) {
	for _, providerRules := range []bool{false, true} {
		for _, format := range []string{"openai", "openai-response"} {
			for _, action := range []string{"stop", "stop-and-cooldown", "continue", "continue-and-cooldown"} {
				for _, scenario := range []string{"empty", "control", "content", "released", "policy"} {
					t.Run(fmt.Sprintf("provider=%t/%s/%s/%s", providerRules, format, action, scenario), func(t *testing.T) {
						fault := &coreauth.Error{HTTPStatus: 500, Message: "fixture-failure"}
						exec := &requestScopedHandlerExecutor{fault: fault, release: scenario == "released"}
						switch scenario {
						case "control", "released":
							exec.preamble = "event: pending\nid: 1\nretry: 1000\n\n"
						case "content":
							exec.preamble = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
						case "policy":
							fault.HTTPStatus = 403
							fault.Message = `{"error":{"code":"misalignment_policy_violation"}}`
							exec.preamble = "event: pending\n\n"
						}
						rules := []internalconfig.RequestScopedErrorRule{{Status: fault.HTTPStatus, Match: []string{fault.Message}, Action: action}}
						manager := coreauth.NewManager(nil, nil, nil)
						manager.RegisterExecutor(exec)
						manager.SetRetryConfig(2, 0, 2)
						if providerRules {
							manager.SetConfig(&internalconfig.Config{OAuthRequestScopedErrors: map[string][]internalconfig.RequestScopedErrorRule{"codex": rules}})
						}
						for _, id := range []string{"error-handler-one", "error-handler-two"} {
							auth := &coreauth.Auth{ID: id, Provider: "codex", Status: coreauth.StatusActive}
							if !providerRules {
								auth.Metadata = map[string]any{"request_scoped_errors": rules}
							}
							if _, err := manager.Register(t.Context(), auth); err != nil {
								t.Fatal(err)
							}
							registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "error-handler-model"}})
							t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
						}
						ctx := t.Context()
						var releaseController *coreexecutor.RequestBodyReleaseController
						if exec.release {
							releaseController = coreexecutor.NewRequestBodyReleaseController(64, []byte("released"))
							ctx = coreexecutor.WithRequestBodyReleaseController(ctx, releaseController)
						}
						h := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{BootstrapRetries: 2}, PassthroughHeaders: true, RequestBodyRelease: sdkconfig.RequestBodyReleaseConfig{Enable: exec.release}}, manager)
						data, _, errs := h.ExecuteStreamWithAuthManager(ctx, format, "error-handler-model", []byte(`{"model":"error-handler-model","input":[]}`), "")
						var output strings.Builder
						var failure error
						for data != nil || errs != nil {
							select {
							case payload, ok := <-data:
								if !ok {
									data = nil
								} else {
									output.Write(payload)
								}
							case msg, ok := <-errs:
								if !ok {
									errs = nil
								} else if msg != nil {
									failure = msg.Error
								}
							}
						}
						wantCalls := 1
						if releaseController != nil && !releaseController.Released() {
							t.Fatal("fixture did not exercise actual body release")
						}
						if strings.HasPrefix(action, "continue") && (scenario == "empty" || scenario == "control") {
							wantCalls = 2
						}
						if exec.Calls() != wantCalls {
							t.Fatalf("upstream calls %d, want %d", exec.Calls(), wantCalls)
						}
						if wantCalls == 1 && !errors.Is(failure, fault) {
							t.Fatal("original error chain was lost")
						}
						if wantCalls == 2 && (failure != nil || !strings.Contains(output.String(), `"delta":"ok"`)) {
							t.Fatal("allowed bootstrap recovery failed")
						}
						if scenario == "content" && !strings.Contains(output.String(), `"delta":"partial"`) {
							t.Fatal("committed output was lost")
						}
						if scenario == "policy" {
							for _, auth := range manager.List() {
								if auth.Unavailable || len(auth.ModelStates) != 0 {
									t.Fatal("policy action changed credential health")
								}
							}
						}
					})
				}
			}
		}
	}
}
