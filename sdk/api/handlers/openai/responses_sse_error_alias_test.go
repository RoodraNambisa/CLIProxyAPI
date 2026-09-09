package openai

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestResponsesSSEErrorAliasesUseNormalErrorProjection(t *testing.T) {
	for _, frame := range []string{
		"event: response.error\ndata: {\"status_code\":429,\"error\":{\"message\":\"original failure\"}}\n\n",
		"data: {\"error\":{\"status_code\":429,\"message\":\"original failure\"}}\n\n",
		"event: response.completed\ndata: {\"type\":\"error\",\"status\":429,\"message\":\"original failure\"}\n\n",
		"data: {\"type\":\"response.completed\",\"response\":{\"status_code\":429,\"error\":{\"message\":\"original failure\"}}}\n\n",
	} {
		for _, rewrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rewrite=%t", strings.Split(frame, "\n")[0], rewrite), func(t *testing.T) {
				framer := &responsesSSEFramer{}
				if rewrite {
					body := map[string]any{"error": map[string]any{"message": "public failure"}}
					h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{ErrorResponseRewrites: []sdkconfig.ErrorResponseRewriteRule{{StatusCode: 429, ResponseStatusCode: 400, ResponseBody: &body}}}, nil))
					framer.rewriteTerminalError = h.rewriteResponsesSSETerminalErrorFrame
				}
				var out bytes.Buffer
				framer.WriteChunk(&out, []byte(frame))
				payload, _ := responsesSSEDataPayload(out.Bytes())
				if !strings.Contains(out.String(), "event: error\n") || strings.Contains(out.String(), "event: response.completed\n") {
					t.Fatal("recognized failure retained a success/unsupported event type")
				}
				if rewrite {
					if !strings.Contains(out.String(), "public failure") || strings.Contains(out.String(), "original failure") || framer.terminalError == nil || framer.terminalError.StatusCode != 400 {
						t.Fatal("error alias bypassed the configured display rule")
					}
				} else if gjson.GetBytes(payload, "type").String() != "error" || !strings.Contains(out.String(), "original failure") {
					t.Fatal("error alias did not become a usable Responses error")
				}
			})
		}
	}
}

func TestResponsesWebsocketErrorRewriteRemovesFlatErrorMirrors(t *testing.T) {
	body := map[string]any{"error": map[string]any{"message": "public failure", "code": "public_code"}}
	h := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{ErrorResponseRewrites: []sdkconfig.ErrorResponseRewriteRule{{StatusCode: 429, ResponseBody: &body}}}, nil)
	original := []byte(`{"type":"error","status":429,"message":"original failure","code":"original_code","param":"original_param","sequence_number":12,"request_id":"fixture-request"}`)
	projected := h.RewriteExecutionErrorResponseForGin(nil, &interfaces.ErrorMessage{StatusCode: 429})
	got, err := rewriteResponsesWebsocketTerminalErrorPayload(original, projected)
	if err != nil || strings.Contains(string(got), "original_") || strings.Contains(string(got), "original failure") || gjson.GetBytes(got, "error.message").String() != "public failure" || gjson.GetBytes(got, "sequence_number").Int() != 12 || gjson.GetBytes(got, "request_id").String() != "fixture-request" {
		t.Fatal("rewritten error retained original flat fields or removed unrelated protocol metadata")
	}
}

func TestResponsesSSEErrorAliasAcrossHTTPClients(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, codex := range []bool{false, true} {
		for _, trust := range []bool{false, true} {
			t.Run(fmt.Sprintf("codex=%t/trust=%t", codex, trust), func(t *testing.T) {
				alias := "event: response.error\ndata: {\"status_code\":429,\"error\":{\"message\":\"original failure\"}}\n\n"
				executor := &responsesMetadataCaptureExecutor{chunks: [][]byte{
					[]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"kept\"}\n\n" + alias),
				}}
				manager := coreauth.NewManager(nil, nil, nil)
				manager.RegisterExecutor(executor)
				model := fmt.Sprintf("sse-error-alias-%t-%t", codex, trust)
				auth := &coreauth.Auth{ID: model, Provider: "codex", Status: coreauth.StatusActive}
				if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(auth.ID, "codex", []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
				h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{TrustUpstreamSSE: trust}}, manager))
				router := gin.New()
				router.POST("/v1/responses", h.Responses)
				request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":%q,"stream":true,"input":"fixture"}`, model)))
				request.Header.Set("Content-Type", "application/json")
				if codex {
					request.Header.Set("User-Agent", "codex_cli_rs/0.153.4")
				}
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, request)
				body := recorder.Body.String()
				if recorder.Code != http.StatusOK || !strings.Contains(body, "kept") || !strings.Contains(body, "original failure") {
					t.Fatal("error alias discarded previously committed output or its cause")
				}
				wantEvent := "event: error\n"
				if codex {
					wantEvent = "event: response.failed\n"
				}
				if trust {
					if !strings.Contains(body, alias) {
						t.Fatal("trust mode changed the original error frame")
					}
				} else if strings.Count(body, wantEvent) != 1 || strings.Contains(body, "event: response.error\n") {
					t.Fatalf("client received the wrong terminal shape: %s", body)
				}
			})
		}
	}
}
