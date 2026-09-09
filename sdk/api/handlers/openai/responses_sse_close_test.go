package openai

import (
	"errors"
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
)

func TestResponsesSSECloseValidationAtHTTPBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, trust := range []bool{false, true} {
		for _, kind := range []string{"empty", "delta", "done", "partial", "failed", "completed"} {
			t.Run(fmt.Sprintf("trust=%t/%s", trust, kind), func(t *testing.T) {
				wire := ""
				switch kind {
				case "delta":
					wire = `data: {"type":"response.output_text.delta","delta":"kept"}` + "\n\n"
				case "done":
					wire = "data: [DONE]\n\n"
				case "partial":
					wire = `data: {"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}` + "\n\n"
				case "failed":
					wire = `event: response.error` + "\ndata: {\"status_code\":429,\"error\":{\"message\":\"limited\"}}\n\n"
				case "completed":
					wire = `data: {"type":"response.completed","response":{"status":"completed","output":[]}}` + "\n\n"
				}
				executor := &responsesMetadataCaptureExecutor{chunks: [][]byte{[]byte(wire)}}
				manager := coreauth.NewManager(nil, nil, nil)
				manager.RegisterExecutor(executor)
				model := fmt.Sprintf("sse-close-%t-%s", trust, kind)
				auth := &coreauth.Auth{ID: model, Provider: "codex", Status: coreauth.StatusActive}
				if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(auth.ID, "codex", []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
				cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{TrustUpstreamSSE: trust}}
				h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(cfg, manager))
				router := gin.New()
				router.POST("/v1/responses", h.Responses)
				request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":%q,"stream":true,"input":"fixture"}`, model)))
				request.Header.Set("Content-Type", "application/json")
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, request)
				body := recorder.Body.String()
				wantStatus := http.StatusOK
				if kind == "failed" && !trust {
					wantStatus = http.StatusTooManyRequests
				}
				// The Manager already rejects an empty upstream stream before SSE forwarding.
				if kind == "empty" {
					wantStatus = http.StatusInternalServerError
				}
				if recorder.Code != wantStatus {
					t.Fatalf("status=%d, want %d; body=%s", recorder.Code, wantStatus, body)
				}
				wantMissing := !trust && (kind == "delta" || kind == "done")
				if strings.Contains(body, errResponsesSSEMissingTerminal.Error()) != wantMissing {
					t.Fatalf("missing terminal result changed: %s", body)
				}
				if kind == "delta" && !strings.Contains(body, "kept") {
					t.Fatal("partial output was discarded by the close check")
				}
			})
		}
	}
}

func TestForwardResponsesCloseReportsMissingTerminal(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	close(data)
	close(errs)
	var canceled error
	h.forwardResponsesStream(c, flusher, func(err error) { canceled = err }, data, errs, nil)
	if !errors.Is(canceled, errResponsesSSEMissingTerminal) || !strings.Contains(recorder.Body.String(), errResponsesSSEMissingTerminal.Error()) {
		t.Fatal("clean data-channel close silently succeeded without a protocol terminal")
	}
}

func TestForwardResponsesCloseDoesNotDuplicateDeliveredFailure(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)
	framer := &responsesSSEFramer{}
	framer.WriteChunk(c.Writer, []byte("data: {\"type\":\"response.failed\",\"status\":429,\"error\":{\"message\":\"limited\"}}\n\n"))
	before := recorder.Body.String()
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	close(data)
	close(errs)
	var canceled error
	h.forwardResponsesStream(c, flusher, func(err error) { canceled = err }, data, errs, framer)
	if canceled == nil || recorder.Body.String() != before || !errors.Is(canceled, framer.terminalError.Error) {
		t.Fatal("delivered failure was duplicated or completed as a success")
	}
}

func TestForwardResponsesCloseDoesNotApplyErrorRulesTwice(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)
	first := map[string]any{"error": map[string]any{"message": "first projection"}}
	second := map[string]any{"error": map[string]any{"message": "second projection"}}
	h.UpdateClients(&sdkconfig.SDKConfig{ErrorResponseRewrites: []sdkconfig.ErrorResponseRewriteRule{
		{StatusCode: 429, ResponseStatusCode: 400, ResponseBody: &first},
		{StatusCode: 400, ResponseStatusCode: 403, ResponseBody: &second},
	}})
	framer := &responsesSSEFramer{rewriteTerminalError: h.rewriteResponsesSSETerminalErrorFrame}
	framer.WriteChunk(c.Writer, []byte("data: {\"type\":\"response.failed\",\"status\":429,\"error\":{\"message\":\"limited\"}}\n\n"))
	original := framer.terminalError
	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	close(data)
	close(errs)
	var canceled error
	h.forwardResponsesStream(c, flusher, func(err error) { canceled = err }, data, errs, framer)
	if original == nil || !errors.Is(canceled, original.Error) || strings.Count(recorder.Body.String(), "first projection") != 1 || strings.Contains(recorder.Body.String(), "second projection") {
		t.Fatal("close processing reapplied a different display rule to the projected error")
	}
}
