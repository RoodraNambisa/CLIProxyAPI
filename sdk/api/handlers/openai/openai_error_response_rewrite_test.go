package openai

import (
	"bytes"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestWriteResponsesStreamErrorUsesExplicitRewriteBody(t *testing.T) {
	body := map[string]any{"custom": "terminal"}
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusTooManyRequests, ResponseStatusCode: http.StatusBadRequest, ResponseBody: &body},
	}}, nil)
	errMsg := base.RewriteExecutionErrorResponse(&interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New("limited"),
	})
	var output bytes.Buffer
	writeResponsesStreamError(&output, errMsg)
	if got := output.String(); got != "\n\nevent: error\ndata: {\"custom\":\"terminal\"}\n\n" {
		t.Fatalf("stream error = %q", got)
	}
}

func TestRewriteResponsesWebsocketTerminalErrorPayload(t *testing.T) {
	body := map[string]any{"error": map[string]any{"message": "too large", "code": "context_too_large"}}
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusTooManyRequests, MessageContains: "maximum context", ResponseStatusCode: http.StatusBadRequest, ResponseBody: &body},
	}}, nil)
	originalPayload := []byte(`{"type":"response.failed","status":429,"response":{"status":"failed","status_code":429,"error":{"status":429,"message":"maximum context exceeded"}}}`)
	originalErr := responsesWebsocketErrorMessageFromPayload(originalPayload)
	projected := base.RewriteExecutionErrorResponse(originalErr)
	rewritten, errRewrite := rewriteResponsesWebsocketTerminalErrorPayload(originalPayload, projected)
	if errRewrite != nil {
		t.Fatalf("rewriteResponsesWebsocketTerminalErrorPayload() error = %v", errRewrite)
	}
	if got := gjson.GetBytes(rewritten, "status").Int(); got != http.StatusBadRequest {
		t.Fatalf("status = %d", got)
	}
	if got := gjson.GetBytes(rewritten, "response.status_code").Int(); got != http.StatusBadRequest {
		t.Fatalf("response.status_code = %d", got)
	}
	if got := gjson.GetBytes(rewritten, "response.error.status").Int(); got != http.StatusBadRequest {
		t.Fatalf("response.error.status = %d", got)
	}
	if got := gjson.GetBytes(rewritten, "response.error.code").String(); got != "context_too_large" {
		t.Fatalf("response.error.code = %q", got)
	}
	if originalErr.StatusCode != http.StatusTooManyRequests || !shouldReleaseResponsesWebsocketPinnedAuth(projected) {
		t.Fatalf("original status/pin behavior changed: original=%d projected=%d", originalErr.StatusCode, projected.StatusCode)
	}
}

func TestRewriteResponsesWebsocketErrorUsesWholeCustomObject(t *testing.T) {
	body := map[string]any{"reason": "blocked"}
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusBadGateway, ResponseBody: &body},
	}}, nil)
	projected := base.RewriteExecutionErrorResponse(&interfaces.ErrorMessage{
		StatusCode: http.StatusBadGateway,
		Error:      errors.New("upstream failed"),
	})
	rewritten, errRewrite := rewriteResponsesWebsocketTerminalErrorPayload([]byte(`{"type":"error","error":{"message":"upstream failed"}}`), projected)
	if errRewrite != nil {
		t.Fatalf("rewriteResponsesWebsocketTerminalErrorPayload() error = %v", errRewrite)
	}
	if got := gjson.GetBytes(rewritten, "error.reason").String(); got != "blocked" {
		t.Fatalf("error.reason = %q", got)
	}
}

func TestRewriteResponsesWebsocketStatusOnlyPreservesUpstreamError(t *testing.T) {
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusTooManyRequests, ResponseStatusCode: http.StatusBadRequest},
	}}, nil)
	originalPayload := []byte(`{"type":"error","status":429,"headers":{"retry-after":"30","x-request-id":"request-1"},"error":{"status":429,"message":"limited","type":"rate_limit_error","code":"original_code","param":"input"}}`)
	projected := base.RewriteExecutionErrorResponse(responsesWebsocketErrorMessageFromPayload(originalPayload))
	rewritten, errRewrite := rewriteResponsesWebsocketTerminalErrorPayload(originalPayload, projected)
	if errRewrite != nil {
		t.Fatalf("rewriteResponsesWebsocketTerminalErrorPayload() error = %v", errRewrite)
	}
	if got := gjson.GetBytes(rewritten, "status").Int(); got != http.StatusBadRequest {
		t.Fatalf("status = %d", got)
	}
	if got := gjson.GetBytes(rewritten, "error.status").Int(); got != http.StatusBadRequest {
		t.Fatalf("error.status = %d", got)
	}
	if got := gjson.GetBytes(rewritten, "error.code").String(); got != "original_code" {
		t.Fatalf("error.code = %q", got)
	}
	if got := gjson.GetBytes(rewritten, "error.param").String(); got != "input" {
		t.Fatalf("error.param = %q", got)
	}
	if gjson.GetBytes(rewritten, "headers.retry-after").Exists() {
		t.Fatalf("Retry-After was not removed: %s", rewritten)
	}
	if got := gjson.GetBytes(rewritten, "headers.x-request-id").String(); got != "request-1" {
		t.Fatalf("x-request-id = %q", got)
	}
}

func TestBuildResponsesWebsocketErrorStatusOnlyUsesOriginalErrorShape(t *testing.T) {
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusTooManyRequests, ResponseStatusCode: http.StatusBadRequest},
	}}, nil)
	projected := base.RewriteExecutionErrorResponse(&interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New("limited"),
	})
	payload, errBuild := buildResponsesWebsocketErrorPayload(projected)
	if errBuild != nil {
		t.Fatalf("buildResponsesWebsocketErrorPayload() error = %v", errBuild)
	}
	if got := gjson.GetBytes(payload, "status").Int(); got != http.StatusBadRequest {
		t.Fatalf("status = %d", got)
	}
	if got := gjson.GetBytes(payload, "error.type").String(); got != "rate_limit_error" {
		t.Fatalf("error.type = %q", got)
	}
	if got := gjson.GetBytes(payload, "error.code").String(); got != "rate_limit_exceeded" {
		t.Fatalf("error.code = %q", got)
	}
}

func TestRewriteResponsesWebsocketBodyOnlyDoesNotAddStatus(t *testing.T) {
	body := map[string]any{"error": map[string]any{"code": "rewritten"}}
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{MessageContains: "blocked", ResponseBody: &body},
	}}, nil)
	originalPayload := []byte(`{"type":"response.failed","headers":{"Content-Length":"999","Content-Encoding":"gzip","ETag":"old","X-Request-Id":"request-1"},"response":{"status":"failed","error":{"message":"blocked"}}}`)
	projected := base.RewriteExecutionErrorResponse(responsesWebsocketErrorMessageFromPayload(originalPayload))
	rewritten, errRewrite := rewriteResponsesWebsocketTerminalErrorPayload(originalPayload, projected)
	if errRewrite != nil {
		t.Fatalf("rewriteResponsesWebsocketTerminalErrorPayload() error = %v", errRewrite)
	}
	if gjson.GetBytes(rewritten, "status").Exists() || gjson.GetBytes(rewritten, "response.status_code").Exists() {
		t.Fatalf("body-only rewrite added status fields: %s", rewritten)
	}
	if got := gjson.GetBytes(rewritten, "response.error.code").String(); got != "rewritten" {
		t.Fatalf("response.error.code = %q", got)
	}
	for _, path := range []string{"headers.Content-Length", "headers.Content-Encoding", "headers.ETag"} {
		if gjson.GetBytes(rewritten, path).Exists() {
			t.Fatalf("%s was not removed: %s", path, rewritten)
		}
	}
	if got := gjson.GetBytes(rewritten, "headers.X-Request-Id").String(); got != "request-1" {
		t.Fatalf("X-Request-Id = %q", got)
	}
}

func TestProjectedWebsocketAffinityUsesOriginalErrorText(t *testing.T) {
	body := map[string]any{"error": map[string]any{"message": "client-safe"}}
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusBadRequest, ResponseBody: &body},
	}}, nil)
	projected := base.RewriteExecutionErrorResponse(&interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      errors.New("previous_response_not_found"),
	})
	if !shouldReleaseResponsesWebsocketPinnedAuth(projected) {
		t.Fatal("projected error hid the original affinity marker")
	}
}

func TestResponsesSSEFramerRewritesTerminalFrameBeforeForwarding(t *testing.T) {
	body := map[string]any{"error": map[string]any{"message": "client-safe", "code": "rewritten"}}
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusTooManyRequests, ResponseStatusCode: http.StatusBadRequest, ResponseBody: &body},
	}}, nil)
	handler := &OpenAIResponsesAPIHandler{BaseAPIHandler: base}
	framer := &responsesSSEFramer{rewriteTerminalError: handler.rewriteResponsesSSETerminalErrorFrame}
	var output bytes.Buffer
	framer.WriteChunk(&output, []byte("event: response.failed\ndata: {\"type\":\"response.failed\",\"status\":429,\"response\":{\"status_code\":429,\"error\":{\"message\":\"upstream-secret\"}}}\n\n"))

	if !framer.terminalRewritten {
		t.Fatal("terminal frame was not marked rewritten")
	}
	if !framer.terminalBeforeData || framer.terminalError == nil || framer.terminalError.StatusCode != http.StatusBadRequest {
		t.Fatalf("first terminal state = before_data:%v error:%#v", framer.terminalBeforeData, framer.terminalError)
	}
	if got := output.String(); !strings.Contains(got, `"code":"rewritten"`) || strings.Contains(got, "upstream-secret") {
		t.Fatalf("rewritten frame = %q", got)
	}
	payload, ok := responsesSSEDataPayload(output.Bytes())
	if !ok || gjson.GetBytes(payload, "status").Int() != http.StatusBadRequest || gjson.GetBytes(payload, "response.status_code").Int() != http.StatusBadRequest {
		t.Fatalf("rewritten status payload = %s", payload)
	}
}

func TestResponsesSSERewriteAddsEventForDataOnlyCustomBody(t *testing.T) {
	body := map[string]any{"error": map[string]any{"message": "client-safe"}}
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusTooManyRequests, ResponseBody: &body},
	}}, nil)
	handler := &OpenAIResponsesAPIHandler{BaseAPIHandler: base}
	frame, _, rewritten := handler.rewriteResponsesSSETerminalErrorFrame([]byte("data: {\"type\":\"error\",\"status\":429,\"error\":{\"message\":\"limited\"}}\n\n"))
	if !rewritten || !strings.HasPrefix(string(frame), "event: error\n") {
		t.Fatalf("rewritten frame = %q, rewritten = %v", frame, rewritten)
	}
}

func TestResponsesSSERewriteMatchesExtractedErrorMessage(t *testing.T) {
	wrongBody := map[string]any{"error": map[string]any{"message": "wrong-rule"}}
	body := map[string]any{"error": map[string]any{"message": "client-safe"}}
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{MessageContains: "response.failed", ResponseBody: &wrongBody},
		{MessageContains: "ACTUAL PROBLEM", ResponseBody: &body},
	}}, nil)
	handler := &OpenAIResponsesAPIHandler{BaseAPIHandler: base}
	frame, _, rewritten := handler.rewriteResponsesSSETerminalErrorFrame([]byte("event: response.failed\ndata: {\"type\":\"response.failed\",\"status\":429,\"response\":{\"error\":{\"message\":\"actual problem\"}}}\n\n"))
	if !rewritten || !strings.Contains(string(frame), "client-safe") {
		t.Fatalf("rewritten frame = %q, rewritten = %v", frame, rewritten)
	}
}

func TestResponsesSSETrustPassthroughRemainsByteForByte(t *testing.T) {
	input := []byte("data: {\"type\":\"response.failed\",\"status\":429}\n\n")
	framer := &responsesSSEFramer{passthrough: true}
	var output bytes.Buffer
	framer.WriteChunk(&output, input)
	if !bytes.Equal(output.Bytes(), input) {
		t.Fatalf("passthrough output = %q, want %q", output.Bytes(), input)
	}
}
