package executor

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/xai"
	internalcache "github.com/router-for-me/CLIProxyAPI/v6/internal/cache"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func makeValidXAIEncryptedContent(seed byte) string {
	buf := make([]byte, 0, 256)
	for i := 0; len(buf) < 256; i++ {
		sum := sha256.Sum256([]byte{seed, byte(i), byte(i >> 8)})
		buf = append(buf, sum[:]...)
	}
	return base64.RawStdEncoding.EncodeToString(buf[:256])
}

func TestXAIUsingAPIRoutingAndChatProxyHeaders(t *testing.T) {
	oauth := &cliproxyauth.Auth{Attributes: map[string]string{
		"auth_kind": "oauth",
		"base_url":  xaiauth.DefaultAPIBaseURL,
	}}
	if got := xaiChatBaseURL(oauth); got != xaiauth.CLIChatProxyBaseURL {
		t.Fatalf("OAuth base URL = %q, want %q", got, xaiauth.CLIChatProxyBaseURL)
	}
	req := httptest.NewRequest(http.MethodPost, xaiauth.CLIChatProxyBaseURL+"/responses", nil)
	applyXAIChatHeaders(req, oauth, "token", true, "conversation")
	if got := req.Header.Get(xaiTokenAuthHeader); got != xaiTokenAuthValue {
		t.Fatalf("%s = %q, want %q", xaiTokenAuthHeader, got, xaiTokenAuthValue)
	}
	if got := req.Header.Get(xaiClientVersionHeader); got != xaiClientVersionValue {
		t.Fatalf("%s = %q, want %q", xaiClientVersionHeader, got, xaiClientVersionValue)
	}

	official := &cliproxyauth.Auth{Attributes: map[string]string{
		"auth_kind":     "oauth",
		"base_url":      xaiauth.DefaultAPIBaseURL,
		xaiUsingAPIAttr: "true",
	}}
	if got := xaiChatBaseURL(official); got != xaiauth.DefaultAPIBaseURL {
		t.Fatalf("using_api base URL = %q, want %q", got, xaiauth.DefaultAPIBaseURL)
	}
	req = httptest.NewRequest(http.MethodPost, xaiauth.DefaultAPIBaseURL+"/responses", nil)
	applyXAIChatHeaders(req, official, "token", true, "")
	if req.Header.Get(xaiTokenAuthHeader) != "" || req.Header.Get(xaiClientVersionHeader) != "" {
		t.Fatalf("official API unexpectedly received CLI headers: %v", req.Header)
	}
}

func TestXAIExecutorCompactUsesCompactEndpoint(t *testing.T) {
	var path string
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		body, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_1","object":"response.compaction","output":[{"type":"compaction","encrypted_content":"opaque"}]}`))
	}))
	defer server.Close()

	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	resp, err := exec.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "grok-4.3",
		Payload: []byte(`{"model":"grok-4.3","stream":true,"input":[{"role":"user","content":"hello"}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],"tool_choice":"auto","parallel_tool_calls":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Alt: "responses/compact"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if path != "/responses/compact" || gjson.GetBytes(body, "stream").Exists() {
		t.Fatalf("compact path=%q body=%s", path, body)
	}
	if gjson.GetBytes(body, "tools").Exists() || gjson.GetBytes(body, "tool_choice").Exists() || gjson.GetBytes(body, "parallel_tool_calls").Exists() {
		t.Fatalf("compact body retained tool controls: %s", body)
	}
	if got := gjson.GetBytes(resp.Payload, "object").String(); got != "response.compaction" {
		t.Fatalf("response object = %q", got)
	}
}

func TestXAIExecutorReturnsTerminalFailureFromSuccessfulHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"generation failed\"}}}\n\n"))
	}))
	defer server.Close()

	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "xai-terminal", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	_, err := exec.Execute(context.Background(), auth, xaiStreamRequest(), cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err == nil || !strings.Contains(err.Error(), "generation failed") {
		t.Fatalf("Execute() error = %v, want terminal failure", err)
	}
}

func TestXAIExecutorPreservesRetryAfterHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"code":"rate_limit","error":"too many requests"}`))
	}))
	defer server.Close()

	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	_, err := exec.Execute(context.Background(), auth, xaiStreamRequest(), cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err == nil {
		t.Fatal("Execute() error = nil, want 429")
	}
	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok {
		t.Fatalf("error type = %T, want RetryAfter support", err)
	}
	if retryable.RetryAfter() == nil || *retryable.RetryAfter() != 2*time.Minute {
		t.Fatalf("RetryAfter = %v, want 2m", retryable.RetryAfter())
	}
}

func TestXAIExecuteStreamEmitsTerminalFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"content_filter\"}}}\n\n"))
	}))
	defer server.Close()

	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "xai-stream-terminal", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	result, err := exec.ExecuteStream(context.Background(), auth, xaiStreamRequest(), cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var terminalErr error
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			terminalErr = chunk.Err
		}
	}
	if terminalErr == nil || !strings.Contains(terminalErr.Error(), "content_filter") {
		t.Fatalf("stream terminal error = %v", terminalErr)
	}
	skipper, ok := terminalErr.(interface{ SkipAuthResult() bool })
	if !ok || !skipper.SkipAuthResult() {
		t.Fatalf("SkipAuthResult = %v, want true", ok)
	}
}

func TestNormalizeXAIToolsAndForeignEncryptedContent(t *testing.T) {
	body := []byte(`{"tools":[{"type":"namespace","name":"codex_app","tools":[{"type":"function","name":"automation_update","strict":true,"parameters":{"oneOf":[{"type":"object"}]}}]},{"type":"tool_search"},{"type":"image_generation"}],"tool_choice":"auto","parallel_tool_calls":true}`)
	out := normalizeXAIToolChoiceForTools(normalizeXAITools(body))
	if got := gjson.GetBytes(out, "tools.#").Int(); got != 1 {
		t.Fatalf("normalized tools = %s", out)
	}
	if got := gjson.GetBytes(out, "tools.0.name").String(); got != "automation_update" {
		t.Fatalf("tool name = %q", got)
	}
	if gjson.GetBytes(out, "tools.0.parameters.oneOf").Exists() || gjson.GetBytes(out, "tools.0.strict").Bool() {
		t.Fatalf("automation schema was not simplified: %s", out)
	}

	valid := makeValidXAIEncryptedContent(7)
	input := []byte(`{"input":[{"type":"reasoning","encrypted_content":"gAAAA-invalid"},{"type":"reasoning","encrypted_content":""}]}`)
	input, _ = sjson.SetBytes(input, "input.1.encrypted_content", valid)
	cleaned := sanitizeXAIInputEncryptedContent(input)
	if gjson.GetBytes(cleaned, "input.0.encrypted_content").Exists() {
		t.Fatalf("foreign encrypted_content remained: %s", cleaned)
	}
	if got := gjson.GetBytes(cleaned, "input.1.encrypted_content").String(); got != valid {
		t.Fatalf("valid Grok encrypted_content = %q", got)
	}
}

func TestNormalizeXAIToolsDropsChoiceForRemovedTool(t *testing.T) {
	body := []byte(`{"tools":[{"type":"tool_search"},{"type":"function","name":"lookup","parameters":{"type":"object"}}],"tool_choice":{"type":"tool_search"},"parallel_tool_calls":true}`)
	out := normalizeXAIToolChoiceForTools(normalizeXAITools(body))
	if got := gjson.GetBytes(out, "tools.#").Int(); got != 1 {
		t.Fatalf("normalized tools = %s", out)
	}
	if gjson.GetBytes(out, "tool_choice").Exists() {
		t.Fatalf("removed tool choice remained: %s", out)
	}
	if !gjson.GetBytes(out, "parallel_tool_calls").Bool() {
		t.Fatalf("parallel_tool_calls should remain with usable tools: %s", out)
	}
}

func TestXAIReasoningReplayCachesAndInjectsClaudeTurn(t *testing.T) {
	internalcache.ClearXAIReasoningReplayCache()
	t.Cleanup(internalcache.ClearXAIReasoningReplayCache)
	valid := makeValidXAIEncryptedContent(9)
	scope := xaiReasoningReplayScope{namespace: "tenant-a", modelName: "grok-4.3", sessionKey: "execution:session-1"}
	completed := []byte(`{"response":{"output":[{"type":"reasoning","encrypted_content":""}]}}`)
	completed, _ = sjson.SetBytes(completed, "response.output.0.encrypted_content", valid)
	cacheXAIReasoningReplayFromCompleted(context.Background(), scope, completed)

	req := cliproxyexecutor.Request{Model: "grok-4.3", Payload: []byte(`{"input":[{"role":"user","content":"next"}]}`)}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatClaude,
		Metadata:     map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "session-1"},
	}
	body, _, err := applyXAIReasoningReplayCacheRequired(context.Background(), "tenant-a", opts.SourceFormat, req, opts, req.Payload)
	if err != nil {
		t.Fatalf("applyXAIReasoningReplayCacheRequired() error = %v", err)
	}
	if got := gjson.GetBytes(body, "input.0.encrypted_content").String(); got != valid {
		t.Fatalf("replayed encrypted_content = %q; body=%s", got, body)
	}
	if !strings.EqualFold(gjson.GetBytes(body, "input.1.role").String(), "user") {
		t.Fatalf("user turn shifted incorrectly: %s", body)
	}
	body, _, err = applyXAIReasoningReplayCacheRequired(context.Background(), "tenant-b", opts.SourceFormat, req, opts, req.Payload)
	if err != nil {
		t.Fatalf("isolated replay error = %v", err)
	}
	if gjson.GetBytes(body, "input.0.encrypted_content").Exists() {
		t.Fatalf("tenant-b received tenant-a replay: %s", body)
	}
}

func TestClearXAIReasoningReplayOnInvalidSignature(t *testing.T) {
	internalcache.ClearXAIReasoningReplayCache()
	t.Cleanup(internalcache.ClearXAIReasoningReplayCache)

	scope := xaiReasoningReplayScope{namespace: "tenant-a", modelName: "grok-4.3", sessionKey: "execution:session-1"}
	completed := []byte(`{"response":{"output":[{"type":"reasoning","encrypted_content":""}]}}`)
	completed, _ = sjson.SetBytes(completed, "response.output.0.encrypted_content", makeValidXAIEncryptedContent(7))
	cacheXAIReasoningReplayFromCompleted(context.Background(), scope, completed)
	if !clearXAIReasoningReplayOnInvalidSignature(context.Background(), scope, http.StatusBadRequest, []byte(`{"type":"error","error":{"code":"invalid_signature","message":"verification failed"}}`)) {
		t.Fatal("expected invalid_signature to clear xAI replay")
	}
	if _, ok := internalcache.GetXAIReasoningReplayItems(scope.modelName, scope.cacheSessionKey()); ok {
		t.Fatal("xAI replay entry still exists")
	}
}

func TestXAIPrepareResponsesAppliesRegisteredThinkingProvider(t *testing.T) {
	exec := NewXAIExecutor(&config.Config{})
	req := cliproxyexecutor.Request{
		Model:   "grok-4.3",
		Payload: []byte(`{"model":"grok-4.3","input":"hello","reasoning":{"effort":"high"}}`),
	}
	prepared, err := exec.prepareResponsesRequest(context.Background(), nil, req, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, false)
	if err != nil {
		t.Fatalf("prepareResponsesRequest() error = %v", err)
	}
	if got := gjson.GetBytes(prepared.body, "reasoning.effort").String(); got != "high" {
		t.Fatalf("reasoning.effort = %q, want high; body=%s", got, prepared.body)
	}
}

func TestXAICountTokensReturnsPositiveInputCount(t *testing.T) {
	exec := NewXAIExecutor(&config.Config{})
	resp, err := exec.CountTokens(context.Background(), nil, cliproxyexecutor.Request{
		Model:   "custom-grok-model",
		Payload: []byte(`{"model":"custom-grok-model","input":"count these tokens"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err != nil {
		t.Fatalf("CountTokens() error = %v", err)
	}
	if got := gjson.GetBytes(resp.Payload, "response.usage.input_tokens").Int(); got <= 0 {
		t.Fatalf("input_tokens = %d; payload=%s", got, resp.Payload)
	}
}

func TestXAIMediaEndpointRouting(t *testing.T) {
	tests := []struct {
		name        string
		format      sdktranslator.Format
		requestPath string
		wantImage   string
		wantVideo   string
	}{
		{name: "image generation", format: sdktranslator.FromString(xaiImageHandlerType), requestPath: "/v1/images/generations", wantImage: xaiImagesGenerationsPath},
		{name: "image edit", format: sdktranslator.FromString(xaiImageHandlerType), requestPath: "/v1/images/edits", wantImage: xaiImagesEditsPath},
		{name: "video generation", format: sdktranslator.FromString(xaiVideoHandlerType), requestPath: "/v1/videos/generations", wantVideo: xaiVideosGenerationsPath},
		{name: "video edit", format: sdktranslator.FromString(xaiVideoHandlerType), requestPath: "/v1/videos/edits", wantVideo: xaiVideosEditsPath},
		{name: "video extension", format: sdktranslator.FromString(xaiVideoHandlerType), requestPath: "/v1/videos/extensions", wantVideo: xaiVideosExtensionsPath},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := cliproxyexecutor.Options{
				SourceFormat: tt.format,
				Metadata:     map[string]any{cliproxyexecutor.RequestPathMetadataKey: tt.requestPath},
			}
			if got := xaiImageEndpointPath(opts); got != tt.wantImage {
				t.Fatalf("xaiImageEndpointPath() = %q, want %q", got, tt.wantImage)
			}
			if got := xaiVideoEndpointPath(opts); got != tt.wantVideo {
				t.Fatalf("xaiVideoEndpointPath() = %q, want %q", got, tt.wantVideo)
			}
		})
	}
}

func TestXAIVideoRetrieveLogsGETWithoutBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	requestMethod := make(chan string, 1)
	requestBody := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		requestMethod <- r.Method
		requestBody <- data
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"video_1","status":"completed"}`))
	}))
	defer server.Close()

	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	exec := NewXAIExecutor(&config.Config{SDKConfig: config.SDKConfig{RequestLog: true}})
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	_, err := exec.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "grok-imagine-video",
		Payload: []byte(`{"request_id":"video_1"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString(xaiVideoHandlerType),
		Metadata:     map[string]any{cliproxyexecutor.RequestPathMetadataKey: "/v1/videos/:request_id"},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := <-requestMethod; got != http.MethodGet {
		t.Fatalf("upstream method = %s, want GET", got)
	}
	if got := <-requestBody; len(got) != 0 {
		t.Fatalf("upstream body = %q, want empty", got)
	}

	raw, ok := ginCtx.Get("API_REQUEST")
	if !ok {
		t.Fatal("API request log missing")
	}
	logBody, ok := raw.([]byte)
	if !ok {
		t.Fatalf("API request log type = %T, want []byte", raw)
	}
	text := string(logBody)
	if !strings.Contains(text, "HTTP Method: GET") || !strings.Contains(text, "Body:\n<empty>") {
		t.Fatalf("unexpected API request log: %s", text)
	}
	if strings.Contains(text, "request_id") {
		t.Fatalf("GET request log retained unsent body: %s", text)
	}
}
