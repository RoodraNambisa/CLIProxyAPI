package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type responsesMetadataCaptureExecutor struct {
	provider             string
	metadata             map[string]any
	effectivePassthrough *bool
	chunks               [][]byte
}

type responsesNotifyingFlusher struct {
	flushed chan struct{}
}

type responsesCommittedErrorExecutor struct {
	responsesMetadataCaptureExecutor
}

func (e *responsesCommittedErrorExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk, 2)
	chunks <- coreexecutor.BootstrapCommitStreamChunk()
	chunks <- coreexecutor.StreamChunk{Err: &coreauth.Error{
		Code:       "upstream_closed",
		Message:    "upstream closed before first response event",
		HTTPStatus: http.StatusBadGateway,
	}}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (flusher *responsesNotifyingFlusher) Flush() {
	select {
	case flusher.flushed <- struct{}{}:
	default:
	}
}

func (e *responsesMetadataCaptureExecutor) Identifier() string {
	if strings.TrimSpace(e.provider) != "" {
		return e.provider
	}
	return "codex"
}

func (e *responsesMetadataCaptureExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *responsesMetadataCaptureExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, _ coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.metadata = make(map[string]any, len(opts.Metadata))
	for key, value := range opts.Metadata {
		e.metadata[key] = value
	}
	if state, ok := opts.Metadata[coreexecutor.ImageGenerationStreamPassthroughStateMetadataKey].(*coreexecutor.ImageGenerationStreamPassthroughState); ok {
		effective := false
		if requested, _ := opts.Metadata[coreexecutor.ImageGenerationStreamPassthroughMetadataKey].(bool); requested {
			effective = true
		}
		if e.effectivePassthrough != nil {
			effective = *e.effectivePassthrough
		}
		state.SetEnabled(effective)
	}
	payloads := e.chunks
	if len(payloads) == 0 {
		payloads = [][]byte{[]byte(`data: {"type":"response.completed","response":{"id":"resp-1","output":[]}}`)}
	}
	chunks := make(chan coreexecutor.StreamChunk, len(payloads))
	for _, payload := range payloads {
		chunks <- coreexecutor.StreamChunk{Payload: payload}
	}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestResponsesStreamingUsesEffectiveImagePassthroughAfterPolicy(t *testing.T) {
	for _, tt := range []struct {
		name       string
		trustSSE   bool
		wantOutput int
	}{
		{name: "removed tool uses normal repair", wantOutput: 1},
		{name: "trusted SSE keeps direct passthrough", trustSSE: true, wantOutput: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			effective := false
			executor := &responsesMetadataCaptureExecutor{
				effectivePassthrough: &effective,
				chunks: [][]byte{
					[]byte("data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc-1\",\"name\":\"lookup\",\"arguments\":\"{}\"}}\n\n"),
					[]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[]}}\n\n"),
				},
			}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			model := "responses-effective-passthrough-" + strings.ReplaceAll(tt.name, " ", "-")
			auth := &coreauth.Auth{ID: model + "-auth", Provider: "codex", Status: coreauth.StatusActive}
			if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

			cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{TrustUpstreamSSE: tt.trustSSE}}
			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(cfg, manager))
			router := gin.New()
			router.POST("/v1/responses", h.Responses)
			body := fmt.Sprintf(`{"model":%q,"stream":true,"input":"draw","tools":[{"type":"function","name":"image_gen.imagegen"}]}`, model)
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", response.Code, response.Body.String())
			}

			var completedOutput gjson.Result
			for _, frame := range strings.Split(strings.TrimSpace(response.Body.String()), "\n\n") {
				payload, okPayload := responsesSSEDataPayload([]byte(frame))
				if okPayload && gjson.GetBytes(payload, "type").String() == "response.completed" {
					completedOutput = gjson.GetBytes(payload, "response.output")
				}
			}
			if !completedOutput.IsArray() || len(completedOutput.Array()) != tt.wantOutput {
				t.Fatalf("completed output = %s, want %d items; body=%s", completedOutput.Raw, tt.wantOutput, response.Body.String())
			}
		})
	}
}

func TestResponsesStreamingDoesNotLoseCommittedErrorWhenDataCloses(t *testing.T) {
	for attempt := 0; attempt < 32; attempt++ {
		executor := &responsesCommittedErrorExecutor{}
		manager := coreauth.NewManager(nil, nil, nil)
		manager.RegisterExecutor(executor)
		model := fmt.Sprintf("responses-committed-error-%d", attempt)
		auth := &coreauth.Auth{ID: model + "-auth", Provider: "codex", Status: coreauth.StatusActive}
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("register auth: %v", err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})

		h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
		router := gin.New()
		router.POST("/v1/responses", h.Responses)
		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/responses",
			strings.NewReader(fmt.Sprintf(`{"model":%q,"stream":true,"input":"hello"}`, model)),
		)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)

		if response.Code != http.StatusBadGateway {
			t.Fatalf("attempt %d status = %d, want 502; body=%s", attempt, response.Code, response.Body.String())
		}
	}
}

func TestResponsesStreamingReturnsBadGatewayForInvalidFirstFrame(t *testing.T) {
	oversizedFrame := []byte("id: " + strings.Repeat("x", responsesSSEMaxPendingBytes))
	splitAt := len(oversizedFrame) / 2
	executor := &responsesMetadataCaptureExecutor{
		chunks: [][]byte{
			oversizedFrame[:splitAt],
			oversizedFrame[splitAt:],
		},
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	model := "responses-invalid-first-frame"
	auth := &coreauth.Auth{ID: model + "-auth", Provider: "codex", Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(auth.ID)

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	router := gin.New()
	router.POST("/v1/responses", h.Responses)
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/responses",
		strings.NewReader(fmt.Sprintf(`{"model":%q,"stream":true,"input":"hello"}`, model)),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "event: error") {
		t.Fatalf("preflight failure should use a JSON error response: %s", response.Body.String())
	}
}

func (e *responsesMetadataCaptureExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *responsesMetadataCaptureExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *responsesMetadataCaptureExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func newResponsesStreamTestHandler(t *testing.T) (*OpenAIResponsesAPIHandler, *httptest.ResponseRecorder, *gin.Context, http.Flusher) {
	t.Helper()

	gin.SetMode(gin.TestMode)
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	h := NewOpenAIResponsesAPIHandler(base)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		t.Fatalf("expected gin writer to implement http.Flusher")
	}

	return h, recorder, c, flusher
}

func TestResponsesStreamingImageToolFormsEnablePassthrough(t *testing.T) {
	tests := []struct {
		name      string
		provider  string
		tools     string
		trustSSE  bool
		wantImage bool
		wantTrust bool
	}{
		{name: "native", tools: `[{"type":"image_generation"}]`, wantImage: true},
		{name: "function", tools: `[{"type":"function","name":"image_gen.imagegen"}]`, wantImage: true},
		{name: "namespace", tools: `[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"imagegen"}]}]`, wantImage: true},
		{name: "trusted function", tools: `[{"type":"function","name":"image_gen.imagegen"}]`, trustSSE: true, wantImage: true, wantTrust: true},
		{name: "chatgpt web trusted function", provider: "chatgpt-web", tools: `[{"type":"function","name":"image_gen.imagegen"}]`, trustSSE: true, wantImage: true, wantTrust: true},
		{name: "trusted text", tools: `[{"type":"function","name":"lookup"}]`, trustSSE: true, wantTrust: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &responsesMetadataCaptureExecutor{provider: tt.provider}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			model := "responses-image-tool-" + strings.ReplaceAll(tt.name, " ", "-")
			auth := &coreauth.Auth{ID: model + "-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
			if auth.Provider == "chatgpt-web" {
				auth.Metadata = map[string]any{"access_token": "token", "lifecycle_state": coreauth.LifecycleStateActive}
			}
			if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

			cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{TrustUpstreamSSE: tt.trustSSE}}
			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(cfg, manager))
			router := gin.New()
			router.POST("/v1/responses", h.Responses)
			body := fmt.Sprintf(`{"model":%q,"stream":true,"input":"draw","tools":%s}`, model, tt.tools)
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", response.Code, response.Body.String())
			}
			if got, _ := executor.metadata[coreexecutor.ImageGenerationStreamPassthroughMetadataKey].(bool); got != tt.wantImage {
				t.Fatalf("image passthrough metadata = %v, want %v", got, tt.wantImage)
			}
			if got, _ := executor.metadata[coreexecutor.TrustUpstreamSSEMetadataKey].(bool); got != tt.wantTrust {
				t.Fatalf("trust SSE metadata = %v, want %v", got, tt.wantTrust)
			}
		})
	}
}

func TestForwardResponsesStreamSeparatesDataOnlySSEChunks(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)

	data := make(chan []byte, 2)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"arguments\":\"{}\"}}")
	data <- []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[]}}")
	close(data)
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, nil)
	body := recorder.Body.String()
	parts := strings.Split(strings.TrimSpace(body), "\n\n")
	if len(parts) != 2 {
		t.Fatalf("expected 2 SSE events, got %d. Body: %q", len(parts), body)
	}

	expectedPart1 := "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"arguments\":\"{}\"}}"
	if parts[0] != expectedPart1 {
		t.Errorf("unexpected first event.\nGot: %q\nWant: %q", parts[0], expectedPart1)
	}

	expectedPart2 := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[{\"type\":\"function_call\",\"arguments\":\"{}\"}]}}"
	if parts[1] != expectedPart2 {
		t.Errorf("unexpected second event.\nGot: %q\nWant: %q", parts[1], expectedPart2)
	}
}

func TestForwardResponsesStreamFlushesTrustedSSEComments(t *testing.T) {
	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{
		EnableStreamFlush:   true,
		StreamFlushMinBytes: 1 << 20,
	}}
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(cfg, nil))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	requestContext, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(requestContext)

	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	flusher := &responsesNotifyingFlusher{flushed: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, &responsesSSEFramer{passthrough: true}, false)
	}()

	data <- []byte(": trusted-upstream-pending\n\n")
	select {
	case <-flusher.flushed:
	case <-time.After(time.Second):
		t.Fatal("trusted upstream SSE comment was not flushed")
	}
	cancelContext()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forwardResponsesStream did not stop after cancellation")
	}
}

func TestForwardResponsesStreamFlushesTranslatedSSEComments(t *testing.T) {
	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{
		EnableStreamFlush:   true,
		StreamFlushMinBytes: 1 << 20,
	}}
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(cfg, nil))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	requestContext, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(requestContext)

	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	flusher := &responsesNotifyingFlusher{flushed: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, &responsesSSEFramer{}, false)
	}()

	data <- []byte(": translated-upstream-pending\n\n")
	select {
	case <-flusher.flushed:
	case <-time.After(time.Second):
		t.Fatal("translated upstream SSE comment was not flushed")
	}
	cancelContext()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forwardResponsesStream did not stop after cancellation")
	}
}

func TestForwardResponsesStreamFlushesSplitImageHeartbeat(t *testing.T) {
	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{
		EnableStreamFlush:   true,
		StreamFlushMinBytes: 1 << 20,
	}}
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(cfg, nil))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	requestContext, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(requestContext)

	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	flusher := &responsesNotifyingFlusher{flushed: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, &responsesSSEFramer{passthrough: true}, true)
	}()

	data <- []byte(": chatgpt-web upstream pending\n")
	select {
	case <-flusher.flushed:
		t.Fatal("partial heartbeat was flushed before its frame completed")
	case <-time.After(20 * time.Millisecond):
	}
	data <- []byte("\n")
	select {
	case <-flusher.flushed:
	case <-time.After(time.Second):
		t.Fatal("split image heartbeat was not flushed")
	}
	cancelContext()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forwardResponsesStream did not stop after cancellation")
	}
}

func TestForwardResponsesStreamUsesRegularFlushWhenImagePassthroughIsDisabled(t *testing.T) {
	enabled := true
	cfg := &sdkconfig.SDKConfig{
		Images: sdkconfig.ImagesConfig{
			EnableStreamFlush:     &enabled,
			StreamFlushIntervalMS: 10_000,
		},
	}
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(cfg, nil))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	requestContext, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(requestContext)

	data := make(chan []byte)
	errs := make(chan *interfaces.ErrorMessage)
	state := &coreexecutor.ImageGenerationStreamPassthroughState{}
	flusher := &responsesNotifyingFlusher{flushed: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.forwardResponsesStream(
			c,
			flusher,
			func(error) {},
			data,
			errs,
			&responsesSSEFramer{passthroughState: state},
			true,
		)
	}()

	data <- []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n")
	select {
	case <-flusher.flushed:
	case <-time.After(time.Second):
		t.Fatal("regular Responses event used disabled image passthrough batching")
	}
	cancelContext()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forwardResponsesStream did not stop after cancellation")
	}
}

func TestResponsesSSEFramerEnablesPassthroughAfterHeartbeat(t *testing.T) {
	state := &coreexecutor.ImageGenerationStreamPassthroughState{}
	framer := &responsesSSEFramer{passthroughState: state}
	var output strings.Builder

	framer.WriteChunk(&output, []byte(": pending\n\n"))
	state.SetEnabled(true)
	framer.WriteChunk(&output, []byte(`data: {"type":"response.completed","response":{"output":[]}}`))

	if got := output.String(); !strings.Contains(got, ": pending") ||
		!strings.Contains(got, `"response":{"output":[]}`) {
		t.Fatalf("dynamic passthrough output = %q", got)
	}
	if len(framer.outputItems) != 0 {
		t.Fatalf("dynamic passthrough unexpectedly repaired output: %#v", framer.outputItems)
	}
}

func TestResponsesSSEFramerPassthroughPreservesSplitFrameBytes(t *testing.T) {
	framer := &responsesSSEFramer{passthrough: true}
	var output strings.Builder
	chunks := []string{
		`data: {"type":"response.`,
		"completed\",\"response\":{\"output\":[]}}\n\n",
	}
	for _, chunk := range chunks {
		framer.WriteChunk(&output, []byte(chunk))
	}
	if got, want := output.String(), strings.Join(chunks, ""); got != want {
		t.Fatalf("passthrough output = %q, want %q", got, want)
	}
}

func TestResponsesStreamErrorStartsAfterIncompletePassthroughFrame(t *testing.T) {
	framer := &responsesSSEFramer{passthrough: true}
	var output strings.Builder
	framer.WriteChunk(&output, []byte(`data: {"type":"response.output_text.delta"`))
	writeResponsesStreamError(&output, &interfaces.ErrorMessage{
		StatusCode: http.StatusBadGateway,
		Error:      errors.New("upstream disconnected"),
	})

	got := output.String()
	if !strings.Contains(got, "delta\"\n\nevent: error\n") {
		t.Fatalf("error event was merged into the incomplete upstream frame: %q", got)
	}
}

func TestResponsesSSEFramerImagePassthroughFramesLogicalChunks(t *testing.T) {
	state := &coreexecutor.ImageGenerationStreamPassthroughState{}
	state.SetEnabled(true)
	framer := &responsesSSEFramer{passthroughState: state}
	var output strings.Builder

	framer.WriteChunk(&output, []byte(`data: {"type":"response.output_item.added"}`))
	framer.WriteChunk(&output, []byte(`data: {"type":"response.completed"}`))

	if got, want := output.String(),
		"data: {\"type\":\"response.output_item.added\"}\n\ndata: {\"type\":\"response.completed\"}\n\n"; got != want {
		t.Fatalf("image passthrough output = %q, want %q", got, want)
	}
}

func TestResponsesSSEFramerImagePassthroughBuffersSplitFrame(t *testing.T) {
	state := &coreexecutor.ImageGenerationStreamPassthroughState{}
	state.SetEnabled(true)
	framer := &responsesSSEFramer{passthroughState: state}
	var output strings.Builder

	framer.WriteChunk(&output, []byte(`data: {"type":"response.output_item.added","item":{"type":"image_`))
	if output.Len() != 0 {
		t.Fatalf("partial image frame was emitted: %q", output.String())
	}
	framer.WriteChunk(&output, []byte("generation_call\"}}\n\n"))

	want := "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"image_generation_call\"}}\n\n"
	if got := output.String(); got != want {
		t.Fatalf("split image frame output = %q, want %q", got, want)
	}
}

func TestResponsesSSEFramerRejectsUnboundedIncompleteFrame(t *testing.T) {
	framer := &responsesSSEFramer{maxPendingBytes: 32}
	var output strings.Builder

	framer.WriteChunk(&output, []byte("event: response.output_text.delta\n"))
	framer.WriteChunk(&output, []byte("id: "+strings.Repeat("x", 32)))

	if framer.Err() == nil {
		t.Fatal("expected incomplete frame limit error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(framer.Err(), &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("frame error = %v", framer.Err())
	}
	if output.Len() != 0 {
		t.Fatalf("partial frame was written before validation: %q", output.String())
	}
}

func TestResponsesSSEFramerAllowsMultipleBoundedFramesInOneChunk(t *testing.T) {
	framer := &responsesSSEFramer{maxPendingBytes: 20}
	var output strings.Builder

	framer.WriteChunk(&output, []byte("data: {\"a\":1}\n\ndata: {\"b\":2}\n\n"))

	if err := framer.Err(); err != nil {
		t.Fatalf("framer error = %v", err)
	}
	if got, want := output.String(), "data: {\"a\":1}\n\ndata: {\"b\":2}\n\n"; got != want {
		t.Fatalf("framed output = %q, want %q", got, want)
	}
}

func TestResponsesSSEFramerForwardsCompleteFrameBeforePartialTail(t *testing.T) {
	framer := &responsesSSEFramer{}
	var output strings.Builder

	framer.WriteChunk(&output, []byte("data: {\"a\":1}\n\ndata: {\"b\":"))

	if err := framer.Err(); err != nil {
		t.Fatalf("framer error = %v", err)
	}
	if got, want := output.String(), "data: {\"a\":1}\n\n"; got != want {
		t.Fatalf("completed output = %q, want %q", got, want)
	}
	if got := string(framer.pending); got != "data: {\"b\":" {
		t.Fatalf("pending tail = %q", got)
	}
}

func TestResponsesSSEFramerPreservesSplitCRLFFrameBoundary(t *testing.T) {
	framer := &responsesSSEFramer{}
	var output strings.Builder

	framer.WriteChunk(&output, []byte("data: {\"a\":1}\r\n\r"))
	if got := output.String(); got != "" {
		t.Fatalf("split CRLF frame was committed early: %q", got)
	}
	framer.WriteChunk(&output, []byte("\n"))

	if err := framer.Err(); err != nil {
		t.Fatalf("framer error = %v", err)
	}
	if got, want := output.String(), "data: {\"a\":1}\r\n\r\n"; got != want {
		t.Fatalf("framed output = %q, want %q", got, want)
	}
}

func TestResponsesSSEFramerFlushesTrailingCRAtEOF(t *testing.T) {
	tests := []struct {
		name  string
		chunk string
		want  string
	}{
		{
			name:  "single data line",
			chunk: "data: {\"a\":1}\r",
			want:  "data: {\"a\":1}\r\r",
		},
		{
			name:  "complete CR-only frame",
			chunk: "event: item\rdata: {\"a\":1}\r\r",
			want:  "event: item\rdata: {\"a\":1}\r\r",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			framer := &responsesSSEFramer{}
			var output strings.Builder

			framer.WriteChunk(&output, []byte(test.chunk))
			if got := output.String(); got != "" {
				t.Fatalf("trailing CR was committed before EOF: %q", got)
			}
			framer.Flush(&output)

			if err := framer.Err(); err != nil {
				t.Fatalf("framer error = %v", err)
			}
			if got := output.String(); got != test.want {
				t.Fatalf("framed output = %q, want %q", got, test.want)
			}
		})
	}
}

func TestResponsesSSEFramerAcceptsMultilineDataAtEOF(t *testing.T) {
	for _, test := range []struct {
		name      string
		separator string
	}{
		{name: "LF", separator: "\n"},
		{name: "CR", separator: "\r"},
		{name: "CRLF", separator: "\r\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			frame := strings.Join([]string{
				"event: response.completed",
				`data: {"type":"response.completed",`,
				`data: "response":{"id":"resp-1","output":[]}}`,
			}, test.separator)
			if !responsesSSECanEmitAtEOF([]byte(frame)) {
				t.Fatalf("multiline %s frame was rejected at EOF: %q", test.name, frame)
			}

			framer := &responsesSSEFramer{}
			var output strings.Builder
			framer.WriteChunk(&output, []byte(frame))
			framer.Flush(&output)
			if err := framer.Err(); err != nil {
				t.Fatalf("framer error = %v", err)
			}
			if !strings.Contains(output.String(), `"type":"response.completed"`) {
				t.Fatalf("framed output = %q", output.String())
			}
		})
	}
}

func TestResponsesSSEFramerDoesNotTreatChunkPrefixAsLineBoundary(t *testing.T) {
	for _, prefix := range []string{"data:", "event:", "id:", "retry:", ":"} {
		t.Run(prefix, func(t *testing.T) {
			framer := &responsesSSEFramer{}
			var output strings.Builder

			framer.WriteChunk(&output, []byte(`data: {"value":"`))
			framer.WriteChunk(&output, []byte(prefix+"text\"}\n\n"))

			if err := framer.Err(); err != nil {
				t.Fatalf("framer error = %v", err)
			}
			if !strings.Contains(output.String(), `"value":"`+prefix+`text"`) {
				t.Fatalf("split data output = %q", output.String())
			}
		})
	}
}

func TestForwardResponsesStreamWritesBoundedFrameError(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)
	data := make(chan []byte, 2)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte("event: response.output_text.delta\n")
	data <- []byte("id: " + strings.Repeat("x", 32))
	close(data)
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, &responsesSSEFramer{maxPendingBytes: 32})

	body := recorder.Body.String()
	if !strings.Contains(body, `"code":"internal_server_error"`) ||
		!strings.Contains(body, "SSE frame exceeds 32 bytes") {
		t.Fatalf("stream error body = %q", body)
	}
	if count := strings.Count(body, "event: error"); count != 1 {
		t.Fatalf("terminal error event count = %d, want 1: %q", count, body)
	}
}

func TestForwardResponsesStreamRepairsEmptyCompletedOutputFromDoneItems(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)

	data := make(chan []byte, 3)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte(`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs-1","summary":[]}}`)
	data <- []byte(`data: {"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"shell","arguments":"{\"cmd\":\"pwd\"}","status":"completed"}}`)
	data <- []byte(`data: {"type":"response.completed","response":{"id":"resp-1","output":[]}}`)
	close(data)
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, nil)

	parts := strings.Split(strings.TrimSpace(recorder.Body.String()), "\n\n")
	if len(parts) != 3 {
		t.Fatalf("expected 3 SSE events, got %d. Body: %q", len(parts), recorder.Body.String())
	}

	payload, ok := responsesSSEDataPayload([]byte(parts[2]))
	if !ok {
		t.Fatalf("completed frame has no data payload: %q", parts[2])
	}
	output := gjson.GetBytes(payload, "response.output")
	if !output.IsArray() || len(output.Array()) != 2 {
		t.Fatalf("expected repaired completed output with 2 items, got %s", output.Raw)
	}
	if got := gjson.GetBytes(payload, "response.output.1.name").String(); got != "shell" {
		t.Fatalf("expected function_call name to be preserved, got %q in %s", got, payload)
	}
	if got := gjson.GetBytes(payload, "response.output.1.arguments").String(); got != `{"cmd":"pwd"}` {
		t.Fatalf("expected function_call arguments to be preserved, got %q in %s", got, payload)
	}
}

func TestForwardResponsesStreamPassthroughSkipsImageOutputRepair(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)

	data := make(chan []byte, 2)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte("data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"image_generation_call\",\"result\":\"ZmluYWw=\"}}\n\n")
	data <- []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[]}}\n\n")
	close(data)
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, &responsesSSEFramer{passthrough: true})

	body := recorder.Body.String()
	if !strings.Contains(body, `"type":"image_generation_call"`) {
		t.Fatalf("image output item should pass through: %s", body)
	}
	completedPayload := gjson.Get(strings.TrimPrefix(strings.Split(strings.TrimSpace(body), "\n\n")[1], "data: "), "response.output")
	if !completedPayload.IsArray() || len(completedPayload.Array()) != 0 {
		t.Fatalf("completed output should not be repaired in passthrough mode: %s", body)
	}
}

func TestForwardResponsesStreamRepairsMixedIndexedAndUnindexedDoneItems(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)

	data := make(chan []byte, 3)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte(`data: {"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"shell","arguments":"{}","status":"completed"}}`)
	data <- []byte(`data: {"type":"response.output_item.done","item":{"type":"message","id":"msg-1","role":"assistant","content":[{"type":"output_text","text":"done"}]}}`)
	data <- []byte(`data: {"type":"response.completed","response":{"id":"resp-1","output":[]}}`)
	close(data)
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, nil)

	parts := strings.Split(strings.TrimSpace(recorder.Body.String()), "\n\n")
	if len(parts) != 3 {
		t.Fatalf("expected 3 SSE events, got %d. Body: %q", len(parts), recorder.Body.String())
	}

	payload, ok := responsesSSEDataPayload([]byte(parts[2]))
	if !ok {
		t.Fatalf("completed frame has no data payload: %q", parts[2])
	}
	output := gjson.GetBytes(payload, "response.output")
	if !output.IsArray() || len(output.Array()) != 2 {
		t.Fatalf("expected repaired completed output with 2 items, got %s", output.Raw)
	}
	if got := gjson.GetBytes(payload, "response.output.0.name").String(); got != "shell" {
		t.Fatalf("expected indexed function_call to be preserved first, got %q in %s", got, payload)
	}
	if got := gjson.GetBytes(payload, "response.output.1.id").String(); got != "msg-1" {
		t.Fatalf("expected unindexed message to be appended, got %q in %s", got, payload)
	}
}

func TestForwardResponsesStreamRepairsMultilineCompletedOutputAsSSEDataLines(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)

	data := make(chan []byte, 2)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte(`data: {"type":"response.output_item.done","item":{"type":"function_call","arguments":"{}"}}`)
	data <- []byte("data: {\"type\":\"response.completed\",\ndata: \"response\":{\"id\":\"resp-1\",\"output\":[]}}\n\n")
	close(data)
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, nil)

	parts := strings.Split(strings.TrimSpace(recorder.Body.String()), "\n\n")
	if len(parts) != 2 {
		t.Fatalf("expected 2 SSE events, got %d. Body: %q", len(parts), recorder.Body.String())
	}

	completedFrame := []byte(parts[1])
	for _, line := range strings.Split(parts[1], "\n") {
		if line != "" && !strings.HasPrefix(line, "data: ") && !strings.HasPrefix(line, "event: ") {
			t.Fatalf("expected every completed payload line to be an SSE data line, got %q in %q", line, parts[1])
		}
	}

	payload, ok := responsesSSEDataPayload(completedFrame)
	if !ok {
		t.Fatalf("expected completed frame to contain data payload: %q", parts[1])
	}
	output := gjson.GetBytes(payload, "response.output")
	if !output.IsArray() || len(output.Array()) != 1 {
		t.Fatalf("expected repaired completed output with 1 item, got %s from %q", output.Raw, payload)
	}
}

func TestForwardResponsesStreamReassemblesSplitSSEEventChunks(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)

	data := make(chan []byte, 3)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte("event: response.created")
	data <- []byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}")
	data <- []byte("\n")
	close(data)
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, nil)

	got := strings.TrimSuffix(recorder.Body.String(), "\n")
	want := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}\n\n"
	if got != want {
		t.Fatalf("unexpected split-event framing.\nGot:  %q\nWant: %q", got, want)
	}
}

func TestForwardResponsesStreamPreservesValidFullSSEEventChunks(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)

	data := make(chan []byte, 1)
	errs := make(chan *interfaces.ErrorMessage)
	chunk := []byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}\n\n")
	data <- chunk
	close(data)
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, nil)

	got := strings.TrimSuffix(recorder.Body.String(), "\n")
	if got != string(chunk) {
		t.Fatalf("unexpected full-event framing.\nGot:  %q\nWant: %q", got, string(chunk))
	}
}

func TestForwardResponsesStreamBuffersSplitDataPayloadChunks(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)

	data := make(chan []byte, 2)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte("data: {\"type\":\"response.created\"")
	data <- []byte(",\"response\":{\"id\":\"resp-1\"}}")
	close(data)
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, nil)

	got := recorder.Body.String()
	want := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}\n\n\n"
	if got != want {
		t.Fatalf("unexpected split-data framing.\nGot:  %q\nWant: %q", got, want)
	}
}

func TestResponsesSSENeedsLineBreakSkipsChunksThatAlreadyStartWithNewline(t *testing.T) {
	if responsesSSENeedsLineBreak([]byte("event: response.created"), []byte("\n")) {
		t.Fatal("expected no injected newline before newline-only chunk")
	}
	if responsesSSENeedsLineBreak([]byte("event: response.created"), []byte("\r\n")) {
		t.Fatal("expected no injected newline before CRLF chunk")
	}
}

func TestForwardResponsesStreamDropsIncompleteTrailingDataChunkOnFlush(t *testing.T) {
	h, recorder, c, flusher := newResponsesStreamTestHandler(t)

	data := make(chan []byte, 1)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte("data: {\"type\":\"response.created\"")
	close(data)
	close(errs)

	h.forwardResponsesStream(c, flusher, func(error) {}, data, errs, nil)

	if got := recorder.Body.String(); got != "\n" {
		t.Fatalf("expected incomplete trailing data to be dropped on flush.\nGot: %q", got)
	}
}
