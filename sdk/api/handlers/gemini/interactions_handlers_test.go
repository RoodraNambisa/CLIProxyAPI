package gemini

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestParseInteractionsRequestTarget(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "model", body: `{"model":"gemini-3.5-flash","input":"hi"}`},
		{name: "agent stream", body: `{"agent":"agents/test-agent","stream":true,"input":"hi"}`},
		{name: "invalid JSON", body: `{`, wantErr: true},
		{name: "missing target", body: `{"input":"hi"}`, wantErr: true},
		{name: "both targets", body: `{"model":"m","agent":"a"}`, wantErr: true},
		{name: "invalid stream", body: `{"model":"m","stream":"true"}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, errParse := parseInteractionsRequestTarget([]byte(tt.body))
			if (errParse != nil) != tt.wantErr {
				t.Fatalf("parseInteractionsRequestTarget() error = %v, wantErr %t", errParse, tt.wantErr)
			}
		})
	}
}

func TestPrepareInteractionsExecutionTargetNormalizesModelResource(t *testing.T) {
	body := []byte(`{"model":"models/gemini-3.5-flash","input":"hi"}`)
	target, errParse := parseInteractionsRequestTarget(body)
	if errParse != nil {
		t.Fatalf("parseInteractionsRequestTarget() error = %v", errParse)
	}
	model, updated := prepareInteractionsExecutionTarget(body, target)
	if model != "gemini-3.5-flash" || gjson.GetBytes(updated, "model").String() != model {
		t.Fatalf("model = %q, body = %s", model, updated)
	}
}

func TestInteractionsRoutesUseNativeExecutorAndRouteMetadata(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &interactionsCaptureExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "interactions-agent-auth", Provider: constant.GeminiInteractions, Status: coreauth.StatusActive}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: interactionsAgentAuthSelectionModel}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("manager.Register() error = %v", errRegister)
	}
	manager.RefreshSchedulerEntry(auth.ID)

	handler := NewGeminiAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	router := gin.New()
	router.POST("/v1/interactions", handler.Interactions)
	router.POST("/v1beta/interactions", handler.Interactions)

	for _, test := range []struct {
		path     string
		version  string
		revision string
	}{
		{path: "/v1/interactions?alt=json", version: "v1", revision: "2026-07-01"},
		{path: "/v1beta/interactions?alt=json", version: "v1beta", revision: "2026-07-02"},
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(`{"agent":"agents/test-agent","input":"hi"}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Api-Revision", test.revision)
		router.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusOK {
			t.Fatalf("%s status = %d, body = %s", test.path, recorder.Code, recorder.Body.String())
		}
		if executor.req.Model != "agents/test-agent" {
			t.Fatalf("%s executor model = %q, want agents/test-agent", test.path, executor.req.Model)
		}
		if got := executor.opts.Metadata[coreexecutor.InteractionsAPIVersionMetadataKey]; got != test.version {
			t.Fatalf("%s version metadata = %v, want %s", test.path, got, test.version)
		}
		if got := executor.opts.Metadata[coreexecutor.InteractionsAPIRevisionMetadataKey]; got != test.revision {
			t.Fatalf("%s revision metadata = %v, want %s", test.path, got, test.revision)
		}
		if executor.opts.Alt != "json" {
			t.Fatalf("%s alt = %q, want json", test.path, executor.opts.Alt)
		}
	}
}

func TestWriteInteractionsStreamChunkPreservesSSEAndWrapsJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name  string
		chunk string
		want  string
	}{
		{name: "SSE", chunk: "event: done\ndata: [DONE]\n\n", want: "event: done\ndata: [DONE]\n\n"},
		{name: "JSON", chunk: `{"event_type":"finish"}`, want: "data: {\"event_type\":\"finish\"}\n\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			writeInteractionsStreamChunk(c, []byte(tt.chunk))
			if got := recorder.Body.String(); got != tt.want {
				t.Fatalf("body = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPendingInteractionsStreamErrorReadsBufferedError(t *testing.T) {
	errChan := make(chan *interfaces.ErrorMessage, 1)
	want := &interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, Error: fmt.Errorf("rate limited")}
	errChan <- want
	close(errChan)

	if got := pendingInteractionsStreamError(errChan); got != want {
		t.Fatalf("pending error = %#v, want %#v", got, want)
	}
}

func TestInteractionsStreamReturnsBufferedBootstrapError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for i := 0; i < 32; i++ {
		executor := &interactionsStreamErrorExecutor{}
		manager := coreauth.NewManager(nil, nil, nil)
		manager.RegisterExecutor(executor)
		auth := &coreauth.Auth{ID: fmt.Sprintf("interactions-stream-error-%d", i), Provider: constant.GeminiInteractions, Status: coreauth.StatusActive}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: interactionsAgentAuthSelectionModel}})
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("manager.Register() error = %v", errRegister)
		}
		manager.RefreshSchedulerEntry(auth.ID)

		handler := NewGeminiAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/interactions", strings.NewReader(`{"agent":"agents/test-agent","stream":true,"input":"hi"}`))
		handler.Interactions(c)
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)

		if recorder.Code != http.StatusTooManyRequests {
			t.Fatalf("iteration %d status = %d, want %d; body=%s", i, recorder.Code, http.StatusTooManyRequests, recorder.Body.String())
		}
		if strings.HasPrefix(recorder.Header().Get("Content-Type"), "text/event-stream") {
			t.Fatalf("iteration %d committed SSE headers before bootstrap error", i)
		}
	}
}

type interactionsCaptureExecutor struct {
	req  coreexecutor.Request
	opts coreexecutor.Options
}

type interactionsStreamErrorExecutor struct {
	interactionsCaptureExecutor
}

type interactionsTestStatusError struct{}

func (interactionsTestStatusError) Error() string   { return "rate limited" }
func (interactionsTestStatusError) StatusCode() int { return http.StatusTooManyRequests }

func (*interactionsStreamErrorExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Err: interactionsTestStatusError{}}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (*interactionsCaptureExecutor) Identifier() string { return constant.GeminiInteractions }

func (e *interactionsCaptureExecutor) Execute(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (coreexecutor.Response, error) {
	e.req = req
	e.opts = opts
	return coreexecutor.Response{Payload: []byte(`{"id":"interaction_1","status":"completed"}`)}, nil
}

func (*interactionsCaptureExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, nil
}

func (*interactionsCaptureExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (*interactionsCaptureExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, nil
}

func (*interactionsCaptureExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}"))}, nil
}
