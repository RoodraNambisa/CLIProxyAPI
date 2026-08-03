package handlers

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type requestBodyReleaseBindingWriter struct {
	gin.ResponseWriter
	controller *coreexecutor.RequestBodyReleaseController
}

func (writer *requestBodyReleaseBindingWriter) BindRequestBodyReleaseController(controller *coreexecutor.RequestBodyReleaseController) {
	writer.controller = controller
}

func TestChatGPTWebForcedBodyReleaseIgnoresGlobalTimerAndSize(t *testing.T) {
	t.Parallel()
	handler := &BaseAPIHandler{Cfg: &config.SDKConfig{RequestBodyRelease: config.RequestBodyReleaseConfig{
		Enable:       true,
		AfterSeconds: 1,
		MinBodyBytes: 1 << 20,
	}}}
	metadata := make(map[string]any)
	ctx, controller := handler.attachRequestBodyRelease(context.Background(), []byte(`{"model":"gpt-5"}`), metadata, true)
	if controller == nil {
		t.Fatal("forced ChatGPT Web controller = nil")
	}
	if coreexecutor.RequestBodyReleaseControllerFromContext(ctx) != controller ||
		coreexecutor.RequestBodyReleaseControllerFromMetadata(metadata) != controller {
		t.Fatal("forced controller was not propagated")
	}
	time.Sleep(1100 * time.Millisecond)
	if controller.Released() {
		t.Fatal("global timer released ChatGPT Web body before the executor commit point")
	}
	if !controller.Release() || controller.Replayable() {
		t.Fatal("explicit Web release did not make the request non-replayable")
	}
}

func TestChatGPTWebForcedBodyReleaseRebindsRequestLogWriter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	bindingWriter := &requestBodyReleaseBindingWriter{ResponseWriter: ginCtx.Writer}
	ginCtx.Writer = bindingWriter

	handler := &BaseAPIHandler{Cfg: &config.SDKConfig{}}
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	_, controller := handler.attachRequestBodyRelease(ctx, []byte(`{"model":"gpt-5"}`), nil, true)
	if controller == nil {
		t.Fatal("forced ChatGPT Web controller = nil")
	}
	if bindingWriter.controller != controller {
		t.Fatal("request log writer was not rebound to the forced controller")
	}
}

func TestChatGPTWebForcedBodyReleaseReplacesGlobalTimedController(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	bindingWriter := &requestBodyReleaseBindingWriter{ResponseWriter: ginCtx.Writer}
	ginCtx.Writer = bindingWriter

	body := []byte(`{"model":"gpt-5","input":"keep until upstream commit"}`)
	globalController := coreexecutor.NewRequestBodyReleaseController(int64(len(body)), []byte("<global timer release>"))
	globalController.StartTimer(10*time.Millisecond, nil)
	ginCtx.Set(coreexecutor.BodyReleaseControllerMetadataKey, globalController)

	handler := &BaseAPIHandler{Cfg: &config.SDKConfig{RequestBodyRelease: config.RequestBodyReleaseConfig{
		Enable:       true,
		AfterSeconds: 1,
	}}}
	metadata := make(map[string]any)
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	ctx, controller := handler.attachRequestBodyRelease(ctx, body, metadata, true)
	if controller == nil || controller == globalController {
		t.Fatal("forced ChatGPT Web release reused the global timed controller")
	}
	if coreexecutor.RequestBodyReleaseControllerFromContext(ctx) != controller ||
		coreexecutor.RequestBodyReleaseControllerFromMetadata(metadata) != controller ||
		bindingWriter.controller != controller {
		t.Fatal("replacement controller was not propagated to every request-body consumer")
	}

	deadline := time.Now().Add(time.Second)
	for !globalController.Released() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !globalController.Released() {
		t.Fatal("global controller timer did not fire")
	}
	if controller.Released() || !controller.Replayable() {
		t.Fatal("global timer made the forced ChatGPT Web request non-replayable")
	}
	if !controller.Release() || controller.Replayable() {
		t.Fatal("replacement controller did not release at the explicit upstream commit point")
	}
}

func TestConfiguredBodyReleaseStillUsesTimerOutsideChatGPTWeb(t *testing.T) {
	t.Parallel()
	handler := &BaseAPIHandler{Cfg: &config.SDKConfig{RequestBodyRelease: config.RequestBodyReleaseConfig{
		Enable:       true,
		AfterSeconds: 1,
	}}}
	_, controller := handler.attachRequestBodyRelease(context.Background(), []byte(`{"model":"gpt-5"}`), nil, false)
	if controller == nil {
		t.Fatal("configured controller = nil")
	}
	deadline := time.Now().Add(1500 * time.Millisecond)
	for !controller.Released() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !controller.Released() {
		t.Fatal("non-Web global timer did not release the request body after cancellation")
	}
}

func TestConfiguredLogOnlyBodyReleasePropagatesExistingController(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	bindingWriter := &requestBodyReleaseBindingWriter{ResponseWriter: ginCtx.Writer}
	ginCtx.Writer = bindingWriter

	body := []byte(`{"model":"gpt-5","input":"keep downstream body"}`)
	controller := coreexecutor.NewRequestBodyReleaseControllerWithMode(int64(len(body)), []byte("<timer release>"), true)
	ginCtx.Set(coreexecutor.BodyReleaseControllerMetadataKey, controller)

	handler := &BaseAPIHandler{Cfg: &config.SDKConfig{RequestBodyRelease: config.RequestBodyReleaseConfig{
		Enable:  true,
		LogOnly: true,
	}}}
	metadata := make(map[string]any)
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	ctx, attached := handler.attachRequestBodyRelease(ctx, body, metadata, false)
	if attached != controller {
		t.Fatal("configured log-only controller was not reused")
	}
	if coreexecutor.RequestBodyReleaseControllerFromContext(ctx) != controller ||
		coreexecutor.RequestBodyReleaseControllerFromMetadata(metadata) != controller {
		t.Fatal("configured log-only controller was not propagated")
	}
	if bindingWriter.controller != controller {
		t.Fatal("request log writer was not bound to the log-only controller")
	}

	req := coreexecutor.Request{Payload: []byte("keep executor payload")}
	opts := coreexecutor.Options{
		OriginalRequest: []byte("keep executor original request"),
		Metadata:        metadata,
	}
	unregister := coreexecutor.RegisterRequestBodyReleaseCleanup(ctx, &req, &opts)
	defer unregister()
	loggingCfg := &config.Config{SDKConfig: config.SDKConfig{RequestLog: true}}
	helps.RecordAPIRequest(ctx, loggingCfg, helps.UpstreamRequestLog{Body: []byte("drop HTTP log body")})
	helps.RecordAPIWebsocketRequest(ctx, loggingCfg, helps.UpstreamRequestLog{Body: []byte("drop WebSocket log body")})

	if !helps.ReleaseRequestBodyAfterStreamEstablished(ctx, opts) {
		t.Fatal("stream-established log-only release did not run")
	}
	if !controller.Replayable() {
		t.Fatal("log-only release made the request non-replayable")
	}
	if string(req.Payload) != "keep executor payload" || string(opts.OriginalRequest) != "keep executor original request" {
		t.Fatal("log-only release cleared executor request bodies")
	}

	for _, key := range []string{"API_REQUEST", "API_WEBSOCKET_TIMELINE"} {
		raw, exists := ginCtx.Get(key)
		if !exists {
			t.Fatalf("%s log missing", key)
		}
		logged, ok := raw.([]byte)
		if !ok {
			t.Fatalf("%s log type = %T, want []byte", key, raw)
		}
		text := string(logged)
		if !strings.Contains(text, "request body log released after stream established") {
			t.Fatalf("%s log missing stream-established placeholder: %s", key, text)
		}
		if strings.Contains(text, "drop HTTP log body") || strings.Contains(text, "drop WebSocket log body") {
			t.Fatalf("%s log retained a released request body: %s", key, text)
		}
	}
}
