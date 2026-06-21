package helps

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestRecordAPIWebsocketRequestReleasePreservesTimeline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(rec)
	ctrl := cliproxyexecutor.NewRequestBodyReleaseController(1024, []byte("<released>"))
	ginCtx.Set(cliproxyexecutor.BodyReleaseControllerMetadataKey, ctrl)
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	cfg := &config.Config{SDKConfig: config.SDKConfig{RequestLog: true}}

	RecordAPIWebsocketRequest(ctx, cfg, UpstreamRequestLog{
		URL:  "wss://example.test/v1/responses",
		Body: []byte("large-websocket-body"),
	})
	RecordAPIWebsocketHandshake(ctx, cfg, http.StatusSwitchingProtocols, http.Header{"Upgrade": []string{"websocket"}})
	AppendAPIWebsocketResponse(ctx, cfg, []byte(`{"type":"response.created"}`))

	ctrl.Release()

	raw, exists := ginCtx.Get(apiWebsocketTimelineKey)
	if !exists {
		t.Fatal("websocket timeline missing")
	}
	timeline, ok := raw.([]byte)
	if !ok {
		t.Fatalf("timeline type = %T, want []byte", raw)
	}
	text := string(timeline)
	if strings.Contains(text, "large-websocket-body") {
		t.Fatalf("timeline still contains released body: %s", text)
	}
	for _, want := range []string{"<released>", "Event: api.websocket.handshake", "Event: api.websocket.response"} {
		if !strings.Contains(text, want) {
			t.Fatalf("timeline missing %q: %s", want, text)
		}
	}
}

func TestRequestBodyRefsReleaseKeepsSlimTranslationMetadata(t *testing.T) {
	ctrl := cliproxyexecutor.NewRequestBodyReleaseController(1024, []byte("<released>"))
	opts := cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.BodyReleaseControllerMetadataKey: ctrl,
		},
	}
	original := []byte(`{"model":"m","stream":true,"messages":[{"content":"large prompt"}],"tools":[{"name":"mcp/server/read","input_schema":{"type":"object"}}]}`)
	translated := []byte(`{"model":"m","contents":[{"parts":[{"text":"large prompt"}]}],"tools":[{"functionDeclarations":[{"name":"mcp_server_read","description":"large"}]}]}`)
	originalRef, translatedRef, unregister := RequestBodyRefs(context.Background(), opts, original, translated)
	defer unregister()

	ctrl.Release()

	gotOriginal := string(originalRef.Bytes())
	if !strings.Contains(gotOriginal, `"stream":true`) || !strings.Contains(gotOriginal, "mcp/server/read") {
		t.Fatalf("original slim body missing translation metadata: %s", gotOriginal)
	}
	if strings.Contains(gotOriginal, "large prompt") || strings.Contains(gotOriginal, "input_schema") {
		t.Fatalf("original slim body retained large fields: %s", gotOriginal)
	}
	gotTranslated := string(translatedRef.Bytes())
	if !strings.Contains(gotTranslated, "mcp_server_read") {
		t.Fatalf("translated slim body missing tool metadata: %s", gotTranslated)
	}
	if strings.Contains(gotTranslated, "large prompt") || strings.Contains(gotTranslated, "description") {
		t.Fatalf("translated slim body retained large fields: %s", gotTranslated)
	}
}
