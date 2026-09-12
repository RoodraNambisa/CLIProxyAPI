package openai

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type imageDeadlineExecutor struct {
	imageCaptureExecutor
	entered     atomic.Int32
	exited      chan struct{}
	firstResult bool
	finishOnce  sync.Once
}

func (e *imageDeadlineExecutor) Execute(ctx context.Context, _ *coreauth.Auth, _ core.Request, _ core.Options) (core.Response, error) {
	call := e.entered.Add(1)
	if e.firstResult && call == 1 {
		return core.Response{Payload: []byte(`{"created_at":1700000000,"output":[{"type":"image_generation_call","result":"aGVsbG8="}]}`)}, nil
	}
	<-ctx.Done()
	e.finishOnce.Do(func() { close(e.exited) })
	return core.Response{}, ctx.Err()
}
func (e *imageDeadlineExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ core.Request, _ core.Options) (*core.StreamResult, error) {
	e.entered.Add(1)
	chunks := make(chan core.StreamChunk)
	go func() {
		defer e.finishOnce.Do(func() { close(e.exited) })
		defer close(chunks)
		if e.firstResult {
			select {
			case chunks <- core.StreamChunk{Payload: []byte("event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"image_generation_call\",\"id\":\"ig_1\"}}\n\n")}:
			case <-ctx.Done():
				return
			}
		}
		<-ctx.Done()
	}()
	return &core.StreamResult{Chunks: chunks}, nil
}

func TestImageRequestTimeoutHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, provider, path      string
		stream, native, aggregate bool
	}{
		{name: "codex-tool", provider: "codex", path: "/v1/images/generations"},
		{name: "codex-native", provider: "codex", path: "/v1/images/generations", native: true},
		{name: "web-edit", provider: "chatgpt-web", path: "/v1/images/edits"},
		{name: "web-n-shared", provider: "chatgpt-web", path: "/v1/images/generations", aggregate: true},
		{name: "codex-stream-first-byte", provider: "codex", path: "/v1/images/generations", stream: true},
		{name: "web-stream-first-byte", provider: "chatgpt-web", path: "/v1/images/generations", stream: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &imageDeadlineExecutor{imageCaptureExecutor: imageCaptureExecutor{provider: tc.provider}, exited: make(chan struct{}), firstResult: tc.aggregate}
			h := newImagesTestHandler(t, &e.imageCaptureExecutor)
			h.AuthManager.RegisterExecutor(e)
			h.Cfg.Images.CodexRequestTimeoutSeconds = 1
			h.Cfg.Images.ChatGPTWeb.RequestTimeoutSeconds = 1
			h.Cfg.Images.EnableNAggregation = &tc.aggregate
			maxN := 2
			h.Cfg.Images.ChatGPTWeb.MaxN = &maxN
			h.Cfg.Images.Native.Generations.Enabled = tc.native
			r := gin.New()
			r.POST(tc.path, func(c *gin.Context) {
				if strings.HasSuffix(tc.path, "edits") {
					h.Edits(c)
				} else {
					h.Generations(c)
				}
			})
			payload := `{"model":"gpt-image-2","prompt":"test"`
			if tc.stream {
				payload += `,"stream":true`
			}
			if tc.aggregate {
				payload += `,"n":2`
			}
			if strings.HasSuffix(tc.path, "edits") {
				payload += `,"images":[{"image_url":"data:image/png;base64,aGVsbG8="}]`
			}
			payload += "}"
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != 504 || !strings.Contains(w.Body.String(), "image_request_timeout") {
				t.Fatalf("response %d: %s", w.Code, w.Body.String())
			}
			expected := int32(1)
			if tc.aggregate {
				expected = 2
			}
			if e.entered.Load() != expected {
				t.Fatalf("unexpected retry count %d", e.entered.Load())
			}
			select {
			case <-e.exited:
			case <-time.After(time.Second):
				t.Fatal("executor leaked")
			}
			for _, a := range h.AuthManager.List() {
				if a.Unavailable || a.NextRetryAfter.After(time.Now()) {
					t.Fatal("local timeout cooled auth")
				}
			}
		})
	}
}

func TestImageRequestTimeoutResponsesTerminalSSE(t *testing.T) {
	e := &imageDeadlineExecutor{imageCaptureExecutor: imageCaptureExecutor{provider: "codex"}, exited: make(chan struct{}), firstResult: true}
	images := newImagesTestHandler(t, &e.imageCaptureExecutor, "gpt-5.4-mini")
	images.AuthManager.RegisterExecutor(e)
	images.Cfg.Images.CodexRequestTimeoutSeconds = 1
	h := NewOpenAIResponsesAPIHandler(images.BaseAPIHandler)
	r := gin.New()
	r.POST("/v1/responses", h.Responses)
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"gpt-5.4-mini","input":"test","stream":true,"tools":[{"type":"image_generation"}]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "image_request_timeout") {
		t.Fatalf("terminal response %d: %s", w.Code, w.Body.String())
	}
	if e.entered.Load() != 1 {
		t.Fatal("terminal timeout was retried")
	}
}

func TestImageRequestTimeoutSnapshotAndScope(t *testing.T) {
	cfg := &sdkconfig.SDKConfig{}
	cfg.Images.CodexRequestTimeoutSeconds = 1800
	h := handlers.NewBaseAPIHandlers(cfg, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/images/generations", nil)
	finish := h.BeginImageRequestBudget(c)
	defer finish()
	cfg.Images.CodexRequestTimeoutSeconds = 1
	ctx, cancel := h.GetContextWithCancel(nil, c, context.Background())
	defer cancel()
	if core.ImageRequestBudgetFromContext(ctx).Limit("codex") != 1800*time.Second {
		t.Fatal("in-flight snapshot changed")
	}
	other, _ := gin.CreateTestContext(httptest.NewRecorder())
	other.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	textCtx, textCancel := h.GetImageContextWithCancel(nil, other, context.Background(), []byte(`{"tools":[{"type":"image_generation"}],"tool_choice":"none"}`))
	defer textCancel()
	if core.ImageRequestBudgetFromContext(textCtx) != nil {
		t.Fatal("text-only request has image timeout")
	}
}

func TestImageRequestTimeoutWebsocketNewLogicalBudget(t *testing.T) {
	e := &imageDeadlineExecutor{imageCaptureExecutor: imageCaptureExecutor{provider: "codex"}, exited: make(chan struct{})}
	images := newImagesTestHandler(t, &e.imageCaptureExecutor, "gpt-5.4-mini")
	images.AuthManager.RegisterExecutor(e)
	images.Cfg.Images.CodexRequestTimeoutSeconds = 1
	h := NewOpenAIResponsesAPIHandler(images.BaseAPIHandler)
	r := gin.New()
	r.GET("/v1/responses/ws", h.ResponsesWebsocket)
	server := httptest.NewServer(r)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for range 2 {
		if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.4-mini","input":"test","tools":[{"type":"image_generation"}]}`)); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(4 * time.Second))
		for {
			_, body, errRead := conn.ReadMessage()
			if errRead != nil {
				t.Fatal(errRead)
			}
			if strings.Contains(string(body), `"type":"error"`) {
				if !strings.Contains(string(body), "image_request_timeout") {
					t.Fatalf("error frame: %s", body)
				}
				break
			}
		}
	}
	if e.entered.Load() != 2 {
		t.Fatalf("second logical request reused expired budget: %d", e.entered.Load())
	}
}

func TestImageRequestTimeoutRewriteAndSanitize(t *testing.T) {
	e := &imageDeadlineExecutor{imageCaptureExecutor: imageCaptureExecutor{provider: "chatgpt-web"}, exited: make(chan struct{})}
	h := newImagesTestHandler(t, &e.imageCaptureExecutor)
	h.AuthManager.RegisterExecutor(e)
	h.Cfg.Images.ChatGPTWeb.RequestTimeoutSeconds = 1
	h.Cfg.Images.ChatGPTWeb.SanitizeErrorResponses = true
	body := map[string]any{"error": map[string]any{"message": "Please retry later.", "type": "server_error", "code": "internal_server_error"}}
	h.Cfg.ErrorResponseRewrites = []sdkconfig.ErrorResponseRewriteRule{{StatusCode: 504, Sources: []string{"chatgpt-web"}, ResponseStatusCode: 503, ResponseBody: &body}}
	r := gin.New()
	r.POST("/v1/images/generations", h.Generations)
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"test"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "Please retry later.") || strings.Contains(w.Body.String(), "chatgpt") {
		t.Fatalf("rewrite/sanitization: %d %s", w.Code, w.Body.String())
	}
	if e.entered.Load() != 1 {
		t.Fatal("rewritten timeout retried")
	}
}
