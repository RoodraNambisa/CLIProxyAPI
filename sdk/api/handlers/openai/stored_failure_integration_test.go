package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type storedFailureOnlyExecutor struct{ calls atomic.Int64 }

func (*storedFailureOnlyExecutor) Identifier() string { return "codex" }
func (e *storedFailureOnlyExecutor) unexpected() error {
	e.calls.Add(1)
	return errors.New("cooling credential reached executor")
}
func (e *storedFailureOnlyExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, e.unexpected()
}
func (e *storedFailureOnlyExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, e.unexpected()
}
func (e *storedFailureOnlyExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, e.unexpected()
}
func (e *storedFailureOnlyExecutor) Refresh(context.Context, *coreauth.Auth) (*coreauth.Auth, error) {
	return nil, e.unexpected()
}
func (e *storedFailureOnlyExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, e.unexpected()
}

func newStoredFailureRouter(t *testing.T) (*gin.Engine, *storedFailureOnlyExecutor, <-chan struct{}) {
	t.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &storedFailureOnlyExecutor{}
	manager.RegisterExecutor(executor)
	id := strings.ReplaceAll(t.Name(), "/", "-")
	models := []string{"gpt-5.4-mini", "gpt-image-2"}
	registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: models[0]}, {ID: models[1]}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: id, Provider: "codex"}); err != nil {
		t.Fatal(err)
	}
	for _, model := range models {
		delay := time.Minute
		manager.MarkResult(coreauth.WithSkipPersist(t.Context()), coreauth.Result{AuthID: id, Provider: "codex", Model: model, RetryAfter: &delay, Error: &coreauth.Error{HTTPStatus: 429, Message: "test-private-previous-body"}})
	}
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{Images: config.ImagesConfig{CodexModel: models[0]}}, manager)
	responses := NewOpenAIResponsesAPIHandler(base)
	engine := gin.New()
	engine.POST("/v1/responses", responses.Responses)
	engine.POST("/v1/responses/compact", responses.Compact)
	engine.POST("/v1/chat/completions", NewOpenAIAPIHandler(base).ChatCompletions)
	engine.POST("/v1/images/generations", NewOpenAIImagesAPIHandler(base).Generations)
	done := make(chan struct{})
	engine.GET("/v1/responses", func(ctx *gin.Context) { defer close(done); responses.ResponsesWebsocket(ctx) })
	return engine, executor, done
}

func TestCodexHTTPStoredFailureWithoutUpstreamCalls(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/responses/compact", "/v1/chat/completions", "/v1/images/generations"} {
		for _, stream := range []bool{false, true} {
			if path == "/v1/responses/compact" && stream {
				continue
			}
			t.Run(fmt.Sprintf("%s/stream=%t", path, stream), func(t *testing.T) {
				engine, executor, _ := newStoredFailureRouter(t)
				model := "gpt-5.4-mini"
				if path == "/v1/images/generations" {
					model = "gpt-image-2"
				}
				body := fmt.Sprintf(`{"model":%q,"input":"hello","messages":[{"role":"user","content":"hello"}],"prompt":"draw a cat","stream":%t}`, model, stream)
				request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				recorder := httptest.NewRecorder()
				engine.ServeHTTP(recorder, request)
				if recorder.Code != 503 || executor.calls.Load() != 0 || strings.Contains(recorder.Body.String(), "test-private") {
					t.Fatalf("selection status=%d executor calls=%d", recorder.Code, executor.calls.Load())
				}
				if path != "/v1/images/generations" && !strings.Contains(recorder.Body.String(), "previous credential failure: HTTP 429") {
					t.Fatal("stored failure summary was lost")
				}
				if recorder.Header().Get("Retry-After") == "" {
					t.Fatal("stored failure lost cooldown hint")
				}
			})
		}
	}
}

func TestCodexWebsocketStoredFailureWithoutUpstreamCalls(t *testing.T) {
	engine, executor, done := newStoredFailureRouter(t)
	server := httptest.NewServer(engine)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-5.4-mini","input":[]}`)); err != nil {
		t.Fatal(err)
	}
	received := make(chan struct {
		payload []byte
		err     error
	}, 1)
	go func() {
		_, payload, err := conn.ReadMessage()
		received <- struct {
			payload []byte
			err     error
		}{payload, err}
	}()
	select {
	case frame := <-received:
		if frame.err != nil {
			t.Fatal(frame.err)
		}
		if gjson.GetBytes(frame.payload, "status").Int() != 503 || !strings.Contains(string(frame.payload), "previous credential failure: HTTP 429") || strings.Contains(string(frame.payload), "test-private") {
			t.Fatal("websocket lost selection status or leaked stored details")
		}
	case <-time.After(5 * time.Second):
		_ = conn.Close()
		<-received
		t.Fatal("websocket did not report the local selection failure")
	}
	_ = conn.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("websocket handler did not release its resources")
	}
	if executor.calls.Load() != 0 {
		t.Fatal("unavailable websocket invoked upstream")
	}
}
