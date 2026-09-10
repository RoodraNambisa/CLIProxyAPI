package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestResponsesWebsocketPreservesMessageTooBigDisconnect(t *testing.T) {
	for _, mapped := range []bool{false, true} {
		t.Run(fmt.Sprint(mapped), func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			exec := &websocketUpstreamDisconnectExecutor{subscribed: make(chan string, 1)}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(exec)
			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
			router := gin.New()
			router.GET("/v1/responses", h.ResponsesWebsocket)
			server := httptest.NewServer(router)
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			var sessionID string
			select {
			case sessionID = <-exec.subscribed:
			case <-time.After(5 * time.Second):
				t.Fatal("subscription missing")
			}
			var cause error = &websocket.CloseError{Code: websocket.CloseMessageTooBig, Text: "private upstream detail"}
			if mapped {
				cause = helps.MapWebsocketMessageTooBigError(cause)
			}
			exec.TriggerDisconnect(sessionID, fmt.Errorf("upstream: %w", cause))
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, _, err = conn.ReadMessage()
			assertResponsesWebsocketTooBigClose(t, err)
		})
	}
}

type websocketTooBigExecutor struct {
	websocketCompactionCaptureExecutor
	calls    atomic.Int32
	deferred bool
}

func (e *websocketTooBigExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.calls.Add(1)
	err := helps.MapWebsocketMessageTooBigError(&websocket.CloseError{Code: websocket.CloseMessageTooBig, Text: "private upstream detail"})
	if !e.deferred {
		return nil, err
	}
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Err: err}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestResponsesWebsocketMessageTooBigDoesNotRotateOrCooldown(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprint(deferred), func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			exec := &websocketTooBigExecutor{deferred: deferred}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(exec)
			model := "close-too-big-model"
			for _, id := range []string{"close-a", "close-b"} {
				auth := &coreauth.Auth{ID: id, Provider: exec.Identifier(), Status: coreauth.StatusActive}
				if _, err := manager.Register(t.Context(), auth); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, auth.Provider, []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			}
			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
			router := gin.New()
			handlerDone := make(chan struct{})
			router.GET("/v1/responses", func(c *gin.Context) { defer close(handlerDone); h.ResponsesWebsocket(c) })
			server := httptest.NewServer(router)
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"close-too-big-model","input":[{"role":"user","content":"test"}]}`)); err != nil {
				t.Fatal(err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, _, err = conn.ReadMessage()
			assertResponsesWebsocketTooBigClose(t, err)
			select {
			case <-handlerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("handler cleanup blocked")
			}
			if exec.calls.Load() != 1 {
				t.Fatalf("upstream calls = %d, want 1", exec.calls.Load())
			}
			for _, id := range []string{"close-a", "close-b"} {
				auth, ok := manager.GetByID(id)
				if !ok || auth.Unavailable || !auth.NextRetryAfter.IsZero() {
					t.Fatal("credential cooled down")
				}
				if state := auth.ModelStates[model]; state != nil && (state.Unavailable || !state.NextRetryAfter.IsZero()) {
					t.Fatal("model cooled down")
				}
			}
		})
	}
}

func assertResponsesWebsocketTooBigClose(t *testing.T, err error) {
	t.Helper()
	var closed *websocket.CloseError
	if !errors.As(err, &closed) || closed.Code != websocket.CloseMessageTooBig {
		t.Fatalf("downstream error = %v, want 1009", err)
	}
	if closed.Text != "upstream websocket message too big" {
		t.Fatalf("unexpected public close reason: %q", closed.Text)
	}
}
