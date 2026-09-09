package openai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type orphanDelegationCaptureExecutor struct {
	compactCaptureExecutor
	requests chan []byte
}

func (e *orphanDelegationCaptureExecutor) Execute(_ context.Context, _ *coreauth.Auth, req core.Request, _ core.Options) (core.Response, error) {
	e.requests <- req.Payload
	return core.Response{Payload: []byte(`{"id":"done","output":[]}`)}, nil
}

func (e *orphanDelegationCaptureExecutor) ExecuteStream(ctx context.Context, credential *coreauth.Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	_, _ = e.Execute(ctx, credential, req, opts)
	chunks := make(chan core.StreamChunk, 1)
	chunks <- core.StreamChunk{Payload: []byte(`data: {"type":"response.completed","response":{"id":"done","status":"completed","output":[{"type":"function_call","call_id":"known","name":"create_thread","namespace":"codex_app","arguments":"{}"}]}}`)}
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func newOrphanDelegationHandler(t *testing.T) (*OpenAIResponsesAPIHandler, *orphanDelegationCaptureExecutor) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	exec := &orphanDelegationCaptureExecutor{requests: make(chan []byte, 3)}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(exec)
	id := uuid.NewString()
	if _, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, Provider: exec.Identifier(), Status: coreauth.StatusActive, Attributes: map[string]string{"websockets": "true"}}); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(id, exec.Identifier(), []*registry.ModelInfo{{ID: "orphan-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	return NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&config.SDKConfig{CodexOrphanDelegationCompatibility: true}, manager)), exec
}

func TestResponsesOrphanDelegationBoundaryAcrossHTTPRoutes(t *testing.T) {
	for _, route := range []string{"responses", "stream", "compact"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", route, enabled), func(t *testing.T) {
				h, exec := newOrphanDelegationHandler(t)
				h.UpdateClients(&config.SDKConfig{CodexOrphanDelegationCompatibility: enabled})
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				payload := fmt.Sprintf(`{"model":"orphan-model","stream":%t,"input":[{"type":"function_call_output","namespace":"codex_app","name":"create_thread","call_id":"missing","output":"text"}]}`, route == "stream")
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(payload))
				c.Request.Header.Set("X-Openai-Subagent", "collab_spawn")
				if route == "compact" {
					h.Compact(c)
				} else {
					h.Responses(c)
				}
				if w.Code != 200 {
					t.Fatalf("status=%d", w.Code)
				}
				got := <-exec.requests
				if (gjson.GetBytes(got, "input.0.type").String() == "message") != enabled {
					t.Fatal("Responses boundary lost compatibility policy")
				}
				ctx := context.WithValue(t.Context(), "gin", c)
				if helps.CodexOrphanDelegationEnabled(ctx, nil, true) {
					t.Fatal("executor could override the boundary snapshot or rewrite a second time")
				}
			})
		}
	}
}

func TestResponsesOrphanDelegationWebsocketPreservesCachedCallsAndTurnPolicy(t *testing.T) {
	h, exec := newOrphanDelegationHandler(t)
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"X-Openai-Subagent": {"collab_spawn"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for index, id := range []string{"missing", "known", "still-missing"} {
		if index == 2 {
			h.UpdateClients(&config.SDKConfig{})
		}
		previous := ""
		if index > 0 {
			previous = `,"previous_response_id":"done"`
		}
		payload := fmt.Sprintf(`{"type":"response.create","model":"orphan-model"%s,"input":[{"type":"function_call_output","namespace":"codex_app","name":"create_thread","call_id":%q,"output":"result"}]}`, previous, id)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
			t.Fatal(err)
		}
		for {
			_, response, err := conn.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			if gjson.GetBytes(response, "type").String() == "error" {
				t.Fatal("WebSocket rejected test request")
			}
			if gjson.GetBytes(response, "type").String() == "response.completed" {
				break
			}
		}
		got := <-exec.requests
		isMessage := gjson.GetBytes(got, "input.0.type").String() == "message"
		if isMessage != (index == 0) {
			t.Fatal("cached pairing or subsequent disabled turn was rewritten")
		}
	}
}
