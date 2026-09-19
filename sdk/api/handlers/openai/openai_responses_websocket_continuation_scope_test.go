package openai

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketFollowupScopeOverridesStaleHandshake(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	c.Request.Header.Set("Thread-Id", "handshake-root")
	c.Request.Header.Set("Session-Id", "root-session")
	var states responsesWebsocketConversations
	defer states.close()
	child, _ := states.selectRequest(c, []byte(`{"type":"response.create","model":"test","client_metadata":{"thread_id":"child","session_id":"child-session"}}`))
	child.lastRequest = []byte(`{"model":"test","client_metadata":{"thread_id":"child","session_id":"child-session"}}`)
	child.lastResponseID = "child-response"
	for _, payload := range []string{`{"type":"response.append","input":[]}`, `{"type":"response.create","previous_response_id":"child-response","input":[]}`} {
		followup, body := states.selectRequest(c, []byte(payload))
		if followup != child {
			t.Fatalf("follow-up returned to handshake identity: %+v", followup.scope)
		}
		nextScope := helps.SnapshotCodexRequestScope(body, c.Request.Header)
		if nextScope != child.scope {
			t.Fatalf("executor sees a different scope: %+v / %+v", nextScope, child.scope)
		}
	}
}

func TestResponsesWebsocketMemoryContinuationKeepsItsScope(t *testing.T) {
	var states responsesWebsocketConversations
	defer states.close()
	main, _ := states.selectRequest(nil, []byte(`{"model":"test","client_metadata":{"thread_id":"root"}}`))
	main.lastRequest = []byte(`{"model":"test","input":[]}`)
	main.lastResponseID = "main-response"
	memory, _ := states.selectRequest(nil, []byte(`{"model":"test","client_metadata":{"thread_id":"root","request_kind":"memory"}}`))
	memory.lastRequest = []byte(`{"model":"test","input":[]}`)
	memory.lastResponseID = "memory-response"
	followup, _ := states.selectRequest(nil, []byte(`{"type":"response.create","previous_response_id":"memory-response","input":[]}`))
	if followup != memory {
		t.Fatal("memory continuation used user-turn history")
	}
	resumed, _ := states.selectRequest(nil, []byte(`{"type":"response.create","previous_response_id":"main-response","input":[]}`))
	if resumed != main {
		t.Fatal("explicit main response did not select the main scope")
	}
}

func TestResponsesWebsocketContinuationDoesNotBorrowForeignModelOrKind(t *testing.T) {
	var states responsesWebsocketConversations
	defer states.close()
	memory, _ := states.selectRequest(nil, []byte(`{"model":"memory-model","client_metadata":{"thread_id":"root","request_kind":"memory"}}`))
	memory.model, memory.lastResponseID = "memory-model", "memory-response"
	foreign, body := states.selectRequest(nil, []byte(`{"type":"response.create","previous_response_id":"memory-response","client_metadata":{"thread_id":"child"},"input":[]}`))
	if foreign.scope.Kind != "turn" || gjson.GetBytes(body, "model").Exists() {
		t.Fatalf("new thread borrowed another scope's model or kind: %s", body)
	}
	warm, _ := states.selectRequest(nil, []byte(`{"model":"warm-model","generate":false,"client_metadata":{"thread_id":"child"}}`))
	if warm.scope.Kind != "prewarm" {
		t.Fatal("warm request lost its scope")
	}
	followup, _ := states.selectRequest(nil, []byte(`{"type":"response.append","input":[]}`))
	if followup != foreign {
		t.Fatal("ordinary append inherited prewarm scope")
	}
}

func TestResponsesWebsocketGenerateTruePreservesHandshakeKind(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	c.Request.Header.Set("X-Codex-Turn-Metadata", `{"thread_id":"root","request_kind":"memory"}`)
	var states responsesWebsocketConversations
	defer states.close()
	state, _ := states.selectRequest(c, []byte(`{"generate":true,"model":"test"}`))
	if state.scope.Kind != "memory" {
		t.Fatal("generate=true overwrote the explicit handshake request kind")
	}
}

type websocketContinuationScopeExecutor struct {
	websocketCompactionCaptureExecutor
}

func (e *websocketContinuationScopeExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	index := len(e.streamPayloads)
	e.streamPayloads = append(e.streamPayloads, bytes.Clone(req.Payload))
	e.mu.Unlock()
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: fmt.Appendf(nil, `{"type":"response.completed","response":{"id":"scope-%d","status":"completed","output":[]}}`, index)}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestResponsesWebsocketContinuationScopesReachExecutor(t *testing.T) {
	executor := &websocketContinuationScopeExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "continuation-scope-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive, Attributes: map[string]string{"websockets": "true"}}
	if _, err := manager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "scope-main"}, {ID: "scope-child"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	router := gin.New()
	router.GET("/v1/responses", handler.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()
	headers := http.Header{"Thread-Id": {"root"}, "Session-Id": {"root-session"}}
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", headers)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	steps := []struct{ request, thread, session, kind, model, code string }{
		{`{"type":"response.create","model":"scope-main","input":[{"role":"user","content":"main prompt"}]}`, "root", "root-session", "turn", "scope-main", ""},
		{`{"type":"response.create","model":"scope-child","client_metadata":{"thread_id":"child","session_id":"child-session"},"input":[{"role":"user","content":"child prompt"}]}`, "child", "child-session", "turn", "scope-child", ""},
		{`{"type":"response.append","input":[{"role":"user","content":"child followup"}]}`, "child", "child-session", "turn", "scope-child", ""},
		{`{"type":"response.create","model":"scope-child","client_metadata":{"request_kind":"memory"},"input":[{"role":"user","content":"memory prompt"}]}`, "child", "child-session", "memory", "scope-child", ""},
		{`{"type":"response.create","previous_response_id":"scope-3","input":[]}`, "child", "child-session", "memory", "scope-child", ""},
		{`{"type":"response.create","previous_response_id":"scope-0","input":[]}`, "root", "root-session", "turn", "scope-main", ""},
		{`{"type":"response.create","previous_response_id":"scope-4","client_metadata":{"thread_id":"root","session_id":"root-session"},"input":[]}`, "", "", "", "", "previous_response_not_found"},
	}
	wantCalls := 0
	for index, step := range steps {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(step.request)); err != nil {
			t.Fatal(err)
		}
		for {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				t.Fatal(errRead)
			}
			if !responsesWebsocketTerminalEvent(gjson.GetBytes(payload, "type").Str) {
				continue
			}
			if code := gjson.GetBytes(payload, "error.code").Str; code != step.code {
				t.Fatalf("step %d: unexpected terminal event: %s", index, payload)
			}
			if step.code == "" && gjson.GetBytes(payload, "type").Str != "response.completed" {
				t.Fatalf("step %d: unsuccessful response: %s", index, payload)
			}
			break
		}
		if step.code == "" {
			wantCalls++
		}
		executor.mu.Lock()
		calls := len(executor.streamPayloads)
		var body []byte
		if calls > 0 {
			body = bytes.Clone(executor.streamPayloads[calls-1])
		}
		executor.mu.Unlock()
		if calls != wantCalls {
			t.Fatalf("step %d: upstream calls = %d, want %d", index, calls, wantCalls)
		}
		if step.code != "" {
			continue
		}
		wantScope := helps.CodexRequestScope{ThreadID: step.thread, SessionID: step.session, Kind: step.kind}
		if got := helps.SnapshotCodexRequestScope(body, headers); got != wantScope || gjson.GetBytes(body, "model").Str != step.model {
			t.Fatalf("step %d: executor received foreign scope or model: %s", index, body)
		}
		if step.kind == "memory" && (bytes.Contains(body, []byte("main prompt")) || bytes.Contains(body, []byte("child prompt"))) {
			t.Fatalf("memory borrowed user history: %s", body)
		}
		if index == 5 && (bytes.Contains(body, []byte("child prompt")) || bytes.Contains(body, []byte("memory prompt"))) {
			t.Fatalf("main borrowed background history: %s", body)
		}
	}
}
