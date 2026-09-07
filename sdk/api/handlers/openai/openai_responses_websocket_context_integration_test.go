package openai

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type websocketResponseReferenceExecutor struct {
	websocketCompactionCaptureExecutor
}

func (e *websocketResponseReferenceExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	index := len(e.streamPayloads)
	e.streamPayloads = append(e.streamPayloads, bytes.Clone(req.Payload))
	e.mu.Unlock()
	chunks := make(chan coreexecutor.StreamChunk, 1)
	if index == 1 {
		chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.failed","response":{"id":"failed-ref","status":"failed","error":{"code":"misalignment_policy_violation","message":"blocked"}}}`)}
	} else {
		chunks <- coreexecutor.StreamChunk{Payload: fmt.Appendf(nil, `{"type":"response.completed","response":{"id":"ref-%d","status":"completed","output":[{"type":"message","role":"assistant","content":"successful output"}]}}`, index)}
	}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestResponsesWebsocketHTTPHistoryUsesOnlyMatchingSuccessfulResponse(t *testing.T) {
	executor := &websocketResponseReferenceExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "response-reference-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "response-reference-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	router := gin.New()
	router.GET("/v1/responses", handler.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	steps := []struct {
		request string
		calls   int
		code    string
	}{
		{`{"type":"response.create","model":"response-reference-model","input":[{"role":"user","content":"original prompt"}]}`, 1, ""},
		{`{"type":"response.create","previous_response_id":"unknown","input":[]}`, 1, "previous_response_not_found"},
		{`{"type":"response.create","previous_response_id":"ref-0","input":[{"role":"user","content":"rejected prompt"}]}`, 2, "misalignment_policy_violation"},
		{`{"type":"response.create","previous_response_id":"failed-ref","input":[]}`, 2, "previous_response_not_found"},
		{`{"type":"response.create","previous_response_id":"ref-0","input":[{"role":"user","content":"valid followup"}]}`, 3, ""},
	}
	for index, step := range steps {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(step.request)); err != nil {
			t.Fatal(err)
		}
		for {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				t.Fatal(errRead)
			}
			if !responsesWebsocketTerminalEvent(gjson.GetBytes(payload, "type").String()) {
				continue
			}
			code := gjson.GetBytes(payload, "error.code").String()
			if code == "" {
				code = gjson.GetBytes(payload, "response.error.code").String()
			}
			if code != step.code {
				t.Fatalf("step %d returned code %q, want %q", index, code, step.code)
			}
			break
		}
		executor.mu.Lock()
		calls := len(executor.streamPayloads)
		executor.mu.Unlock()
		if calls != step.calls {
			t.Fatalf("step %d called upstream %d times, want %d", index, calls, step.calls)
		}
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	last := executor.streamPayloads[2]
	if gjson.GetBytes(last, "previous_response_id").Exists() || bytes.Contains(last, []byte("rejected prompt")) {
		t.Fatal("HTTP replay retained a state pointer or failed request history")
	}
	if !bytes.Contains(last, []byte("original prompt")) || !bytes.Contains(last, []byte("successful output")) || !bytes.Contains(last, []byte("valid followup")) {
		t.Fatal("HTTP replay omitted matching successful history")
	}
}
