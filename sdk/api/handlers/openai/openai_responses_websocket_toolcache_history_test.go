package openai

import (
	"bytes"
	"context"
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

type websocketHistoryOutcomeExecutor struct {
	websocketCompactionCaptureExecutor
}

func (e *websocketHistoryOutcomeExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	index := len(e.streamPayloads)
	e.streamPayloads = append(e.streamPayloads, bytes.Clone(req.Payload))
	e.mu.Unlock()
	chunks := make(chan coreexecutor.StreamChunk, 2)
	if index == 1 {
		chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"response-b","name":"tool","arguments":"{}"}}`)}
		chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.failed","response":{"status":"failed","error":{"code":"misalignment_policy_violation","type":"policy_violation","message":"blocked"}}}`)}
	} else {
		chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"status":"completed","output":[{"type":"message","role":"assistant","id":"reply-a","content":"success answer"}]}}`)}
	}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestResponsesWebsocketFailedTurnRetainsLastSuccessfulHistory(t *testing.T) {
	executor := &websocketHistoryOutcomeExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "history-outcome-auth", Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "history-outcome-model"}})
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
	for _, request := range []string{
		`{"type":"response.create","model":"history-outcome-model","input":[{"type":"message","role":"user","id":"request-a","content":"success prompt"}]}`,
		`{"type":"response.create","input":[{"type":"message","role":"user","content":"failure-body"},{"type":"function_call","call_id":"request-b","name":"tool","arguments":"{}"},{"type":"function_call_output","call_id":"request-b","output":"failed request result"}]}`,
		`{"type":"response.create","input":[{"type":"function_call_output","call_id":"request-b","output":"unpaired"},{"type":"function_call_output","call_id":"response-b","output":"unpaired"},{"type":"message","role":"user","id":"request-c","content":"continue"}]}`,
	} {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(request)); err != nil {
			t.Fatal(err)
		}
		for {
			_, response, errRead := conn.ReadMessage()
			if errRead != nil {
				t.Fatal(errRead)
			}
			if responsesWebsocketTerminalEvent(gjson.GetBytes(response, "type").String()) {
				break
			}
		}
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if len(executor.streamPayloads) != 3 {
		t.Fatal("failed request unexpectedly changed upstream attempt count")
	}
	last := executor.streamPayloads[2]
	items := gjson.GetBytes(last, "input").Array()
	if len(items) != 3 || items[0].Get("id").String() != "request-a" || items[1].Get("id").String() != "reply-a" || items[2].Get("id").String() != "request-c" {
		t.Fatal("failed turn replaced or polluted the last successful history")
	}
	if bytes.Contains(last, []byte("failure-body")) || bytes.Contains(last, []byte("request-b")) || bytes.Contains(last, []byte("response-b")) {
		t.Fatal("failed request or response tools escaped the pending cache")
	}
}
