package openai

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketNotFoundPinClassification(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       bool
	}{
		{"model", `{"error":{"type":"invalid_request_error","code":"model_not_found","message":"model unavailable"}}`, true},
		{"plain upstream", `Not Found`, true},
		{"request item", "Item with id 'fixture' not found. Items are not persisted when `store` is set to false", false},
		{"policy", `{"error":{"code":"misalignment_policy_violation"}}`, false},
		{"invalid input", `{"error":{"code":"invalid_value"}}`, false},
		{"previous response recovery", `{"error":{"code":"previous_response_not_found"}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := &interfaces.ErrorMessage{StatusCode: http.StatusNotFound, Error: errors.New(tc.body)}
			body := map[string]any{"error": map[string]any{"code": "display_only"}}
			base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{{StatusCode: 404, ResponseStatusCode: 400, ResponseBody: &body}}}, nil)
			for _, message := range []*interfaces.ErrorMessage{original, base.RewriteExecutionErrorResponse(original)} {
				if got := shouldClearResponsesWebsocketPinnedAuth("fixture-a", "fixture-a", message); got != tc.want {
					t.Fatalf("release pin = %v, want %v", got, tc.want)
				}
			}
		})
	}
	if shouldClearResponsesWebsocketPinnedAuth("fixture-a", "fixture-a", &interfaces.ErrorMessage{StatusCode: 404, Error: committedContextError{}}) {
		t.Fatal("committed request error released its pinned credential")
	}
}

type websocketNotFoundExecutor struct {
	websocketAuthCaptureExecutor
	bodies [][]byte
}

func (e *websocketNotFoundExecutor) ExecuteStream(_ context.Context, credential *coreauth.Auth, req core.Request, _ core.Options) (*core.StreamResult, error) {
	e.mu.Lock()
	e.authIDs = append(e.authIDs, credential.ID)
	e.bodies = append(e.bodies, bytes.Clone(req.Payload))
	failed := len(e.authIDs) == 2
	e.mu.Unlock()
	if failed {
		return nil, &coreauth.Error{HTTPStatus: 404, Message: `{"error":{"code":"model_not_found","message":"model unavailable"}}`}
	}
	chunks := make(chan core.StreamChunk, 1)
	chunks <- core.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"resp-fixture","output":[{"type":"message","id":"out-fixture","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}}`)}
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func TestResponsesWebsocketModelNotFoundReleasesPinBeforeNextTurn(t *testing.T) {
	executor := &websocketNotFoundExecutor{}
	manager := coreauth.NewManager(nil, &orderedWebsocketSelector{order: []string{"not-found-a", "not-found-b"}}, nil)
	manager.SetRetryConfig(0, 0, 1)
	manager.RegisterExecutor(executor)
	for _, id := range []string{"not-found-a", "not-found-b"} {
		credential := &coreauth.Auth{ID: id, Provider: executor.Identifier(), Status: coreauth.StatusActive, Attributes: map[string]string{"websockets": "true"}}
		if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), credential); err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(id, credential.Provider, []*registry.ModelInfo{{ID: "not-found-model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	}
	handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&config.SDKConfig{}, manager))
	router := gin.New()
	router.GET("/v1/responses", handler.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for index, request := range []string{
		`{"type":"response.create","model":"not-found-model","input":[{"role":"user","content":"original question"}]}`,
		`{"type":"response.create","model":"not-found-model","input":[{"role":"user","content":"failed turn"}]}`,
		`{"type":"response.create","model":"not-found-model","previous_response_id":"resp-fixture","input":[{"role":"user","content":"retry turn"}]}`,
	} {
		if err := connection.WriteMessage(websocket.TextMessage, []byte(request)); err != nil {
			t.Fatal(err)
		}
		_, payload, err := connection.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		kind := gjson.GetBytes(payload, "type").String()
		if index == 1 {
			if kind != "error" && kind != "response.failed" {
				t.Fatal("missing model availability failure")
			}
		} else if kind != "response.completed" {
			t.Fatalf("turn %d did not complete: %s", index, kind)
		}
	}
	if got := executor.AuthIDs(); !reflect.DeepEqual(got, []string{"not-found-a", "not-found-a", "not-found-b"}) {
		t.Fatal("model failure did not release the connection pin", got)
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if gjson.GetBytes(executor.bodies[2], "previous_response_id").Exists() || !bytes.Contains(executor.bodies[2], []byte("original question")) {
		t.Fatal("account switch reused a foreign response ID or lost replayable history")
	}
}
