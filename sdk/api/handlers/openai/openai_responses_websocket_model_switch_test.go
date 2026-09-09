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
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type websocketModelSwitchExecutor struct {
	websocketAuthCaptureExecutor
	bodies [][]byte
}

func (e *websocketModelSwitchExecutor) ExecuteStream(ctx context.Context, a *coreauth.Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	e.mu.Lock()
	e.bodies = append(e.bodies, bytes.Clone(req.Payload))
	e.mu.Unlock()
	return e.websocketAuthCaptureExecutor.ExecuteStream(ctx, a, req, opts)
}

func TestResponsesWebsocketRechecksPinnedModelCapability(t *testing.T) {
	for _, supportsBoth := range []bool{false, true} {
		for _, reference := range []bool{false, true} {
			t.Run(fmt.Sprintf("both=%t/reference=%t", supportsBoth, reference), func(t *testing.T) {
				executor := &websocketModelSwitchExecutor{}
				manager := coreauth.NewManager(nil, &orderedWebsocketSelector{order: []string{"switch-a", "switch-b"}}, nil)
				manager.RegisterExecutor(executor)
				for _, id := range []string{"switch-a", "switch-b"} {
					a := &coreauth.Auth{ID: id, Provider: executor.Identifier(), Status: coreauth.StatusActive, Attributes: map[string]string{"websockets": "true"}}
					if _, err := manager.Register(t.Context(), a); err != nil {
						t.Fatal(err)
					}
					models := []*registry.ModelInfo{{ID: "switch-first"}}
					if id == "switch-b" {
						models = []*registry.ModelInfo{{ID: "switch-second"}}
					} else if supportsBoth {
						models = append(models, &registry.ModelInfo{ID: "switch-second"})
					}
					registry.GetGlobalRegistry().RegisterClient(id, a.Provider, models)
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				}
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
				second := `{"type":"response.create","model":"switch-second","input":[{"role":"user","content":"next"}]}`
				if reference {
					second = `{"type":"response.create","model":"switch-second","previous_response_id":"resp-upstream","input":[{"role":"user","content":"next"}]}`
				}
				for i, request := range []string{`{"type":"response.create","model":"switch-first","input":[{"role":"user","content":"original"}]}`, second} {
					if err := conn.WriteMessage(websocket.TextMessage, []byte(request)); err != nil {
						t.Fatal(err)
					}
					for {
						_, payload, errRead := conn.ReadMessage()
						if errRead != nil {
							t.Fatal(errRead)
						}
						kind := gjson.GetBytes(payload, "type").String()
						if responsesWebsocketTerminalEvent(kind) {
							if kind != "response.completed" {
								t.Fatalf("request %d failed: %s", i, payload)
							}
							break
						}
					}
				}
				ids := executor.AuthIDs()
				want := "switch-b"
				if supportsBoth {
					want = "switch-a"
				}
				if len(ids) != 2 || ids[0] != "switch-a" || ids[1] != want {
					t.Fatalf("selection sequence = %v", ids)
				}
				executor.mu.Lock()
				defer executor.mu.Unlock()
				if reference && !supportsBoth && (gjson.GetBytes(executor.bodies[1], "previous_response_id").Exists() || !bytes.Contains(executor.bodies[1], []byte("original"))) {
					t.Fatal("credential switch reused a foreign response ID or omitted replayable history")
				}
			})
		}
	}
}
