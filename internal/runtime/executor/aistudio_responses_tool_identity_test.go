package executor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/wsrelay"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestAIStudioResponsesToolsAcrossRelayResults(t *testing.T) {
	for _, operation := range []string{"execute", "stream", "stream-http-response"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", operation, enabled), func(t *testing.T) {
				const authID = "aistudio-output-fixture"
				connected := make(chan struct{})
				relay := wsrelay.NewManager(wsrelay.Options{ProviderFactory: func(*http.Request) (string, error) { return authID, nil }, OnConnected: func(string) { close(connected) }})
				server := httptest.NewServer(relay.Handler())
				defer server.Close()
				defer relay.Stop(context.Background())
				conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+relay.Path(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = conn.Close() }()
				select {
				case <-connected:
				case <-t.Context().Done():
					t.Fatal("relay setup canceled")
				}
				controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
				clientErr := make(chan error, 1)
				go func() {
					var request wsrelay.Message
					if err := conn.ReadJSON(&request); err != nil {
						clientErr <- err
						return
					}
					body, _ := request.Payload["body"].(string)
					if gjson.Get(body, "tools.0.functionDeclarations.0.name").String() != "collaboration__spawn_agent" {
						clientErr <- fmt.Errorf("relay declaration lost its namespace")
						_ = conn.Close()
						return
					}
					controller.Release()
					response := `{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"collaboration__spawn_agent","args":{"message":"work"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}`
					messages := []wsrelay.Message{{ID: request.ID, Type: wsrelay.MessageTypeHTTPResp, Payload: map[string]any{"status": http.StatusOK, "body": response}}}
					if operation == "stream" {
						messages = []wsrelay.Message{
							{ID: request.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}},
							{ID: request.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": "data: " + response + "\n\n"}},
							{ID: request.ID, Type: wsrelay.MessageTypeStreamEnd},
						}
					}
					for _, message := range messages {
						if err := conn.WriteJSON(message); err != nil {
							clientErr <- err
							_ = conn.Close()
							return
						}
					}
					clientErr <- nil
				}()
				executor := NewAIStudioExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}}, authID, relay)
				req := core.Request{Model: "gemini-2.5-flash", Payload: []byte(`{"input":[],"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"type":"object"}}]}]}`)}
				opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, Metadata: map[string]any{core.BodyReleaseControllerMetadataKey: controller}}
				var items []gjson.Result
				if operation == "execute" {
					result, err := executor.Execute(t.Context(), &cliproxyauth.Auth{ID: authID}, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					items = append(items, gjson.GetBytes(result.Payload, "output.0"))
				} else {
					result, err := executor.ExecuteStream(t.Context(), &cliproxyauth.Auth{ID: authID}, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						for _, line := range bytes.Split(chunk.Payload, []byte("\n")) {
							event := gjson.ParseBytes(helps.JSONPayload(line))
							if event.Get("item.type").String() == "function_call" {
								items = append(items, event.Get("item"))
							}
							if event.Get("type").String() == "response.completed" {
								items = append(items, event.Get("response.output.0"))
							}
						}
					}
				}
				if err := <-clientErr; err != nil {
					t.Fatal(err)
				}
				if !controller.Released() || operation == "execute" && len(items) != 1 || operation != "execute" && len(items) != 3 {
					t.Fatalf("release/output events: released=%t, items=%d", controller.Released(), len(items))
				}
				for _, item := range items {
					if item.Get("status").String() != "in_progress" && gjson.Get(item.Get("arguments").String(), "message").String() != "work" {
						t.Fatal("relay conversion changed the completed tool arguments")
					}
					if item.Get("name").String() != "spawn_agent" || item.Get("namespace").String() != "collaboration" || item.Get("call_id").String() == "" || item.Get("call_id").String() != items[0].Get("call_id").String() {
						t.Fatal("tool identity changed after release")
					}
					if item.Get("encrypted_function_args").Exists() != enabled || enabled && item.Get("encrypted_function_args").Raw != "[]" {
						t.Fatal("plaintext marker did not follow the optional policy")
					}
				}
			})
		}
	}
}
