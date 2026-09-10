package executor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexWebsocketExecuteRebuildsEmptyTerminalOutputWithinEachTurn(t *testing.T) {
	for _, from := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatCodex} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/partial=%t", from, partial), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					for turn := 0; turn < 2; turn++ {
						if _, _, errRead := conn.ReadMessage(); errRead != nil {
							return
						}
						if turn == 0 {
							for _, frame := range []string{
								`{"type":"response.output_item.done","output_index":1,"item":{"type":"custom_tool_call","id":"ctc_test","call_id":"pair","name":"lookup","input":"free form"}}`,
								`{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"msg_test","role":"assistant","content":[{"type":"output_text","text":"answer"}]}}`,
								`{"type":"response.output_item.done","item":{"type":"image_generation_call","id":"img_test","result":"aW1hZ2U="}}`,
							} {
								if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(frame)); errWrite != nil {
									return
								}
							}
						}
						status, kind := "completed", "done"
						if partial {
							status, kind = "incomplete", "incomplete"
						}
						terminal := fmt.Sprintf(`{"type":"response.%s","response":{"status":%q,"output":[],"incomplete_details":{"reason":"max_tokens"}}}`, kind, status)
						if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(terminal)); errWrite != nil {
							return
						}
					}
				}))
				defer server.Close()
				executor := NewCodexWebsocketsExecutor(&config.Config{})
				executor.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
				defer executor.CloseExecutionSession(t.Name())
				credential := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
				opts := core.Options{SourceFormat: from, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
				for turn := 0; turn < 2; turn++ {
					response, err := executor.Execute(t.Context(), credential, core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"input":[]}`)}, opts)
					if err != nil {
						t.Fatal(err)
					}
					output := gjson.GetBytes(response.Payload, "output")
					if from == translator.FormatCodex {
						output = gjson.GetBytes(response.Payload, "response.output")
					}
					if turn == 0 {
						if len(output.Array()) != 3 || output.Get("0.id").String() != "msg_test" || output.Get("1.call_id").String() != "pair" || output.Get("1.input").String() != "free form" || output.Get("2.result").String() != "aW1hZ2U=" {
							t.Fatal("done output was lost, reordered or altered")
						}
					} else if !output.IsArray() || len(output.Array()) != 0 {
						t.Fatal("output from a previous turn leaked into the next completion")
					}
				}
			})
		}
	}
}
