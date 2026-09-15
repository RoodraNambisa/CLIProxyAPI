package executor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIWebsocketFoldedToolsRestoreEveryStreamEvent(t *testing.T) {
	for _, downstreamWS := range []bool{false, true} {
		t.Run(fmt.Sprintf("downstream_websocket=%t", downstreamWS), func(t *testing.T) {
			var declarations []any
			for i := range 201 {
				declarations = append(declarations, map[string]any{"type": "function", "name": fmt.Sprintf("tool_%d", i), "parameters": map[string]any{"type": "object"}})
			}
			body, err := json.Marshal(map[string]any{"input": []any{}, "tools": []any{map[string]any{"type": "namespace", "name": "workspace", "tools": declarations}}})
			if err != nil {
				t.Fatal(err)
			}
			completedCall := `{"type":"function_call","id":"fc_one","call_id":"call_one","name":"workspace","arguments":"{\"name\":\"tool_1\",\"arguments\":{\"x\":2}}"}`
			events := []string{
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_one","call_id":"call_one","name":"workspace","arguments":""}}`,
				`{"type":"response.function_call_arguments.delta","item_id":"fc_one","output_index":0,"delta":"{\"name\":\"tool_1\","}`,
				`{"type":"response.output_text.delta","item_id":"msg_one","output_index":1,"content_index":0,"delta":"Still streaming"}`,
				`{"type":"response.function_call_arguments.delta","item_id":"fc_one","output_index":0,"delta":"\"arguments\":{\"x\":2}}"}`,
				`{"type":"response.function_call_arguments.done","item_id":"fc_one","output_index":0,"arguments":"{\"name\":\"tool_1\",\"arguments\":{\"x\":2}}"}`,
				`{"type":"response.output_item.done","output_index":0,"item":` + completedCall + `}`,
				`{"type":"response.completed","response":{"id":"resp_one","status":"completed","output":[` + completedCall + `]}}`,
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, errUpgrade := upgrader.Upgrade(w, r, nil)
				if errUpgrade != nil {
					t.Error(errUpgrade)
					return
				}
				defer func() { _ = conn.Close() }()
				_, request, errRead := conn.ReadMessage()
				if errRead != nil {
					t.Error(errRead)
					return
				}
				if gjson.GetBytes(request, "tools.#").Int() != 1 || gjson.GetBytes(request, "tools.0.name").String() != "workspace" {
					t.Error("fixture did not exercise namespace folding")
				}
				for _, event := range events {
					if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(event)); errWrite != nil {
						t.Error(errWrite)
						return
					}
				}
			}))
			defer server.Close()
			exec := NewXAIWebsocketsExecutor(&config.Config{})
			ctx := t.Context()
			if downstreamWS {
				ctx = core.WithDownstreamWebsocket(ctx)
			}
			auth := &coreauth.Auth{ID: "fold-fixture", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
			result, err := exec.ExecuteStream(ctx, auth, core.Request{Model: "grok-4.5", Payload: body}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
			if err != nil {
				t.Fatal(err)
			}
			var arguments strings.Builder
			var received []gjson.Result
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					t.Fatal(chunk.Err)
				}
				for _, line := range bytes.Split(chunk.Payload, []byte("\n")) {
					if event := gjson.ParseBytes(helps.JSONPayload(line)); event.Get("type").Exists() {
						received = append(received, event)
						if event.Get("type").String() == "response.function_call_arguments.delta" {
							arguments.WriteString(event.Get("delta").String())
						}
					}
				}
			}
			if arguments.String() != `{"x":2}` {
				t.Fatalf("client received dispatcher arguments: %s", arguments.String())
			}
			if len(received) != 6 || received[0].Get("type").String() != "response.output_text.delta" {
				t.Fatalf("ordinary text was held or tool events duplicated: %v", received)
			}
			for _, index := range []int{1, 4, 5} {
				item := received[index].Get("item")
				if index == 5 {
					item = received[index].Get("response.output.0")
				}
				if item.Get("name").String() != "tool_1" || item.Get("namespace").String() != "workspace" || item.Get("call_id").String() != received[1].Get("item.call_id").String() {
					t.Fatalf("tool identity was not restored: %s", item.Raw)
				}
			}
			if received[3].Get("arguments").String() != arguments.String() {
				t.Fatal("done arguments differ from the streamed arguments")
			}
		})
	}
}
