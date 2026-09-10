package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexClaudeParallelToolsAcrossTransports(t *testing.T) {
	for _, transport := range []string{"http", "websocket"} {
		for _, release := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/release=%t", transport, release), func(t *testing.T) {
				controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var conn *websocket.Conn
					if transport == "websocket" {
						upgrader := websocket.Upgrader{}
						var err error
						conn, err = upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, err = conn.ReadMessage(); err != nil {
							t.Error(err)
							return
						}
					} else {
						_, _ = io.Copy(io.Discard, r.Body)
						w.Header().Set("Content-Type", "text/event-stream")
					}
					if release {
						controller.Release()
					}
					for _, event := range []string{
						`{"type":"response.created","response":{"id":"test","model":"gpt-5.4"}}`,
						`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_a","call_id":"call_a","name":"run"}}`,
						`{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"fc_b","call_id":"call_b","name":"run"}}`,
						`{"type":"response.function_call_arguments.delta","item_id":"fc_b","delta":"{\"b\":2}"}`,
						`{"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","id":"fc_b","call_id":"call_b","name":"run","arguments":"{\"b\":2}"}}`,
						`{"type":"response.output_text.delta","delta":"after tools"}`,
						`{"type":"response.function_call_arguments.delta","item_id":"fc_a","delta":"{\"a\":"}`,
						`{"type":"response.completed","response":{"id":"test","status":"completed","output":[{"type":"function_call","id":"fc_a","call_id":"call_a","name":"run","arguments":"{\"a\":1}"},{"type":"function_call","id":"fc_b","call_id":"call_b","name":"run","arguments":"{\"b\":2}"}],"usage":{"input_tokens":1,"output_tokens":1}}}`,
					} {
						if !gjson.Valid(event) {
							t.Error("invalid test event")
							return
						}
						if conn != nil {
							_ = conn.WriteMessage(websocket.TextMessage, []byte(event))
						} else {
							_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
						}
					}
				}))
				defer server.Close()
				var exec auth.ProviderExecutor = NewCodexExecutor(&config.Config{})
				payload := []byte(`{"messages":[{"role":"user","content":"tools"}],"tools":[{"name":"run","input_schema":{"type":"object"}}]}`)
				opts := core.Options{SourceFormat: translator.FormatClaude, OriginalRequest: payload}
				if release {
					opts.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: controller}
				}
				if transport == "websocket" {
					exec = NewCodexWebsocketsExecutor(&config.Config{})
					payload = translator.TranslateRequest(translator.FormatClaude, translator.FormatCodex, "gpt-5.4", payload, true)
				}
				credential := &auth.Auth{Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
				result, err := exec.ExecuteStream(t.Context(), credential, core.Request{Model: "gpt-5.4", Payload: payload}, opts)
				if err != nil {
					t.Fatal(err)
				}
				open, count := -1, 0
				arguments := map[int]string{}
				var answer string
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
					for _, line := range strings.Split(string(chunk.Payload), "\n") {
						if !strings.HasPrefix(line, "data: ") {
							continue
						}
						event := gjson.Parse(strings.TrimPrefix(line, "data: "))
						index := int(event.Get("index").Int())
						switch event.Get("type").String() {
						case "content_block_start":
							if open != -1 || index != count {
								t.Fatal("tool block overlapped or reused an index")
							}
							if index < 2 && event.Get("content_block.id").String() != []string{"call_a", "call_b"}[index] {
								t.Fatal("tool ID lost")
							}
							open, count = index, count+1
						case "content_block_delta":
							if open != index {
								t.Fatal("delta outside its block")
							}
							arguments[index] += event.Get("delta.partial_json").String()
							answer += event.Get("delta.text").String()
						case "content_block_stop":
							if open != index {
								t.Fatal("stop outside its block")
							}
							open = -1
						case "message_delta", "message_stop":
							if open != -1 {
								t.Fatal("terminal preceded a block stop")
							}
						}
					}
				}
				if count != 3 || open != -1 || arguments[0] != `{"a":1}` || arguments[1] != `{"b":2}` || answer != "after tools" || controller.Released() != release {
					t.Fatalf("invalid output: count=%d args=%v answer=%q", count, arguments, answer)
				}
			})
		}
	}
}
