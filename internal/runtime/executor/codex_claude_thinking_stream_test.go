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

func TestCodexClaudeMultipartThinkingAcrossTransports(t *testing.T) {
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
						`{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_test","encrypted_content":"placeholder"}}`,
						`{"type":"response.reasoning_summary_part.added","item_id":"rs_test","summary_index":0}`,
						`{"type":"response.reasoning_summary_text.delta","item_id":"rs_test","summary_index":0,"delta":"First"}`,
						`{"type":"response.reasoning_summary_part.done","item_id":"rs_test","summary_index":0}`,
						`{"type":"response.reasoning_summary_part.added","item_id":"rs_test","summary_index":1}`,
						`{"type":"response.reasoning_summary_text.delta","item_id":"rs_test","summary_index":1,"delta":"Second"}`,
						`{"type":"response.reasoning_summary_part.done","item_id":"rs_test","summary_index":1}`,
						`{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs_test","encrypted_content":"final-signature"}}`,
						`{"type":"response.output_text.delta","delta":"Answer"}`,
						`{"type":"response.completed","response":{"id":"test","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`,
					} {
						if conn != nil {
							_ = conn.WriteMessage(websocket.TextMessage, []byte(event))
						} else {
							_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
						}
					}
				}))
				defer server.Close()
				var exec auth.ProviderExecutor = NewCodexExecutor(&config.Config{})
				payload := []byte(`{"messages":[{"role":"user","content":"think"}]}`)
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
				starts, stops, signatures := 0, 0, 0
				var thought, answer strings.Builder
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
					for _, line := range strings.Split(string(chunk.Payload), "\n") {
						if !strings.HasPrefix(line, "data: ") {
							continue
						}
						event := gjson.Parse(strings.TrimPrefix(line, "data: "))
						switch event.Get("type").String() {
						case "content_block_start":
							starts++
						case "content_block_stop":
							stops++
						case "content_block_delta":
							thought.WriteString(event.Get("delta.thinking").String())
							answer.WriteString(event.Get("delta.text").String())
							if event.Get("delta.type").String() == "signature_delta" {
								signatures++
								if event.Get("delta.signature").String() != "final-signature" {
									t.Error("placeholder signature reached the client")
								}
							}
						}
					}
				}
				if starts != 2 || stops != 2 || signatures != 1 || thought.String() != "First\n\nSecond" || answer.String() != "Answer" || controller.Released() != release {
					t.Fatalf("starts=%d stops=%d signatures=%d thought=%q released=%t", starts, stops, signatures, thought.String(), controller.Released())
				}
			})
		}
	}
}
