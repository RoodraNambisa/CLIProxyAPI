package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexClaudeToolPairingAtUpstream(t *testing.T) {
	for _, mode := range []string{"http", "sse", "ws", "ws-stream"} {
		for _, release := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/release=%t", mode, release), func(t *testing.T) {
				captured := make(chan []byte, 1)
				websocketMode := strings.HasPrefix(mode, "ws")
				stream := mode == "sse" || mode == "ws-stream"
				terminal := []byte(`{"type":"response.completed","response":{"id":"fixture","status":"completed","output":[]}}`)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if websocketMode {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, payload, errRead := conn.ReadMessage(); errRead == nil {
							captured <- payload
							_ = conn.WriteMessage(websocket.TextMessage, terminal)
						}
						return
					}
					body, _ := io.ReadAll(r.Body)
					captured <- body
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprintf(w, "data: %s\n\n", terminal)
				}))
				defer server.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				body := []byte(`{"model":"gpt-5.4-mini","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call-a","name":"a","input":{}},{"type":"tool_use","id":"call-b","name":"b","input":{}}]},{"role":"system","content":"reminder"},{"role":"user","content":[{"type":"text","text":"follow-up"},{"type":"tool_result","tool_use_id":"call-b","content":"B"},{"type":"tool_result","tool_use_id":"call-a","content":"A"}]}]}`)
				var executor coreauth.ProviderExecutor = NewCodexExecutor(&config.Config{})
				if websocketMode {
					executor = NewCodexWebsocketsExecutor(&config.Config{})
				}
				req := core.Request{Model: "gpt-5.4-mini", Payload: body}
				opts := core.Options{SourceFormat: translator.FormatClaude, Stream: stream}
				if mode == "ws-stream" {
					// The streaming WS executor consumes already prepared Responses frames.
					req.Payload = translator.TranslateRequest(translator.FormatClaude, translator.FromString("codex"), req.Model, body, true)
					opts.OriginalRequest = body
				}
				controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
				if release {
					opts.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: controller}
				}
				if stream {
					response, err := executor.ExecuteStream(ctx, auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range response.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else if _, err := executor.Execute(ctx, auth, req, opts); err != nil {
					t.Fatal(err)
				}
				got := <-captured
				if controller.Released() != release {
					t.Fatal("request body release did not follow the selected mode")
				}
				items := gjson.GetBytes(got, "input").Array()
				if len(items) != 6 || items[2].Get("call_id").String() != "call-a" || items[2].Get("output").String() != "A" || items[3].Get("call_id").String() != "call-b" || items[3].Get("output").String() != "B" || !strings.Contains(items[4].Get("content.0.text").String(), "reminder") || items[4].Get("role").String() != "user" || items[5].Get("content.0.text").String() != "follow-up" {
					t.Fatal("upstream request lost pairing, reminder role/order or caller follow-up")
				}
			})
		}
	}
}
