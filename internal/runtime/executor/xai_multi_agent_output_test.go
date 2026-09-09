package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIResponsesOutputIdentityAndOptionalPlaintext(t *testing.T) {
	runXAIResponsesOutputIdentity(t, false)
}

func TestXAIAdditionalToolsOutputIdentityAndOptionalPlaintext(t *testing.T) {
	runXAIResponsesOutputIdentity(t, true)
}

func runXAIResponsesOutputIdentity(t *testing.T, additional bool) {
	for _, operation := range []string{"execute", "stream", "compact", "websocket", "websocket-sse"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", operation, enabled), func(t *testing.T) {
				cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}}
				call := `{"type":"function_call","id":"fc_pair","call_id":"pair","name":"spawn_agent","arguments":"{\"message\":\"work\"}"}`
				events := []string{`{"type":"response.output_item.added","output_index":0,"item":` + call + `}`, `{"type":"response.output_item.done","output_index":0,"item":` + call + `}`, `{"type":"response.completed","response":{"id":"result","status":"completed","output":[` + call + `],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}}`}
				inspect := func(body []byte) {
					if operation == "compact" {
						if gjson.GetBytes(body, "tools").Exists() {
							t.Error("compact restored removed tools")
						}
					} else if gjson.GetBytes(body, "tools.0.name").String() != "spawn_agent" {
						t.Error("xAI's existing outgoing short tool name changed")
					}
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					_, body, err := conn.ReadMessage()
					if err != nil {
						t.Error(err)
						return
					}
					inspect(body)
					for _, event := range events {
						if err := conn.WriteMessage(websocket.TextMessage, []byte(event)); err != nil {
							return
						}
					}
				}))
				defer server.Close()
				ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						return nil, err
					}
					inspect(body)
					cfg.Codex.OptimizeMultiAgentV2 = !enabled
					w := httptest.NewRecorder()
					if operation == "compact" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"compact","object":"response.compaction","output":[{"type":"compaction","encrypted_content":"fixture-opaque"}]}`)
					} else {
						w.Header().Set("Content-Type", "text/event-stream")
						for _, event := range events {
							_, _ = io.WriteString(w, "data: "+event+"\n\n")
						}
					}
					return w.Result(), nil
				}))
				var executor multiAgentTranslationExecutor = NewXAIExecutor(cfg)
				if strings.HasPrefix(operation, "websocket") {
					executor = NewXAIWebsocketsExecutor(cfg)
				}
				if operation == "websocket" {
					ctx = core.WithDownstreamWebsocket(ctx)
				}
				auth := &cliproxyauth.Auth{ID: "xai-output-fixture", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				req := core.Request{Model: "grok-4", Payload: []byte(`{"input":[],"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"type":"object"}}]}]}`)}
				if additional {
					req.Payload = []byte(`{"input":[{"type":"additional_tools","tools":` + gjson.GetBytes(req.Payload, "tools").Raw + `}]}`)
				}
				opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
				var items []gjson.Result
				var responseUsage gjson.Result
				if operation == "execute" || operation == "compact" {
					if operation == "compact" {
						opts.Alt = "responses/compact"
					}
					result, err := executor.Execute(ctx, auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					if operation == "compact" {
						if gjson.GetBytes(result.Payload, "object").String() != "response.compaction" || gjson.GetBytes(result.Payload, "output.0.encrypted_content").String() != "fixture-opaque" {
							t.Fatal("compact result changed")
						}
						return
					}
					items = append(items, gjson.GetBytes(result.Payload, "output.0"))
					responseUsage = gjson.GetBytes(result.Payload, "usage")
				} else {
					result, err := executor.ExecuteStream(ctx, auth, req, opts)
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
								responseUsage = event.Get("response.usage")
							}
						}
					}
				}
				if responseUsage.Get("total_tokens").Int() != 3 || responseUsage.Get("input_tokens_details.cached_tokens").Raw != "0" || responseUsage.Get("output_tokens_details.reasoning_tokens").Raw != "0" {
					t.Fatal("xAI Responses usage details were not completed")
				}
				want := 3
				if operation == "execute" {
					want = 1
				}
				if len(items) != want {
					t.Fatalf("output items = %d, want %d", len(items), want)
				}
				for _, item := range items {
					if item.Get("name").String() != "spawn_agent" || item.Get("namespace").String() != "collaboration" || item.Get("call_id").String() == "" || item.Get("call_id").String() != items[0].Get("call_id").String() || item.Get("arguments").String() != `{"message":"work"}` {
						t.Fatalf("source tool identity changed: name=%q namespace=%q call=%q first=%q args=%q", item.Get("name").String(), item.Get("namespace").String(), item.Get("call_id").String(), items[0].Get("call_id").String(), item.Get("arguments").String())
					}
					if item.Get("encrypted_function_args").Exists() != enabled || enabled && item.Get("encrypted_function_args").Raw != "[]" {
						t.Fatal("optional plaintext policy changed during execution")
					}
				}
			})
		}
	}
}
