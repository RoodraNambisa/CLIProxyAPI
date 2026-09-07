package executor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeCustomToolOutputAfterRequestRelease(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				if gjson.GetBytes(body, "tools.0.name").String() != "collaboration__spawn_agent" || gjson.GetBytes(body, "tools.0.input_schema.properties.input.type").String() != "string" {
					t.Error("custom string schema did not reach the fake upstream")
				}
				controller.Release()
				events := []string{
					`{"type":"message_start","message":{"id":"result","role":"assistant","content":[]}}`,
					`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"pair","name":"collaboration__spawn_agent","input":{}}}`,
					`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"input\":\"one\\ntwo\"}"}}`,
					`{"type":"content_block_stop","index":0}`,
					`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}`,
					`{"type":"message_stop"}`,
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: "+strings.Join(events, "\n\ndata: ")+"\n\n")
			}))
			defer server.Close()
			ctx := t.Context()
			executor := NewClaudeExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}})
			auth := &cliproxyauth.Auth{ID: "custom-output-fixture", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
			req := core.Request{Model: "claude-sonnet-4-6", Payload: []byte(`{"input":[],"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"custom","name":"spawn_agent","format":{"type":"text"}}]}]}`)}
			opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, Metadata: map[string]any{core.BodyReleaseControllerMetadataKey: controller}}
			var item gjson.Result
			if stream {
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
						if event.Get("type").String() == "response.completed" {
							item = event.Get("response.output.0")
						}
					}
				}
			} else {
				result, err := executor.Execute(ctx, auth, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				item = gjson.GetBytes(result.Payload, "output.0")
			}
			if !controller.Released() || calls != 1 {
				t.Fatalf("release/calls: released=%t calls=%d", controller.Released(), calls)
			}
			if item.Get("type").String() != "custom_tool_call" || item.Get("id").String() != "ctc_pair" || item.Get("call_id").String() != "pair" || item.Get("name").String() != "spawn_agent" || item.Get("namespace").String() != "collaboration" || item.Get("input").String() != "one\ntwo" || item.Get("arguments").Exists() || item.Get("encrypted_function_args").Exists() {
				t.Fatal("released custom output lost its type, identity, input or was relabeled as a plaintext function")
			}
		})
	}
}
