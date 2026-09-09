package executor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeResponsesCarriersReachHTTPAndSSEClients(t *testing.T) {
	events := []string{
		`{"type":"message_start","message":{"id":"carrier","type":"message","role":"assistant","usage":{"input_tokens":2}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":"part-"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"visible"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"tail"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"redacted_thinking","data":"opaque-fixture"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}`,
		`{"type":"message_stop"}`,
	}
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: "+strings.Join(events, "\n\ndata: ")+"\n\n")
			}))
			t.Cleanup(server.Close)
			manager := claudeCompatibilityManager(t, server.URL, false)
			raw := []byte(`{"model":"bound-claude","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"question"}]}]}`)
			req := core.Request{Model: "bound-claude", Payload: raw}
			opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, OriginalRequest: raw}
			var result gjson.Result
			if stream {
				earlyEvents := 0
				reply, err := manager.ExecuteStream(t.Context(), []string{"claude"}, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range reply.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
					for _, line := range bytes.Split(chunk.Payload, []byte("\n")) {
						if !bytes.HasPrefix(line, []byte("data:")) {
							continue
						}
						event := gjson.ParseBytes(bytes.TrimPrefix(line, []byte("data:")))
						if kind := event.Get("type").String(); kind == "response.created" || kind == "response.in_progress" {
							earlyEvents++
							if event.Get("response.model").String() != "bound-claude" || event.Get("response.output").Raw != "[]" {
								t.Error("early events lost the public model alias or empty output array")
							}
						}
						if event.Get("type").String() == "response.completed" {
							result = event.Get("response")
						}
					}
				}
				if earlyEvents != 2 {
					t.Fatalf("received %d early response events, want 2", earlyEvents)
				}
			} else {
				reply, err := manager.Execute(t.Context(), []string{"claude"}, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				result = gjson.ParseBytes(reply.Payload)
			}
			if result.Get("model").String() != "bound-claude" || result.Get("output.#").Int() != 2 || result.Get("output.0.encrypted_content").String() != "part-tail" ||
				result.Get("output.0.summary.0.text").String() != "visible" ||
				result.Get("output.1.encrypted_content").String() != "claude-redacted-thinking:opaque-fixture" || result.Get("output.1.summary").Raw != "[]" {
				t.Fatal("actual client response lost reasoning or redacted carriers")
			}
		})
	}
}
