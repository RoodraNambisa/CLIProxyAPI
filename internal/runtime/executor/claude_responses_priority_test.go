package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeExecutorResponsesPriorityOnlyAffectsGeneration(t *testing.T) {
	for _, operation := range []string{"execute", "stream", "count"} {
		for _, priority := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/priority=%t", operation, priority), func(t *testing.T) {
				requests := make(chan []byte, 1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, _ := io.ReadAll(r.Body)
					requests <- body
					w.Header().Set("Content-Type", "application/json")
					if operation == "count" {
						_, _ = io.WriteString(w, `{"input_tokens":3}`)
					} else if operation == "execute" {
						_, _ = io.WriteString(w, `{"id":"fixture","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":1}}`)
					} else {
						w.Header().Set("Content-Type", "text/event-stream")
						for _, event := range []string{
							`{"type":"message_start","message":{"id":"fixture","model":"claude-sonnet-4-5","usage":{"input_tokens":3,"output_tokens":0}}}`,
							`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
							`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}`,
							`{"type":"content_block_stop","index":0}`,
							`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}`,
							`{"type":"message_stop"}`,
						} {
							_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
						}
					}
				}))
				defer server.Close()
				tier := "auto"
				if priority {
					tier = "priority"
				}
				exec := NewClaudeExecutor(&config.Config{})
				auth := &coreauth.Auth{Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
				request := core.Request{Model: "claude-sonnet-4-5", Payload: []byte(`{"input":[{"role":"user","content":"fixture"}],"service_tier":"` + tier + `"}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
				var err error
				switch operation {
				case "execute":
					_, err = exec.Execute(t.Context(), auth, request, opts)
				case "count":
					_, err = exec.CountTokens(t.Context(), auth, request, opts)
				case "stream":
					var result *core.StreamResult
					result, err = exec.ExecuteStream(t.Context(), auth, request, opts)
					if err == nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								t.Fatal(chunk.Err)
							}
						}
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				want := ""
				if priority && operation != "count" {
					want = "fast"
				}
				if got := gjson.GetBytes(<-requests, "speed").String(); got != want {
					t.Fatalf("upstream speed = %q, want %q", got, want)
				}
			})
		}
	}
}
