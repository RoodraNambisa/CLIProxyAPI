package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestOpenAICompatResponsesAssistantToolHistoryReachesUpstream(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			calls := 0
			ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				body, err := io.ReadAll(r.Body)
				if err != nil {
					return nil, err
				}
				messages := gjson.GetBytes(body, "messages").Array()
				if r.URL.Path != "/v1/chat/completions" || len(messages) != 3 {
					t.Error("unexpected history route or message count")
				} else if messages[0].Get("content").String() != "checking" || messages[0].Get("reasoning_content").String() != "plan" || messages[0].Get("tool_calls.0.id").String() != "call_fixture" || messages[1].Get("tool_call_id").String() != "call_fixture" || messages[2].Get("role").String() != "user" {
					t.Error("assistant content, reasoning or tool adjacency lost on outbound request")
				}
				response, contentType := `{"id":"fixture","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`, "application/json"
				if stream {
					contentType = "text/event-stream"
					response = "data: " + `{"id":"fixture","choices":[{"index":0,"delta":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(response))}, nil
			}))
			exec := NewOpenAICompatExecutor("history-fixture", &config.Config{})
			auth := &coreauth.Auth{Provider: "history-fixture", Attributes: map[string]string{"api_key": "fixture", "base_url": "https://history-fixture.invalid/v1"}}
			req := core.Request{Model: "model", Payload: []byte(`{"input":[{"type":"reasoning","summary":[{"type":"summary_text","text":"plan"}]},{"role":"assistant","content":"checking"},{"type":"function_call","call_id":"call_fixture","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"call_fixture","output":"found"},{"role":"user","content":"next"}]}`)}
			opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}
			if stream {
				result, err := exec.ExecuteStream(ctx, auth, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else if _, err := exec.Execute(ctx, auth, req, opts); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("upstream calls=%d", calls)
			}
		})
	}
}
