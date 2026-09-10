package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestOpenAICompatResponsesToolImagesReachUpstream(t *testing.T) {
	for _, kind := range []string{"function_call", "custom_tool_call"} {
		for _, stream := range []bool{false, true} {
			for _, encoded := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/encoded=%t", kind, stream, encoded), func(t *testing.T) {
					calls := 0
					ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
						calls++
						body, err := io.ReadAll(r.Body)
						if err != nil {
							return nil, err
						}
						if r.URL.Host != "tool-image-fixture.invalid" || r.URL.Path != "/v1/chat/completions" {
							t.Error("unexpected upstream route")
						}
						message := gjson.GetBytes(body, `messages.#(role=="tool")`)
						if message.Get("tool_call_id").String() != "call_fixture" || message.Get("content.0.text").String() != "fixture" || message.Get("content.1.image_url.url").String() != "https://fixture.invalid/image.png" || message.Get("content.1.image_url.detail").String() != "low" {
							t.Error("tool image or pairing was lost before upstream")
						}
						response, contentType := `{"id":"fixture","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`, "application/json"
						if stream {
							contentType = "text/event-stream"
							response = "data: " + `{"id":"fixture","choices":[{"index":0,"delta":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"
						}
						return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(response))}, nil
					}))
					exec := NewOpenAICompatExecutor("tool-image-fixture", &config.Config{})
					auth := &coreauth.Auth{Provider: "tool-image-fixture", Attributes: map[string]string{"api_key": "fixture", "base_url": "https://tool-image-fixture.invalid/v1"}}
					output := `[{"type":"input_text","text":"fixture"},{"type":"image_url","image_url":{"url":"https://fixture.invalid/image.png","detail":"low"}}]`
					if encoded {
						output = strconv.Quote(output)
					}
					req := core.Request{Model: "model", Payload: []byte(fmt.Sprintf(`{"input":[{"type":%q,"name":"tool","call_id":"call_fixture","arguments":"{}","input":"fixture"},{"type":%q,"call_id":"call_fixture","output":%s}]}`, kind, kind+"_output", output))}
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
	}
}
