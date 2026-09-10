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

func TestOpenAICompatResponsesStructuredOutputReachesUpstream(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			calls := 0
			ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.Host != "format-fixture.invalid" || r.URL.Path != "/v1/chat/completions" {
					t.Error("unexpected compatibility route")
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					return nil, err
				}
				if gjson.GetBytes(body, "response_format.json_schema.schema.properties.n.const").Raw != "9007199254740993" || gjson.GetBytes(body, "response_format.json_schema.strict").Raw != "false" || gjson.GetBytes(body, "response_format.json_schema.name").String() != "answer" {
					t.Error("structured output constraint lost on outbound request")
				}
				response := `{"id":"fixture","model":"test-model","choices":[{"index":0,"message":{"role":"assistant","content":"{}"},"finish_reason":"stop"}]}`
				contentType := "application/json"
				if stream {
					contentType = "text/event-stream"
					response = "data: " + `{"id":"fixture","model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","content":"{}"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(response))}, nil
			}))
			exec := NewOpenAICompatExecutor("format-fixture", &config.Config{})
			auth := &coreauth.Auth{Provider: "format-fixture", Attributes: map[string]string{"api_key": "fixture", "base_url": "https://format-fixture.invalid/v1"}}
			req := core.Request{Model: "test-model", Payload: []byte(`{"input":[{"role":"user","content":"test"}],"text":{"format":{"type":"json_schema","name":"answer","strict":false,"schema":{"type":"object","properties":{"n":{"const":9007199254740993}}}}}}`)}
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
				t.Fatalf("unexpected upstream calls: %d", calls)
			}
		})
	}
}
