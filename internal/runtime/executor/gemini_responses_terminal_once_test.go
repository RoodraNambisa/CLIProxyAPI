package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestGoogleResponsesTerminalDoesNotTrustInvalidUpstreamDone(t *testing.T) {
	for _, provider := range []string{"gemini", "vertex", "vertex-service-account"} {
		for _, outcome := range []string{"stop", "invalid_done", "truncated"} {
			for _, release := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/release=%t", provider, outcome, release), func(t *testing.T) {
					controller := core.NewRequestBodyReleaseController(1, []byte("released"))
					calls := 0
					ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
						if r.URL.Host == "oauth-fixture.invalid" {
							return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"access_token":"fixture","token_type":"Bearer","expires_in":3600}`))}, nil
						}
						calls++
						if release {
							controller.Release()
						}
						body := "data: " + `{"candidates":[{"content":{"parts":[{"text":"answer"}]}}]}` + "\n\n"
						if outcome == "stop" {
							body += "data: " + `{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":1,"totalTokenCount":3}}` + "\n\n"
							body += "data: " + `{"candidates":[{"finishReason":"STOP"}]}` + "\n\n"
						}
						if outcome == "invalid_done" {
							body += "data: [DONE]\n\n"
						}
						return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
					}))
					exec, auth := newGoogleMultiAgentFixtureExecutor(t, provider, &config.Config{})
					request := core.Request{Model: "gemini-2.5-flash", Payload: []byte(`{"input":[{"role":"user","content":"test"}]}`)}
					opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}
					if release {
						opts.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: controller}
					}
					result, err := exec.ExecuteStream(ctx, auth, request, opts)
					if err != nil {
						t.Fatal(err)
					}
					completions, failures := 0, 0
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							failures++
						}
						for _, line := range bytes.Split(chunk.Payload, []byte("\n")) {
							if gjson.GetBytes(helps.JSONPayload(line), "type").String() == "response.completed" {
								completions++
							}
						}
					}
					if calls != 1 {
						t.Fatalf("unexpected upstream calls: %d", calls)
					}
					if outcome == "stop" {
						if completions != 1 || failures != 0 {
							t.Fatalf("normal terminal counts=%d errors=%d", completions, failures)
						}
					} else if completions != 0 || failures != 1 {
						t.Fatalf("invalid/truncated upstream appeared successful: completions=%d errors=%d", completions, failures)
					}
				})
			}
		}
	}
}
