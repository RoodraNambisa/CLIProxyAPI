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
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestOpenAICompatResponsesNonStreamIncomplete(t *testing.T) {
	for _, reason := range []string{"length", "max_tokens", "content_filter"} {
		for _, custom := range []bool{false, true} {
			for _, release := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/custom=%t/release=%t", reason, custom, release), func(t *testing.T) {
					controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if _, err := io.Copy(io.Discard, r.Body); err != nil {
							t.Error(err)
							return
						}
						if release {
							controller.Release()
						}
						w.Header().Set("Content-Type", "application/json")
						_, _ = fmt.Fprintf(w, `{"id":"limited","choices":[{"message":{"tool_calls":[{"id":"paired","function":{"name":"tool","arguments":""}}]},"finish_reason":%q}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`, reason)
					}))
					t.Cleanup(server.Close)
					executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
					kind, field := "function", "arguments"
					if custom {
						kind, field = "custom", "input"
					}
					req := core.Request{Model: "model", Payload: []byte(`{"model":"model","input":"question","tools":[{"type":"` + kind + `","name":"tool"}]}`)}
					opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
					if release {
						opts.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: controller}
					}
					response, err := executor.Execute(t.Context(), &coreauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					result := gjson.ParseBytes(response.Payload)
					wantReason := "max_output_tokens"
					if reason == "content_filter" {
						wantReason = reason
					}
					if result.Get("status").String() != "incomplete" || result.Get("incomplete_details.reason").String() != wantReason || result.Get("output.0.status").String() != "incomplete" || result.Get("output.0."+field).String() != "" || result.Get("usage.total_tokens").Int() != 5 || controller.Released() != release {
						t.Fatal("actual non-stream response lost partial status, usage, or request metadata")
					}
				})
			}
		}
	}
}
