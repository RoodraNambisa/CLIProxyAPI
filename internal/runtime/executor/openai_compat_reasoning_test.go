package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestOpenAICompatResponsesReasoningFallbackAndEmptyTools(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, release := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/release=%t", stream, release), func(t *testing.T) {
				controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						t.Error(err)
						return
					}
					if release {
						controller.Release()
					}
					if !stream {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"fallback","choices":[{"message":{"reasoning":"first second","content":"answer","tool_calls":[]},"finish_reason":"stop"}]}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					for _, delta := range []string{`{"reasoning":"first","tool_calls":[]}`, `{"reasoning":" second","tool_calls":[]}`, `{"content":"answer","tool_calls":[]}`, `{}`} {
						finish := "null"
						if delta == `{}` {
							finish = `"stop"`
						}
						_, _ = io.WriteString(w, `data: {"id":"fallback","object":"chat.completion.chunk","choices":[{"index":0,"delta":`+delta+`,"finish_reason":`+finish+`}]}`+"\n\n")
					}
					_, _ = io.WriteString(w, "data: [DONE]\n\n")
				}))
				t.Cleanup(server.Close)
				executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
				auth := &coreauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				req := core.Request{Model: "test-model", Payload: []byte(`{"model":"test-model","input":"question"}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
				if release {
					opts.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: controller}
				}
				var result gjson.Result
				if stream {
					response, err := executor.ExecuteStream(t.Context(), auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range response.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						for _, line := range strings.Split(string(chunk.Payload), "\n") {
							if strings.HasPrefix(line, "data:") {
								event := gjson.Parse(strings.TrimPrefix(line, "data:"))
								if event.Get("type").String() == "response.completed" {
									result = event.Get("response")
								}
							}
						}
					}
				} else {
					response, err := executor.Execute(t.Context(), auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					result = gjson.ParseBytes(response.Payload)
				}
				if result.Get("output.#").Int() != 2 || result.Get("output.0.summary.0.text").String() != "first second" || result.Get("output.1.content.0.text").String() != "answer" || controller.Released() != release {
					t.Fatal("actual response lost reasoning continuity or release behavior")
				}
			})
		}
	}
}
