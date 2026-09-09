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

func TestOpenAICompatResponsesUnindexedTools(t *testing.T) {
	for _, kind := range []string{"function", "custom"} {
		for _, release := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/release=%t", kind, release), func(t *testing.T) {
				controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					if release {
						controller.Release()
					}
					w.Header().Set("Content-Type", "text/event-stream")
					frames := []string{
						`[{"index":7,"id":"a","function":{"name":"run","arguments":"{\"input\":\""}},{"index":3,"id":"b","function":{"name":"run","arguments":"{\"input\":\""}}]`,
						`[{"function":{"arguments":"BAD"}}]`,
						`[{"id":"b","function":{"arguments":"second\"}"}},{"id":"a","function":{"arguments":"first\"}"}}]`,
					}
					for i, tools := range frames {
						finish := "null"
						if i == len(frames)-1 {
							finish = `"tool_calls"`
						}
						_, _ = io.WriteString(w, `data: {"id":"identity","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":`+tools+`},"finish_reason":`+finish+`}]}`+"\n\n")
					}
					_, _ = io.WriteString(w, "data: [DONE]\n\n")
				}))
				t.Cleanup(server.Close)
				executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
				request := core.Request{Model: "model", Payload: []byte(`{"model":"model","input":"question","tools":[{"type":"` + kind + `","name":"run"}]}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
				if release {
					opts.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: controller}
				}
				result, err := executor.ExecuteStream(t.Context(), &coreauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}, request, opts)
				if err != nil {
					t.Fatal(err)
				}
				var final gjson.Result
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
					if strings.Contains(string(chunk.Payload), "BAD") {
						t.Fatal("ambiguous delta escaped through an actual response")
					}
					for _, line := range strings.Split(string(chunk.Payload), "\n") {
						if strings.HasPrefix(line, "data:") {
							event := gjson.Parse(strings.TrimPrefix(line, "data:"))
							if event.Get("type").String() == "response.completed" {
								final = event.Get("response")
							}
						}
					}
				}
				field, first, second := "arguments", `{"input":"first"}`, `{"input":"second"}`
				if kind == "custom" {
					field, first, second = "input", "first", "second"
				}
				if final.Get("output.#").Int() != 2 || final.Get("output.0.call_id").String() != "a" || final.Get("output.1.call_id").String() != "b" || final.Get("output.0."+field).String() != first || final.Get("output.1."+field).String() != second || controller.Released() != release {
					t.Fatal("actual tool response lost stable pairing or released request metadata")
				}
			})
		}
	}
}
