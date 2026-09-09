package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestOpenAICompatResponsesMessageSegments(t *testing.T) {
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
					for _, choice := range []string{
						`{"index":0,"delta":{"content":"before"}}`,
						`{"index":0,"delta":{"tool_calls":[{"index":0,"id":"paired","function":{"name":"run","arguments":"{}"}}]}}`,
						`{"index":0,"delta":{"content":"after"},"finish_reason":"stop"}`,
					} {
						_, _ = io.WriteString(w, `data: {"id":"segments","choices":[`+choice+`],"object":"chat.completion.chunk"}`+"\n\n")
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
				done := map[string]gjson.Result{}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
					for _, line := range strings.Split(string(chunk.Payload), "\n") {
						if !strings.HasPrefix(line, "data:") {
							continue
						}
						event := gjson.Parse(strings.TrimPrefix(line, "data:"))
						switch event.Get("type").String() {
						case "response.output_item.done":
							done[event.Get("item.id").String()] = event.Get("item")
						case "response.output_text.delta":
							if done[event.Get("item_id").String()].Exists() {
								t.Fatal("actual stream appended text to a completed message")
							}
						case "response.completed":
							final = event.Get("response")
						}
					}
				}
				items := final.Get("output").Array()
				if len(items) != 3 || items[0].Get("content.0.text").String() != "before" || items[1].Get("call_id").String() != "paired" || items[2].Get("content.0.text").String() != "after" || controller.Released() != release {
					t.Fatal("actual response lost message segments, tool pairing, or release behavior")
				}
				for _, item := range items {
					if !reflect.DeepEqual(item.Value(), done[item.Get("id").String()].Value()) {
						t.Fatal("actual completed response changed an earlier item")
					}
				}
			})
		}
	}
}
