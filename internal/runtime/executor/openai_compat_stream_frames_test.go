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

func TestOpenAICompatMultilineFramesPreserveToolsAndUsage(t *testing.T) {
	for _, ending := range []string{"\n", "\r\n", "\r"} {
		for _, format := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI} {
			for _, release := range []bool{false, true} {
				t.Run(fmt.Sprintf("%q/%s/release=%t", ending, format, release), func(t *testing.T) {
					controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						_, _ = io.Copy(io.Discard, r.Body)
						if release {
							controller.Release()
						}
						w.Header().Set("Content-Type", "text/event-stream")
						wire := strings.Join([]string{
							": keep-alive", "", "data:", "", "event: message", "id: upstream",
							`data: {"id":"multiline","object":"chat.completion.chunk","choices":[`,
							`data: {"index":0,"delta":{"tool_calls":[{"index":0,"id":"paired","function":{"name":"run","arguments":"{\"input\":\"command\"}"}}]},"finish_reason":"tool_calls"}]}`,
							"", `data: {"choices":[],"usage":`, `data: {"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`, "", "data: [DONE]", "", "",
							`data: {"choices":[{"index":0,"delta":{"content":"late"}}]}`, "", `data: {"choices":[],"usage":{"total_tokens":999}}`, "", "",
						}, ending)
						for offset := 0; offset < len(wire); offset += 7 {
							end := min(offset+7, len(wire))
							_, _ = io.WriteString(w, wire[offset:end])
							w.(http.Flusher).Flush()
						}
					}))
					t.Cleanup(server.Close)
					executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
					payload := []byte(`{"model":"model","input":"question","tools":[{"type":"custom","name":"run"}]}`)
					if format == translator.FormatOpenAI {
						payload = []byte(`{"model":"model","messages":[{"role":"user","content":"question"}],"tools":[{"type":"function","function":{"name":"run","parameters":{}}}]}`)
					}
					opts := core.Options{SourceFormat: format, Stream: true, Metadata: map[string]any{core.StreamTerminalMarkerMetadataKey: true}}
					if release {
						opts.Metadata[core.BodyReleaseControllerMetadataKey] = controller
					}
					response, err := executor.ExecuteStream(t.Context(), &coreauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}, core.Request{Model: "model", Payload: payload}, opts)
					if err != nil {
						t.Fatal(err)
					}
					var final gjson.Result
					markers, nativeData := 0, 0
					for chunk := range response.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						if core.IsSuccessfulStreamTerminalChunk(chunk) {
							markers++
							continue
						}
						if format == translator.FormatOpenAI {
							if !gjson.ValidBytes(chunk.Payload) || strings.ContainsAny(string(chunk.Payload), "\r\n") {
								t.Fatal("native Chat chunk was not normalized to one JSON payload")
							}
							nativeData++
							if event := gjson.ParseBytes(chunk.Payload); event.Get("usage.total_tokens").Int() == 5 {
								final = event
							}
							continue
						}
						for _, line := range strings.Split(string(chunk.Payload), "\n") {
							if !strings.HasPrefix(line, "data:") {
								continue
							}
							raw := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
							if raw == "[DONE]" {
								continue
							}
							if !gjson.Valid(raw) {
								t.Fatal("joined JSON was forwarded as an invalid native data line")
							}
							event := gjson.Parse(raw)
							if event.Get("type").String() == "response.completed" {
								final = event.Get("response")
							}
							if event.Get("usage.total_tokens").Int() == 5 {
								final = event
							}
						}
					}
					if format == translator.FormatOpenAIResponse && (final.Get("output.0.type").String() != "custom_tool_call" || final.Get("output.0.input").String() != "command" || final.Get("output.0.call_id").String() != "paired") {
						t.Fatal("multiline conversion lost custom tool identity or arguments")
					}
					if markers != 1 || final.Get("usage.total_tokens").Int() != 5 || controller.Released() != release || (format == translator.FormatOpenAI && nativeData != 2) {
						t.Fatal("multiline frames lost usage, terminal count, or request release behavior")
					}
				})
			}
		}
	}
}
