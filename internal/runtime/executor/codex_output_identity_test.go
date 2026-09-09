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
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexHTTPHydratesOnlyMissingOutputIDs(t *testing.T) {
	for _, mode := range []string{"http", "sse", "image", "trusted"} {
		for _, release := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/release=%t", mode, release), func(t *testing.T) {
				controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					if release {
						controller.Release()
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: "+`{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"msg_first","content":[{"type":"output_text","text":"answer"}]}}`+"\n\n")
					_, _ = io.WriteString(w, "data: "+`{"type":"response.output_item.done","output_index":1,"item":{"type":"message","id":"msg_other","content":[]}}`+"\n\n")
					_, _ = io.WriteString(w, "data: "+`{"type":"response.completed","response":{"id":"output_ids","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"answer"}]},{"type":"message","id":"msg_saved","content":[]}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}`+"\n\n")
				}))
				t.Cleanup(server.Close)
				executor := NewCodexExecutor(&config.Config{})
				auth := &coreauth.Auth{ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","input":"question","tools":[{"type":"image_generation"}]}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Stream: mode != "http", Metadata: map[string]any{
					core.TrustUpstreamSSEMetadataKey: mode == "trusted", core.ImageGenerationStreamPassthroughMetadataKey: mode == "image",
				}}
				if release {
					opts.Metadata[core.BodyReleaseControllerMetadataKey] = controller
				}
				var final gjson.Result
				if mode == "http" {
					response, err := executor.Execute(t.Context(), auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					final = gjson.ParseBytes(response.Payload)
				} else {
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
									final = event.Get("response")
								}
							}
						}
					}
				}
				want := "msg_first"
				if mode == "trusted" {
					want = ""
				}
				if final.Get("output.0.id").String() != want || final.Get("output.1.id").String() != "msg_saved" || final.Get("output.0.content.0.text").String() != "answer" || controller.Released() != release {
					t.Fatal("output ID hydration, existing ID, trusted bypass, or release behavior differs")
				}
			})
		}
	}
}
