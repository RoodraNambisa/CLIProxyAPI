package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexPartialResponseAcrossTransports(t *testing.T) {
	for _, mode := range []string{"http", "sse", "trusted", "trusted-multiline", "image", "ws", "ws-stream"} {
		for index, kind := range []string{"response.incomplete", "response.completed", "response.done"} {
			for _, release := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/release=%t", mode, kind, release), func(t *testing.T) {
					reason := []string{"max_tokens", "max_output_tokens", "content_filter"}[index]
					terminal := []byte(fmt.Sprintf(`{"type":%q,"response":{"id":"partial","status":"incomplete","error":null,"incomplete_details":{"reason":%q},"output":[{"type":"message","content":[{"type":"output_text","text":"available"}]}],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}`, kind, reason))
					websocketMode := strings.HasPrefix(mode, "ws")
					stream := mode != "http" && mode != "ws"
					controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if websocketMode {
							upgrader := websocket.Upgrader{}
							conn, err := upgrader.Upgrade(w, r, nil)
							if err != nil {
								t.Error(err)
								return
							}
							defer func() { _ = conn.Close() }()
							if _, _, err := conn.ReadMessage(); err != nil {
								return
							}
							_ = conn.WriteMessage(websocket.TextMessage, terminal)
							_, _, _ = conn.ReadMessage()
							return
						}
						_, _ = io.Copy(io.Discard, r.Body)
						w.Header().Set("Content-Type", "text/event-stream")
						data := string(terminal)
						if mode == "trusted-multiline" {
							data = strings.Replace(data, `,"response":`, ",\ndata: \"response\":", 1)
						}
						_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, data)
						if stream {
							w.(http.Flusher).Flush()
							<-r.Context().Done()
						} else {
							_, _ = io.WriteString(w, "data: "+`{"type":"response.completed","response":{"status":"completed","output":[]}}`+"\n\n")
						}
					}))
					defer server.Close()
					var executor coreauth.ProviderExecutor = NewCodexExecutor(&config.Config{})
					if websocketMode {
						executor = NewCodexWebsocketsExecutor(&config.Config{})
					}
					ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
					defer cancel()
					auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
					req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","input":"question","tools":[{"type":"image_generation"}]}`)}
					opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Stream: stream, Metadata: map[string]any{core.StreamTerminalMarkerMetadataKey: true, core.TrustUpstreamSSEMetadataKey: strings.HasPrefix(mode, "trusted"), core.ImageGenerationStreamPassthroughMetadataKey: mode == "image"}}
					if release {
						opts.Metadata[core.BodyReleaseControllerMetadataKey] = controller
					}
					var output []byte
					var result gjson.Result
					if stream {
						response, err := executor.ExecuteStream(ctx, auth, req, opts)
						if err != nil {
							t.Fatal(err)
						}
						markers := 0
						for chunk := range response.Chunks {
							if chunk.Err != nil {
								t.Fatal(chunk.Err)
							}
							if core.IsSuccessfulStreamTerminalChunk(chunk) {
								markers++
							} else {
								output = append(output, chunk.Payload...)
								output = append(output, '\n')
							}
						}
						if markers != 1 {
							t.Fatalf("markers=%d, want one clean stream end", markers)
						}
						data, _ := codexSSEFrameDataPayload(output)
						result = gjson.GetBytes(data, "response")
						if !strings.HasPrefix(mode, "trusted") && bytes.Contains(output, []byte("event:")) && !bytes.Contains(output, []byte("event: response.incomplete")) {
							t.Fatal("event name disagrees with the normalized terminal data")
						}
					} else {
						response, err := executor.Execute(ctx, auth, req, opts)
						if err != nil {
							t.Fatal(err)
						}
						result = gjson.ParseBytes(response.Payload)
					}
					if result.Get("status").String() != "incomplete" || result.Get("incomplete_details.reason").String() != reason || result.Get("output.0.content.0.text").String() != "available" || result.Get("usage.total_tokens").Int() != 5 || controller.Released() != release {
						t.Fatal("partial result, reason, usage or release state was lost")
					}
					if ctx.Err() != nil {
						t.Fatal("partial response waited for upstream close")
					}
					if !helps.IsCodexPartialResponse(terminal) || isCodexSuccessfulCompletion(terminal) {
						t.Fatal("partial result confused with cache-eligible completion")
					}
				})
			}
		}
	}
}
