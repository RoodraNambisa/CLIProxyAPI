package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestOpenAICompatResponsesDoneRequiresUsableToolTerminal(t *testing.T) {
	for _, format := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI} {
		for _, complete := range []bool{false, true} {
			for _, marker := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/complete=%t/marker=%t", format, complete, marker), func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if _, err := io.Copy(io.Discard, r.Body); err != nil {
							t.Error(err)
							return
						}
						args := "{"
						if complete {
							args = `{"x":1}`
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: "+`{"id":"done","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call","function":{"name":"tool","arguments":%q}}]}}]}`+"\n\ndata: [DONE]\n\n", args)
						if !complete && format == translator.FormatOpenAIResponse {
							w.(http.Flusher).Flush()
							<-r.Context().Done()
						}
					}))
					defer server.Close()
					// Ensure a failed assertion cancels the deliberately open local peer before Close.
					defer cancel()
					executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
					opts := core.Options{SourceFormat: format, Metadata: map[string]any{core.StreamTerminalMarkerMetadataKey: marker}}
					response, err := executor.ExecuteStream(ctx, &coreauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}, core.Request{Model: "model", Payload: []byte(`{"model":"model","input":"question","messages":[{"role":"user","content":"question"}]}`)}, opts)
					if err != nil {
						t.Fatal(err)
					}
					type outcome struct {
						failures, markers int
						payload           string
						err               error
					}
					finished := make(chan outcome, 1)
					go func() {
						var got outcome
						var payload strings.Builder
						for chunk := range response.Chunks {
							if core.IsSuccessfulStreamTerminalChunk(chunk) {
								got.markers++
								continue
							}
							if chunk.Err != nil {
								got.failures++
								got.err = chunk.Err
							}
							payload.Write(chunk.Payload)
						}
						got.payload = payload.String()
						finished <- got
					}()
					select {
					case got := <-finished:
						if format == translator.FormatOpenAIResponse && !complete {
							if got.failures != 1 || got.markers != 0 || got.err == nil || !strings.Contains(got.err.Error(), "without a successful terminal event") || strings.Contains(got.payload, `"type":"response.completed"`) || strings.Contains(got.payload, `"type":"response.function_call_arguments.done"`) {
								t.Fatal("partial tool was promoted to a successful stream")
							}
						} else {
							wantMarkers := 0
							if marker {
								wantMarkers = 1
							}
							if got.failures != 0 || got.markers != wantMarkers {
								t.Fatal("valid completion or legacy Chat streaming changed")
							}
							if format == translator.FormatOpenAIResponse && !strings.Contains(got.payload, `"type":"response.completed"`) {
								t.Fatal("complete tool did not emit a Responses terminal")
							}
						}
					case <-time.After(time.Second):
						t.Fatal("invalid DONE waited for peer closure")
					}
				})
			}
		}
	}
}
