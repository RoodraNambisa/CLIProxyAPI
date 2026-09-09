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

func TestStreamTerminalOrderStopsAtFirstOutcome(t *testing.T) {
	for _, provider := range []string{"claude", "compat"} {
		for _, native := range []bool{false, true} {
			for _, fail := range []bool{false, true} {
				for _, marker := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/native=%t/fail=%t/marker=%t", provider, native, fail, marker), func(t *testing.T) {
						ctx, cancel := context.WithCancel(t.Context())
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							if _, err := io.Copy(io.Discard, r.Body); err != nil {
								t.Error(err)
								return
							}
							w.Header().Set("Content-Type", "text/event-stream")
							start := `data: {"id":"ordered","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"before"}}]}` + "\n\n"
							end := `data: {"id":"ordered","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"
							if provider == "claude" {
								start = `data: {"type":"message_start","message":{"id":"ordered","role":"assistant"}}` + "\n\n" + `data: {"type":"content_block_start","index":0,"content_block":{"type":"text"}}` + "\n\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"before"}}` + "\n\n"
								end = `data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}` + "\n\n" + `data: {"type":"message_stop"}` + "\n\n"
							}
							_, _ = io.WriteString(w, start)
							failure := `data: {"type":"error","error":{"code":"misalignment_policy_violation","message":"request denied"}}` + "\n\n"
							if fail {
								_, _ = io.WriteString(w, failure)
							}
							_, _ = io.WriteString(w, end)
							if !fail {
								_, _ = io.WriteString(w, failure)
							}
							w.(http.Flusher).Flush()
							<-r.Context().Done()
						}))
						defer server.Close()
						defer cancel()
						cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
						var executor coreauth.ProviderExecutor
						format := translator.FormatOpenAIResponse
						if provider == "claude" {
							executor = NewClaudeExecutor(cfg)
							if native {
								format = translator.FormatClaude
							}
						} else {
							executor = NewOpenAICompatExecutor("compat", cfg)
							if native {
								format = translator.FormatOpenAI
							}
						}
						result, err := executor.ExecuteStream(ctx, &coreauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}, core.Request{Model: "model", Payload: []byte(`{"model":"model","stream":true,"input":"question","messages":[{"role":"user","content":"question"}]}`)}, core.Options{SourceFormat: format, Metadata: map[string]any{core.StreamTerminalMarkerMetadataKey: marker}})
						if err != nil {
							t.Fatal(err)
						}
						type outcome struct {
							body    string
							err     error
							markers int
						}
						finished := make(chan outcome, 1)
						go func() {
							var got outcome
							var body strings.Builder
							for chunk := range result.Chunks {
								if core.IsSuccessfulStreamTerminalChunk(chunk) {
									got.markers++
									continue
								}
								if chunk.Err != nil {
									got.err = chunk.Err
								}
								body.Write(chunk.Payload)
							}
							got.body = body.String()
							finished <- got
						}()
						select {
						case got := <-finished:
							if fail {
								if !coreauth.IsPolicyRefusalError(got.err) || got.markers != 0 || strings.Contains(got.body, `"type":"response.completed"`) || strings.Contains(got.body, `"type":"message_stop"`) || strings.Contains(got.body, "data: [DONE]") {
									t.Fatal("source error was followed by successful output")
								}
							} else {
								want := 0
								if marker {
									want = 1
								}
								if got.err != nil || got.markers != want || strings.Contains(got.body, "request denied") {
									t.Fatal("late error changed a completed request")
								}
								if provider == "claude" && native && !strings.HasSuffix(got.body, "\n\n") {
									t.Fatal("Claude terminal SSE frame was not delimited")
								}
							}
						case <-time.After(time.Second):
							t.Fatal("terminal processing waited for the open peer")
						}
					})
				}
			}
		}
	}
}

func TestStreamTerminalCancellationReleasesBlockedDelivery(t *testing.T) {
	for _, provider := range []string{"claude", "compat"} {
		for _, native := range []bool{false, true} {
			for _, phase := range []string{"payload", "error", "marker"} {
				t.Run(fmt.Sprintf("%s/native=%t/%s", provider, native, phase), func(t *testing.T) {
					wire := "data: {\"id\":\"cancel\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
					if provider == "claude" {
						wire = "data: {\"type\":\"message_start\",\"message\":{\"id\":\"cancel\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n"
					}
					if phase == "error" {
						wire = "data: {\"error\":{\"code\":\"misalignment_policy_violation\",\"message\":\"denied\"}}\n\n"
					}
					peerClosed := make(chan struct{})
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						defer close(peerClosed)
						_, _ = io.Copy(io.Discard, r.Body)
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, wire)
						w.(http.Flusher).Flush()
						<-r.Context().Done()
					}))
					defer server.Close()
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					var executor coreauth.ProviderExecutor
					format := translator.FormatOpenAIResponse
					if provider == "claude" {
						executor = NewClaudeExecutor(nil)
						if native {
							format = translator.FormatClaude
						}
					} else {
						executor = NewOpenAICompatExecutor("compat", nil)
						if native {
							format = translator.FormatOpenAI
						}
					}
					result, err := executor.ExecuteStream(ctx, &coreauth.Auth{ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}, core.Request{Model: "model", Payload: []byte(`{"model":"model","stream":true,"input":"question","messages":[{"role":"user","content":"question"}]}`)}, streamTerminalOptions(format, true))
					if err != nil {
						t.Fatal(err)
					}
					if phase == "marker" {
						for chunk := range result.Chunks {
							if strings.Contains(string(chunk.Payload), `"type":"response.completed"`) || strings.Contains(string(chunk.Payload), `"type":"message_stop"`) || strings.Contains(string(chunk.Payload), "data: [DONE]") {
								break
							}
						}
					}
					cancel()
					select {
					case <-peerClosed:
					case <-time.After(time.Second):
						go func() {
							for range result.Chunks {
							}
						}()
						t.Fatal("cancelled delivery did not release the response body")
					}
					for pending := 0; ; pending++ {
						select {
						case _, ok := <-result.Chunks:
							if !ok {
								return
							}
							if pending > 0 {
								go func() {
									for range result.Chunks {
									}
								}()
								t.Fatal("cancelled producer continued delivering buffered output")
							}
						case <-time.After(time.Second):
							t.Fatal("cancelled producer did not close its channel")
						}
					}
				})
			}
		}
	}
}
