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
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexHTTPUsageMeasuresContentSeparatelyFromMetadata(t *testing.T) {
	for _, mode := range []string{"http", "sse", "trusted", "trusted-multiline", "image-passthrough", "compact"} {
		for _, result := range []string{"content", "empty", "failure", "partial-failure"} {
			for _, release := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/release=%t", mode, result, release), func(t *testing.T) {
					failure := strings.HasSuffix(result, "failure")
					collector := &alphaUsageCollector{authID: t.Name()}
					usage.RegisterPlugin(collector)
					t.Cleanup(func() { collector.mu.Lock(); collector.closed = true; collector.records = nil; collector.mu.Unlock() })
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						_, _ = io.Copy(io.Discard, r.Body)
						if mode == "compact" {
							w.Header().Set("Content-Type", "application/json")
							if failure {
								w.WriteHeader(http.StatusBadRequest)
								_, _ = io.WriteString(w, `{"error":{"code":"invalid_request_error","message":"fixture"}}`)
							} else {
								_, _ = io.WriteString(w, `{"object":"response.compaction","output":[{"type":"compaction","encrypted_content":"fixture"}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}`)
							}
							return
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, ": ping\n\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"fixture\"}}\n\n")
						w.(http.Flusher).Flush()
						if result == "content" {
							data := `{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"fixture"}`
							if mode == "trusted-multiline" {
								data = strings.Replace(data, `,"delta":`, ",\ndata: \"delta\":", 1)
							}
							_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
						}
						if result == "partial-failure" {
							_, _ = io.WriteString(w, "data: {\"type\":\"response.failed\",\"response\":{\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"partial\"}]}],\"error\":{\"code\":\"server_error\",\"message\":\"fixture\"}}}\n\n")
						} else if failure {
							_, _ = io.WriteString(w, "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"misalignment_policy_violation\",\"message\":\"fixture\"}}}\n\n")
						} else {
							_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":1,\"total_tokens\":3}}}\n\n")
						}
					}))
					defer server.Close()
					ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
					defer cancel()
					auth := &coreauth.Auth{ID: collector.authID, Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
					executor := NewCodexExecutor(&config.Config{})
					req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","input":"fixture","tools":[{"type":"image_generation"}]}`)}
					opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Metadata: map[string]any{core.TrustUpstreamSSEMetadataKey: strings.HasPrefix(mode, "trusted"), core.ImageGenerationStreamPassthroughMetadataKey: mode == "image-passthrough"}}
					controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
					if release {
						opts.Metadata[core.BodyReleaseControllerMetadataKey] = controller
					}
					var executionErr error
					if mode == "compact" || mode == "http" {
						if mode == "compact" {
							opts.Alt = "responses/compact"
						}
						_, executionErr = executor.Execute(ctx, auth, req, opts)
					} else {
						var response *core.StreamResult
						response, executionErr = executor.ExecuteStream(ctx, auth, req, opts)
						if executionErr == nil {
							for chunk := range response.Chunks {
								if chunk.Err != nil {
									executionErr = chunk.Err
								}
							}
						}
					}
					if (executionErr != nil) != failure {
						t.Fatalf("unexpected execution result: %v", executionErr)
					}
					if err := usage.DefaultManager().Barrier(ctx); err != nil {
						t.Fatal(err)
					}
					collector.mu.Lock()
					records := append([]usage.Record(nil), collector.records...)
					collector.mu.Unlock()
					if len(records) != 1 {
						t.Fatalf("records=%d, want one", len(records))
					}
					record := records[0]
					wantContent := (result == "content" || result == "partial-failure") && mode != "compact"
					if record.FirstPacketLatency <= 0 || (record.TTFT > 0) != wantContent || (wantContent && record.TTFT < record.FirstPacketLatency) || record.Failed != failure {
						t.Fatalf("invalid timing: packet=%v content=%v failed=%t", record.FirstPacketLatency, record.TTFT, record.Failed)
					}
					if mode != "compact" && controller.Released() != release {
						t.Fatal("timing changed request-body release")
					}
				})
			}
		}
	}
}
