package executor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestGoogleModelCompatibilityPreservesThinkingAcrossOperations(t *testing.T) {
	for _, provider := range []string{"gemini", "gemini-interactions", "vertex"} {
		for _, operation := range []string{"execute", "stream", "count"} {
			for _, compat := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/compat=%t", provider, operation, compat), func(t *testing.T) {
					type fields struct {
						thoughts              int
						signature, visibility string
						userThinking          bool
					}
					captured := make(chan fields, 2)
					var calls atomic.Int32
					interactions := provider == "gemini-interactions" && operation != "count"
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
							return
						}
						got := fields{userThinking: bytes.Contains(body, []byte("untrusted"))}
						if interactions {
							got.visibility = gjson.GetBytes(body, "generation_config.thinking_summaries").String()
							for _, step := range gjson.GetBytes(body, "input").Array() {
								if step.Get("type").String() == "thought" {
									got.thoughts++
									if step.Get("content.0.text").String() != "" {
										t.Error("empty thought acquired unrelated text")
									}
								}
							}
						} else {
							got.visibility = gjson.GetBytes(body, "generationConfig.thinkingConfig.includeThoughts").Raw
							for _, content := range gjson.GetBytes(body, "contents").Array() {
								for _, part := range content.Get("parts").Array() {
									if part.Get("thought").Bool() {
										got.thoughts++
										got.signature = part.Get("thoughtSignature").String()
										if content.Get("role").String() != "model" || part.Get("text").String() != "" {
											t.Error("thought lost its role or text")
										}
									}
								}
							}
						}
						captured <- got
						result := `{"candidates":[{"content":{"role":"model","parts":[{"text":"done"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}`
						if interactions {
							result = `{"id":"interaction_fixture","status":"completed","outputs":[{"type":"text","text":"done"}],"usage":{"total_input_tokens":1,"total_output_tokens":1,"total_tokens":2}}`
						}
						if operation == "count" {
							if !strings.HasSuffix(r.URL.Path, ":countTokens") {
								t.Error("count used an unexpected route")
							}
							result = `{"totalTokens":3}`
						}
						if operation == "stream" {
							w.Header().Set("Content-Type", "text/event-stream")
							if interactions {
								_, _ = fmt.Fprintf(w, "event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":%s}\n\n", result)
							} else {
								_, _ = fmt.Fprintf(w, "data: %s\n\n", result)
							}
						} else {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, result)
						}
					}))
					t.Cleanup(server.Close)
					manager := googleCapabilityManager(t, provider, server.URL, []string{"high"}, compat)
					raw := []byte(`{"thinking":{"type":"enabled","display":"omitted","budget_tokens":1024},"messages":[{"role":"user","content":[{"type":"thinking","thinking":"untrusted","signature":"opaque-user"},{"type":"text","text":"question"}]},{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":"opaque-fixture"},{"type":"text","text":"answer"}]}]}`)
					before := bytes.Clone(raw)
					req := core.Request{Model: "bound-google", Payload: raw}
					opts := core.Options{SourceFormat: translator.FormatClaude, OriginalRequest: raw}
					switch operation {
					case "count":
						if _, err := manager.ExecuteCount(t.Context(), []string{provider}, req, opts); err != nil {
							t.Fatal(err)
						}
					case "stream":
						result, err := manager.ExecuteStream(t.Context(), []string{provider}, req, opts)
						if err != nil {
							t.Fatal(err)
						}
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								t.Fatal(chunk.Err)
							}
						}
					default:
						if _, err := manager.Execute(t.Context(), []string{provider}, req, opts); err != nil {
							t.Fatal(err)
						}
					}
					select {
					case got := <-captured:
						want := 0
						if compat {
							want = 1
						}
						if got.thoughts != want || got.userThinking || (compat && !interactions && got.signature != "opaque-fixture") {
							t.Fatal("selected compatibility lost a thought/signature or promoted user thinking")
						}
						if operation != "count" {
							wantVisibility := "false"
							if interactions {
								wantVisibility = "none"
							}
							if got.visibility != wantVisibility {
								t.Fatal("compatibility changed summary visibility")
							}
						}
					default:
						t.Fatal("request did not reach the local upstream")
					}
					if calls.Load() != 1 || !bytes.Equal(raw, before) {
						t.Fatal("compatibility changed the attempt count or original request")
					}
				})
			}
		}
	}
}
