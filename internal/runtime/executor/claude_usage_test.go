package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestClaudeUsageMatchesActualResponseAcrossTransports(t *testing.T) {
	for _, format := range []translator.Format{translator.FormatClaude, translator.FormatOpenAIResponse, translator.FormatOpenAI} {
		for _, stream := range []bool{false, true} {
			for _, correction := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/correction=%t", format, stream, correction), func(t *testing.T) {
					collector := &alphaUsageCollector{authID: t.Name()}
					usage.RegisterPlugin(collector)
					t.Cleanup(func() { collector.mu.Lock(); collector.closed = true; collector.records = nil; collector.mu.Unlock() })
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
							return
						}
						if !gjson.GetBytes(body, "stream").Bool() {
							w.Header().Set("Content-Type", "application/json")
							stats := `{"input_tokens":2,"output_tokens":3,"cache_read_input_tokens":4,"cache_creation_input_tokens":5,"thinking_tokens":2}`
							if correction {
								stats = `{"input_tokens":1,"output_tokens":3,"cache_read_input_tokens":0,"cache_creation_input_tokens":5,"thinking_tokens":0}`
							}
							_, _ = io.WriteString(w, `{"id":"usage","type":"message","role":"assistant","content":[],"stop_reason":"end_turn","usage":`+stats+`}`)
							return
						}
						w.Header().Set("Content-Type", "text/event-stream")
						events := []string{`{"type":"message_start","message":{"id":"usage","role":"assistant","usage":{"input_tokens":2,"output_tokens":0,"cache_read_input_tokens":4,"cache_creation_input_tokens":5}}}`,
							`{"type":"message_delta","usage":{"output_tokens":3,"output_tokens_details":{"thinking_tokens":2}}}`}
						if correction {
							events = append(events, `{"type":"message_delta","usage":{"input_tokens":1,"cache_read_input_tokens":0,"output_tokens_details":{"thinking_tokens":0}}}`)
						}
						events = append(events, `{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`, `{"type":"message_stop"}`,
							`{"type":"message_delta","usage":{"input_tokens":999,"output_tokens":999}}`, `{"type":"message_stop"}`)
						_, _ = io.WriteString(w, "data: "+strings.Join(events, "\n\ndata: ")+"\n\n")
					}))
					t.Cleanup(server.Close)
					manager := claudeCompatibilityManager(t, server.URL, false)
					raw := []byte(`{"model":"bound-claude","messages":[{"role":"user","content":"question"}]}`)
					if format == translator.FormatOpenAIResponse {
						raw = []byte(`{"model":"bound-claude","input":"question"}`)
					}
					raw, _ = sjson.SetBytes(raw, "stream", stream)
					req := core.Request{Model: "bound-claude", Payload: raw}
					opts := core.Options{SourceFormat: format, OriginalRequest: raw}
					if stream {
						result, err := manager.ExecuteStream(t.Context(), []string{"claude"}, req, opts)
						if err != nil {
							t.Fatal(err)
						}
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								t.Fatal(chunk.Err)
							}
						}
					} else if _, err := manager.Execute(t.Context(), []string{"claude"}, req, opts); err != nil {
						t.Fatal(err)
					}
					if err := usage.DefaultManager().Barrier(t.Context()); err != nil {
						t.Fatal(err)
					}
					collector.mu.Lock()
					records := append([]usage.Record(nil), collector.records...)
					collector.mu.Unlock()
					want := usage.Detail{InputTokens: 11, OutputTokens: 3, CachedTokens: 4, CacheCreationTokens: 5, ReasoningTokens: 2, TotalTokens: 14}
					if correction {
						want.InputTokens, want.CachedTokens, want.ReasoningTokens, want.TotalTokens = 6, 0, 0, 9
					}
					if len(records) != 1 || records[0].Failed || records[0].Detail != want || records[0].Stream != stream {
						t.Fatal("usage sink lost start counts, later output, or explicit zero corrections")
					}
				})
			}
		}
	}
}
