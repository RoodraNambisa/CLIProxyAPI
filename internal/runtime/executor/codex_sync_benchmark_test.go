package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	localusage "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

// BenchmarkCodexSyncRequest includes request preparation, a local HTTP upstream,
// and response translation. It excludes fixture construction and history storage.
func BenchmarkCodexSyncRequest(b *testing.B) {
	previousStatistics := localusage.StatisticsEnabled()
	localusage.SetStatisticsEnabled(false)
	b.Cleanup(func() { localusage.SetStatisticsEnabled(previousStatistics) })
	for _, format := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI} {
		for _, fixture := range []struct {
			name  string
			turns int
			bytes int
		}{{"short", 1, 128}, {"history100", 100, 128}, {"history1000", 1000, 128}, {"payload1MiB", 1, 1 << 20}, {"payload10MiB", 1, 10 << 20}} {
			b.Run(fmt.Sprintf("%s/%s", format, fixture.name), func(b *testing.B) {
				messages := make([]map[string]any, 0, 2*fixture.turns-1)
				content := strings.Repeat("x", fixture.bytes)
				for index := 0; index < 2*fixture.turns-1; index++ {
					role := "user"
					if index%2 == 1 {
						role = "assistant"
					}
					messages = append(messages, map[string]any{"role": role, "content": content})
				}
				field := "input"
				if format == translator.FormatOpenAI {
					field = "messages"
				}
				payload, err := json.Marshal(map[string]any{"model": "gpt-5.4-mini", field: messages})
				if err != nil {
					b.Fatal(err)
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"fixture\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"fixture\",\"output_index\":0,\"content_index\":0}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"fixture\"}]}],\"usage\":{\"input_tokens\":2,\"output_tokens\":1,\"total_tokens\":3}}}\n\n")
				}))
				defer server.Close()
				auth := &coreauth.Auth{ID: "benchmark-fixture", Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				executor := NewCodexExecutor(&config.Config{})
				req := core.Request{Model: "gpt-5.4-mini", Payload: payload}
				opts := core.Options{SourceFormat: format, Stream: true}
				var firstContent time.Duration
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					started := time.Now()
					response, errExecute := executor.ExecuteStream(context.Background(), auth, req, opts)
					if errExecute != nil {
						b.Fatal(errExecute)
					}
					found := false
					for chunk := range response.Chunks {
						if chunk.Err != nil {
							b.Fatal(chunk.Err)
						}
						if !found && (bytes.Contains(chunk.Payload, []byte(`"response.output_text.delta"`)) || bytes.Contains(chunk.Payload, []byte(`"content":"fixture"`))) {
							firstContent += time.Since(started)
							found = true
						}
					}
					if !found {
						b.Fatal("fixture did not produce actual client content")
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(firstContent.Nanoseconds())/float64(b.N), "first-content-ns/op")
			})
		}
	}
}
