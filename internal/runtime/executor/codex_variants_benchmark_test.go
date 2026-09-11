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

func codexVariantBenchmarkPayload(format translator.Format, variant string) ([]byte, error) {
	const size = 10 << 20
	content := strings.Repeat("x", size)
	if variant == "escaped_text" {
		fragment := "中文 \"quoted\" \\path\n"
		content = strings.Repeat(fragment, size/len(fragment))
	}
	native := format == translator.FormatOpenAIResponse
	field := "messages"
	if native {
		field = "input"
	}
	message := map[string]any{"role": "user", "content": content}
	if native {
		message["type"] = "message"
		message["id"] = "msg_fixture"
	}
	payload := map[string]any{"model": "gpt-5.4-mini", field: []any{message}}
	if variant == "explicit_reasoning" {
		payload["reasoning"] = map[string]any{"summary": "auto"}
		if native {
			payload["reasoning"].(map[string]any)["effort"] = "high"
		} else {
			payload["reasoning_effort"] = "high"
		}
	}
	if variant == "extra_body" {
		message["content"] = "short input"
		payload["extra_body"] = map[string]any{"unrelated_blob": content}
	}
	if variant == "image_input" {
		url := "data:image/png;base64," + strings.Repeat("A", size)
		if native {
			message["content"] = []any{map[string]any{"type": "input_image", "image_url": url}}
		} else {
			message["content"] = []any{map[string]any{"type": "image_url", "image_url": map[string]any{"url": url}}}
		}
	}
	if variant == "tools" || variant == "tool_result" || variant == "large_schema" {
		parameters := map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}}
		if variant == "large_schema" {
			message["content"] = "short input"
			parameters["description"] = content
		}
		function := map[string]any{"name": "read", "parameters": parameters}
		if native {
			function["type"] = "function"
			payload["tools"] = []any{function}
		} else {
			payload["tools"] = []any{map[string]any{"type": "function", "function": function}}
		}
		if variant == "tool_result" {
			if native {
				payload[field] = []any{
					map[string]any{"type": "function_call", "id": "fc_read", "call_id": "call_read", "name": "read", "arguments": "{}"},
					map[string]any{"type": "function_call_output", "call_id": "call_read", "output": content},
				}
			} else {
				payload[field] = []any{
					map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"id": "call_read", "type": "function", "function": map[string]any{"name": "read", "arguments": "{}"}}}},
					map[string]any{"role": "tool", "tool_call_id": "call_read", "content": content},
				}
			}
		}
	}
	return json.Marshal(payload)
}

// BenchmarkCodexRequestVariants extends the unchanged baseline benchmark with
// protocol controls, escaped text, inline images, tools and large metadata.
func BenchmarkCodexRequestVariants(b *testing.B) {
	previousStatistics := localusage.StatisticsEnabled()
	localusage.SetStatisticsEnabled(false)
	b.Cleanup(func() { localusage.SetStatisticsEnabled(previousStatistics) })
	for _, format := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI} {
		for _, variant := range []string{"plain_with_id", "explicit_reasoning", "tools", "escaped_text", "image_input", "tool_result", "large_schema", "extra_body"} {
			b.Run(fmt.Sprintf("%s/%s", format, variant), func(b *testing.B) {
				payload, err := codexVariantBenchmarkPayload(format, variant)
				if err != nil {
					b.Fatal(err)
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"fixture\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"fixture\",\"output_index\":0,\"content_index\":0}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"fixture\"}]}],\"usage\":{\"input_tokens\":2,\"output_tokens\":1,\"total_tokens\":3}}}\n\n")
				}))
				defer server.Close()
				auth := &coreauth.Auth{ID: "variant-fixture", Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				executor := NewCodexExecutor(&config.Config{})
				req := core.Request{Model: "gpt-5.4-mini", Payload: payload}
				opts := core.Options{SourceFormat: format, Stream: true}
				var firstContent time.Duration
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
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
