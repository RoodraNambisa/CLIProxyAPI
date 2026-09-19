package executor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexIdentityMultilineSSEAcrossNetworkChunks(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, trusted := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/trusted=%t", stream, trusted), func(t *testing.T) {
				window := uuid.Must(uuid.NewV7()).String()
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, _ := io.ReadAll(r.Body)
					echo := gjson.GetBytes(body, "client_metadata.context_window_id").Str
					response, _ := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]any{"id": "a", "status": "completed", "context_window_id": echo, "output": []any{map[string]any{"type": "message", "id": "a", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "a id"}}}}}})
					frame := append([]byte("event: response.completed\r\ndata: "), bytes.Replace(response, []byte(`,"type":"response.completed"`), []byte(",\r\ndata: \"type\":\"response.completed\""), 1)...)
					frame = append(frame, []byte("\r\n\r\n")...)
					w.Header().Set("Content-Type", "text/event-stream")
					for start := 0; start < len(frame); start += 7 {
						end := min(start+7, len(frame))
						_, _ = w.Write(frame[start:end])
						w.(http.Flusher).Flush()
					}
				}))
				defer server.Close()
				exec := NewCodexExecutor(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true}, Codex: config.CodexConfig{IdentityConfuse: true}})
				auth := &coreauth.Auth{ID: "sse-identity", Provider: "codex", ProxyURL: "direct", Metadata: map[string]any{"access_token": "test-token", "codex_fingerprint_mode": "off"}, Attributes: map[string]string{"base_url": server.URL}}
				payload, _ := json.Marshal(map[string]any{"model": "gpt-5.4", "input": "hello", "prompt_cache_key": "a", "client_metadata": map[string]string{"context_window_id": window}})
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Stream: stream, Metadata: map[string]any{core.TrustUpstreamSSEMetadataKey: trusted}}
				req := core.Request{Model: "gpt-5.4", Payload: payload}
				var result []byte
				if stream {
					response, err := exec.ExecuteStream(t.Context(), auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range response.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						result = append(result, chunk.Payload...)
					}
				} else {
					response, err := exec.Execute(t.Context(), auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					result = response.Payload
				}
				if !bytes.Contains(result, []byte(window)) || !bytes.Contains(result, []byte(`"text":"a id"`)) || !bytes.Contains(result, []byte(`"id":"a"`)) {
					t.Fatalf("changed business content or failed context restoration: %s", result)
				}
			})
		}
	}
}
