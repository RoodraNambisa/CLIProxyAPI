package executor

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexClaudeAgentReplayIsolatedAtHTTPDispatch(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			var calls atomic.Int32
			signatureBytes := make([]byte, 73)
			signatureBytes[0] = 0x80
			signature := base64.RawURLEncoding.EncodeToString(signatureBytes)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, errRead := io.ReadAll(r.Body)
				if errRead != nil {
					t.Error(errRead)
				}
				call := calls.Add(1)
				if replayed := gjson.GetBytes(body, `input.#(type=="reasoning")`).Exists(); replayed != (call == 3) {
					t.Errorf("upstream call %d has replay=%t", call, replayed)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"status\":\"completed\",\"output\":[{\"type\":\"reasoning\",\"encrypted_content\":%q},{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n", signature)
			}))
			defer upstream.Close()
			executor := NewCodexExecutor(&config.Config{})
			credential := &auth.Auth{ID: t.Name() + ":" + uuid.NewString(), Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}}
			for _, agent := range []string{"worker-a", "worker-b", "worker-a"} {
				headers := http.Header{"X-Claude-Code-Session-Id": {t.Name()}, "X-Claude-Code-Agent-Id": {agent}}
				options := core.Options{SourceFormat: translator.FormatClaude, Stream: stream, Headers: headers, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
				request := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"messages":[{"role":"user","content":"continue"}]}`)}
				if !stream {
					if _, err := executor.Execute(t.Context(), credential, request, options); err != nil {
						t.Fatal(err)
					}
				} else {
					result, err := executor.ExecuteStream(t.Context(), credential, request, options)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				}
			}
			if calls.Load() != 3 {
				t.Fatalf("upstream calls = %d, want 3", calls.Load())
			}
		})
	}
}
