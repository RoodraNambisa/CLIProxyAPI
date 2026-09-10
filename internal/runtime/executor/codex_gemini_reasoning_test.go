package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexGeminiReasoningSurvivesHTTPStreamAndRelease(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, release := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/release=%t", stream, release), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					if strings.Contains(string(body), "hidden") || !strings.Contains(string(body), "public system") {
						t.Error("hidden Gemini thoughts leaked into the actual Codex request")
					}
					w.Header().Set("Content-Type", "text/event-stream")
					for _, event := range []string{
						`{"type":"response.created","response":{"id":"thinking","model":"gpt-5.4-mini"}}`,
						`{"type":"response.reasoning_text.delta","item_id":"rs_one","delta":"visible thought"}`,
						`{"type":"response.output_text.delta","delta":"answer"}`,
						`{"type":"response.completed","response":{"id":"thinking","status":"completed","output":[{"type":"reasoning","content":[{"type":"reasoning_text","text":"visible thought"}]},{"type":"message","content":[{"type":"output_text","text":"answer"}]}],"usage":{"input_tokens":2,"output_tokens":3}}}`,
					} {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
					}
				}))
				defer server.Close()
				executor := NewCodexExecutor(&config.Config{})
				auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
				opts := core.Options{SourceFormat: translator.FormatGemini, Stream: stream, Metadata: map[string]any{}}
				if release {
					opts.Metadata[core.BodyReleaseControllerMetadataKey] = controller
				}
				req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"systemInstruction":{"parts":[{"thought":true,"text":"hidden system"},{"text":"public system"}]},"contents":[{"role":"model","parts":[{"thought":true,"text":"hidden history"}]},{"role":"user","parts":[{"text":"ask"}]}],"generationConfig":{"thinkingConfig":{"includeThoughts":true}}}`)}
				var outputs [][]byte
				if stream {
					result, err := executor.ExecuteStream(t.Context(), auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						outputs = append(outputs, chunk.Payload)
					}
				} else {
					result, err := executor.Execute(t.Context(), auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					outputs = append(outputs, result.Payload)
				}
				var thought, answer strings.Builder
				for _, out := range outputs {
					for _, part := range gjson.GetBytes(out, "candidates.0.content.parts").Array() {
						if part.Get("thought").Bool() {
							thought.WriteString(part.Get("text").String())
						} else {
							answer.WriteString(part.Get("text").String())
						}
					}
				}
				if thought.String() != "visible thought" || answer.String() != "answer" || controller.Released() != release {
					t.Fatal("actual Gemini entry lost reasoning or mixed it into the answer")
				}
			})
		}
	}
}
