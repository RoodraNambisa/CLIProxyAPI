package executor

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestInteractionsExecutorResponsesReasoningSignatureBoundary(t *testing.T) {
	envelope := make([]byte, 73)
	envelope[0], envelope[8] = 0x80, 1
	valid := base64.URLEncoding.EncodeToString(envelope)
	for _, keep := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("valid=%t/stream=%t", keep, stream), func(t *testing.T) {
				candidate, want := "EtoRforeignGeminiSignature", ""
				if keep {
					candidate, want = valid, valid
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					if !stream {
						_, _ = fmt.Fprintf(w, `{"id":"fixture","status":"completed","steps":[{"type":"thought","signature":%q,"content":[{"type":"text","text":"summary"}]}],"usage":{"total_tokens":3}}`, candidate)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					for _, event := range []string{
						`{"event_type":"step.start","index":0,"step":{"type":"thought"}}`,
						`{"event_type":"step.delta","index":0,"delta":{"type":"thought_summary","text":"summary"}}`,
						fmt.Sprintf(`{"event_type":"step.delta","index":0,"delta":{"type":"thought_signature","signature":%q}}`, candidate),
						`{"event_type":"step.stop","index":0}`,
						`{"event_type":"interaction.completed","interaction":{"id":"fixture","status":"completed","usage":{"total_tokens":3}}}`,
					} {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
					}
				}))
				defer server.Close()
				exec := NewGeminiInteractionsExecutor(&config.Config{})
				auth := &coreauth.Auth{Provider: "gemini-interactions", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
				request := core.Request{Model: "gemini-3.5-flash", Payload: []byte(`{"input":"fixture"}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
				var response []byte
				if stream {
					result, err := exec.ExecuteStream(t.Context(), auth, request, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						for _, line := range bytes.Split(chunk.Payload, []byte("\n")) {
							payload := helps.JSONPayload(line)
							if gjson.GetBytes(payload, "type").String() == "response.completed" {
								response = []byte(gjson.GetBytes(payload, "response").Raw)
							}
						}
					}
				} else {
					result, err := exec.Execute(t.Context(), auth, request, opts)
					if err != nil {
						t.Fatal(err)
					}
					response = result.Payload
				}
				if gjson.GetBytes(response, "output.0.encrypted_content").String() != want || gjson.GetBytes(response, "output.0.summary.0.text").String() != "summary" {
					t.Fatal("actual executor response lost the summary or violated the encrypted content boundary")
				}
			})
		}
	}
}
