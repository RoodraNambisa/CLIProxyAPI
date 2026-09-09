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
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeSummaryDisplayControlsFinalBetaAcrossOperations(t *testing.T) {
	for _, operation := range []string{"execute", "stream", "count"} {
		for _, source := range []translator.Format{translator.FormatClaude, translator.FormatOpenAIResponse, translator.FormatOpenAI} {
			for _, summary := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/summary=%t", operation, source, summary), func(t *testing.T) {
					captured := make(chan bool, 2)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
							return
						}
						betas := r.Header.Get("Anthropic-Beta")
						hasRedact := false
						for _, beta := range strings.Split(betas, ",") {
							hasRedact = hasRedact || strings.TrimSpace(beta) == "redact-thinking-2026-02-12"
						}
						captured <- hasRedact != summary && strings.Contains(betas, "retained-beta") &&
							(gjson.GetBytes(body, "thinking.display").String() == "summarized") == summary
						responseKind := "stream"
						if operation == "count" {
							responseKind = "count"
						}
						writeClaudeCompatibilityFixture(w, responseKind)
					}))
					t.Cleanup(server.Close)
					cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
					executor := NewClaudeExecutor(cfg)
					auth := &coreauth.Auth{ID: t.Name(), Provider: "claude", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL,
						"header:Anthropic-Beta": "retained-beta,redact-thinking-2026-02-12"}}
					body := `{"messages":[{"role":"user","content":"question"}],"thinking":{"type":"enabled","budget_tokens":2048}}`
					if source == translator.FormatOpenAIResponse {
						body = `{"input":[{"role":"user","content":"question"}],"reasoning":{"effort":"medium"}}`
					} else if source == translator.FormatOpenAI {
						body = `{"messages":[{"role":"user","content":"question"}],"reasoning_effort":"medium","thinking":{"type":"enabled"}}`
						if !summary {
							body = strings.Replace(body, `,"reasoning_effort":"medium"`, "", 1)
						}
					}
					if summary {
						if source == translator.FormatClaude {
							body = strings.Replace(body, `"budget_tokens":2048`, `"budget_tokens":2048,"display":"summarized"`, 1)
						} else if source == translator.FormatOpenAIResponse {
							body = strings.Replace(body, `"effort":"medium"`, `"effort":"medium","summary":"detailed"`, 1)
						} else {
							body = strings.Replace(body, `"type":"enabled"`, `"type":"enabled","include_thoughts":true`, 1)
						}
					}
					req := core.Request{Model: "claude-fixture", Payload: []byte(body)}
					opts := core.Options{SourceFormat: source, OriginalRequest: req.Payload}
					var err error
					switch operation {
					case "count":
						_, err = executor.CountTokens(t.Context(), auth, req, opts)
					case "stream":
						var result *core.StreamResult
						result, err = executor.ExecuteStream(t.Context(), auth, req, opts)
						if err == nil {
							for chunk := range result.Chunks {
								if chunk.Err != nil {
									err = chunk.Err
								}
							}
						}
					default:
						_, err = executor.Execute(t.Context(), auth, req, opts)
					}
					if err != nil {
						t.Fatal(err)
					}
					select {
					case valid := <-captured:
						if !valid {
							t.Fatal("final body display and beta header disagree")
						}
					default:
						t.Fatal("request did not reach the local upstream")
					}
				})
			}
		}
	}
}
