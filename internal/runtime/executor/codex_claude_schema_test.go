package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexClaudeStructuredOutputStrictAtUpstream(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("explicit=%t/stream=%t", explicit, stream), func(t *testing.T) {
				captured := make(chan []byte, 1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, _ := io.ReadAll(r.Body)
					captured <- body
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"status\":\"completed\",\"output\":[]}}\n\n")
				}))
				defer server.Close()
				auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				strict := ""
				if explicit {
					strict = `,"strict":true`
				}
				const schema = `{"type":"object","properties":{"optional":{"type":"string"}}}`
				body := []byte(`{"model":"gpt-5.4-mini","messages":[{"role":"user","content":"fixture"}],"output_config":{"format":{"type":"json_schema","schema":` + schema + strict + `}}}`)
				executor := NewCodexExecutor(&config.Config{})
				req := core.Request{Model: "gpt-5.4-mini", Payload: body}
				opts := core.Options{SourceFormat: translator.FormatClaude, Stream: stream}
				if stream {
					response, err := executor.ExecuteStream(t.Context(), auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range response.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else if _, err := executor.Execute(t.Context(), auth, req, opts); err != nil {
					t.Fatal(err)
				}
				got := <-captured
				if gjson.GetBytes(got, "text.format.strict").Bool() != explicit || gjson.GetBytes(got, "text.format.schema").Raw != schema {
					t.Fatal("executor overrode the inferred strict mode or the explicit caller choice")
				}
			})
		}
	}
}
