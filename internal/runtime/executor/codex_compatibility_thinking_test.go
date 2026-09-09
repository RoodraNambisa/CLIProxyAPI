package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexCompatibilityClaudeThinkingReachesHTTPAndSSE(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, compat := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/compat=%t", stream, compat), func(t *testing.T) {
				type fields struct {
					count                         int
					signature, summary, firstType string
				}
				captured := make(chan fields, 2)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					captured <- fields{len(gjson.GetBytes(body, "input").Array()), gjson.GetBytes(body, "input.0.encrypted_content").Raw,
						gjson.GetBytes(body, "reasoning.summary").Raw, gjson.GetBytes(body, "input.0.type").String()}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fixture\",\"status\":\"completed\",\"output\":[]}}\n\n")
				}))
				t.Cleanup(server.Close)
				cfg := &config.Config{CodexKey: []config.CodexKey{{APIKey: "fixture", BaseURL: server.URL, Models: []config.CodexModel{{Name: "upstream", Alias: "alias", IsCompat: compat}}}}}
				manager := coreauth.NewManager(nil, nil, nil)
				manager.SetConfig(cfg)
				manager.RegisterExecutor(NewCodexExecutor(cfg))
				if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: t.Name(), Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}); err != nil {
					t.Fatal(err)
				}
				reg := registry.GetGlobalRegistry()
				reg.RegisterClient(t.Name(), "codex", []*registry.ModelInfo{{ID: "alias"}})
				t.Cleanup(func() { reg.UnregisterClient(t.Name()) })
				raw := []byte(`{"thinking":{"type":"enabled","display":"omitted","budget_tokens":1024},"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":""},{"type":"text","text":"answer"}]}]}`)
				req := core.Request{Model: "alias", Payload: raw}
				opts := core.Options{SourceFormat: translator.FormatClaude, OriginalRequest: raw}
				if stream {
					result, err := manager.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else if _, err := manager.Execute(t.Context(), []string{"codex"}, req, opts); err != nil {
					t.Fatal(err)
				}
				select {
				case got := <-captured:
					wantCount, wantSignature, wantType := 1, "", "message"
					if compat {
						wantCount, wantSignature, wantType = 2, `""`, "reasoning"
					}
					if got.count != wantCount || got.signature != wantSignature || got.firstType != wantType || got.summary != "" {
						t.Fatal("selected compatibility or summary intent was lost before the upstream request")
					}
				default:
					t.Fatal("request did not reach the local upstream")
				}
			})
		}
	}
}
