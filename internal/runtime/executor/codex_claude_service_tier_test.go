package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexExplicitFastTierAtActualTransport(t *testing.T) {
	for _, transport := range []string{"http", "websocket", "compact"} {
		for _, stream := range []bool{false, true} {
			if transport == "compact" && stream {
				continue
			}
			for _, fast := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/fast=%t", transport, stream, fast), func(t *testing.T) {
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var body []byte
						var conn *websocket.Conn
						var err error
						if transport == "websocket" {
							upgrader := websocket.Upgrader{}
							conn, err = upgrader.Upgrade(w, r, nil)
							if err != nil {
								t.Error(err)
								return
							}
							defer func() { _ = conn.Close() }()
							_, body, err = conn.ReadMessage()
						} else {
							body, err = io.ReadAll(r.Body)
						}
						if err != nil {
							t.Error(err)
							return
						}
						want := ""
						if fast {
							want = "priority"
						}
						if tier := gjson.GetBytes(body, "service_tier"); tier.String() != want || tier.Exists() != fast {
							t.Error("explicit fast intent was lost or ordinary request was upgraded")
						}
						terminal := `{"type":"response.completed","response":{"status":"completed","output":[]}}`
						if conn != nil {
							_ = conn.WriteMessage(websocket.TextMessage, []byte(terminal))
						} else if transport == "compact" {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{"object":"response.compaction","output":[]}`)
						} else {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = fmt.Fprintf(w, "data: %s\n\n", terminal)
						}
					}))
					defer upstream.Close()
					var executor coreauth.ProviderExecutor = NewCodexExecutor(&config.Config{})
					if transport == "websocket" {
						executor = NewCodexWebsocketsExecutor(&config.Config{})
					}
					credential := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}}
					payload := []byte(`{"messages":[{"role":"user","content":"fixture"}],"speed":"standard"}`)
					if fast {
						payload = []byte(`{"messages":[{"role":"user","content":"fixture"}],"speed":"fast","service_tier":"auto"}`)
					}
					req := core.Request{Model: "gpt-5.4-mini", Payload: payload}
					opts := core.Options{SourceFormat: translator.FormatClaude, OriginalRequest: payload, Stream: stream}
					if transport == "compact" {
						// The compact HTTP route accepts Responses input, not Claude messages.
						opts.Alt = "responses/compact"
						opts.SourceFormat = translator.FormatOpenAIResponse
						req.Payload = []byte(`{"input":[{"role":"user","content":"fixture"}]}`)
						if fast {
							req.Payload = []byte(`{"input":[{"role":"user","content":"fixture"}],"service_tier":"priority"}`)
						}
						opts.OriginalRequest = req.Payload
					}
					if stream {
						if transport == "websocket" {
							req.Payload = translator.TranslateRequest(translator.FormatClaude, translator.FormatCodex, req.Model, payload, true)
						}
						result, err := executor.ExecuteStream(t.Context(), credential, req, opts)
						if err != nil {
							t.Fatal(err)
						}
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								t.Fatal(chunk.Err)
							}
						}
					} else if _, err := executor.Execute(t.Context(), credential, req, opts); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}
