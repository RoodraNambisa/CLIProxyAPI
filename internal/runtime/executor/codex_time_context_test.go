package executor

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexTimeContextFinalHTTPStreamCompactAndWS(t *testing.T) {
	for _, mode := range []string{"http", "stream", "compact", "websocket", "websocket-stream"} {
		t.Run(mode, func(t *testing.T) {
			wire := make(chan []byte, 1)
			const completion = `{"type":"response.completed","response":{"id":"resp_1","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
					u := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
					c, err := u.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = c.Close() }()
					_, body, err := c.ReadMessage()
					if err != nil {
						t.Error(err)
						return
					}
					wire <- body
					_ = c.WriteMessage(websocket.TextMessage, []byte(completion))
					return
				}
				body, _ := io.ReadAll(r.Body)
				wire <- body
				if mode == "compact" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, gjson.Get(completion, "response").Raw)
				} else {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: "+completion+"\n\ndata: [DONE]\n\n")
				}
			}))
			defer server.Close()
			cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{BaseURL: server.URL, TimeContext: config.CodexTimeContextConfig{Enabled: true, Mode: "manual", Timezone: "Asia/Tokyo"}}}
			a := &auth.Auth{ID: "time-fixture", Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}
			text := "<environment_context>\n<cwd>/safe</cwd>\n<timezone>UTC</timezone>\n<current_date>2000-01-01</current_date>\n</environment_context>"
			payload, _ := json.Marshal(map[string]any{"model": "gpt-5.4", "input": []any{map[string]any{"role": "user", "content": text}}})
			req := core.Request{Model: "gpt-5.4", Payload: payload}
			opts := core.Options{SourceFormat: translator.FormatCodex}
			var ex auth.ProviderExecutor = NewCodexExecutor(cfg)
			if mode == "compact" {
				opts.Alt = "responses/compact"
			}
			if strings.HasPrefix(mode, "websocket") {
				ex = NewCodexWebsocketsExecutor(cfg)
			}
			if strings.HasSuffix(mode, "stream") {
				result, err := ex.ExecuteStream(t.Context(), a, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else {
				if _, err := ex.Execute(t.Context(), a, req, opts); err != nil {
					t.Fatal(err)
				}
			}
			body := <-wire
			if !strings.Contains(string(body), "Asia/Tokyo") || strings.Contains(string(body), "2000-01-01") {
				t.Fatalf("final wire time context missing: %s", body)
			}
		})
	}
}
