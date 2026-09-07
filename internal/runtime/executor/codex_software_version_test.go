package executor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexAstraSoftwareMinimumOnHTTPAndWebsocket(t *testing.T) {
	for _, mode := range []string{"http", "http-stream", "compact", "websocket", "websocket-stream"} {
		useWebsocket := mode == "websocket" || mode == "websocket-stream"
		for _, enforce := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode=%s/enforce=%t", mode, enforce), func(t *testing.T) {
				headers := make(chan http.Header, 1)
				completed := []byte(`{"type":"response.completed","response":{"id":"resp_astra","output":[]}}`)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					headers <- r.Header.Clone()
					if useWebsocket {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, errRead := conn.ReadMessage(); errRead != nil {
							t.Error(errRead)
							return
						}
						if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
							t.Error(errWrite)
						}
					} else if mode == "compact" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write([]byte(`{"id":"resp_astra","output":[]}`))
					} else {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = w.Write(append(append([]byte("data: "), completed...), []byte("\n\n")...))
					}
				}))
				defer server.Close()
				const savedAgent = "local-codex/0.148.0 (Linux; arm64) tmux/3.5"
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{EnforceSoftwareIdentity: &enforce}, CodexHeaderDefaults: config.CodexHeaderDefaults{UserAgent: savedAgent}}
				auth := &cliproxyauth.Auth{ID: "astra-auth", Provider: "codex", Attributes: map[string]string{"base_url": server.URL}, Metadata: map[string]any{"access_token": "test-oauth", "codex_fingerprint_mode": "off"}}
				var executor cliproxyauth.ProviderExecutor = NewCodexExecutor(cfg)
				if useWebsocket {
					executor = NewCodexWebsocketsExecutor(cfg)
				}
				req := cliproxyexecutor.Request{Model: "gpt-6-astra(high)", Payload: []byte(`{"model":"gpt-6-astra","input":"hello"}`)}
				opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}
				var err error
				if mode == "http-stream" || mode == "websocket-stream" {
					var result *cliproxyexecutor.StreamResult
					result, err = executor.ExecuteStream(t.Context(), auth, req, opts)
					if err == nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				} else {
					if mode == "compact" {
						opts.Alt = "responses/compact"
					}
					_, err = executor.Execute(t.Context(), auth, req, opts)
				}
				if err != nil {
					t.Fatal(err)
				}
				got := <-headers
				if enforce {
					if got.Get("Version") != "0.153.4" || got.Get("User-Agent") != "local-codex/0.153.4 (Linux; arm64) tmux/3.5" || got.Get("Originator") != "local-codex" {
						t.Fatal("Astra wire software identity is not compatible")
					}
				} else if got.Get("User-Agent") != savedAgent {
					t.Fatal("disabled identity enforcement changed the saved user agent")
				}
				if cfg.CodexHeaderDefaults.UserAgent != savedAgent {
					t.Fatal("software normalization rewrote user configuration")
				}
			})
		}
	}
}
