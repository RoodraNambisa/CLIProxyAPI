package executor

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexAstraMinimumOnReusedWebsocketConnection(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, scenario := range []struct {
			name        string
			version     string
			enforce     bool
			incremental bool
		}{
			{"old full", "0.148.0", true, false},
			{"old incremental", "0.148.0", true, true},
			{"compatible", "0.153.0", true, false},
			{"opt out", "0.148.0", false, false},
		} {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, scenario.name), func(t *testing.T) {
				var dials atomic.Int64
				wire := make(chan http.Header, 4)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					dials.Add(1)
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					for {
						if _, _, errRead := conn.ReadMessage(); errRead != nil {
							return
						}
						wire <- r.Header.Clone()
						if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"stable","status":"completed","output":[]}}`)); err != nil {
							return
						}
					}
				}))
				defer server.Close()
				savedAgent := "local-codex/" + scenario.version + " (Linux; arm64) tmux/3.5"
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{EnforceSoftwareIdentity: &scenario.enforce}, CodexHeaderDefaults: config.CodexHeaderDefaults{UserAgent: savedAgent}}
				credential := &cliproxyauth.Auth{ID: "model-switch-auth", Provider: "codex", Attributes: map[string]string{"base_url": server.URL}, Metadata: map[string]any{"access_token": "test-oauth", "codex_fingerprint_mode": "off"}}
				exec := NewCodexWebsocketsExecutor(cfg)
				exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
				defer exec.CloseExecutionSession("model-switch")
				opts := core.Options{SourceFormat: sdktranslator.FromString("codex"), Metadata: map[string]any{core.ExecutionSessionMetadataKey: "model-switch"}}
				run := func(model, input string) error {
					req := core.Request{Model: model, Payload: []byte(input)}
					if !stream {
						_, err := exec.Execute(t.Context(), credential, req, opts)
						return err
					}
					result, err := exec.ExecuteStream(t.Context(), credential, req, opts)
					if err != nil {
						return err
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							return chunk.Err
						}
					}
					return nil
				}
				if err := run("gpt-5.4", `{"model":"gpt-5.4","input":[]}`); err != nil {
					t.Fatal(err)
				}
				if got := (<-wire).Get("User-Agent"); got != savedAgent {
					t.Fatal("generic model changed a supported saved software version")
				}
				if scenario.incremental {
					sess := exec.getOrCreateSession("model-switch")
					sess.connMu.Lock()
					original := sess.conn
					sess.connMu.Unlock()
					err := run("gpt-6-astra(high)", `{"model":"gpt-6-astra","previous_response_id":"stable","input":[]}`)
					var replay *core.UpstreamWebsocketReplayRequiredError
					if !errors.As(err, &replay) || len(wire) != 0 || dials.Load() != 1 {
						t.Fatal("incompatible incremental request crossed its handshake context")
					}
					sess.connMu.Lock()
					preserved := sess.conn == original
					sess.connMu.Unlock()
					if !preserved {
						t.Fatal("replay rejection destroyed the previous usable connection")
					}
				}
				if err := run("gpt-6-astra(high)", `{"model":"gpt-6-astra","input":[]}`); err != nil {
					t.Fatal(err)
				}
				wantVersion, wantDials := scenario.version, int64(2)
				if scenario.enforce && scenario.version == "0.148.0" {
					wantVersion, wantDials = "0.153.4", 2
				}
				got := <-wire
				if got.Get("User-Agent") != "local-codex/"+wantVersion+" (Linux; arm64) tmux/3.5" || (scenario.enforce && got.Get("Version") != wantVersion) || dials.Load() != wantDials {
					t.Fatal("reused handshake does not satisfy the model minimum or reconnected unnecessarily")
				}
				if err := run("gpt-5.4", `{"model":"gpt-5.4","input":[]}`); err != nil {
					t.Fatal(err)
				}
				<-wire
				if dials.Load() != wantDials+1 || cfg.CodexHeaderDefaults.UserAgent != savedAgent {
					t.Fatal("model switch did not isolate the connection or rewrote configuration")
				}
			})
		}
	}
}
