package executor

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexFailureDiagnosticsAcrossTransports(t *testing.T) {
	for _, transport := range []string{"http", "sse", "websocket"} {
		t.Run(transport, func(t *testing.T) {
			const failure = `{"type":"error","status":500,"error":{"type":"server_error","code":"resource_exhausted","message":"capacity exhausted"}}`
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Request-Id", "req-http-fixture")
				if transport == "websocket" {
					upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() {
						if errClose := conn.Close(); errClose != nil {
							t.Error(errClose)
						}
					}()
					if _, _, err := conn.ReadMessage(); err != nil {
						t.Error(err)
						return
					}
					if err := conn.WriteMessage(websocket.TextMessage, []byte(failure)); err != nil {
						t.Error(err)
					}
					return
				}
				if transport == "sse" {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_fixture\"}}\n\ndata: " + failure + "\n\n"))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(500)
				_, _ = w.Write([]byte(failure))
			}))
			t.Cleanup(upstream.Close)
			id := "codex-diagnostics-" + transport
			records := make(chan coreusage.Record, 2)
			coreusage.RegisterPlugin(&mixedProviderUsagePlugin{authIDs: map[string]struct{}{id: {}}, records: records, done: t.Context().Done()})
			cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
			auth := &cliproxyauth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"api_key": "test-upstream-fixture", "base_url": upstream.URL}}
			req := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"fixture"}`)}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}
			if transport == "http" {
				if _, err := NewCodexExecutor(cfg).Execute(t.Context(), auth, req, opts); err == nil {
					t.Fatal("expected upstream failure")
				}
			} else {
				var result *cliproxyexecutor.StreamResult
				var err error
				if transport == "websocket" {
					result, err = NewCodexWebsocketsExecutor(cfg).ExecuteStream(t.Context(), auth, req, opts)
				} else {
					result, err = NewCodexExecutor(cfg).ExecuteStream(t.Context(), auth, req, opts)
				}
				if err != nil {
					t.Fatal(err)
				}
				failed := false
				for chunk := range result.Chunks {
					failed = failed || chunk.Err != nil
				}
				if !failed {
					t.Fatal("stream failure disappeared")
				}
			}
			got := collectMixedProviderUsageRecords(t, records)
			if len(got) != 1 || !got[0].Failed || got[0].StatusCode != 500 || got[0].ErrorCode != "resource_exhausted" || got[0].ErrorMessage != "capacity exhausted" {
				t.Fatal("transport failure details were lost or duplicated")
			}
			if transport != "websocket" {
				wantHTTP := 500
				if transport == "sse" {
					wantHTTP = 200
				}
				if got[0].UpstreamStatusCode != wantHTTP || got[0].UpstreamRequestID != "req-http-fixture" {
					t.Fatal("HTTP transport status or request ID was lost")
				}
			}
		})
	}
}
