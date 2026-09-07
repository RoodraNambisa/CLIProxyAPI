package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexWebsocketBodyReleaseDuringBootstrap(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			raw := []byte(`{"model":"gpt-5.4","input":"hello"}`)
			ctrl := core.NewRequestBodyReleaseController(int64(len(raw)), []byte("<released>"))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = conn.Close() }()
				if _, _, err := conn.ReadMessage(); err != nil {
					t.Error(err)
					return
				}
				ctrl.Release()
				for _, event := range []string{
					`{"type":"response.created","response":{"id":"resp_release","output":[]}}`,
					`{"type":"response.completed","response":{"id":"resp_release","status":"completed","output":[]}}`,
				} {
					if err := conn.WriteMessage(websocket.TextMessage, []byte(event)); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			executor := NewCodexWebsocketsExecutor(&config.Config{Codex: config.CodexConfig{StreamBootstrapBuffering: true}})
			auth := &cliproxyauth.Auth{ID: "release-race", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
			ctx := core.WithRequestBodyReleaseController(t.Context(), ctrl)
			req := core.Request{Model: "gpt-5.4", Payload: raw}
			opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: raw}
			if !stream {
				response, err := executor.Execute(ctx, auth, req, opts)
				if err != nil || gjson.GetBytes(response.Payload, "id").String() != "resp_release" {
					t.Fatalf("execute after release: %v", err)
				}
			} else {
				result, err := executor.ExecuteStream(ctx, auth, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				completed := false
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
					completed = completed || gjson.GetBytes(chunk.Payload, "type").String() == "response.completed" || containsCompletedReleaseEvent(chunk.Payload)
				}
				if !completed {
					t.Fatal("stream lost its completion after release")
				}
			}
			if !ctrl.Released() {
				t.Fatal("fixture did not release its body")
			}
		})
	}
}

func containsCompletedReleaseEvent(payload []byte) bool {
	if len(payload) >= 5 && string(payload[:5]) == "data:" {
		return gjson.GetBytes(payload[5:], "type").String() == "response.completed"
	}
	return false
}

func TestCodexWebsocketFallbackRetainsDistinctRawBodiesUntilRelease(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, released := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/released=%t", stream, released), func(t *testing.T) {
				payload := []byte(`{"model":"gpt-5.4","input":[{"role":"user","content":"working request"}]}`)
				original := []byte(`{"model":"gpt-5.4","input":[{"role":"user","content":"original request"}],"metadata":{"explicit":true}}`)
				ctrl := core.NewRequestBodyReleaseController(int64(len(original)), []byte("<released>"))
				var httpCalls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if websocket.IsWebSocketUpgrade(r) {
						if released {
							ctrl.Release()
						}
						w.WriteHeader(http.StatusUpgradeRequired)
						return
					}
					httpCalls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					if gjson.GetBytes(body, "input.0.content").String() != "working request" {
						t.Error("fallback lost or substituted the working request")
					}
					if gjson.GetBytes(body, "metadata.explicit").Exists() {
						t.Error("fallback lost the original request used to check payload defaults")
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fallback\",\"status\":\"completed\",\"output\":[]}}\n\n")
				}))
				defer server.Close()
				cfg := &config.Config{Payload: config.PayloadConfig{Default: []config.PayloadRule{{
					Models: []config.PayloadModelRule{{Name: "gpt-5.4"}},
					Params: map[string]any{"metadata.explicit": "unexpected default"},
				}}}}
				executor := NewCodexWebsocketsExecutor(cfg)
				auth := &cliproxyauth.Auth{ID: "fallback-bodies", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
				ctx := core.WithRequestBodyReleaseController(t.Context(), ctrl)
				req := core.Request{Model: "gpt-5.4", Payload: payload}
				opts := core.Options{SourceFormat: sdktranslator.FormatCodex, OriginalRequest: original}
				var err error
				if stream {
					var result *core.StreamResult
					result, err = executor.ExecuteStream(ctx, auth, req, opts)
					if err == nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
								break
							}
						}
					}
				} else {
					_, err = executor.Execute(ctx, auth, req, opts)
				}
				if released {
					if err == nil || httpCalls.Load() != 0 {
						t.Fatalf("released body was replayed: error=%v, calls=%d", err, httpCalls.Load())
					}
				} else if err != nil || httpCalls.Load() != 1 {
					t.Fatalf("HTTP fallback failed: error=%v, calls=%d", err, httpCalls.Load())
				}
			})
		}
	}
}
