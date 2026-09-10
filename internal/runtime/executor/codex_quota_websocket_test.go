package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexQuotaWebsocketUsesFreshPolicyOnReusedConnection(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			for _, buffering := range []bool{false, true} {
				t.Run(fmt.Sprintf("stream=%t/enabled=%t/buffering=%t", stream, enabled, buffering), func(t *testing.T) {
					cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{ObserveQuota: enabled, StreamBootstrapBuffering: buffering}}
					manager := coreauth.NewManager(nil, nil, nil)
					manager.SetConfig(cfg)
					ws := NewCodexWebsocketsExecutor(cfg)
					manager.RegisterExecutor(ws)
					var connections, frames atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						connections.Add(1)
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, http.Header{"X-Codex-Plan-Type": {"pro"}})
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						for {
							if _, _, errRead := conn.ReadMessage(); errRead != nil {
								return
							}
							frame := frames.Add(1)
							if frame == 1 {
								next := *cfg
								next.Codex.ObserveQuota = !enabled
								manager.SetConfig(&next)
							}
							quota := fmt.Sprintf(`{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":%d,"window_minutes":300,"reset_after_seconds":0}},"credits":{"has_credits":false}}`, frame)
							for _, payload := range []string{quota, `{"type":"response.completed","response":{"id":"resp_fixture","output":[]}}`} {
								if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(payload)); errWrite != nil {
									t.Error(errWrite)
									return
								}
							}
						}
					}))
					t.Cleanup(server.Close)
					t.Cleanup(func() { ws.CloseExecutionSession(t.Name()) })
					id := t.Name()
					if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL, "websockets": "true"}}); err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
					for turn := 0; turn < 2; turn++ {
						req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[]}`)}
						opts := core.Options{SourceFormat: translator.FormatCodex, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
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
						current, _ := manager.GetByID(id)
						observation := current.CodexQuotaSnapshot()
						if turn == 0 && !enabled {
							if observation != nil {
								t.Fatal("disabled first turn captured handshake or frame quota")
							}
							continue
						}
						want := "1"
						if !enabled {
							want = "2"
						}
						if observation == nil || observation.Source != "websocket" || len(observation.Signals) != 4 || observation.Signals["X-Codex-Primary-Used-Percent"] != want || observation.Signals["X-Codex-Primary-Reset-After-Seconds"] != "0" {
							t.Fatalf("wrong per-turn quota: %+v", observation)
						}
					}
					if connections.Load() != 1 || frames.Load() != 2 {
						t.Fatal("observation replaced the connection or sent extra requests")
					}
				})
			}
		}
	}
}

func TestCodexQuotaWebsocketFallbackAndBufferedErrors(t *testing.T) {
	for _, operation := range []string{"fallback", "handshake-error", "buffered-error"} {
		t.Run(operation, func(t *testing.T) {
			cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{ObserveQuota: true, StreamBootstrapBuffering: true}}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.SetConfig(cfg)
			manager.RegisterExecutor(NewCodexWebsocketsExecutor(cfg))
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				if operation == "buffered-error" {
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
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","status":503,"error":{"code":"server_is_overloaded","message":"busy"},"headers":{"X-Codex-Primary-Used-Percent":"75","Authorization":"fixture-private"}}`))
					return
				}
				w.Header().Set("X-Codex-Primary-Used-Percent", fmt.Sprint(call))
				if websocket.IsWebSocketUpgrade(r) {
					next := *cfg
					next.Codex.ObserveQuota = false
					manager.SetConfig(&next)
					if operation == "handshake-error" {
						w.WriteHeader(http.StatusForbidden)
					} else {
						w.WriteHeader(http.StatusUpgradeRequired)
					}
					_, _ = fmt.Fprint(w, `{"error":{"message":"fixture"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fixture\",\"output\":[]}}\n\n")
			}))
			t.Cleanup(server.Close)
			id := t.Name()
			if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL, "websockets": "true"}}); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			result, err := manager.ExecuteStream(t.Context(), []string{"codex"}, core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[]}`)}, core.Options{SourceFormat: translator.FormatCodex})
			if result != nil {
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						err = chunk.Err
					}
				}
			}
			want, source, wantCalls := "1", "http", int32(1)
			if operation == "fallback" {
				want, wantCalls = "2", 2
			} else if operation == "buffered-error" {
				want, source = "75", "websocket"
			}
			current, _ := manager.GetByID(id)
			observation := current.CodexQuotaSnapshot()
			if (err == nil) != (operation == "fallback") || calls.Load() != wantCalls || observation == nil || len(observation.Signals) != 1 || observation.Source != source || observation.Signals["X-Codex-Primary-Used-Percent"] != want {
				t.Fatalf("quota altered fallback/error behavior: calls=%d err=%v observation=%+v", calls.Load(), err, observation)
			}
		})
	}
}

func TestCodexQuotaWebsocketBootstrapObservesFrameOnceBeforeCancellation(t *testing.T) {
	var observations atomic.Int32
	received := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"codex.rate_limits","credits":{"has_credits":false}}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_text.delta","delta":"fixture","output_index":0,"content_index":0}`))
		_, _, _ = conn.ReadMessage()
	}))
	t.Cleanup(server.Close)
	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{StreamBootstrapBuffering: true}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx = core.WithCodexQuotaObserver(ctx, func(_, _, source string, headers http.Header) {
		if source == "http" {
			return
		}
		if source != "websocket" || headers.Get("X-Codex-Credits-Has-Credits") != "false" {
			t.Error("unexpected quota signal")
		}
		if observations.Add(1) == 1 {
			close(received)
		}
	})
	result, err := NewCodexWebsocketsExecutor(cfg).ExecuteStream(ctx, &coreauth.Auth{ID: t.Name(), Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}, core.Request{Model: "gpt-5.4", Payload: []byte(`{"input":[]}`)}, core.Options{SourceFormat: translator.FormatCodex})
	if err != nil {
		t.Fatal(err)
	}
	<-received
	// Drain the buffered quota frame before canceling its otherwise idle stream.
	if chunk, open := <-result.Chunks; !open || chunk.Err != nil {
		t.Fatal("quota observation changed the existing event stream")
	}
	cancel()
	for range result.Chunks {
	}
	if observations.Load() != 1 {
		t.Fatal("bootstrap replay observed the same frame twice")
	}
}
