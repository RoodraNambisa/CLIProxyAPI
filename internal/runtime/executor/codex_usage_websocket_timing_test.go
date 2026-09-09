package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexWebsocketUsageContentAndBootstrapTiming(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, buffering := range []bool{false, true} {
			for _, result := range []string{"content", "empty", "failure", "overload", "no-usage"} {
				t.Run(fmt.Sprintf("stream=%t/buffering=%t/%s", stream, buffering, result), func(t *testing.T) {
					collector := &alphaUsageCollector{authID: t.Name()}
					usage.RegisterPlugin(collector)
					t.Cleanup(func() { collector.mu.Lock(); collector.closed = true; collector.records = nil; collector.mu.Unlock() })
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, errRead := conn.ReadMessage(); errRead != nil {
							return
						}
						_ = conn.WriteMessage(websocket.PingMessage, []byte("fixture"))
						_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"fixture"}}`))
						if result == "content" || result == "no-usage" {
							_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.function_call_arguments.delta","item_id":"fc_fixture","delta":"{}"}`))
						}
						terminal := `{"type":"response.completed","response":{"id":"fixture","status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}}`
						switch result {
						case "failure":
							terminal = `{"type":"response.failed","response":{"error":{"code":"misalignment_policy_violation","message":"fixture"}}}`
						case "overload":
							terminal = `{"type":"response.failed","response":{"error":{"code":"server_is_overloaded","type":"service_unavailable_error","message":"fixture"}}}`
						case "no-usage":
							terminal = `{"type":"response.completed","response":{"id":"fixture","status":"completed","output":[]}}`
						}
						_ = conn.WriteMessage(websocket.TextMessage, []byte(terminal))
						_, _, _ = conn.ReadMessage()
					}))
					defer server.Close()
					ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
					defer cancel()
					executor := NewCodexWebsocketsExecutor(&config.Config{Codex: config.CodexConfig{StreamBootstrapBuffering: buffering}})
					auth := &coreauth.Auth{ID: collector.authID, Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
					req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","input":"fixture"}`)}
					opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
					var executionErr error
					if stream {
						var response *core.StreamResult
						response, executionErr = executor.ExecuteStream(ctx, auth, req, opts)
						if executionErr == nil {
							for chunk := range response.Chunks {
								if chunk.Err != nil {
									executionErr = chunk.Err
								}
							}
						}
					} else {
						_, executionErr = executor.Execute(ctx, auth, req, opts)
					}
					failure := result == "failure" || result == "overload"
					if (executionErr != nil) != failure {
						t.Fatalf("unexpected execution result: %v", executionErr)
					}
					if err := usage.DefaultManager().Barrier(ctx); err != nil {
						t.Fatal(err)
					}
					collector.mu.Lock()
					records := append([]usage.Record(nil), collector.records...)
					collector.mu.Unlock()
					if len(records) != 1 {
						t.Fatalf("records=%d, want one", len(records))
					}
					record := records[0]
					wantContent := result == "content" || result == "no-usage"
					if record.FirstPacketLatency <= 0 || (record.TTFT > 0) != wantContent || (wantContent && record.TTFT < record.FirstPacketLatency) || record.Failed != failure || record.Stream != stream {
						t.Fatalf("invalid timing: packet=%v content=%v failed=%t", record.FirstPacketLatency, record.TTFT, record.Failed)
					}
					if result == "no-usage" && record.Detail.TotalTokens != 0 {
						t.Fatal("missing usage fabricated token counts")
					}
				})
			}
		}
	}
}

func TestCodexWebsocketUsageCancellationAfterMetadata(t *testing.T) {
	collector := &alphaUsageCollector{authID: t.Name()}
	usage.RegisterPlugin(collector)
	t.Cleanup(func() { collector.mu.Lock(); collector.closed = true; collector.records = nil; collector.mu.Unlock() })
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(closed)
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"fixture"}}`))
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	executor := NewCodexWebsocketsExecutor(&config.Config{})
	auth := &coreauth.Auth{ID: collector.authID, Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
	response, err := executor.ExecuteStream(ctx, auth, core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","input":"fixture"}`)}, core.Options{SourceFormat: translator.FormatOpenAIResponse})
	if err != nil {
		t.Fatal(err)
	}
	for range response.Chunks {
		cancel()
	}
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer waitCancel()
	if err := usage.DefaultManager().Barrier(waitCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-waitCtx.Done():
		t.Fatal("canceled websocket was not closed")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if len(collector.records) != 1 || !collector.records[0].Failed || collector.records[0].TTFT != 0 || collector.records[0].FirstPacketLatency <= 0 {
		t.Fatal("canceled metadata-only turn lost its packet or fabricated model content")
	}
}
