package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const codexTestBootstrapCreated = `{"type":"response.created","response":{"id":"first","output":[]}}`
const codexTestBootstrapFailure = `{"type":"response.failed","response":{"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"busy"}}}`
const codexTestBootstrapCompleted = `{"type":"response.completed","response":{"id":"done","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`

func TestCodexBootstrapWireAttemptsRespectExistingBudget(t *testing.T) {
	for _, transport := range []string{"http", "websocket", "images"} {
		for _, tc := range []struct {
			name        string
			enabled     bool
			prefix      string
			limit       int
			wantCalls   int64
			wantSuccess bool
		}{
			{"disabled", false, codexTestBootstrapCreated, 0, 1, false},
			{"enabled", true, codexTestBootstrapCreated, 0, 2, true},
			{"existing credential limit", true, codexTestBootstrapCreated, 1, 1, false},
			{"content committed", true, `{"type":"response.output_text.delta","delta":"visible"}`, 0, 1, false},
			{"unknown event committed", true, `{"type":"future.event"}`, 0, 1, false},
		} {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				var calls atomic.Int64
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					attempt := calls.Add(1)
					events := []string{codexTestBootstrapCompleted}
					if attempt == 1 {
						events = []string{tc.prefix, codexTestBootstrapFailure}
					}
					if transport == "websocket" {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						_, body, errRead := conn.ReadMessage()
						if errRead != nil {
							t.Error(errRead)
							return
						}
						if gjson.GetBytes(body, "reasoning.summary").Exists() {
							t.Error("retry restored hidden summary")
						}
						for _, event := range events {
							if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(event)); errWrite != nil {
								t.Error(errWrite)
								return
							}
						}
						// Rejections may be followed immediately by a connection close.
						if attempt != 1 {
							_, _, _ = conn.ReadMessage()
						}
						return
					}
					body, errRead := io.ReadAll(r.Body)
					if errRead != nil {
						t.Error(errRead)
						return
					}
					if transport != "images" && gjson.GetBytes(body, "reasoning.summary").Exists() {
						t.Error("retry restored hidden summary")
					}
					w.Header().Set("Content-Type", "text/event-stream")
					for _, event := range events {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
					}
				}))
				defer server.Close()
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{StreamBootstrapBuffering: tc.enabled}}
				manager := auth.NewManager(nil, &auth.FillFirstSelector{}, nil)
				manager.SetRetryConfig(0, 0, tc.limit)
				var exec auth.ProviderExecutor = NewCodexExecutor(cfg)
				opts := core.Options{SourceFormat: translator.FromString("codex"), Stream: true}
				ctx := t.Context()
				var disconnect <-chan error
				if transport == "websocket" {
					ws := NewCodexWebsocketsExecutor(cfg)
					session := uuid.NewString()
					defer ws.CloseExecutionSession(session)
					sess := ws.getOrCreateSession(session)
					disconnect = sess.upstreamDisconnectCh
					opts.Metadata = map[string]any{core.ExecutionSessionMetadataKey: session}
					ctx = core.WithDownstreamWebsocket(ctx)
					exec = ws
				}
				manager.RegisterExecutor(exec)
				model := "gpt-5.4"
				if transport == "images" {
					model = "gpt-image-2"
					opts.SourceFormat = translator.FromString(codexOpenAIImageSourceFormat)
					opts.Alt = codexOpenAIImageGenerations
				}
				for _, label := range []string{"a", "b"} {
					id := uuid.NewString() + label
					if _, err := manager.Register(t.Context(), &auth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}); err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: model}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				}
				req := core.Request{Model: model, Payload: []byte(fmt.Sprintf(`{"model":%q,"input":[],"prompt":"draw"}`, model))}
				if transport != "images" {
					req.Payload, _ = sjson.SetBytes(req.Payload, "reasoning.summary", nil)
				}
				ctrl := core.NewRequestBodyReleaseController(int64(len(req.Payload)), []byte("released"))
				ctx = core.WithRequestBodyReleaseController(ctx, ctrl)
				stream, err := manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
				var output strings.Builder
				if err == nil {
					for chunk := range stream.Chunks {
						if chunk.Err != nil {
							err = chunk.Err
						}
						output.Write(chunk.Payload)
					}
				}
				if calls.Load() != tc.wantCalls || (err == nil) != tc.wantSuccess {
					t.Fatalf("calls=%d success=%t; want calls=%d success=%t", calls.Load(), err == nil, tc.wantCalls, tc.wantSuccess)
				}
				if tc.wantSuccess {
					if transport != "images" && (!ctrl.Released() || ctrl.Replayable()) {
						t.Fatal("successful summary request did not release replay state")
					}
					if strings.Contains(output.String(), `"id":"first"`) || strings.Contains(output.String(), "server_is_overloaded") {
						t.Fatal("rejected attempt leaked into successful stream")
					}
					if disconnect != nil {
						select {
						case <-disconnect:
							t.Fatal("retry teardown closed downstream session")
						default:
						}
					}
				}
			})
		}
	}
}

func TestCodexBootstrapSnapshotSurvivesExecutorReplacement(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		cfg := &config.Config{Codex: config.CodexConfig{StreamBootstrapBuffering: enabled}}
		exec := NewCodexExecutor(cfg)
		req := core.Request{Payload: []byte(`{"input":[]}`)}
		opts, err := exec.ensureCodexPreparedSessionIdentity(t.Context(), req, core.Options{}, core.RequestOperationStream)
		if err != nil {
			t.Fatal(err)
		}
		next := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{StreamBootstrapBuffering: !enabled}})
		if next.codexPreparedSessionIdentity(t.Context(), req, opts).StreamBootstrapBuffering != enabled {
			t.Fatal("retry or transport fallback changed bootstrap policy")
		}
	}
}

func TestCodexBootstrapCancellationAndBodyRelease(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(fmt.Sprint(release), func(t *testing.T) {
			started := make(chan struct{})
			proceed := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "data: %s\n\n", codexTestBootstrapCreated)
				w.(http.Flusher).Flush()
				close(started)
				select {
				case <-r.Context().Done():
					return
				case <-proceed:
				}
				_, _ = fmt.Fprintf(w, "data: %s\n\n", codexTestBootstrapFailure)
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctrl := core.NewRequestBodyReleaseController(10, []byte("released"))
			ctx = core.WithRequestBodyReleaseController(ctx, ctrl)
			exec := NewCodexExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{StreamBootstrapBuffering: true}})
			credential := &auth.Auth{ID: "bootstrap-cancel", Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
			done := make(chan error, 1)
			go func() {
				stream, err := exec.ExecuteStream(ctx, credential, core.Request{Model: "gpt-5.4", Payload: []byte(`{"input":[]}`)}, core.Options{SourceFormat: translator.FromString("codex")})
				if stream != nil {
					for range stream.Chunks {
					}
				}
				done <- err
			}()
			<-started
			if release {
				ctrl.Release()
				close(proceed)
			} else {
				cancel()
			}
			err := <-done
			if release && err != nil {
				t.Fatal("released request became a synchronous retryable overload")
			}
			if !release && !errors.Is(err, context.Canceled) {
				t.Fatal("canceled probe did not return cancellation")
			}
			if release && helps.RequestBodyReplayable(ctx, core.Options{}) {
				t.Fatal("probe restored a released request")
			}
		})
	}
}

func TestCodexBootstrapPreservesRateLimitStatusAndHeaders(t *testing.T) {
	for _, useWS := range []bool{false, true} {
		t.Run(fmt.Sprint(useWS), func(t *testing.T) {
			const event = `{"type":"error","status":429,"headers":{"X-Test-Rate":"retained"},"error":{"code":"server_is_overloaded","type":"service_unavailable_error","message":"busy"}}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if useWS {
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					_, _, _ = conn.ReadMessage()
					_ = conn.WriteMessage(websocket.TextMessage, []byte(event))
					_, _, _ = conn.ReadMessage()
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Test-Rate", "retained")
				_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
			}))
			defer server.Close()
			cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{StreamBootstrapBuffering: true}}
			var exec auth.ProviderExecutor = NewCodexExecutor(cfg)
			if useWS {
				exec = NewCodexWebsocketsExecutor(cfg)
			}
			credential := &auth.Auth{ID: "rate-limit", Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
			_, err := exec.ExecuteStream(t.Context(), credential, core.Request{Model: "gpt-5.4", Payload: []byte(`{"input":[]}`)}, core.Options{SourceFormat: translator.FromString("codex")})
			var status interface{ StatusCode() int }
			var headers interface{ Headers() http.Header }
			if !errors.As(err, &status) || status.StatusCode() != 429 || !errors.As(err, &headers) || headers.Headers().Get("X-Test-Rate") != "retained" {
				t.Fatal("bootstrap changed actual status or public headers")
			}
		})
	}
}

func TestCodexBootstrapDoesNotReplayIncrementalWebsocketContext(t *testing.T) {
	var connections atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connections.Add(1)
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(codexTestBootstrapCompleted))
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(codexTestBootstrapCreated))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(codexTestBootstrapFailure))
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	ws := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{StreamBootstrapBuffering: true}})
	session := uuid.NewString()
	defer ws.CloseExecutionSession(session)
	credential := &auth.Auth{ID: "incremental", Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
	opts := core.Options{SourceFormat: translator.FromString("codex"), Metadata: map[string]any{core.ExecutionSessionMetadataKey: session}}
	for index, body := range []string{`{"input":[]}`, `{"input":[],"previous_response_id":"done"}`} {
		stream, err := ws.ExecuteStream(t.Context(), credential, core.Request{Model: "gpt-5.4", Payload: []byte(body)}, opts)
		if err != nil {
			t.Fatal("incremental context became a synchronous failover")
		}
		failed := false
		for chunk := range stream.Chunks {
			failed = failed || chunk.Err != nil
		}
		if failed != (index == 1) {
			t.Fatal("incremental failure did not use ordinary in-stream semantics")
		}
	}
	if connections.Load() != 1 {
		t.Fatal("incremental failure opened another connection")
	}
}

func TestCodexBootstrapWebsocketCancellationReleasesProbeState(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(codexTestBootstrapCreated))
		close(started)
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	ws := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{StreamBootstrapBuffering: true}})
	session := uuid.NewString()
	defer ws.CloseExecutionSession(session)
	credential := &auth.Auth{ID: "bootstrap-ws-cancel", Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
	opts := core.Options{SourceFormat: translator.FromString("codex"), Metadata: map[string]any{core.ExecutionSessionMetadataKey: session}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type probeResult struct {
		err       error
		hadStream bool
	}
	done := make(chan probeResult, 1)
	go func() {
		stream, err := ws.ExecuteStream(ctx, credential, core.Request{Model: "gpt-5.4", Payload: []byte(`{"input":[]}`)}, opts)
		if stream != nil {
			for chunk := range stream.Chunks {
				if chunk.Err != nil {
					err = chunk.Err
				}
			}
		}
		done <- probeResult{err: err, hadStream: stream != nil}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("bootstrap did not start")
	}
	cancel()
	select {
	case result := <-done:
		// A canceled producer may close its stream without delivering an error
		// chunk. The caller's canceled context remains authoritative in that case.
		if result.err != nil && !errors.Is(result.err, context.Canceled) {
			t.Fatal("bootstrap replaced cancellation")
		}
		if result.err == nil && (!result.hadStream || ctx.Err() != context.Canceled) {
			t.Fatal("bootstrap did not report cancellation or close a canceled stream")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel left bootstrap blocked")
	}
	sess := ws.getOrCreateSession(session)
	if !sess.reqMu.TryLock() {
		t.Fatal("canceled probe kept the request lock")
	}
	sess.reqMu.Unlock()
	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	if sess.bootstrapDisconnectGate != nil {
		t.Fatal("canceled probe retained its disconnect gate")
	}
}
