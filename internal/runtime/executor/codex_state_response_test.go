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

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func seedManagedStateResponseTest(t *testing.T, cfg *config.Config, a *auth.Auth) func() {
	t.Helper()
	r := registry.GetGlobalRegistry()
	r.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "gpt-6-astra"}})
	codexstate.Default.Sync(cfg.Codex.StateOverride, []codexstate.Credential{helps.StateCredential(a, "gpt-6-astra")})
	t.Cleanup(func() {
		codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
		codexstate.Default.Wait()
		r.UnregisterClient(a.ID)
	})
	acquire := func() {
		codexstate.Default.Action(a.ID, "gpt-6-astra", "acquire")
		codexstate.Default.Tick(t.Context(), time.Now(), func(context.Context, codexstate.Credential, config.CodexStateOverrideConfig) (codexstate.Result, error) {
			return codexstate.Result{State: strings.Repeat("s", 292), Model: "gpt-6-astra", Completed: true}, nil
		})
		codexstate.Default.Wait()
	}
	acquire()
	return acquire
}

func TestCodexStateResponseInvalidationAcrossTransports(t *testing.T) {
	for _, mode := range []string{"http", "sse", "trusted-sse", "image-passthrough", "compact", "ws", "ws-stream", "custom-test"} {
		for _, criterion := range []string{"length", "model", "model-with-name-rewrite"} {
			t.Run(mode+"/"+criterion, func(t *testing.T) {
				rewrite := criterion == "model-with-name-rewrite"
				if rewrite && mode == "custom-test" {
					return
				}
				var calls atomic.Int32
				model, responseState := "gpt-6-astra", ""
				if criterion != "length" {
					model = "other-model"
				} else {
					responseState = strings.Repeat("r", 312)
				}
				response := fmt.Sprintf(`{"id":"fixture","model":%q,"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"unchanged answer"}]}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}`, model)
				terminal := `{"type":"response.completed","response":` + response + `}`
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.Header.Get("X-Codex-Turn-State") != strings.Repeat("s", 292) {
						t.Error("request did not send the selected state")
					}
					if strings.HasPrefix(mode, "ws") {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, http.Header{"X-Codex-Turn-State": {responseState}})
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, errRead := conn.ReadMessage(); errRead == nil {
							_ = conn.WriteMessage(websocket.TextMessage, []byte(terminal))
							_, _, _ = conn.ReadMessage()
						}
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					if responseState != "" {
						w.Header().Set("X-Codex-Turn-State", responseState)
					}
					if mode == "compact" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, response)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					if mode == "trusted-sse" {
						_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\ndata: \"response\":%s}\n\n", response)
					} else {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", terminal)
					}
				}))
				enforce := false
				cfg := &config.Config{Codex: config.CodexConfig{EnforceSoftwareIdentity: &enforce, StateOverride: config.CodexStateOverrideConfig{Enabled: true, Acquisition: "manual", InvalidateOnStateLengthMismatch: criterion == "length", InvalidateOnModelMismatch: criterion != "length"}}}
				a := &auth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL}, Metadata: map[string]any{"access_token": "fixture", "account_id": "fixture-owner"}}
				var manager *auth.Manager
				if rewrite {
					cfg.ResponseModelRewrite = config.ResponseModelRewriteConfig{Enabled: true, Rules: []config.ResponseModelRewriteRule{{Providers: []string{"codex"}}}}
					manager = auth.NewManager(nil, nil, nil)
					manager.SetConfig(cfg)
					var err error
					a, err = manager.Register(auth.WithSkipPersist(t.Context()), a)
					if err != nil {
						t.Fatal(err)
					}
				}
				seedManagedStateResponseTest(t, cfg, a)
				var executor auth.ProviderExecutor = NewCodexExecutor(cfg)
				var ws *CodexWebsocketsExecutor
				if strings.HasPrefix(mode, "ws") {
					ws = NewCodexWebsocketsExecutor(cfg)
					executor = ws
				}
				if manager != nil {
					manager.RegisterExecutor(executor)
				}
				t.Cleanup(func() {
					if ws != nil {
						ws.CloseExecutionSession(t.Name())
					}
					server.Close()
				})
				req := core.Request{Model: "gpt-6-astra", Payload: []byte(`{"model":"gpt-6-astra","input":"fixture","tools":[{"type":"image_generation"}]}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name(), core.TrustUpstreamSSEMetadataKey: mode == "trusted-sse", core.ImageGenerationStreamPassthroughMetadataKey: mode == "image-passthrough"}}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				if mode == "custom-test" {
					ctx = helps.WithCodexStateDiagnostic(ctx, "custom", strings.Repeat("s", 292), nil)
				}
				var output []byte
				if mode == "sse" || mode == "trusted-sse" || mode == "image-passthrough" || mode == "ws-stream" {
					var result *core.StreamResult
					var err error
					if manager != nil {
						result, err = manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
					} else {
						result, err = executor.ExecuteStream(ctx, a, req, opts)
					}
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						output = append(output, chunk.Payload...)
					}
				} else {
					if mode == "compact" {
						opts.Alt = "responses/compact"
					}
					var result core.Response
					var err error
					if manager != nil {
						result, err = manager.Execute(ctx, []string{"codex"}, req, opts)
					} else {
						result, err = executor.Execute(ctx, a, req, opts)
					}
					if err != nil {
						t.Fatal(err)
					}
					output = result.Payload
				}
				wantModel := model
				if rewrite {
					wantModel = "gpt-6-astra"
				}
				if calls.Load() != 1 || !strings.Contains(string(output), "unchanged answer") || !strings.Contains(string(output), wantModel) {
					t.Fatalf("business response changed or replayed: calls=%d output=%s", calls.Load(), output)
				}
				if manager != nil {
					stats := manager.AuthResponseModelRewriteSummary(a, true)
					if stats.Total != 1 || len(stats.Recent) != 1 || stats.Recent[0].OriginalModel != model || strings.Contains(string(output), model) {
						t.Fatalf("name rewrite missed response or counter: %+v", stats)
					}
				}
				s := codexstate.Default.Snapshots(a.ID, time.Now())[0]
				if mode == "custom-test" {
					if s.Invalidations != 0 || s.Status != "valid" {
						t.Fatal("manual test invalidated a managed value with identical contents")
					}
					return
				}
				if s.Invalidations != 1 || s.Status != "queued" {
					t.Fatalf("response mismatch did not invalidate/queue: %+v", s)
				}
				if ws != nil {
					sess := ws.getOrCreateSession(t.Name())
					sess.connMu.Lock()
					rejected, conn := sess.managedStateRejected, sess.conn
					sess.connMu.Unlock()
					if !rejected || conn == nil {
						t.Fatal("current websocket was not marked for retirement after completing the response")
					}
				}
			})
		}
	}
}

func TestCodexStateResponseWebsocketRetiresOldConnectionWithoutDeletingNewState(t *testing.T) {
	enforce := false
	cfg := &config.Config{Codex: config.CodexConfig{EnforceSoftwareIdentity: &enforce, StateOverride: config.CodexStateOverrideConfig{Enabled: true, Acquisition: "manual", InvalidateOnModelMismatch: true}}}
	a := &auth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct"}
	acquire := seedManagedStateResponseTest(t, cfg, a)
	ctx := core.WithCodexStateSnapshot(t.Context())
	headers := http.Header{}
	if err := helps.ApplyManagedState(ctx, cfg, a, "gpt-6-astra", headers); err != nil {
		t.Fatal(err)
	}
	use := helps.ManagedStateUse(ctx, a, "gpt-6-astra", headers)
	exec := NewCodexWebsocketsExecutor(cfg)
	conn := &websocket.Conn{}
	sess := &codexWebsocketSession{conn: conn, readerConn: conn, authID: a.ID, authInstanceID: a.RuntimeInstanceID(), proxyIdentity: websocketProxyIdentity(cfg, a), wsURL: "ws://unused.invalid", managedStateModel: "gpt-6-astra", managedStateUse: use}
	acquire()
	exec.observeManagedWebsocketState(t.Context(), a, sess, conn, "gpt-6-astra", nil, nil, []byte(`{"response":{"model":"other"}}`))
	if s := codexstate.Default.Snapshots(a.ID, time.Now())[0]; s.Status != "valid" || s.Invalidations != 0 {
		t.Fatal("old connection invalidated a newer acquisition")
	}
	if !sess.managedStateRejected || sess.conn != conn {
		t.Fatal("old response interrupted its current connection")
	}
	_, _, err := exec.ensureUpstreamConn(core.WithRequiredUpstreamWebsocket(t.Context()), a, sess, a.ID, sess.wsURL, http.Header{}, "gpt-6-astra")
	var replay *core.UpstreamWebsocketReplayRequiredError
	if !errors.As(err, &replay) || sess.conn != conn {
		t.Fatalf("unsafe replay boundary: %v", err)
	}
}
