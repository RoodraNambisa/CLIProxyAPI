package executor

import (
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
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexRequestScopedActionsAcrossWireTransports(t *testing.T) {
	for _, transport := range []string{"http", "sse", "websocket", "images", "compact"} {
		for _, action := range []string{"stop", "stop-and-cooldown", "continue", "continue-and-cooldown"} {
			t.Run(transport+"/"+action, func(t *testing.T) {
				var calls atomic.Int64
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					attempt := calls.Add(1)
					failure := `{"type":"response.failed","response":{"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"rule-fixture"}}}`
					if transport == "websocket" {
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
						event := codexTestBootstrapCompleted
						if attempt == 1 {
							event = failure
						}
						if err := conn.WriteMessage(websocket.TextMessage, []byte(event)); err != nil {
							t.Error(err)
							return
						}
						_, _, _ = conn.ReadMessage()
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					if attempt == 1 && (transport == "http" || transport == "compact") {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusServiceUnavailable)
						_, _ = io.WriteString(w, `{"error":{"code":"server_is_overloaded","message":"rule-fixture"}}`)
						return
					}
					if transport == "compact" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"done","object":"response.compaction","output":[]}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					event := codexTestBootstrapCompleted
					if attempt == 1 {
						event = failure
					}
					_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
				}))
				defer server.Close()
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
				manager := auth.NewManager(nil, &auth.FillFirstSelector{}, nil)
				manager.SetConfig(cfg)
				manager.SetRetryConfig(2, 0, 0)
				var executor auth.ProviderExecutor = NewCodexExecutor(cfg)
				ctx := t.Context()
				opts := core.Options{SourceFormat: translator.FromString("codex")}
				model := "gpt-5.4"
				if transport == "websocket" {
					ws := NewCodexWebsocketsExecutor(cfg)
					session := uuid.NewString()
					defer ws.CloseExecutionSession(session)
					opts.Metadata = map[string]any{core.ExecutionSessionMetadataKey: session}
					ctx = core.WithDownstreamWebsocket(ctx)
					executor = ws
				}
				if transport == "images" {
					model = "gpt-image-2"
					opts.SourceFormat = translator.FromString(codexOpenAIImageSourceFormat)
					opts.Alt = codexOpenAIImageGenerations
				}
				if transport == "compact" {
					opts.Alt = "responses/compact"
				}
				manager.RegisterExecutor(executor)
				ruleStatus := http.StatusServiceUnavailable
				if transport == "sse" || transport == "websocket" || transport == "images" {
					// Without bootstrap buffering, the existing streamed failure
					// translator uses 500 when this event has no HTTP status.
					ruleStatus = http.StatusInternalServerError
				}
				for range 2 {
					id := uuid.NewString()
					credential := &auth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}, Metadata: map[string]any{"request_scoped_errors": []config.RequestScopedErrorRule{{Status: ruleStatus, Match: []string{"rule-fixture"}, Action: action}}}}
					if _, err := manager.Register(auth.WithSkipPersist(t.Context()), credential); err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: model}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				}
				req := core.Request{Model: model, Payload: []byte(fmt.Sprintf(`{"model":%q,"input":[],"prompt":"draw"}`, model))}
				var err error
				if transport == "http" || transport == "compact" {
					_, err = manager.Execute(ctx, []string{"codex"}, req, opts)
				} else {
					var stream *core.StreamResult
					stream, err = manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
					if stream != nil {
						for chunk := range stream.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				}
				stop := action == "stop" || action == "stop-and-cooldown"
				want := int64(2)
				if stop {
					want = 1
				}
				if calls.Load() != want || (stop && (err == nil || !strings.Contains(err.Error(), "rule-fixture"))) || (!stop && err != nil) {
					t.Fatalf("calls=%d want=%d error=%v", calls.Load(), want, err)
				}
				cooled := 0
				for _, credential := range manager.List() {
					active := credential.NextRetryAfter.After(time.Now())
					for _, state := range credential.ModelStates {
						active = active || state.NextRetryAfter.After(time.Now())
					}
					if active {
						cooled++
					}
				}
				wantCooled := 0
				if transport != "compact" && (action == "stop-and-cooldown" || action == "continue-and-cooldown") {
					wantCooled = 1
				}
				if cooled != wantCooled {
					t.Fatalf("cooled=%d want=%d", cooled, wantCooled)
				}
			})
		}
	}
}
