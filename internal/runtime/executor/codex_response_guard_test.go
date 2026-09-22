package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexResponseGuardIndependentTransportAndRewrite(t *testing.T) {
	for _, transport := range []string{"http", "sse", "trusted-sse", "ws", "ws-stream"} {
		for _, tc := range []struct {
			name, returned, mode string
			length               int
			allowed              bool
			missing              bool
			wantBlock            bool
		}{
			{"model-mismatch", "wrong", "enforce", 292, false, false, true},
			{"allowed-model", "alternate", "enforce", 292, true, false, false},
			{"allowed-model-bad-length", "alternate", "enforce", 312, true, false, true},
			{"length-only", "requested", "enforce", 312, false, false, true},
			{"observe", "wrong", "observe", 312, false, false, false},
			{"missing-allow", "", "enforce", 0, false, false, false},
			{"missing-reject", "", "enforce", 0, false, true, true},
		} {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				var calls atomic.Int32
				completed := fmt.Sprintf(`{"type":"response.completed","response":{"id":"done","model":%q,"status":"completed","output":[{"id":"msg","type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}`, tc.returned)
				created := fmt.Sprintf(`{"type":"response.created","response":{"id":"done","model":%q,"status":"in_progress","output":[]}}`, tc.returned)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.Header.Get("X-Codex-Turn-State") != "" || r.Header.Get("Cookie") != "" {
						t.Error("monitoring enabled resource injection")
					}
					h := http.Header{}
					if tc.length > 0 {
						h.Set("X-Codex-Turn-State", strings.Repeat("s", tc.length))
					}
					if strings.HasPrefix(transport, "ws") {
						u := websocket.Upgrader{}
						conn, err := u.Upgrade(w, r, h)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, err = conn.ReadMessage(); err != nil {
							return
						}
						_ = conn.WriteMessage(websocket.TextMessage, []byte(created))
						_ = conn.WriteMessage(websocket.TextMessage, []byte(completed))
						_, _, _ = conn.ReadMessage()
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					for k, v := range h {
						w.Header()[k] = v
					}
					if transport == "trusted-sse" {
						_, _ = fmt.Fprintf(w, "data: %s\n\ndata: {\"type\":\"response.completed\",\ndata: \"response\":%s}\n\n", created, strings.TrimSuffix(strings.SplitN(completed, `"response":`, 2)[1], "}"))
					} else {
						_, _ = fmt.Fprintf(w, "data: %s\n\ndata: %s\n\n", created, completed)
					}
				}))
				cfg := &config.Config{Codex: config.CodexConfig{EnforceSoftwareIdentity: new(false), ResponseGuard: config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new(tc.mode), LengthMode: new("allow"), Lengths: new([]int{292})}}}}
				cfg.ResponseModelRewrite = config.ResponseModelRewriteConfig{Enabled: true, Rules: []config.ResponseModelRewriteRule{{Providers: []string{"codex"}}}}
				if tc.allowed {
					cfg.Codex.ResponseGuard.AllowedReturnedModels = new([]string{"alternate"})
				}
				if tc.missing {
					cfg.Codex.ResponseGuard.MissingModel = new("reject")
					cfg.Codex.ResponseGuard.MissingState = new("reject")
				}
				m := auth.NewManager(nil, nil, nil)
				m.SetConfig(cfg)
				m.SetRetryConfig(0, 0, 1)
				a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}})
				if err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "requested"}})
				var executor auth.ProviderExecutor = NewCodexExecutor(cfg)
				var ws *CodexWebsocketsExecutor
				if strings.HasPrefix(transport, "ws") {
					ws = NewCodexWebsocketsExecutor(cfg)
					executor = ws
				}
				t.Cleanup(func() {
					if ws != nil {
						ws.CloseExecutionSession(t.Name())
					}
					server.Close()
					registry.GetGlobalRegistry().UnregisterClient(a.ID)
				})
				m.RegisterExecutor(executor)
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				req := core.Request{Model: "requested", Payload: []byte(`{"model":"requested","input":"question"}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, OriginalRequest: req.Payload, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name(), core.TrustUpstreamSSEMetadataKey: transport == "trusted-sse"}}
				var output strings.Builder
				if transport == "http" || transport == "ws" {
					var response core.Response
					response, err = m.Execute(ctx, []string{"codex"}, req, opts)
					output.Write(response.Payload)
				} else {
					var stream *core.StreamResult
					stream, err = m.ExecuteStream(ctx, []string{"codex"}, req, opts)
					if err == nil {
						for chunk := range stream.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
							output.Write(chunk.Payload)
						}
					}
				}
				if core.IsResponseGuardError(err) != tc.wantBlock {
					t.Fatalf("block=%v want=%v err=%v output=%s", core.IsResponseGuardError(err), tc.wantBlock, err, output.String())
				}
				if !tc.wantBlock && err != nil {
					t.Fatal(err)
				}
				if tc.wantBlock && output.Len() != 0 {
					t.Fatalf("leaked rejected output: %s", output.String())
				}
				if calls.Load() != 1 {
					t.Fatalf("unexpected retry: %d", calls.Load())
				}
				stats := m.AuthResponseModelRewriteSummary(a, true)
				if len(stats.Recent) != 1 || stats.Recent[0].Validation == nil {
					t.Fatalf("guard/rewrite records duplicated or missing: %+v", stats)
				}
				r := stats.Recent[0].Validation
				if r.OriginalModel != tc.returned || r.StateLength != tc.length {
					t.Fatalf("wrong raw evidence: %+v", r)
				}
				if tc.wantBlock && (stats.Blocked != 1 || stats.Total != 0 || r.ResponseModel != "") {
					t.Fatalf("bad rejected record: %+v", stats)
				}
				if !tc.wantBlock && tc.returned != "" && (stats.Total != 1 || r.ResponseModel != "requested") {
					t.Fatalf("allowed rewrite lost: %+v", stats)
				}
			})
		}
	}
}

func TestResponseGuardRetriesOnlyDifferentCredentials(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, allWrong := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%v/all_wrong=%v", stream, allWrong), func(t *testing.T) {
				var calls atomic.Int32
				var firstAuth atomic.Value
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					n := calls.Add(1)
					if n == 1 {
						firstAuth.Store(r.Header.Get("Authorization"))
					} else if firstAuth.Load() == r.Header.Get("Authorization") {
						t.Error("retried blocked credential")
					}
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					model := "requested"
					if n == 1 || allWrong {
						model = "wrong"
					}
					_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.created\",\"response\":{\"model\":%q}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"done\",\"model\":%q,\"status\":\"completed\",\"output\":[],\"usage\":{\"total_tokens\":3}}}\n\n", model, model)
				}))
				defer upstream.Close()
				cfg := &config.Config{Codex: config.CodexConfig{ResponseGuard: config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("enforce"), OnReject: new("retry")}}}}
				m := auth.NewManager(nil, &auth.FillFirstSelector{}, nil)
				m.SetConfig(cfg)
				m.SetRetryConfig(2, 0, 0)
				m.RegisterExecutor(NewCodexExecutor(cfg))
				var accounts []*auth.Auth
				for _, id := range []string{"a", "b"} {
					a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: t.Name() + id, Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": id, "base_url": upstream.URL}})
					if err != nil {
						t.Fatal(err)
					}
					accounts = append(accounts, a)
					registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "requested"}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
				}
				req := core.Request{Model: "requested", Payload: []byte(`{"model":"requested","input":"question"}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, OriginalRequest: req.Payload}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				ctx = core.WithRequestBodyReleaseController(ctx, core.NewRequestBodyReleaseController(int64(len(req.Payload)), []byte("released")))
				var err error
				if stream {
					var result *core.StreamResult
					result, err = m.ExecuteStream(ctx, []string{"codex"}, req, opts)
					if err == nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				} else {
					_, err = m.Execute(ctx, []string{"codex"}, req, opts)
				}
				if calls.Load() != 2 || (err != nil) != allWrong || allWrong && !core.IsResponseGuardError(err) {
					t.Fatalf("calls=%d allWrong=%v err=%v", calls.Load(), allWrong, err)
				}
				var blocked uint64
				for _, a := range accounts {
					stats := m.AuthResponseModelRewriteSummary(a, true)
					blocked += stats.Blocked
					if len(stats.Recent) != 1 {
						t.Fatalf("attempt missing: %+v", stats)
					}
					current, _ := m.GetByID(a.ID)
					if current.Disabled || current.Quota.Exceeded || !current.NextRetryAfter.IsZero() {
						t.Fatal("guard triggered quota cooldown")
					}
				}
				want := uint64(1)
				if allWrong {
					want = 2
				}
				if blocked != want {
					t.Fatalf("blocked=%d want=%d", blocked, want)
				}
			})
		}
	}
}

func TestResponseGuardLateStreamPolicy(t *testing.T) {
	for _, mode := range []string{"observe", "abort"} {
		t.Run(mode, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"model\":\"requested\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"visible\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"done\",\"model\":\"wrong\",\"status\":\"completed\",\"output\":[]}}\n\n")
			}))
			defer upstream.Close()
			cfg := &config.Config{Codex: config.CodexConfig{ResponseGuard: config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("enforce"), OnReject: new("retry"), LateMismatch: new(mode)}}}}
			a := &auth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result, err := NewCodexExecutor(cfg).ExecuteStream(ctx, a, core.Request{Model: "requested", Payload: []byte(`{"input":"question"}`)}, core.Options{SourceFormat: translator.FormatOpenAIResponse, Metadata: map[string]any{core.TrustUpstreamSSEMetadataKey: true}})
			if err != nil {
				t.Fatal(err)
			}
			var output strings.Builder
			for chunk := range result.Chunks {
				output.Write(chunk.Payload)
				if chunk.Err != nil {
					err = chunk.Err
				}
			}
			if !strings.Contains(output.String(), "visible") || core.IsResponseGuardError(err) != (mode == "abort") {
				t.Fatalf("mode=%s err=%v output=%s", mode, err, output.String())
			}
			if guardErr, ok := err.(*core.ResponseGuardError); ok && (!guardErr.RequestCommitted() || guardErr.RetryOtherAuth()) {
				t.Fatal("late error allowed replay")
			}
		})
	}
}
