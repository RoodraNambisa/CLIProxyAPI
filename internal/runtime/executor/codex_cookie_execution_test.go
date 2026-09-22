package executor

import (
	"context"
	"fmt"
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

func seedCookieExecution(t *testing.T, cfg *config.Config, a *auth.Auth) func(string) {
	t.Helper()
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "cookie-model"}})
	c := helps.StateCredential(a, "cookie-model")
	codexstate.Default.Sync(cfg.Codex.StateOverride, []codexstate.Credential{c})
	t.Cleanup(func() {
		codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
		codexstate.Default.Wait()
		registry.GetGlobalRegistry().UnregisterClient(a.ID)
	})
	return func(value string) {
		codexstate.Default.CookieAction(c.ID, c.Model, "acquire")
		codexstate.Default.Tick(t.Context(), time.Now(), func(context.Context, codexstate.Credential, config.CodexStateOverrideConfig) (codexstate.Result, error) {
			now := time.Now()
			return codexstate.Result{Completed: true, Model: c.Model, State: "valid", ReceivedAt: now, Cookies: codexstate.CaptureCookies(a.Attributes["base_url"]+"/responses", http.Header{"Set-Cookie": {"__oailb=" + value + "; Path=/; Max-Age=3600"}}, now)}, nil
		})
		codexstate.Default.Wait()
		if s := codexstate.Default.CookieSnapshot(c.ID, time.Now()); s == nil || s.Main == nil {
			t.Fatalf("no cookie: %+v", s)
		}
	}
}

const cookieCompletion = `{"type":"response.completed","response":{"id":"fixture","model":"cookie-model","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer kept"}]}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}}`

func TestCodexCookieResponseCompletionAcrossHTTPModes(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, complete := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%v/complete=%v", stream, complete), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("X-Codex-Turn-State") != "" || !strings.Contains(r.Header.Get("Cookie"), "__oailb=route") {
						t.Error("incorrect final identity headers")
					}
					w.Header().Set("Content-Type", "text/event-stream")
					w.Header().Set("X-Codex-Turn-State", "wrong length")
					if complete {
						_, _ = fmt.Fprintf(w, "data: %s\n\n", cookieCompletion)
					} else {
						_, _ = fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\"}}\n\n")
					}
				}))
				defer server.Close()
				cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Strategy: "cookie-only", Acquisition: "manual", Lengths: []int{5}, InvalidateOnStateLengthMismatch: true}}}
				a := &auth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL}, Metadata: map[string]any{"access_token": "fixture"}}
				seedCookieExecution(t, cfg, a)("route")
				ctx := core.WithCodexStateSnapshot(t.Context())
				req := core.Request{Model: "cookie-model", Payload: []byte(`{"model":"cookie-model","input":"unchanged"}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
				e := NewCodexExecutor(cfg)
				if stream {
					res, err := e.ExecuteStream(ctx, a, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for range res.Chunks {
					}
				} else {
					_, _ = e.Execute(ctx, a, req, opts)
				}
				snap := codexstate.Default.CookieSnapshot(a.ID, time.Now())
				if (snap.Main == nil) != complete {
					t.Fatalf("completed=%v snapshot=%+v", complete, snap)
				}
			})
		}
	}
}

func TestCodexCookieWebsocketReusesAndReconnectsWithActualVersion(t *testing.T) {
	var handshakes atomic.Int32
	seen := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handshakes.Add(1)
		seen <- r.Header.Get("Cookie")
		if r.Header.Get("X-Codex-Turn-State") != "" {
			t.Error("WS sent State")
		}
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, http.Header{"X-Codex-Turn-State": {"valid"}})
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
			if err = conn.WriteMessage(websocket.TextMessage, []byte(cookieCompletion)); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	enforce := false
	cfg := &config.Config{Codex: config.CodexConfig{EnforceSoftwareIdentity: &enforce, StateOverride: config.CodexStateOverrideConfig{Enabled: true, Strategy: "cookie-only", Acquisition: "manual", Lengths: []int{5}, MissingReturnedState: "reject", InvalidateOnStateLengthMismatch: true}}}
	a := &auth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL}, Metadata: map[string]any{"access_token": "fixture"}}
	acquire := seedCookieExecution(t, cfg, a)
	acquire("first")
	e := NewCodexWebsocketsExecutor(cfg)
	t.Cleanup(func() { e.CloseExecutionSession(t.Name()) })
	run := func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		ctx = core.WithCodexStateSnapshot(ctx)
		res, err := e.Execute(ctx, a, core.Request{Model: "cookie-model", Payload: []byte(`{"model":"cookie-model","input":"unchanged"}`)}, core.Options{SourceFormat: translator.FormatOpenAIResponse, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(res.Payload), "answer kept") {
			t.Fatal("business response changed")
		}
	}
	run()
	run()
	if handshakes.Load() != 1 {
		t.Fatal("valid cookie unnecessarily reconnected")
	}
	if got := <-seen; !strings.Contains(got, "__oailb=first") {
		t.Fatal("first handshake lacked cookie")
	}
	acquire("second")
	run()
	if handshakes.Load() != 2 {
		t.Fatal("new cookie applied without real handshake")
	}
	if got := <-seen; !strings.Contains(got, "__oailb=second") {
		t.Fatal("reconnect froze old cookie")
	}
	if s := codexstate.Default.CookieSnapshot(a.ID, time.Now()); s.Main == nil || s.Completed < 3 {
		t.Fatalf("missing WS observations: %+v", s)
	}
}

func TestCodexCookieReselectsWithinLogicalRequest(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Strategy: "cookie-only", Acquisition: "manual", Lengths: []int{5}}}}
	a := &auth.Auth{ID: t.Name(), Provider: "codex", Attributes: map[string]string{"base_url": "https://chatgpt.com/backend-api/codex"}}
	acquire := seedCookieExecution(t, cfg, a)
	acquire("first")
	ctx := core.WithCodexStateSnapshot(t.Context())
	headers := http.Header{"Cookie": {"__oailb=custom; unrelated=kept"}, "X-Codex-Turn-State": {"client"}}
	if err := helps.ApplyManagedState(ctx, cfg, a, "cookie-model", headers); err != nil {
		t.Fatal(err)
	}
	acquire("second")
	if err := helps.ApplyManagedState(ctx, cfg, a, "cookie-model", headers); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(headers.Get("Cookie"), "__oailb=second") || !strings.Contains(headers.Get("Cookie"), "unrelated=kept") || headers.Get("X-Codex-Turn-State") != "" {
		t.Fatal("wrong retry headers")
	}
	codexstate.Default.CookieAction(a.ID, "", "clear")
	if err := helps.ApplyManagedState(ctx, cfg, a, "cookie-model", headers); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(headers.Get("Cookie"), "__oailb") {
		t.Fatal("missing policy reused cleared/custom cookie")
	}
}

func TestCodexStateRetryDoesNotMistakeRetiredManagedValueForClientState(t *testing.T) {
	for _, mode := range []string{"missing", "override"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Acquisition: "manual", Mode: mode, MissingPolicy: "continue"}}}
			a := &auth.Auth{ID: t.Name(), Provider: "codex"}
			seedManagedStateResponseTest(t, cfg, a)
			ctx := core.WithCodexStateSnapshot(t.Context())
			headers := http.Header{}
			if err := helps.ApplyManagedState(ctx, cfg, a, "gpt-6-astra", headers); err != nil {
				t.Fatal(err)
			}
			if headers.Get("X-Codex-Turn-State") == "" {
				t.Fatal("managed State missing")
			}
			codexstate.Default.Action(a.ID, "gpt-6-astra", "clear")
			if err := helps.ApplyManagedState(ctx, cfg, a, "gpt-6-astra", headers); err != nil {
				t.Fatal(err)
			}
			if headers.Get("X-Codex-Turn-State") != "" {
				t.Fatal("retired State leaked into retry")
			}
		})
	}
}
