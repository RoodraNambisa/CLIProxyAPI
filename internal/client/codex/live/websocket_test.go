package live

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func newDirectLiveFixture(t *testing.T, cfg *config.Config, upstream http.HandlerFunc) (*Handler, *auth.Manager, string, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg.ProxyURL = "direct"
	provider := httptest.NewServer(upstream)
	t.Cleanup(provider.Close)
	m := auth.NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	m.RegisterExecutor(executor.NewCodexAutoExecutor(cfg))
	a := &auth.Auth{ID: "live-direct-" + t.Name(), Provider: "codex", Attributes: map[string]string{"priority": "9"}, Metadata: map[string]any{"access_token": "fixture-oauth", "account_id": "fixture-account", "installation_id": "fixture-installation"}}
	if _, errRegister := m.Register(t.Context(), a); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(a.ID, "codex", registry.GetCodexRealtimeModels())
	t.Cleanup(func() { reg.UnregisterClient(a.ID) })
	h := NewHandler(cfg, m)
	h.websocketBaseURL = "ws" + strings.TrimPrefix(provider.URL, "http") + "/v1"
	t.Cleanup(h.Close)
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		switch c.GetHeader("Authorization") {
		case "Bearer fixture-caller":
			c.Set("apiKey", "fixture-caller")
		case "Bearer denied-caller":
			c.Set("apiKey", "denied-caller")
			c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: "claude"})
		}
	})
	engine.GET("/v1/realtime", h.HandleDirectWebsocket)
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	return h, m, server.URL + "/v1/realtime", a.ID
}

func liveTestDial(t *testing.T, endpoint, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	dialer := websocket.Dialer{Subprotocols: []string{"realtime", "openai-insecure-api-key.fixture-browser"}}
	return dialer.Dial("ws"+strings.TrimPrefix(endpoint, "http"), http.Header{"Authorization": {"Bearer " + token}, "Cookie": {"downstream=fixture"}, "Chatgpt-Account-Id": {"wrong-account"}})
}

func TestLiveDirectWebsocketRemainsActiveAfterDisableAndEndsOnRetirement(t *testing.T) {
	var calls atomic.Int32
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, manager, endpoint, authID := newDirectLiveFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/realtime" || r.URL.Query().Get("model") != registry.CodexRealtimeModelID || r.Header.Get("Authorization") != "Bearer fixture-oauth" || r.Header.Get("Chatgpt-Account-Id") != "fixture-account" || r.Header.Get("Cookie") != "" || r.Header.Get("Sec-Websocket-Protocol") != "realtime" {
			t.Error("native realtime request leaked caller identity or changed routing")
		}
		upgrader := websocket.Upgrader{Subprotocols: []string{"realtime"}}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer closeRealtimeSocket(conn)
		for {
			kind, data, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			if conn.WriteMessage(kind, data) != nil {
				return
			}
		}
	})
	conn, response, errDial := liveTestDial(t, endpoint, "fixture-caller")
	closeRealtimeResponse(response)
	if errDial != nil {
		t.Fatal(errDial)
	}
	defer closeRealtimeSocket(conn)
	for i := 0; i < 2; i++ {
		if i == 1 {
			h.UpdateConfig(&config.Config{})
		}
		event := []byte(`{"type":"input_audio_buffer.append","audio":"fixture"}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, event); errWrite != nil {
			t.Fatal(errWrite)
		}
		_, got, errRead := conn.ReadMessage()
		if errRead != nil || string(got) != string(event) {
			t.Fatal("established realtime connection did not preserve native events")
		}
	}
	second, rejected, errSecond := liveTestDial(t, endpoint, "fixture-caller")
	if second != nil {
		closeRealtimeSocket(second)
		t.Fatal("disabled handler opened another connection")
	}
	if errSecond == nil || rejected == nil || rejected.StatusCode != 503 {
		t.Fatal("disabled connection did not fail before upgrade")
	}
	body, _ := io.ReadAll(rejected.Body)
	closeRealtimeResponse(rejected)
	if !strings.Contains(string(body), liveDisabledCode) || calls.Load() != 1 {
		t.Fatal("disabled request reached the upstream or lost its error code")
	}
	if errDelete := manager.Delete(t.Context(), authID); errDelete != nil {
		t.Fatal(errDelete)
	}
	closed := make(chan error, 1)
	go func() { _, _, errRead := conn.ReadMessage(); closed <- errRead }()
	select {
	case errRead := <-closed:
		if errRead == nil {
			t.Fatal("retired connection remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("credential retirement leaked realtime connection")
	}
}

func TestLiveDirectRejectsUnauthenticatedUnsupportedAndMalformedRequestsWithoutUpstream(t *testing.T) {
	var calls atomic.Int32
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, _, endpoint, _ := newDirectLiveFixture(t, cfg, func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(500) })
	for _, tc := range []struct {
		token  string
		status int
	}{{"missing", 401}, {"denied-caller", 403}} {
		conn, response, errDial := liveTestDial(t, endpoint, tc.token)
		if conn != nil {
			closeRealtimeSocket(conn)
			t.Fatal("unauthorized connection upgraded")
		}
		if errDial == nil || response == nil || response.StatusCode != tc.status {
			t.Fatal("wrong access rejection")
		}
		closeRealtimeResponse(response)
	}
	for _, upgrade := range []bool{false, true} {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint, nil)
		req.Header.Set("Authorization", "Bearer fixture-caller")
		want := 426
		if upgrade {
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
			req.Header.Set("Sec-WebSocket-Version", "13")
			req.Header.Set("Sec-WebSocket-Key", "invalid")
			want = 400
		}
		response, errRequest := http.DefaultClient.Do(req)
		if errRequest != nil {
			t.Fatal(errRequest)
		}
		closeRealtimeResponse(response)
		if response.StatusCode != want {
			t.Fatal("wrong handshake preflight result")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("rejected request selected and connected upstream")
	}
	h.Close()
}

func TestLiveDirectDisableCancelsAnUpstreamHandshake(t *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, _, endpoint, _ := newDirectLiveFixture(t, cfg, func(_ http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(cancelled) })
	finished := make(chan *http.Response, 1)
	go func() {
		conn, response, _ := liveTestDial(t, endpoint, "fixture-caller")
		closeRealtimeSocket(conn)
		finished <- response
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("upstream handshake did not start")
	}
	h.UpdateConfig(&config.Config{})
	select {
	case response := <-finished:
		if response == nil || response.StatusCode != 503 {
			t.Fatal("cancelled setup did not reject before upgrade")
		}
		body, _ := io.ReadAll(response.Body)
		closeRealtimeResponse(response)
		if !strings.Contains(string(body), liveDisabledCode) {
			t.Fatal("disabled setup lost its error code")
		}
	case <-time.After(time.Second):
		t.Fatal("disabled handshake did not cancel")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("upstream handshake socket leaked")
	}
}

func TestLiveDirectHandshakeErrorsPreserveStatusAndResponseRewriteSource(t *testing.T) {
	for _, code := range []int{401, 429, 404, 500} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls atomic.Int32
			cfg := &config.Config{}
			cfg.Codex.LiveEnabled = true
			cfg.ErrorResponseRewrites = []config.ErrorResponseRewriteRule{{Sources: []string{"codex"}, AuthPriorities: []int{9}, StatusCode: 500, ResponseStatusCode: 418}}
			_, _, endpoint, _ := newDirectLiveFixture(t, cfg, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "3")
				w.Header().Set("Set-Cookie", "private=fixture")
				w.WriteHeader(code)
				_, _ = io.WriteString(w, `{"error":{"code":"misalignment_policy_violation","message":"fixture"}}`)
			})
			conn, response, errDial := liveTestDial(t, endpoint, "fixture-caller")
			if conn != nil {
				closeRealtimeSocket(conn)
				t.Fatal("failed handshake upgraded")
			}
			want := code
			if code == 404 {
				want = 501
			}
			if code == 500 {
				want = 418
			}
			if errDial == nil || response == nil || response.StatusCode != want || calls.Load() != 1 {
				t.Fatal("handshake status, rewrite source or attempt count changed")
			}
			body, _ := io.ReadAll(response.Body)
			closeRealtimeResponse(response)
			if response.Header.Get("Set-Cookie") != "" {
				t.Fatal("upstream cookie leaked")
			}
			if code == 429 && response.Header.Get("Retry-After") != "3" {
				t.Fatal("rate limit retry header lost")
			}
			if code == 404 {
				if !strings.Contains(string(body), "realtime_capability_not_supported") {
					t.Fatal("unsupported capability was not explicit")
				}
			} else if !strings.Contains(string(body), "misalignment_policy_violation") {
				t.Fatal("public upstream error changed")
			}
		})
	}
}

type liveDialCompletedExecutor struct {
	*executor.CodexAutoExecutor
	after func()
}

func (e *liveDialCompletedExecutor) DialCodexLiveWebsocket(ctx context.Context, a *auth.Auth, target string, headers http.Header, protocols []string) (*websocket.Conn, *http.Response, error) {
	conn, response, errDial := e.CodexAutoExecutor.DialCodexLiveWebsocket(ctx, a, target, headers, protocols)
	if errDial == nil {
		e.after()
	}
	return conn, response, errDial
}

func TestLiveDirectRollsBackUpstreamAllocatedBeforeDisable(t *testing.T) {
	closed := make(chan struct{})
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, m, endpoint, _ := newDirectLiveFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer closeRealtimeSocket(conn)
		_, _, _ = conn.ReadMessage()
		close(closed)
	})
	m.RegisterExecutor(&liveDialCompletedExecutor{CodexAutoExecutor: executor.NewCodexAutoExecutor(cfg), after: func() { h.UpdateConfig(&config.Config{}) }})
	conn, response, errDial := liveTestDial(t, endpoint, "fixture-caller")
	if conn != nil {
		closeRealtimeSocket(conn)
		t.Fatal("old admission committed after disabling")
	}
	if errDial == nil || response == nil || response.StatusCode != 503 {
		t.Fatal("allocated upstream bypassed the final admission check")
	}
	body, _ := io.ReadAll(response.Body)
	closeRealtimeResponse(response)
	if !strings.Contains(string(body), liveDisabledCode) {
		t.Fatal("rollback lost the disabled error code")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("uncommitted upstream connection leaked")
	}
}

func TestLiveDirectResolvesCredentialPrefixAndModelAlias(t *testing.T) {
	model := make(chan string, 1)
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	_, m, endpoint, authID := newDirectLiveFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		model <- r.URL.Query().Get("model")
		upgrader := websocket.Upgrader{}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer closeRealtimeSocket(conn)
		_, _, _ = conn.ReadMessage()
	})
	a, _ := m.GetByID(authID)
	a.Prefix = "team"
	if _, errUpdate := m.Update(t.Context(), a); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	m.SetOAuthModelAlias(map[string][]config.OAuthModelAlias{"codex": {{Name: registry.CodexLiveModelID, Alias: "voice"}}})
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: "team/voice", UpstreamID: registry.CodexLiveModelID, Type: registry.CodexRealtimeModelType}})
	conn, response, errDial := liveTestDial(t, endpoint+"?model=team%2Fvoice", "fixture-caller")
	closeRealtimeResponse(response)
	if errDial != nil {
		t.Fatal(errDial)
	}
	closeRealtimeSocket(conn)
	if got := <-model; got != registry.CodexRealtimeModelID {
		t.Fatal("public prefix or alias was sent as the upstream model")
	}
}

func TestLiveDirectDefaultOriginatorAndExplicitHeader(t *testing.T) {
	enabled, disabled := true, false
	for _, identity := range []struct {
		name                  string
		enforce               *bool
		userAgent, originator string
	}{{"default", nil, "", "codex_cli_rs"}, {"enforced", &enabled, "local-codex/0.153.4 (Linux; arm64) tmux/3.5", "local-codex"}, {"disabled", &disabled, "", ""}} {
		for _, originator := range []string{"", "Fixture Client"} {
			t.Run(identity.name+"/"+map[bool]string{true: "default", false: "explicit"}[originator == ""], func(t *testing.T) {
				observed := make(chan string, 1)
				cfg := &config.Config{}
				cfg.Codex.LiveEnabled = true
				cfg.Codex.EnforceSoftwareIdentity = identity.enforce
				cfg.CodexHeaderDefaults.UserAgent = identity.userAgent
				_, _, endpoint, _ := newDirectLiveFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
					observed <- r.Header.Get("Originator")
					upgrader := websocket.Upgrader{}
					conn, errUpgrade := upgrader.Upgrade(w, r, nil)
					if errUpgrade != nil {
						t.Error(errUpgrade)
						return
					}
					defer closeRealtimeSocket(conn)
					_, _, _ = conn.ReadMessage()
				})
				headers := http.Header{"Authorization": {"Bearer fixture-caller"}}
				if originator != "" {
					headers.Set("Originator", originator)
				}
				conn, response, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(endpoint, "http"), headers)
				closeRealtimeResponse(response)
				if errDial != nil {
					t.Fatal(errDial)
				}
				closeRealtimeSocket(conn)
				want := originator
				if want == "" {
					want = "Codex Desktop"
				}
				if identity.originator != "" {
					want = identity.originator
				}
				if got := <-observed; got != want {
					t.Fatalf("upstream Originator=%q, want %q", got, want)
				}
				if cfg.CodexHeaderDefaults.UserAgent != identity.userAgent {
					t.Fatal("native fallback rewrote the saved software identity")
				}
			})
		}
	}
}
