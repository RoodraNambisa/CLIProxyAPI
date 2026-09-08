package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type apiLiveDialExecutor struct {
	*executor.CodexAutoExecutor
	upstream string
	calls    atomic.Int32
}

func (e *apiLiveDialExecutor) DialCodexLiveWebsocket(ctx context.Context, a *auth.Auth, target string, headers http.Header, protocols []string) (*websocket.Conn, *http.Response, error) {
	e.calls.Add(1)
	original, errURL := url.Parse(target)
	if errURL != nil {
		return nil, nil, errURL
	}
	return e.CodexAutoExecutor.DialCodexLiveWebsocket(ctx, a, e.upstream+original.RequestURI(), headers, protocols)
}

func TestServerCodexLiveRouteAlwaysAuthenticatesAndDefaultsOff(t *testing.T) {
	s := newTestServerWithConfig(t, func(cfg *config.Config) { cfg.Debug = false; cfg.WebsocketAuth = false })
	defer s.codexLive.Close()
	for _, token := range []string{"", "test-key"} {
		r := httptest.NewRequest(http.MethodGet, "/v1/realtime", nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, r)
		want := 401
		if token != "" {
			want = 503
		}
		if w.Code != want {
			t.Fatalf("realtime status=%d, want %d", w.Code, want)
		}
		if token != "" && !strings.Contains(w.Body.String(), "codex_live_disabled") {
			t.Fatal("default-off route did not report the live error code")
		}
	}
}

func TestServerCodexLiveRouteSharesReadinessAndPreservesGetAuditExemption(t *testing.T) {
	startup := NewStartupState()
	s := newTestServerWithConfigAndOptions(t, func(cfg *config.Config) {
		cfg.Debug = false
		cfg.Codex.LiveEnabled = true
		cfg.RequestBodyAudit = config.RequestBodyAuditConfig{Enable: true, Keywords: []string{"blocked-live-fixture"}, Error: config.RequestBodyAuditErrorConfig{StatusCode: 451, Code: "fixture_block", Message: "fixture block"}}
	}, WithStartupState(startup))
	defer s.codexLive.Close()
	for _, ready := range []bool{false, true} {
		if ready {
			startup.MarkReady()
		}
		r := httptest.NewRequest(http.MethodGet, "/v1/realtime", strings.NewReader(`{"fixture":"blocked-live-fixture"}`))
		r.Header.Set("Authorization", "Bearer test-key")
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, r)
		want := 503
		if ready {
			// The existing body auditor exempts GET, HEAD and OPTIONS requests.
			want = 426
		}
		if w.Code != want {
			t.Fatalf("realtime changed readiness/GET handling: %d, want %d", w.Code, want)
		}
	}
}

func TestServerCodexLiveHotUpdateAndShutdownReachEstablishedConnection(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			kind, data, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			if conn.WriteMessage(kind, data) != nil {
				return
			}
		}
	}))
	defer upstream.Close()
	s := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.Debug = false
		cfg.ProxyURL = "direct"
		cfg.Codex.LiveEnabled = true
		cfg.WebsocketAuth = false
	})
	defer s.codexLive.Close()
	manager := s.handlers.AuthManager
	e := &apiLiveDialExecutor{CodexAutoExecutor: executor.NewCodexAutoExecutor(s.currentConfig()), upstream: "ws" + strings.TrimPrefix(upstream.URL, "http")}
	manager.RegisterExecutor(e)
	a := &auth.Auth{ID: "api-live-" + t.Name(), Provider: "codex", Metadata: map[string]any{"access_token": "fixture", "installation_id": "fixture-installation"}}
	if _, errRegister := manager.Register(t.Context(), a); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(a.ID, "codex", registry.GetCodexRealtimeModels())
	defer reg.UnregisterClient(a.ID)
	server := httptest.NewServer(s.engine)
	defer server.Close()
	target := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime"
	dial := func() (*websocket.Conn, *http.Response, error) {
		return websocket.DefaultDialer.Dial(target, http.Header{"Authorization": {"Bearer test-key"}})
	}
	conn, response, errDial := dial()
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if errDial != nil {
		t.Fatal(errDial)
	}
	defer func() { _ = conn.Close() }()
	updated, errClone := config.Clone(s.currentConfig())
	if errClone != nil {
		t.Fatal(errClone)
	}
	updated.Codex.LiveEnabled = false
	if errUpdate := s.UpdateClients(updated); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte("still-active")); errWrite != nil {
		t.Fatal(errWrite)
	}
	_, data, errRead := conn.ReadMessage()
	if errRead != nil || string(data) != "still-active" {
		t.Fatal("server config update interrupted an established connection")
	}
	second, rejected, errSecond := dial()
	if second != nil {
		_ = second.Close()
		t.Fatal("server config update left new admission open")
	}
	if errSecond == nil || rejected == nil || rejected.StatusCode != 503 {
		t.Fatal("new connection did not fail before upgrade")
	}
	body, _ := io.ReadAll(rejected.Body)
	_ = rejected.Body.Close()
	if !strings.Contains(string(body), "codex_live_disabled") || e.calls.Load() != 1 {
		t.Fatal("disabled server attempted another upstream connection")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if errStop := s.Stop(ctx); errStop != nil {
		t.Fatal(errStop)
	}
	closed := make(chan error, 1)
	go func() { _, _, errRead := conn.ReadMessage(); closed <- errRead }()
	select {
	case errRead := <-closed:
		if errRead == nil {
			t.Fatal("server shutdown left the websocket open")
		}
	case <-time.After(time.Second):
		t.Fatal("server shutdown did not release native realtime")
	}
}
