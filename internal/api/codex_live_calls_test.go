package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type apiLiveHTTPExecutor struct {
	*executor.CodexAutoExecutor
	upstream string
}

func (e *apiLiveHTTPExecutor) HttpRequest(ctx context.Context, a *auth.Auth, request *http.Request) (*http.Response, error) {
	target, errURL := url.Parse(e.upstream + request.URL.RequestURI())
	if errURL != nil {
		return nil, errURL
	}
	copy := request.Clone(ctx)
	copy.URL, copy.Host = target, target.Host
	return e.CodexAutoExecutor.HttpRequest(ctx, a, copy)
}

func liveAPIPost(s *Server, path string, token bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"sdp":"v=0"}`))
	if token {
		r.Header.Set("Authorization", "Bearer test-key")
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, r)
	return w
}

func TestServerCodexLiveCreateAliasesAuthenticateAndDefaultOff(t *testing.T) {
	s := newTestServerWithConfig(t, func(cfg *config.Config) { cfg.Debug = false })
	defer s.codexLive.Close()
	for _, path := range []string{"/v1/live", "/v1/realtime", "/v1/realtime/calls"} {
		for _, token := range []bool{false, true} {
			w := liveAPIPost(s, path, token)
			want := 401
			if token {
				want = 503
			}
			if w.Code != want || (token && !strings.Contains(w.Body.String(), "codex_live_disabled")) {
				t.Fatalf("alias %s status=%d, want %d", path, w.Code, want)
			}
		}
	}
	w := liveAPIPost(s, "/v1/realtime/calls/unknown/hangup", true)
	if w.Code != 404 || strings.Contains(w.Body.String(), "codex_live_disabled") {
		t.Fatal("disabled new admission blocked cleanup routing")
	}
}

func TestServerCodexLiveCallsPreservePostBodyAudit(t *testing.T) {
	s := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.Debug = false
		cfg.Codex.LiveEnabled = true
		cfg.RequestBodyAudit = config.RequestBodyAuditConfig{Enable: true, Keywords: []string{"v=0"}, Error: config.RequestBodyAuditErrorConfig{StatusCode: 451, Code: "fixture_live_audit", Message: "fixture"}}
	})
	defer s.codexLive.Close()
	for _, path := range []string{"/v1/live", "/v1/realtime", "/v1/realtime/calls"} {
		if w := liveAPIPost(s, path, true); w.Code != 451 || !strings.Contains(w.Body.String(), "fixture_live_audit") {
			t.Fatal("native call bypassed configured body audit")
		}
	}
}

func TestServerCodexLiveCreatesAndHangsUpAfterHotDisable(t *testing.T) {
	var creations, hangups atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if strings.HasSuffix(r.URL.Path, "/hangup") {
			hangups.Add(1)
			w.WriteHeader(204)
			return
		}
		if r.URL.Path != "/backend-api/codex/realtime/calls" {
			t.Error("wrong native route")
		}
		w.Header().Set("Location", fmt.Sprintf("/v1/realtime/calls/call_server_%d", creations.Add(1)))
		w.WriteHeader(201)
		_, _ = w.Write([]byte("v=0"))
	}))
	defer upstream.Close()
	s := newTestServerWithConfig(t, func(cfg *config.Config) { cfg.Debug = false; cfg.ProxyURL = "direct"; cfg.Codex.LiveEnabled = true })
	defer s.codexLive.Close()
	m := s.handlers.AuthManager
	m.RegisterExecutor(&apiLiveHTTPExecutor{CodexAutoExecutor: executor.NewCodexAutoExecutor(s.currentConfig()), upstream: upstream.URL})
	a := &auth.Auth{ID: "live-call-server-" + t.Name(), Provider: "codex", Metadata: map[string]any{"access_token": "fixture-oauth"}}
	if _, errRegister := m.Register(t.Context(), a); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(a.ID, "codex", registry.GetCodexRealtimeModels())
	defer reg.UnregisterClient(a.ID)
	for _, path := range []string{"/v1/live", "/v1/realtime", "/v1/realtime/calls"} {
		if w := liveAPIPost(s, path, true); w.Code != 201 {
			t.Fatalf("native creation failed: %d", w.Code)
		}
	}
	updated, errClone := config.Clone(s.currentConfig())
	if errClone != nil {
		t.Fatal(errClone)
	}
	updated.Codex.LiveEnabled = false
	if errUpdate := s.UpdateClients(updated); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	for i := 1; i <= 3; i++ {
		if w := liveAPIPost(s, fmt.Sprintf("/v1/realtime/calls/call_server_%d/hangup", i), true); w.Code != 204 {
			t.Fatalf("disabled cleanup failed: %d", w.Code)
		}
	}
	if creations.Load() != 3 || hangups.Load() != 3 {
		t.Fatal("native route made extra requests")
	}
}

func TestServerCodexLiveUnsupportedCapabilitiesStayExplicit(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		s := newTestServerWithConfig(t, func(cfg *config.Config) { cfg.Debug = false; cfg.Codex.LiveEnabled = enabled })
		defer s.codexLive.Close()
		for _, path := range []string{"/v1/realtime/transcription_sessions", "/v1/realtime/translations", "/v1/realtime/translations/client_secrets", "/v1/realtime/calls/call_fixture/accept", "/v1/realtime/calls/call_fixture/reject", "/v1/realtime/calls/call_fixture/refer"} {
			if w := liveAPIPost(s, path, true); w.Code != 501 || !strings.Contains(w.Body.String(), "realtime_capability_not_supported") {
				t.Fatalf("unsupported %s did not return an explicit result", path)
			}
			if w := liveAPIPost(s, path, false); w.Code != 401 {
				t.Fatal("unsupported route bypassed authentication")
			}
		}
		r := httptest.NewRequest(http.MethodGet, "/v1/realtime/translations", nil)
		r.Header.Set("Authorization", "Bearer test-key")
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, r)
		if w.Code != 501 {
			t.Fatal("translation WebSocket entry was advertised as supported")
		}
	}
}
