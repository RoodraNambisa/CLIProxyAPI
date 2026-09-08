package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func liveSecretAPIRequest(s *Server, method, path, token, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, r)
	return w
}

func issueLiveAPISecret(t *testing.T, s *Server, legacy bool) string {
	t.Helper()
	path, body := "/v1/realtime/client_secrets", `{"session":{"model":"gpt-realtime","instructions":"local fixture"}}`
	if legacy {
		path, body = "/v1/realtime/sessions", `{"model":"gpt-realtime","instructions":"local fixture"}`
	}
	w := liveSecretAPIRequest(s, http.MethodPost, path, "test-key", body)
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("local issuance failed: %d", w.Code)
	}
	var payload struct {
		Value        string `json:"value"`
		ClientSecret struct {
			Value string `json:"value"`
		} `json:"client_secret"`
	}
	if errJSON := json.Unmarshal(w.Body.Bytes(), &payload); errJSON != nil {
		t.Fatal("invalid local credential response")
	}
	if legacy {
		payload.Value = payload.ClientSecret.Value
	}
	if !strings.HasPrefix(payload.Value, "ek_") {
		t.Fatal("missing local credential")
	}
	return payload.Value
}

func TestServerCodexLiveSecretSigningAndRouteScope(t *testing.T) {
	s := newTestServerWithConfig(t, func(cfg *config.Config) { cfg.Debug = false; cfg.Codex.LiveEnabled = true })
	defer s.codexLive.Close()
	for _, legacy := range []bool{false, true} {
		token := issueLiveAPISecret(t, s, legacy)
		for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/alpha/search", "/v1/realtime/client_secrets", "/v1/realtime/sessions", "/v1/realtime/calls/unknown/hangup"} {
			if w := liveSecretAPIRequest(s, http.MethodPost, path, token, `{}`); w.Code != 401 {
				t.Fatalf("temporary credential escaped route scope at %s: %d", path, w.Code)
			}
		}
		for _, path := range []string{"/v1/realtime", "/v1/live/unknown", "/v1/realtime/calls/unknown"} {
			if w := liveSecretAPIRequest(s, http.MethodGet, path, token, ""); w.Code != 426 {
				t.Fatalf("temporary credential did not reach native handshake validation at %s: %d", path, w.Code)
			}
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
	for _, path := range []string{"/v1/realtime/client_secrets", "/v1/realtime/sessions"} {
		for _, token := range []string{"", "test-key"} {
			w := liveSecretAPIRequest(s, http.MethodPost, path, token, `{}`)
			want := 401
			if token != "" {
				want = 503
			}
			if w.Code != want {
				t.Fatalf("disabled signing status=%d, want %d", w.Code, want)
			}
		}
	}
}

type liveAccessFailureProvider struct{ err *sdkaccess.AuthError }

func (liveAccessFailureProvider) Identifier() string { return "live-fixture" }
func (p liveAccessFailureProvider) Authenticate(context.Context, *http.Request) (*sdkaccess.Result, *sdkaccess.AuthError) {
	return nil, p.err
}

func TestServerCodexLiveSecretPreservesConfiguredAuthAndFailsClosed(t *testing.T) {
	configured := "ek_" + strings.Repeat("a", 43)
	s := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.Debug = false
		cfg.Codex.LiveEnabled = true
		cfg.APIKeys = append(cfg.APIKeys, configured)
	})
	defer s.codexLive.Close()
	if w := liveSecretAPIRequest(s, http.MethodGet, "/v1/realtime", configured, ""); w.Code != 426 {
		t.Fatal("valid configured ek_ key was mistaken for a local credential")
	}
	token := issueLiveAPISecret(t, s, false)
	for _, failure := range []*sdkaccess.AuthError{
		sdkaccess.NewInternalAuthError("fixture unavailable", nil),
		{Code: "fixture_denied", Message: "fixture denied", StatusCode: 403},
	} {
		s.accessManager.SetProviders([]sdkaccess.Provider{liveAccessFailureProvider{err: failure}})
		if w := liveSecretAPIRequest(s, http.MethodGet, "/v1/realtime", token, ""); w.Code != failure.StatusCode {
			t.Fatal("local credential bypassed an authentication service failure")
		}
	}
	s.accessManager.SetProviders(nil)
	if w := liveSecretAPIRequest(s, http.MethodGet, "/v1/realtime", "", ""); w.Code != 401 {
		t.Fatal("native realtime became anonymous when no providers were installed")
	}
	if w := liveSecretAPIRequest(s, http.MethodGet, "/v1/realtime", token, ""); w.Code != 426 {
		t.Fatal("explicit local grant stopped working without configured providers")
	}
}

func TestServerCodexLiveSecretNativeTransportsAndHotDisable(t *testing.T) {
	var dials, calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = io.Copy(io.Discard, r.Body)
			if strings.HasSuffix(r.URL.Path, "/hangup") {
				w.WriteHeader(204)
				return
			}
			w.Header().Set("Location", fmt.Sprintf("/v1/realtime/calls/call_secret_%d", calls.Add(1)))
			w.WriteHeader(201)
			_, _ = w.Write([]byte("v=0"))
			return
		}
		dials.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-oauth" || strings.Contains(r.Header.Get("Sec-WebSocket-Protocol"), "openai-insecure-api-key.") {
			t.Error("client credential reached upstream")
		}
		upgrader := websocket.Upgrader{Subprotocols: []string{"realtime"}}
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
	s := newTestServerWithConfig(t, func(cfg *config.Config) { cfg.Debug = false; cfg.ProxyURL = "direct"; cfg.Codex.LiveEnabled = true })
	defer s.codexLive.Close()
	m := s.handlers.AuthManager
	m.RegisterExecutor(&apiLiveHTTPExecutor{CodexAutoExecutor: executor.NewCodexAutoExecutor(s.currentConfig()), upstream: upstream.URL})
	a := &auth.Auth{ID: "live-secret-api-" + t.Name(), Provider: "codex", Metadata: map[string]any{"access_token": "fixture-oauth"}}
	if _, errRegister := m.Register(t.Context(), a); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(a.ID, "codex", registry.GetCodexRealtimeModels())
	defer reg.UnregisterClient(a.ID)
	token := issueLiveAPISecret(t, s, false)
	if calls.Load() != 0 || dials.Load() != 0 {
		t.Fatal("issuing a local credential connected upstream")
	}
	server := httptest.NewServer(s.engine)
	defer server.Close()
	base := "ws" + strings.TrimPrefix(server.URL, "http")
	var active *websocket.Conn
	for _, browser := range []bool{false, true} {
		dialer := *websocket.DefaultDialer
		headers := http.Header{"Authorization": {"Bearer " + token}}
		if browser {
			headers = nil
			dialer.Subprotocols = []string{"realtime", "openai-insecure-api-key." + token}
		}
		conn, response, errDial := dialer.Dial(base+"/v1/realtime", headers)
		if response != nil {
			_ = response.Body.Close()
		}
		if errDial != nil {
			t.Fatal("temporary direct connection failed")
		}
		defer func() { _ = conn.Close() }()
		_, seed, errRead := conn.ReadMessage()
		if errRead != nil || !strings.Contains(string(seed), "session.update") {
			t.Fatal("initial session was not relayed")
		}
		active = conn
	}
	for _, path := range []string{"/v1/live", "/v1/realtime", "/v1/realtime/calls"} {
		if w := liveSecretAPIRequest(s, http.MethodPost, path, token, `{"sdp":"v=0"}`); w.Code != 201 {
			t.Fatalf("temporary native call failed: %d", w.Code)
		}
	}
	conn, response, errDial := websocket.DefaultDialer.Dial(base+"/v1/realtime?call_id=call_secret_1", http.Header{"Authorization": {"Bearer " + token}})
	if response != nil {
		_ = response.Body.Close()
	}
	if errDial != nil {
		t.Fatal("temporary sideband connection failed")
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
	for _, existing := range []*websocket.Conn{active, conn} {
		if errWrite := existing.WriteMessage(websocket.TextMessage, []byte("still-running")); errWrite != nil {
			t.Fatal(errWrite)
		}
		_, body, errRead := existing.ReadMessage()
		if errRead != nil || string(body) != "still-running" {
			t.Fatal("hot disable interrupted an established temporary connection")
		}
	}
	for _, path := range []string{"/v1/realtime", "/v1/live/call_secret_2", "/v1/realtime/calls/call_secret_3", "/v1/realtime?call_id=call_secret_1"} {
		_, rejected, errRejected := websocket.DefaultDialer.Dial(base+path, http.Header{"Authorization": {"Bearer " + token}})
		if errRejected == nil || rejected == nil || rejected.StatusCode != 503 {
			t.Fatal("old credential bypassed disabled connection admission")
		}
		body, _ := io.ReadAll(rejected.Body)
		_ = rejected.Body.Close()
		if !strings.Contains(string(body), "codex_live_disabled") {
			t.Fatal("wrong local admission error")
		}
	}
	for _, path := range []string{"/v1/live", "/v1/realtime", "/v1/realtime/calls"} {
		if w := liveSecretAPIRequest(s, http.MethodPost, path, token, `{"sdp":"v=0"}`); w.Code != 503 {
			t.Fatal("old credential created a disabled call")
		}
	}
	if w := liveSecretAPIRequest(s, http.MethodPost, "/v1/realtime/calls/call_secret_1/hangup", "test-key", `{}`); w.Code != 204 {
		t.Fatal("issuer could not clean up its temporary call while disabled")
	}
	if calls.Load() != 3 || dials.Load() != 3 {
		t.Fatal("native temporary routes made extra upstream calls")
	}
}
