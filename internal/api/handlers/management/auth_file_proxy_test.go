package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestAuthFileProxyRoutePrecedenceAndSafeDisplay(t *testing.T) {
	cfg := &config.Config{}
	cfg.ProxyURL = "socks5h://global-user:global-secret@global.example:1080"
	h := &Handler{cfg: cfg}
	for _, test := range []struct{ raw, source, mode, address string }{
		{"", "global", "proxy", "socks5h://global.example:1080"},
		{"http://credential-user:credential-secret@127.0.0.1:9999", "auth", "proxy", "http://127.0.0.1:9999"},
		{"direct", "auth", "direct", ""},
		{"unsupported://username:password@invalid.example", "auth", "invalid", ""},
	} {
		auth := &coreauth.Auth{ID: "test.json", ProxyURL: test.raw}
		got := h.authFileProxyRoute(auth, authFileRuntimeSummary{})
		if got.Source != test.source || got.Mode != test.mode || got.Address != test.address || got.IP != "" || got.CheckedAt != nil {
			t.Fatalf("incorrect route: %+v", got)
		}
		encoded, _ := json.Marshal(got)
		for _, secret := range []string{"credential-secret", "global-secret", "credential-user", "global-user", "password", "username"} {
			if strings.Contains(string(encoded), secret) {
				t.Fatal("proxy authentication exposed")
			}
		}
	}
}

func TestAuthFileProxyCheckUsesCredentialRouteWithoutAccountHeadersAndInvalidates(t *testing.T) {
	var calls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("X-Secret") != "" {
			t.Error("account or management credentials sent to IP checker")
		}
		_, _ = w.Write([]byte("ip=203.0.113.7\nloc=JP\n"))
	}))
	defer proxy.Close()
	previous := proxyTraceURL
	proxyTraceURL = "http://egress-check.invalid/cdn-cgi/trace"
	t.Cleanup(func() { proxyTraceURL = previous })
	cfg := &config.Config{}
	cfg.ProxyURL = "http://127.0.0.1:1"
	manager := coreauth.NewManager(nil, nil, nil)
	auth, err := manager.Register(t.Context(), &coreauth.Auth{ID: "chosen.json", FileName: "chosen.json", Provider: "xai", ProxyURL: proxy.URL, Attributes: map[string]string{"header:X-Secret": "account-header"}, Metadata: map[string]any{"access_token": "account-secret"}})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, authManager: manager}
	before := h.authFileProxyRoute(auth, authFileRuntimeSummary{})
	if calls.Load() != 0 {
		t.Fatal("display checked the network")
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/proxy/check", strings.NewReader(`{"name":"chosen.json"}`))
	c.Request.Header.Set("Authorization", "Bearer management-secret")
	h.CheckAuthFileProxy(c)
	var result struct {
		Route authFileProxyRoute `json:"proxy_route"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || result.Route.IP != "203.0.113.7" || result.Route.OK == nil || !*result.Route.OK || calls.Load() != 1 {
		t.Fatalf("check failed or wrong route: %d %s calls=%d", w.Code, w.Body.String(), calls.Load())
	}
	current := h.authFileProxyRoute(auth, authFileRuntimeSummary{})
	if current.IP != result.Route.IP || current.ID != before.ID || calls.Load() != 1 {
		t.Fatal("check was not retained without reprobe")
	}
	changed := auth.Clone()
	changed.ProxyURL = "direct"
	if _, err := manager.Update(coreauth.WithSkipPersist(t.Context()), changed); err != nil {
		t.Fatal(err)
	}
	current = h.authFileProxyRoute(changed, authFileRuntimeSummary{})
	if current.IP != "" || current.ID == before.ID {
		t.Fatal("old proxy IP survived a route change")
	}
}

func TestAuthFileProxyCheckRefusesChangedRouteDuringProbe(t *testing.T) {
	cfg := &config.Config{}
	manager := coreauth.NewManager(nil, nil, nil)
	var auth *coreauth.Auth
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		changed := auth.Clone()
		changed.ProxyURL = "direct"
		_, _ = manager.Update(coreauth.WithSkipPersist(t.Context()), changed)
		_, _ = w.Write([]byte("ip=203.0.113.8\nloc=US\n"))
	}))
	defer proxy.Close()
	var err error
	auth, err = manager.Register(t.Context(), &coreauth.Auth{ID: "chosen.json", FileName: "chosen.json", Provider: "codex", ProxyURL: proxy.URL})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, authManager: manager}
	previous := proxyTraceURL
	proxyTraceURL = "http://egress-check.invalid/trace"
	t.Cleanup(func() { proxyTraceURL = previous })
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/proxy/check", strings.NewReader(`{"name":"chosen.json"}`))
	h.CheckAuthFileProxy(c)
	if w.Code != 409 || len(h.authProxyChecks.results) != 0 {
		t.Fatalf("stale check was accepted: %s", w.Body.String())
	}
}
