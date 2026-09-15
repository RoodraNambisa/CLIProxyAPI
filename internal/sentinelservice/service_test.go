package sentinelservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

func testSharedService(t *testing.T, cfg sentinelconfig.Server) *Service {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start("127.0.0.1:8317"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ctx, cancel := context.WithCancel(context.Background()); cancel(); _ = s.Stop(ctx) })
	return s
}

func TestSharedSolverDispatchIsolationAndPendingPath(t *testing.T) {
	cfg := sentinelconfig.Server{Enabled: true, AccessPath: "/afhkajf/Sentinel", APIKeys: []string{"solver-key"}}
	s := testSharedService(t, cfg)
	var proxyCalls atomic.Int32
	handler := s.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { proxyCalls.Add(1); w.WriteHeader(418) }))
	request := func(path, key string) int {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+key)
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, r)
		return out.Code
	}
	for _, key := range []string{"", "management-key", "client-api-key"} {
		if got := request(cfg.Path()+"/health", key); got != 401 {
			t.Fatalf("auth isolation: %s got %d", key, got)
		}
	}
	if got := request(cfg.Path()+"/health", "solver-key"); got != 200 {
		t.Fatalf("health %d", got)
	}
	for _, suffix := range []string{"/../health", "//health", "/%68ealth", "/health?token=x"} {
		if got := request(cfg.Path()+suffix, "solver-key"); got != 404 {
			t.Fatalf("noncanonical %s: %d", suffix, got)
		}
	}
	if proxyCalls.Load() != 0 {
		t.Fatal("solver requests reached proxy middleware/logging")
	}
	if got := request("/v1/models", "client-api-key"); got != 418 {
		t.Fatal("proxy request intercepted")
	}
	if got := request(cfg.Path()+"Other/health", "solver-key"); got != 418 {
		t.Fatal("path boundary lost")
	}
	cfg.AccessPath = "/changed/Sentinel"
	if err := s.UpdateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if !s.Snapshot().RestartRequired || s.Snapshot().AccessPath != "/afhkajf/Sentinel" {
		t.Fatal("active path not pinned")
	}
	if request("/afhkajf/Sentinel/health", "solver-key") != 200 || request("/changed/Sentinel/health", "solver-key") != 404 {
		t.Fatal("pending path became active")
	}
	if proxyCalls.Load() != 2 {
		t.Fatal("pending path reached logs")
	}
	cfg.Enabled = false
	if err := s.UpdateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if request("/afhkajf/Sentinel/health", "solver-key") != 404 || s.Snapshot().Running {
		t.Fatal("disabled solver still exposed")
	}
	cfg.Enabled = true
	cfg.APIKeys = []string{"new-key"}
	if err := s.UpdateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if request("/afhkajf/Sentinel/health", "solver-key") != 401 || request("/afhkajf/Sentinel/health", "new-key") != 200 {
		t.Fatal("key rotation not immediate")
	}
}

func TestSharedSolverRemoteSessionAndDrain(t *testing.T) {
	cfg := sentinelconfig.Server{Enabled: true, AccessPath: "/afhkajf/Sentinel", APIKeys: []string{"solver-key"}}
	s := testSharedService(t, cfg)
	var proxyCalls atomic.Int32
	server := httptest.NewServer(s.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { proxyCalls.Add(1); http.NotFound(w, r) })))
	defer server.Close()
	node := sentinelconfig.Node{Name: "node", URL: server.URL + cfg.Path(), APIKey: "solver-key"}
	if _, err := chatgptweb.TestSentinelComputeNode(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	pool := chatgptweb.NewSentinelComputePool()
	defer pool.Close()
	if err := pool.UpdateConfig("remote", sentinelconfig.Remote{Nodes: []sentinelconfig.Node{node}}, chatgptweb.SentinelRuntimeConfig{}); err != nil {
		t.Fatal(err)
	}
	credential := &chatgptweb.Credential{Persona: chatgptweb.DefaultPersona(), DeviceID: "fixture"}
	chatgptweb.ResolveCredentialPersona(credential, "fixture")
	input := chatgptweb.SentinelComputeInput{Format: "conversation", Flow: "conversation", Clock: time.Now(), Environment: chatgptweb.SentinelComputeEnvironment{Persona: credential.Persona, DeviceID: "fixture", BrowserEnvironment: chatgptweb.ResolveCredentialBrowserEnvironment(credential, "fixture"), PageStartedAt: time.Now(), Location: "https://chatgpt.com/"}}
	session, err := pool.Begin(t.Context(), "images", input, chatgptweb.SentinelComputeHooks{})
	if err != nil {
		t.Fatal(err)
	}
	if session.RequirementsToken() == "" {
		t.Fatal("no requirements result")
	}
	defer session.Close()
	stopped := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	go func() { _ = s.Stop(ctx); close(stopped) }()
	deadline := time.Now().Add(time.Second)
	for !s.Snapshot().Runtime.Draining && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !s.Snapshot().Runtime.Draining {
		t.Fatal("drain did not start")
	}
	response, err := server.Client().Get(server.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("new proxy requests were accepted during solver drain")
	}
	if _, err = session.Solve(ctx, chatgptweb.SentinelComputeChallenge{}); err != nil {
		t.Fatal(err)
	}
	if _, err = session.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	session.Close()
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("session could not close on the mounted path")
	}
	if s.Snapshot().Runtime.Sessions != 0 || proxyCalls.Load() != 0 {
		t.Fatal("session leaked or reached business middleware")
	}
}

func TestSharedSolverRejectsInvalidBodyBeforeProxy(t *testing.T) {
	s := testSharedService(t, sentinelconfig.Server{Enabled: true, APIKeys: []string{"solver-key"}})
	handler := s.Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("RPC reached proxy logger") }))
	r := httptest.NewRequest("POST", "/v1/sentinel/sessions", strings.NewReader(`{"version":1,"private":"challenge"}`))
	r.Header.Set("Authorization", "Bearer solver-key")
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, r)
	if out.Code != 400 {
		t.Fatalf("invalid RPC response: %d", out.Code)
	}
	if strings.Contains(out.Body.String(), "challenge") {
		t.Fatal("private body echoed")
	}
}
