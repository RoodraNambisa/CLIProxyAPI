package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

func TestSentinelOnlyHasNoBusinessRoutesOrManagers(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "management-key")
	t.Setenv("PGSTORE_DSN", "postgres://must-not-connect")
	cfg := &config.Config{SentinelSolver: sentinelconfig.Server{Enabled: true, Listen: "127.0.0.1:0", APIKeys: []string{"solver-key"}}}
	s, err := NewSentinelOnlyServer(cfg, filepath.Join(t.TempDir(), "config.yaml"), "management-key")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Stop(context.Background()) }()
	if s.handlers != nil || s.accessManager != nil || s.codexLive != nil {
		t.Fatal("business runtime constructed")
	}
	for _, path := range []string{"/v1/models", "/v1/images/generations", "/v0/management/auth-files", "/v0/management/usage"} {
		out := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("Authorization", "Bearer management-key")
		s.engine.ServeHTTP(out, r)
		if out.Code != 404 {
			t.Fatalf("business route exposed: %s", path)
		}
	}
	for _, key := range []string{"management-key", "solver-key"} {
		out := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/v0/management/runtime/capabilities", nil)
		r.RemoteAddr = "127.0.0.1:1234"
		r.Header.Set("Authorization", "Bearer "+key)
		s.engine.ServeHTTP(out, r)
		if key == "management-key" && out.Code != 200 || key == "solver-key" && out.Code == 200 {
			t.Fatalf("management authentication isolation failed: %d", out.Code)
		}
	}
}
