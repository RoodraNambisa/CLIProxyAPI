package api

import (
	"context"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
)

func TestSentinelOnlyHasNoBusinessRoutesOrManagers(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "management-key")
	t.Setenv("PGSTORE_DSN", "postgres://must-not-connect")
	cfg := &config.Config{SentinelSolver: sentinelconfig.Server{Enabled: true, APIKeys: []string{"solver-key"}}}
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

func TestSentinelSharedMainListenerAuthAndMiddleware(t *testing.T) {
	var middlewareCalls atomic.Int32
	s := newTestServerWithConfigAndOptions(t, func(cfg *config.Config) {
		cfg.Host = "127.0.0.1"
		cfg.SentinelSolver = sentinelconfig.Server{Enabled: true, AccessPath: "/afhkajf/Sentinel", APIKeys: []string{"solver-key"}}
	}, WithMiddleware(func(c *gin.Context) { middlewareCalls.Add(1); c.Next() }))
	addr, done, err := s.StartListening()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Stop(ctx)
		<-done
	}()
	base := "http://" + addr.String()
	get := func(path, key string) int {
		r, err := http.NewRequest(http.MethodGet, base+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer "+key)
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		return response.StatusCode
	}
	if get("/afhkajf/Sentinel/health", "solver-key") != 200 || get("/afhkajf/Sentinel/health", "test-key") != 401 {
		t.Fatal("independent solver authentication failed")
	}
	if middlewareCalls.Load() != 0 {
		t.Fatal("solver traversed proxy middleware")
	}
	if get("/healthz", "") != 200 || middlewareCalls.Load() != 1 {
		t.Fatal("main routes no longer available")
	}
	if get("/v1/models", "solver-key") != 401 {
		t.Fatal("solver key authorized business API")
	}
	if s.sentinelSolver.Snapshot().Address != addr.String() {
		t.Fatal("solver did not share the main listener")
	}
}

func TestSentinelOnlyMountedOnManagementPort(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "management-key")
	cfg := &config.Config{Host: "127.0.0.1", SentinelSolver: sentinelconfig.Server{Enabled: true, AccessPath: "/private/Sentinel", APIKeys: []string{"solver-key"}}}
	s, err := NewSentinelOnlyServer(cfg, filepath.Join(t.TempDir(), "config.yaml"), "management-key")
	if err != nil {
		t.Fatal(err)
	}
	addr, done, err := s.StartListening()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Stop(ctx)
		<-done
	}()
	for _, test := range []struct {
		path, key string
		status    int
	}{
		{"/private/Sentinel/health", "solver-key", 200},
		{"/private/Sentinel/health", "management-key", 401},
		{"/v0/management/runtime/capabilities", "management-key", 200},
		{"/v0/management/runtime/capabilities", "solver-key", 401},
		{"/v1/models", "solver-key", 404},
	} {
		r, _ := http.NewRequest("GET", "http://"+addr.String()+test.path, nil)
		r.Header.Set("Authorization", "Bearer "+test.key)
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != test.status {
			t.Errorf("%s: %d", test.path, response.StatusCode)
		}
	}
}
