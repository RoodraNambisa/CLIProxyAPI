package api

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestManagementPanelCompressionStaysOnActiveEnabledSurface(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MANAGEMENT_STATIC_PATH", dir)
	if err := os.WriteFile(filepath.Join(dir, "management.html"), []byte("<html>panel</html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := newTestServerWithConfig(t, func(cfg *config.Config) { cfg.RemoteManagement.AccessPath = "panel-test" })
	get := func(path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Accept-Encoding", "gzip")
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, r)
		return w
	}
	if w := get("/panel-test/management.html"); w.Code != 200 || w.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("panel response = %d %v", w.Code, w.Header())
	}
	if w := get("/management.html"); w.Code != 404 {
		t.Fatal("inactive path exposed")
	}
	if w := get("/healthz"); w.Code != 200 || w.Header().Get("Content-Encoding") != "" {
		t.Fatal("API compression changed")
	}
	cfg := *s.currentConfig()
	cfg.RemoteManagement.DisableControlPanel = true
	s.setCurrentConfig(&cfg)
	if w := get("/panel-test/management.html"); w.Code != 404 || w.Header().Get("ETag") != "" {
		t.Fatal("disabled panel was cached")
	}
}
