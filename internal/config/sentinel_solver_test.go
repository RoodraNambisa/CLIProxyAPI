package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSentinelSolverPathDoesNotHideManagement(t *testing.T) {
	cfg := &Config{}
	cfg.RemoteManagement.AccessPath = "secret"
	for _, path := range []string{"/secret", "/secret/v0", "/secret/v0/management", "/secret/management.html/health", "/secret/codex", "/secret/anthropic/callback"} {
		cfg.SentinelSolver.AccessPath = path
		if err := cfg.ValidateSentinelSolver(); err == nil {
			t.Errorf("accepted overlap %q", path)
		}
	}
	cfg.SentinelSolver.AccessPath = "/secret/Sentinel"
	if err := cfg.ValidateSentinelSolver(); err != nil {
		t.Fatal(err)
	}
	cfg.RemoteManagement.AccessPath = ""
	cfg.SentinelSolver.AccessPath = "/codex"
	if err := cfg.ValidateSentinelSolver(); err == nil {
		t.Fatal("solver obscured a default OAuth callback")
	}
}

func TestSentinelSolverSharedPathSaveReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("port: 8317\nsentinel-solver:\n  enabled: false # preserve\n  access-path: /original\n  listen: invalid-address\n  tls: {enable: true, cert: /missing/cert, key: /missing/key}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.SentinelSolver.AccessPath = "/afhkajf/Sentinel"
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil || loaded.SentinelSolver.Path() != "/afhkajf/Sentinel" || loaded.Port != 8317 || loaded.TLS.Enable {
		t.Fatalf("reload: %v", err)
	}
}
