package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigOptional_CodexHeaderDefaults(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	configYAML := []byte(`
codex-header-defaults:
  user-agent: "  my-codex-client/1.0  "
  originator: "  my-originator  "
  beta-features: "  feature-a,feature-b  "
codex-fingerprint:
  ja3: true
  force-http1: true
  images-force-http1: true
codex:
  identity-confuse: true
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := LoadConfigOptional(configPath, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}

	if got := cfg.CodexHeaderDefaults.UserAgent; got != "my-codex-client/1.0" {
		t.Fatalf("UserAgent = %q, want %q", got, "my-codex-client/1.0")
	}
	if got := cfg.CodexHeaderDefaults.Originator; got != "my-originator" {
		t.Fatalf("Originator = %q, want %q", got, "my-originator")
	}
	if got := cfg.CodexHeaderDefaults.BetaFeatures; got != "feature-a,feature-b" {
		t.Fatalf("BetaFeatures = %q, want %q", got, "feature-a,feature-b")
	}
	if !cfg.CodexFingerprint.JA3 {
		t.Fatalf("CodexFingerprint.JA3 = false, want true")
	}
	if !cfg.CodexFingerprint.ForceHTTP1 {
		t.Fatalf("CodexFingerprint.ForceHTTP1 = false, want true")
	}
	if !cfg.CodexFingerprint.ImagesForceHTTP1 {
		t.Fatalf("CodexFingerprint.ImagesForceHTTP1 = false, want true")
	}
	if !cfg.Codex.IdentityConfuse {
		t.Fatalf("Codex.IdentityConfuse = false, want true")
	}
}
