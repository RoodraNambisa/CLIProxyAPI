package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigOptional_RoutingSessionAffinityFailover(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	configYAML := []byte(`
routing:
  session-affinity: true
  session-affinity-failover: false
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := LoadConfigOptional(configPath, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}

	if cfg.Routing.SessionAffinityFailover == nil {
		t.Fatalf("SessionAffinityFailover = nil, want false pointer")
	}
	if *cfg.Routing.SessionAffinityFailover {
		t.Fatalf("SessionAffinityFailover = true, want false")
	}
}
