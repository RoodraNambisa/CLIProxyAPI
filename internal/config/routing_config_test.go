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

func TestLoadConfigOptional_RoutingPriorityOverrides(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	configYAML := []byte(`
routing:
  fill-first-range: 0
  fill-first-per-auth-rpm: -10
  priority-overrides:
    - priority: 0
      strategy: ff
      max-retry-credentials: 2
      fill-first-range: 5
      fill-first-per-auth-rpm: 0
    - priority: -1
      max-retry-credentials: -4
      fill-first-range: -2
      fill-first-per-auth-rpm: -3
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := LoadConfigOptional(configPath, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if len(cfg.Routing.PriorityOverrides) != 2 {
		t.Fatalf("PriorityOverrides length = %d, want 2", len(cfg.Routing.PriorityOverrides))
	}
	if cfg.Routing.FillFirstRange != 1 {
		t.Fatalf("FillFirstRange = %d, want 1", cfg.Routing.FillFirstRange)
	}
	if cfg.Routing.FillFirstPerAuthRPM != 0 {
		t.Fatalf("FillFirstPerAuthRPM = %d, want 0", cfg.Routing.FillFirstPerAuthRPM)
	}
	first := cfg.Routing.PriorityOverrides[0]
	if first.Priority != 0 || first.Strategy != "fill-first" {
		t.Fatalf("first override = %+v, want priority 0 fill-first", first)
	}
	if first.MaxRetryCredentials == nil || *first.MaxRetryCredentials != 2 {
		t.Fatalf("first MaxRetryCredentials = %v, want 2", first.MaxRetryCredentials)
	}
	if first.FillFirstRange == nil || *first.FillFirstRange != 5 {
		t.Fatalf("first FillFirstRange = %v, want 5", first.FillFirstRange)
	}
	if first.FillFirstPerAuthRPM == nil || *first.FillFirstPerAuthRPM != 0 {
		t.Fatalf("first FillFirstPerAuthRPM = %v, want 0", first.FillFirstPerAuthRPM)
	}
	second := cfg.Routing.PriorityOverrides[1]
	if second.Priority != -1 {
		t.Fatalf("second priority = %d, want -1", second.Priority)
	}
	if second.MaxRetryCredentials == nil || *second.MaxRetryCredentials != 0 {
		t.Fatalf("second MaxRetryCredentials = %v, want 0", second.MaxRetryCredentials)
	}
	if second.FillFirstRange == nil || *second.FillFirstRange != 1 {
		t.Fatalf("second FillFirstRange = %v, want 1", second.FillFirstRange)
	}
	if second.FillFirstPerAuthRPM == nil || *second.FillFirstPerAuthRPM != 0 {
		t.Fatalf("second FillFirstPerAuthRPM = %v, want 0", second.FillFirstPerAuthRPM)
	}
}

func TestLoadConfigOptional_RejectsDuplicateRoutingPriorityOverrides(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	configYAML := []byte(`
routing:
  priority-overrides:
    - priority: 0
      strategy: fill-first
    - priority: 0
      strategy: random
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	if _, err := LoadConfigOptional(configPath, false); err == nil {
		t.Fatalf("LoadConfigOptional() error = nil, want duplicate priority error")
	}
}

func TestLoadConfigOptional_RejectsFillFirstRangeRPMConflict(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	configYAML := []byte(`
routing:
  strategy: fill-first
  fill-first-range: 2
  fill-first-per-auth-rpm: 60
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	if _, err := LoadConfigOptional(configPath, false); err == nil {
		t.Fatalf("LoadConfigOptional() error = nil, want fill-first conflict")
	}
}

func TestLoadConfigOptional_RejectsInheritedFillFirstRangeRPMConflict(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	configYAML := []byte(`
routing:
  strategy: fill-first
  fill-first-range: 1
  fill-first-per-auth-rpm: 60
  priority-overrides:
    - priority: 0
      fill-first-range: 2
`)
	if err := os.WriteFile(configPath, configYAML, 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	if _, err := LoadConfigOptional(configPath, false); err == nil {
		t.Fatalf("LoadConfigOptional() error = nil, want inherited fill-first conflict")
	}
}
