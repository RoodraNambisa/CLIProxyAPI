package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAdmissionDefaultsAndConfigRoundTrip(t *testing.T) {
	var empty Config
	count, bytes := empty.Admission.Limits()
	if count != 1024 || bytes != 256<<20 || empty.Routing.Queue.Enabled || empty.Routing.PerAuthConcurrency != 0 {
		t.Fatal("unexpected defaults")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	source := `api-keys: [fixture]
admission:
  max-queued-requests: 512
  future-limit: retained
routing:
  per-auth-concurrency: 2
  queue: {enabled: true, max-waiting: 20, wait-timeout-seconds: 0, future: retained}
  priority-overrides:
    - priority: 5
      per-auth-concurrency: 0
      queue: {enabled: false}
      subscription-overrides:
        - credentials: [named]
          per-auth-concurrency: 1
api-key-groups:
  - api-key: fixture
    concurrency-limit: 2
    rpm-limit: 10
    queue: {enabled: true, max-waiting: 5, wait-timeout-seconds: 0}
`
	if err := os.WriteFile(path, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if *cfg.Routing.PriorityOverrides[0].PerAuthConcurrency != 0 || cfg.Routing.PriorityOverrides[0].Queue.Enabled || cfg.APIKeyGroups[0].RPMLimit != 10 {
		t.Fatal("lost explicit values")
	}
	cfg.Routing.PerAuthConcurrency = 3
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(saved), "future-limit: retained") || !strings.Contains(string(saved), "future: retained") {
		t.Fatal("lost unknown fields")
	}
	again, err := LoadConfig(path)
	if err != nil || again.Routing.PerAuthConcurrency != 3 || again.APIKeyGroups[0].ConcurrencyLimit != 2 {
		t.Fatal("failed round trip", err)
	}
}
