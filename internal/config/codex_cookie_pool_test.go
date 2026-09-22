package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCookieBackupCountInheritanceAndPersistence(t *testing.T) {
	var cfg Config
	if err := yaml.Unmarshal([]byte(`codex:
  state-override:
    enabled: true
    strategy: cookie-only
    cookie-backup-count: 2
    rules:
      - id: rule
        settings:
          cookie-backup-count: 0
        model-overrides:
          - id: model
            models: [alternate]
            settings:
              cookie-backup-count: 3
`), &cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateCodexStateOverride(); err != nil {
		t.Fatal(err)
	}
	cloned, err := Clone(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	for model, count := range map[string]int{"normal": 0, "alternate": 3} {
		p, match, ok := cloned.Codex.StateOverride.PolicyFor(CodexStateScope{Model: model})
		if !ok || p.CookieBackupCount != count || match.Sources["cookie-backup-count"] == "default" {
			t.Fatalf("wrong override %s: %+v", model, p)
		}
	}
	file := filepath.Join(t.TempDir(), "config.yaml")
	if err = os.WriteFile(file, []byte("codex:\n  state-override:\n    extension: keep\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = SaveConfigPreserveComments(file, cloned); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "cookie-backup-count: 0") || !strings.Contains(string(data), "extension: keep") {
		t.Fatalf("lost zero or extension: %s", data)
	}
	for _, bad := range []int{-1, 11} {
		cloned.Codex.StateOverride.CookieBackupCount = bad
		if cloned.ValidateCodexStateOverride() == nil {
			t.Fatal("invalid global count accepted")
		}
		if (CodexStateStrategySettings{CookieBackupCount: &bad}).validateExplicit() == nil {
			t.Fatal("invalid inherited count accepted")
		}
	}
}
