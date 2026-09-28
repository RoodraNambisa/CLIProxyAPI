package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexBaseURLValidation(t *testing.T) {
	for _, raw := range []string{"/codex", "ftp://example.test", "https://user:secret@example.test", "https://example.test?token=secret", "https://example.test#fragment", "https://example.test/\npath", "https://example.test\\path", "https://example.test:invalid"} {
		if _, err := NormalizeCodexBaseURL(raw); err == nil {
			t.Fatalf("accepted invalid endpoint %q", raw)
		}
	}
	for _, tc := range []struct{ raw, want string }{{"", ""}, {"  HTTPS://EXAMPLE.TEST:443/prefix/codex/// ", "https://example.test/prefix/codex"}, {"http://localhost:8317/codex", "http://localhost:8317/codex"}} {
		got, err := NormalizeCodexBaseURL(tc.raw)
		if err != nil || got != tc.want {
			t.Fatalf("normalize %q: %q %v", tc.raw, got, err)
		}
	}
}

func TestCodexBaseURLLoadSaveAndClearInheritance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "# keep\ndefaults: &defaults\n  base-url: https://inherited.test/codex\ncodex:\n  <<: *defaults\n  future-field: keep\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil || cfg.Codex.BaseURL != "https://inherited.test/codex" {
		t.Fatalf("load: %#v %v", cfg, err)
	}
	for _, value := range []string{"", "https://override.test/prefix/codex", "", "http://localhost:8317/codex"} {
		cfg.Codex.BaseURL = value
		if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
			t.Fatal(errSave)
		}
		reloaded, errLoad := LoadConfig(path)
		if errLoad != nil || reloaded.Codex.BaseURL != value {
			t.Fatalf("reload %q failed: %v", value, errLoad)
		}
		raw, errRead := os.ReadFile(path)
		if errRead != nil || !strings.Contains(string(raw), "# keep") || !strings.Contains(string(raw), "future-field: keep") {
			t.Fatal("save removed unrelated config")
		}
		if !strings.Contains(string(raw), "base-url: https://inherited.test/codex") {
			t.Fatal("save changed the shared YAML anchor")
		}
	}
	cfg.Codex.BaseURL = "https://example.test?secret=fixture"
	before, _ := os.ReadFile(path)
	if errSave := SaveConfigPreserveComments(path, cfg); errSave == nil {
		t.Fatal("invalid save succeeded")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("invalid save changed file")
	}
	if errWrite := os.WriteFile(path, []byte("codex: {base-url: 'file:///tmp/socket'}"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, errLoad := LoadConfigOptional(path, true); errLoad == nil {
		t.Fatal("optional load accepted invalid URL")
	}
}
