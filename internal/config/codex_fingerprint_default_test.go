package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexFingerprintDefaultMode(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "omitted", want: DefaultCodexFingerprintMode},
		{name: "off", value: " off ", want: "off"},
		{name: "device", value: "device", want: "device"},
		{name: "session", value: "SESSION", want: "session"},
		{name: "full", value: "full", want: "full"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NormalizeCodexFingerprintMode(test.value)
			if err != nil {
				t.Fatalf("NormalizeCodexFingerprintMode() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("NormalizeCodexFingerprintMode() = %q, want %q", got, test.want)
			}
		})
	}

	if _, err := NormalizeCodexFingerprintMode("aggressive"); err == nil ||
		!strings.Contains(err.Error(), "codex-fingerprint.default-mode") {
		t.Fatalf("NormalizeCodexFingerprintMode(invalid) error = %v", err)
	}
}

func TestLoadConfigNormalizesCodexFingerprintDefaultMode(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("codex-fingerprint:\n  default-mode: SESSION\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if got := cfg.CodexFingerprint.DefaultMode; got != "session" {
		t.Fatalf("CodexFingerprint.DefaultMode = %q, want session", got)
	}

	if err := os.WriteFile(configPath, []byte("codex-fingerprint:\n  default-mode: invalid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadConfig(configPath); err == nil {
		t.Fatal("LoadConfig() error = nil, want invalid mode rejection")
	}
}
