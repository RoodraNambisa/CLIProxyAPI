package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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

func TestCodexFingerprintSessionIdentityPoolSize(t *testing.T) {
	if got := (CodexFingerprintConfig{}).ResolvedSessionIdentityPoolSize(); got != DefaultCodexSessionIdentityPoolSize {
		t.Fatalf("zero-valued pool size = %d, want %d", got, DefaultCodexSessionIdentityPoolSize)
	}
	if got := (CodexFingerprintConfig{SessionIdentityPoolSize: 4}).ResolvedSessionIdentityPoolSize(); got != 4 {
		t.Fatalf("configured pool size = %d, want 4", got)
	}

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	for _, test := range []struct {
		name    string
		content string
		want    int
		wantErr bool
	}{
		{name: "omitted", content: "codex-fingerprint:\n  default-mode: session\n", want: 1},
		{name: "configured", content: "codex-fingerprint:\n  session-identity-pool-size: 4\n", want: 4},
		{name: "zero", content: "codex-fingerprint:\n  session-identity-pool-size: 0\n", wantErr: true},
		{name: "too large", content: "codex-fingerprint:\n  session-identity-pool-size: 65\n", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(configPath, []byte(test.content), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(configPath)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "codex-fingerprint.session-identity-pool-size") {
					t.Fatalf("LoadConfig() error = %v, want pool size rejection", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if got := cfg.CodexFingerprint.SessionIdentityPoolSize; got != test.want {
				t.Fatalf("SessionIdentityPoolSize = %d, want %d", got, test.want)
			}
		})
	}
}

func TestCodexFingerprintSessionIdentityPoolSizeOmitsZeroWhenMarshaled(t *testing.T) {
	payload, err := yaml.Marshal(Config{})
	if err != nil {
		t.Fatalf("yaml.Marshal() error = %v", err)
	}
	if strings.Contains(string(payload), "session-identity-pool-size") {
		t.Fatalf("yaml.Marshal() unexpectedly emitted zero pool size:\n%s", payload)
	}
}
