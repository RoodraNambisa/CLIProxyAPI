package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthFilesPaginationConfigDefaultsAndStrictValidation(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantEnabled bool
		wantError   string
	}{
		{name: "omitted", body: "port: 8317\n"},
		{name: "null remote management", body: "remote-management: null\n"},
		{name: "enabled", body: "remote-management:\n  auth-files-pagination:\n    enabled: true\n", wantEnabled: true},
		{name: "disabled", body: "remote-management:\n  auth-files-pagination:\n    enabled: false\n"},
		{name: "null object", body: "remote-management:\n  auth-files-pagination: null\n", wantError: "must be an object"},
		{name: "scalar object", body: "remote-management:\n  auth-files-pagination: true\n", wantError: "must be an object"},
		{name: "null enabled", body: "remote-management:\n  auth-files-pagination:\n    enabled: null\n", wantError: "must not be null"},
		{name: "wrong enabled type", body: "remote-management:\n  auth-files-pagination:\n    enabled: yes-please\n", wantError: "cannot unmarshal"},
		{name: "numeric enabled type", body: "remote-management:\n  auth-files-pagination:\n    enabled: 1\n", wantError: "cannot unmarshal"},
		{name: "unknown field", body: "remote-management:\n  auth-files-pagination:\n    enabled: true\n    provider: codex\n", wantError: "provider is not supported"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if errWrite := os.WriteFile(path, []byte(test.body), 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			cfg, errLoad := LoadConfig(path)
			if test.wantError != "" {
				if errLoad == nil || !strings.Contains(errLoad.Error(), test.wantError) {
					t.Fatalf("LoadConfig() error = %v, want substring %q", errLoad, test.wantError)
				}
				return
			}
			if errLoad != nil {
				t.Fatal(errLoad)
			}
			if cfg.RemoteManagement.AuthFilesPagination.Enabled != test.wantEnabled {
				t.Fatalf("enabled = %v, want %v", cfg.RemoteManagement.AuthFilesPagination.Enabled, test.wantEnabled)
			}
		})
	}
}
