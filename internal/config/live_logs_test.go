package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLiveLogsConfigDefaultsAndStrictValidation(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		wantEnabled bool
		wantError   string
	}{
		{name: "omitted", yaml: "remote-management: {}\n"},
		{name: "enabled", yaml: "remote-management:\n  live-logs:\n    enabled: true\n", wantEnabled: true},
		{name: "disabled", yaml: "remote-management:\n  live-logs:\n    enabled: false\n"},
		{name: "null object", yaml: "remote-management:\n  live-logs: null\n", wantError: "live-logs must be an object"},
		{name: "null enabled", yaml: "remote-management:\n  live-logs:\n    enabled: null\n", wantError: "enabled must not be null"},
		{name: "wrong enabled type", yaml: "remote-management:\n  live-logs:\n    enabled: yes-please\n", wantError: "cannot unmarshal"},
		{name: "unknown field", yaml: "remote-management:\n  live-logs:\n    buffer: 10\n", wantError: "buffer is not supported"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var cfg Config
			err := yaml.Unmarshal([]byte(test.yaml), &cfg)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error = %v, want containing %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("unmarshal config: %v", err)
			}
			if cfg.RemoteManagement.LiveLogs.Enabled != test.wantEnabled {
				t.Fatalf("enabled = %v, want %v", cfg.RemoteManagement.LiveLogs.Enabled, test.wantEnabled)
			}
		})
	}
}

func TestManagementDiagnosticsConfigDefaultsAndStrictValidation(t *testing.T) {
	tests := []struct {
		name      string
		yaml      string
		wantLevel string
		wantError string
	}{
		{name: "omitted", yaml: "remote-management: {}\n", wantLevel: ManagementDiagnosticsDetailSafe},
		{name: "empty object", yaml: "remote-management:\n  diagnostics: {}\n", wantLevel: ManagementDiagnosticsDetailSafe},
		{name: "safe", yaml: "remote-management:\n  diagnostics:\n    detail-level: safe\n", wantLevel: ManagementDiagnosticsDetailSafe},
		{name: "full", yaml: "remote-management:\n  diagnostics:\n    detail-level: full\n", wantLevel: ManagementDiagnosticsDetailFull},
		{name: "null object", yaml: "remote-management:\n  diagnostics: null\n", wantError: "diagnostics must be an object"},
		{name: "null detail", yaml: "remote-management:\n  diagnostics:\n    detail-level: null\n", wantError: "detail-level must not be null"},
		{name: "invalid detail", yaml: "remote-management:\n  diagnostics:\n    detail-level: everything\n", wantError: "must be safe or full"},
		{name: "wrong type", yaml: "remote-management:\n  diagnostics:\n    detail-level: 42\n", wantError: "must be safe or full"},
		{name: "unknown field", yaml: "remote-management:\n  diagnostics:\n    raw: true\n", wantError: "raw is not supported"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var cfg Config
			err := yaml.Unmarshal([]byte(test.yaml), &cfg)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error = %v, want containing %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("unmarshal config: %v", err)
			}
			if got := cfg.RemoteManagement.Diagnostics.ResolvedDetailLevel(); got != test.wantLevel {
				t.Fatalf("detail level = %q, want %q", got, test.wantLevel)
			}
		})
	}
}

func TestManagementDiagnosticsZeroValueCloneUsesSafeDetail(t *testing.T) {
	cloned, errClone := Clone(&Config{})
	if errClone != nil {
		t.Fatalf("clone config: %v", errClone)
	}
	if got := cloned.RemoteManagement.Diagnostics.ResolvedDetailLevel(); got != ManagementDiagnosticsDetailSafe {
		t.Fatalf("cloned detail level = %q", got)
	}
}
