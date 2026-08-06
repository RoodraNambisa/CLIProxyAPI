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
