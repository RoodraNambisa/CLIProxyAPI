package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeCodexTurnStatePolicy(t *testing.T) {
	tests := []struct {
		name      string
		input     CodexTurnStatePolicy
		want      CodexTurnStatePolicy
		wantError bool
	}{
		{name: "default", want: CodexTurnStatePolicyGuardCrossAccount},
		{name: "passthrough", input: CodexTurnStatePolicyPassthrough, want: CodexTurnStatePolicyPassthrough},
		{name: "guard", input: CodexTurnStatePolicy(" GUARD-CROSS-ACCOUNT "), want: CodexTurnStatePolicyGuardCrossAccount},
		{name: "same only", input: CodexTurnStatePolicySameAccountOnly, want: CodexTurnStatePolicySameAccountOnly},
		{name: "strip", input: CodexTurnStatePolicyStrip, want: CodexTurnStatePolicyStrip},
		{name: "invalid", input: CodexTurnStatePolicy("unknown"), wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NormalizeCodexTurnStatePolicy(test.input)
			if (err != nil) != test.wantError {
				t.Fatalf("NormalizeCodexTurnStatePolicy() error = %v, wantError=%v", err, test.wantError)
			}
			if got != test.want {
				t.Fatalf("NormalizeCodexTurnStatePolicy() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestLoadConfigCodexTurnStatePolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("codex:\n  turn-state-policy: same-account-only\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if got := cfg.Codex.TurnStatePolicy; got != CodexTurnStatePolicySameAccountOnly {
		t.Fatalf("TurnStatePolicy = %q, want %q", got, CodexTurnStatePolicySameAccountOnly)
	}

	if err = os.WriteFile(path, []byte("codex:\n  turn-state-policy: unsafe\n"), 0o600); err != nil {
		t.Fatalf("write invalid config: %v", err)
	}
	if _, err = LoadConfig(path); err == nil || !strings.Contains(err.Error(), "codex.turn-state-policy") {
		t.Fatalf("LoadConfig() error = %v, want turn-state validation error", err)
	}
}
