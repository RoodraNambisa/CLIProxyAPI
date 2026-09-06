package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigCodexPromptCachePassthrough(t *testing.T) {
	for _, tc := range []struct {
		name, yaml    string
		want, invalid bool
	}{
		{name: "default", yaml: "codex: {}\n"},
		{name: "enabled", yaml: "codex:\n  passthrough-prompt-cache-key: true\n", want: true},
		{name: "disabled", yaml: "codex:\n  passthrough-prompt-cache-key: false\n"},
		{name: "invalid", yaml: "codex:\n  passthrough-prompt-cache-key: []\n", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid boolean was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Codex.PassthroughPromptCacheKey != tc.want {
				t.Fatal("unexpected passthrough default or value")
			}
		})
	}
}
