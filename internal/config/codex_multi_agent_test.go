package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadSaveCodexMultiAgentV2Policy(t *testing.T) {
	for _, value := range []string{"", "true", "false", "[]", "{}", "1", `"false"`} {
		t.Run(value, func(t *testing.T) {
			data := "codex: {}\n"
			if value != "" {
				data = "codex:\n  optimize-multi-agent-v2: " + value + "\n"
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			valid := value == "" || value == "true" || value == "false"
			if (err == nil) != valid {
				t.Fatalf("boolean validation failed: %v", err)
			}
			if !valid {
				return
			}
			if cfg.Codex.OptimizeMultiAgentV2 != (value == "true") {
				t.Fatal("wrong default or loaded policy")
			}
			if err := SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatal(err)
			}
			reloaded, err := LoadConfig(path)
			if err != nil || reloaded.Codex.OptimizeMultiAgentV2 != cfg.Codex.OptimizeMultiAgentV2 {
				t.Fatal("saved policy changed after reload")
			}
		})
	}
}

func TestCodexMultiAgentV2JSONAndDerivedField(t *testing.T) {
	for _, value := range []string{"true", "false", "null", "1", `"true"`, "[]"} {
		var cfg Config
		err := json.Unmarshal([]byte(`{"codex":{"optimize-multi-agent-v2":`+value+"}}"), &cfg)
		if (err == nil) != (value == "true" || value == "false" || value == "null") {
			t.Fatal("wrong JSON boolean handling")
		}
	}
	cfg := Config{SDKConfig: SDKConfig{CodexOptimizeMultiAgentV2: true}, Codex: CodexConfig{OptimizeMultiAgentV2: true}}
	raw, err := json.Marshal(cfg)
	if err != nil || strings.Contains(string(raw), "CodexOptimizeMultiAgentV2") || strings.Count(string(raw), "optimize-multi-agent-v2") != 1 {
		t.Fatal("derived policy leaked into saved JSON")
	}
}
