package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexLiveEnabledTypesAndDefaults(t *testing.T) {
	for _, value := range []string{"", "true", "false", "null", `"true"`, "1", "[]", "{}", "yes", "!!bool yes", "!!bool invalid"} {
		valid := value == "" || value == "true" || value == "false" || value == "null"
		for _, optional := range []bool{false, true} {
			path := filepath.Join(t.TempDir(), "config.yaml")
			body := "codex: {}\n"
			if value != "" {
				body = "codex:\n  live-enabled: " + value + "\n"
			}
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfigOptional(path, optional)
			if !valid {
				if err == nil || !strings.Contains(err.Error(), "live-enabled") {
					t.Fatal("invalid live control bypassed validation")
				}
				continue
			}
			if err != nil || cfg.Codex.LiveEnabled != (value == "true") {
				t.Fatal("incorrect live default or configured value")
			}
		}
		if value != "" && value != "yes" {
			var cfg Config
			if err := json.Unmarshal([]byte(`{"codex":{"live-enabled":`+value+`}}`), &cfg); (err == nil) != valid {
				t.Fatal("JSON live control accepted an invalid type")
			}
		}
	}
}

func TestCodexLiveEnabledMergeSaveAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "# preserve\ndefaults: &live\n  live-enabled: true\ncodex:\n  <<: *live\n  future-field: keep\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil || !cfg.Codex.LiveEnabled {
		t.Fatal("live merge was not loaded")
	}
	for _, enabled := range []bool{false, true, false} {
		cfg.Codex.LiveEnabled = enabled
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		reloaded, err := LoadConfig(path)
		if err != nil || reloaded.Codex.LiveEnabled != enabled {
			t.Fatal("saved live control changed after reload")
		}
		saved, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(saved), "# preserve") || !strings.Contains(string(saved), "future-field: keep") {
			t.Fatal("live save removed unrelated YAML")
		}
	}
	invalid := strings.Replace(body, "live-enabled: true", "live-enabled: []", 1)
	if err := os.WriteFile(path, []byte(invalid), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfigOptional(path, true); err == nil {
		t.Fatal("invalid inherited control was accepted")
	}
	if err := os.WriteFile(path, []byte(invalid+"  live-enabled: false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if cfg, err := LoadConfig(path); err != nil || cfg.Codex.LiveEnabled {
		t.Fatal("explicit live control did not override a merge")
	}
}

func TestCodexPolicyFalseOverridesInheritedTrueWithoutEditingAnchors(t *testing.T) {
	for _, shape := range []string{"map merge", "alias", "root merge"} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		fields := "  live-enabled: true\n  optimize-multi-agent-v2: true\n  orphan-delegation-compatibility: true\n  stream-bootstrap-buffering: true\n  passthrough-prompt-cache-key: true\n  identity-confuse: true\n  spoof-session-identity: true\n  future-field: keep\n"
		body := "defaults: &defaults\n" + fields + "codex:\n  <<: *defaults\n"
		if shape == "alias" {
			body = "defaults: &defaults\n" + fields + "codex: *defaults\n"
		}
		if shape == "root merge" {
			body = "defaults: &defaults\n  codex:\n" + strings.ReplaceAll(fields, "  ", "    ") + "<<: *defaults\n"
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfig(path)
		if err != nil || !cfg.Codex.LiveEnabled {
			t.Fatal("inherited policy fixture was not loaded")
		}
		cfg.Codex = CodexConfig{}
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		got, err := LoadConfig(path)
		if err != nil || got.Codex.LiveEnabled || got.Codex.OptimizeMultiAgentV2 || got.Codex.OrphanDelegationCompatibility || got.Codex.StreamBootstrapBuffering || got.Codex.PassthroughPromptCacheKey || got.Codex.IdentityConfuse || got.Codex.SpoofSessionIdentity {
			t.Fatal("saved false re-inherited an enabled policy")
		}
		saved, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(saved), "future-field: keep") || !strings.Contains(string(saved), "live-enabled: true") {
			t.Fatal("save changed shared defaults or removed unknown fields")
		}
	}
}
