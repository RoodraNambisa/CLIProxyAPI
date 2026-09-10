package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRequestPolicyBooleansRejectInvalidOptionalConfig(t *testing.T) {
	fields := map[string][]string{
		"codex":   {"passthrough-prompt-cache-key", "stream-bootstrap-buffering", "optimize-multi-agent-v2", "orphan-delegation-compatibility", "estimate-claude-input-tokens", "observe-quota"},
		"routing": {"session-affinity-lcp", "session-affinity-subagents", "session-affinity-across-priorities"},
	}
	for section, keys := range fields {
		for _, key := range keys {
			for _, optional := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/optional=%t", section, key, optional), func(t *testing.T) {
					for _, value := range []string{"true", "false", "null", "[]", "{}", "1", `"false"`, "yes", "on", "off", "!!bool yes", "!!bool invalid"} {
						valid := value == "true" || value == "false" || value == "null"
						for _, body := range []string{
							fmt.Sprintf("%s:\n  %s: %s\n", section, key, value),
							fmt.Sprintf("defaults: &policy\n  %s: %s\n%s:\n  <<: *policy\n", key, value, section),
							fmt.Sprintf("defaults: &policy\n  %s:\n    %s: %s\n<<: *policy\n", section, key, value),
							fmt.Sprintf("value: &policy %s\n%s:\n  %s: *policy\n", value, section, key),
						} {
							path := filepath.Join(t.TempDir(), "config.yaml")
							if err := os.WriteFile(path, []byte("port: 8317\n"+body), 0o600); err != nil {
								t.Fatal(err)
							}
							cfg, err := LoadConfigOptional(path, optional)
							if !valid {
								if err == nil || !strings.Contains(err.Error(), key) || cfg != nil {
									t.Fatalf("%s must reject %q without accepting an empty config; got error %v", key, value, err)
								}
							} else if err != nil || cfg == nil || cfg.Port != 8317 {
								t.Fatalf("valid %s=%s lost the original config: %v", key, value, err)
							}
						}
					}
				})
			}
		}
	}
}

func TestRequestPolicyBooleansExplicitNullShadowsInvalidMerge(t *testing.T) {
	for _, section := range []string{"codex", "routing"} {
		key := "observe-quota"
		if section == "routing" {
			key = "session-affinity-lcp"
		}
		body := fmt.Sprintf("defaults: &policy\n  %s: yes\n%s:\n  <<: *policy\n  %s: null\nport: 8317\n", key, section, key)
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if cfg, err := LoadConfigOptional(path, true); err != nil || cfg == nil || cfg.Port != 8317 || cfg.Codex.ObserveQuota || cfg.Routing.SessionAffinityLCP {
			t.Fatalf("explicit null failed to shadow unused invalid inheritance: %v", err)
		}
	}
}

func TestRequestPolicyBooleansPreserveLegacyOptionalFallback(t *testing.T) {
	for _, body := range []string{"port: []\n", "codex: [\n"} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfigOptional(path, true)
		if err != nil || cfg == nil || cfg.Port != 0 {
			t.Fatalf("unrelated optional fallback changed: %v", err)
		}
	}
}
