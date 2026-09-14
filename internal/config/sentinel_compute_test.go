package config

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSentinelComputeConfigurationDefaultsAndRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	input := []byte("port: 8317\nchatgpt-web:\n  sentinel:\n    mode: remote\n    remote:\n      scopes: []\n      nodes: []\n      budget-seconds: 9\nsentinel-solver:\n  enabled: false\n  queue-size: 0\n")
	if err := os.WriteFile(path, input, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ChatGPTWeb.Sentinel.Remote.Scopes == nil || len(cfg.ChatGPTWeb.Sentinel.Remote.ScopeList()) != 0 {
		t.Fatal("explicit empty scopes lost")
	}
	if cfg.SentinelSolver.Limits().QueueSize != 0 || cfg.SentinelSolver.Limits().MaxSessions != 128 {
		t.Fatal("server defaults or explicit zero lost")
	}
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.ChatGPTWeb.Sentinel.Remote.Scopes == nil || reloaded.ChatGPTWeb.Sentinel.Remote.Budget() != 9 || reloaded.SentinelSolver.Limits().QueueSize != 0 {
		t.Fatal("saved values lost")
	}
}

func TestSentinelComputeConfigurationRejectsInvalid(t *testing.T) {
	for _, body := range []string{
		"chatgpt-web: {sentinel: {mode: unknown}}",
		"chatgpt-web: {sentinel: {mode: remote}}",
		"chatgpt-web: {sentinel: {remote: {budget-seconds: -1}}}",
		"chatgpt-web: {sentinel: {remote: {scopes: [unknown]}}}",
		"chatgpt-web: {sentinel: {remote: {scopes: null}}}",
		"chatgpt-web: {sentinel: {remote: {unknown: true}}}",
		"chatgpt-web: {sentinel: {remote: {nodes: [{name: a, url: 'http://example.com', api-key: x}]}}}",
		"sentinel-solver: {enabled: true}",
		"sentinel-solver: {unknown: true}",
		"sentinel-solver: {max-sessions: 0}",
	} {
		t.Run(body, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadConfig(path); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestSentinelComputeLocalDefault(t *testing.T) {
	var cfg Config
	if err := yaml.Unmarshal([]byte("chatgpt-web: {sentinel: {sdk-runtime-enabled: false}}"), &cfg); err != nil {
		t.Fatal(err)
	}
	resolved := cfg.ChatGPTWeb.Sentinel.Resolved()
	if resolved.Mode != "local" || resolved.SDKRuntimeEnabled || resolved.Remote.Budget() != 30 || len(resolved.Remote.ScopeList()) != 1 || cfg.SentinelSolver.Enabled {
		t.Fatal("local defaults changed")
	}
}
