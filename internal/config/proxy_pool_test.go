package config

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestNormalizeProxyConfiguration(t *testing.T) {
	pools, rules, err := NormalizeProxyConfiguration(
		[]ProxyPoolConfig{{
			Name:           " Residential ",
			SpreadBindings: true,
			Entries: []ProxyPoolEntryConfig{{
				ID:          " home ",
				URLTemplate: "socks5h://user:pass@10.0.0.6",
				Ports:       "6000,3336-5999,3334",
			}},
		}},
		[]ProxyRuleConfig{{
			Name:       " web ",
			Pool:       "residential",
			Providers:  []string{" ChatGPT-Web ", "codex", "CODEX"},
			Priorities: []int{-1, 0, -1},
		}},
	)
	if err != nil {
		t.Fatalf("NormalizeProxyConfiguration() error = %v", err)
	}
	pool := pools[0]
	if pool.Name != "Residential" || pool.CheckIntervalSeconds != 300 || pool.BindAttempts != 3 || !pool.SpreadBindings {
		t.Fatalf("normalized pool = %#v", pool)
	}
	if got, want := pool.Entries[0].Ports, "3334,3336-6000"; got != want {
		t.Fatalf("ports = %q, want %q", got, want)
	}
	if got, want := rules[0].Providers, []string{"chatgpt-web", "codex"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("providers = %#v, want %#v", got, want)
	}
	if got := rules[0].Priorities; len(got) != 2 || got[0] != 0 || got[1] != -1 {
		t.Fatalf("priorities = %#v", got)
	}
}

func TestNormalizeProxyConfigurationRejectsInvalidReferences(t *testing.T) {
	validPool := ProxyPoolConfig{
		Name:    "one",
		Entries: []ProxyPoolEntryConfig{{ID: "node", URLTemplate: "http://proxy.example:8080"}},
	}
	tests := []struct {
		name  string
		pools []ProxyPoolConfig
		rules []ProxyRuleConfig
	}{
		{name: "duplicate pool", pools: []ProxyPoolConfig{validPool, validPool}},
		{name: "unknown pool", pools: []ProxyPoolConfig{validPool}, rules: []ProxyRuleConfig{{Name: "rule", Pool: "missing"}}},
		{name: "missing proxy port", pools: []ProxyPoolConfig{{Name: "one", Entries: []ProxyPoolEntryConfig{{ID: "node", URLTemplate: "http://proxy.example"}}}}},
		{name: "unsupported placeholder", pools: []ProxyPoolConfig{{Name: "one", PlaceholderCharset: "abc@", Entries: []ProxyPoolEntryConfig{{ID: "node", URLTemplate: "http://proxy.example:8080"}}}}},
		{name: "unaddressable pool name", pools: []ProxyPoolConfig{{Name: "one/two", Entries: validPool.Entries}}},
		{name: "negative check interval", pools: []ProxyPoolConfig{{Name: "one", CheckIntervalSeconds: -1, Entries: validPool.Entries}}},
		{name: "negative bind attempts", pools: []ProxyPoolConfig{{Name: "one", BindAttempts: -1, Entries: validPool.Entries}}},
		{name: "explicit empty providers", pools: []ProxyPoolConfig{validPool}, rules: []ProxyRuleConfig{{Name: "rule", Pool: "one", Providers: []string{" "}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := NormalizeProxyConfiguration(test.pools, test.rules); err == nil {
				t.Fatal("NormalizeProxyConfiguration() error = nil")
			}
		})
	}
}

func TestNormalizeProxyConfigurationRejectsOverflowingCheckInterval(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("an int cannot represent a time.Duration-overflowing seconds value on 32-bit platforms")
	}
	maxCheckIntervalSeconds := maxProxyPoolCheckIntervalSeconds
	pool := ProxyPoolConfig{
		Name:                 "one",
		CheckIntervalSeconds: int(maxCheckIntervalSeconds),
		Entries:              []ProxyPoolEntryConfig{{ID: "node", URLTemplate: "http://proxy.example:8080"}},
	}
	pools, _, errMax := NormalizeProxyConfiguration([]ProxyPoolConfig{pool}, nil)
	if errMax != nil {
		t.Fatalf("NormalizeProxyConfiguration(max) error = %v", errMax)
	}
	if interval := time.Duration(pools[0].CheckIntervalSeconds) * time.Second; interval <= 0 {
		t.Fatalf("maximum check interval overflowed: %s", interval)
	}

	pool.CheckIntervalSeconds++
	if _, _, errOverflow := NormalizeProxyConfiguration([]ProxyPoolConfig{pool}, nil); errOverflow == nil {
		t.Fatal("NormalizeProxyConfiguration(overflowing check interval) error = nil")
	}
}

func TestNormalizeProxyHealthCheckConfigurationDefaultsAndValidatesEndpoints(t *testing.T) {
	defaults, errDefaults := NormalizeProxyHealthCheckConfiguration(ProxyHealthCheckConfig{})
	if errDefaults != nil {
		t.Fatalf("NormalizeProxyHealthCheckConfiguration(defaults) error = %v", errDefaults)
	}
	if defaults.Concurrency != 8 || defaults.EndpointTimeoutSeconds != 8 || defaults.FailureThreshold != 1 {
		t.Fatalf("defaults = %#v", defaults)
	}
	if len(defaults.Endpoints) != 1 || defaults.Endpoints[0].Mode != ProxyHealthCheckModeCloudflareTrace {
		t.Fatalf("default endpoints = %#v", defaults.Endpoints)
	}

	configured, errConfigured := NormalizeProxyHealthCheckConfiguration(ProxyHealthCheckConfig{
		Concurrency:            24,
		EndpointTimeoutSeconds: 11,
		FailureThreshold:       3,
		Endpoints: []ProxyHealthCheckEndpointConfig{
			{Name: " primary ", URL: "https://primary.example/health", Mode: "HTTP-STATUS"},
			{Name: "backup", URL: "http://backup.example/trace"},
		},
	})
	if errConfigured != nil {
		t.Fatalf("NormalizeProxyHealthCheckConfiguration(configured) error = %v", errConfigured)
	}
	if configured.Endpoints[0].Name != "primary" || configured.Endpoints[0].Mode != ProxyHealthCheckModeHTTPStatus || configured.Endpoints[1].Mode != ProxyHealthCheckModeCloudflareTrace {
		t.Fatalf("configured endpoints = %#v", configured.Endpoints)
	}

	invalid := []ProxyHealthCheckConfig{
		{Concurrency: -1},
		{EndpointTimeoutSeconds: -1},
		{FailureThreshold: -1},
		{Endpoints: []ProxyHealthCheckEndpointConfig{{Name: "endpoint", URL: "file:///tmp/health"}}},
		{Endpoints: []ProxyHealthCheckEndpointConfig{{Name: "endpoint", URL: "https://example.test", Mode: "unknown"}}},
		{Endpoints: []ProxyHealthCheckEndpointConfig{{Name: "same", URL: "https://one.test"}, {Name: "SAME", URL: "https://two.test"}}},
	}
	for index, candidate := range invalid {
		if _, errInvalid := NormalizeProxyHealthCheckConfiguration(candidate); errInvalid == nil {
			t.Fatalf("invalid configuration %d was accepted: %#v", index, candidate)
		}
	}
}

func TestMatchProxyRuleUsesFirstMatch(t *testing.T) {
	rules := []ProxyRuleConfig{
		{Name: "codex-zero", Pool: "first", Providers: []string{"codex"}, Priorities: []int{0}},
		{Name: "codex-any", Pool: "second", Providers: []string{"codex"}},
		{Name: "fallback", Pool: "third"},
	}
	if got, ok := MatchProxyRule(rules, "CODEX", 0); !ok || got != "first" {
		t.Fatalf("MatchProxyRule(codex, 0) = %q, %t", got, ok)
	}
	if got, ok := MatchProxyRule(rules, "codex", -1); !ok || got != "second" {
		t.Fatalf("MatchProxyRule(codex, -1) = %q, %t", got, ok)
	}
	if got, ok := MatchProxyRule(rules, "xai", 0); !ok || got != "third" {
		t.Fatalf("MatchProxyRule(xai, 0) = %q, %t", got, ok)
	}
}

func TestLoadConfigNormalizesProxyConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	raw := `
proxy-pools:
  - name: residential
    entries:
      - id: node
        url-template: socks5h://user:pass@10.0.0.6
        ports: 3336-3338,3334
  - name: fallback
    entries:
      - id: node
        url-template: http://proxy.example:8080
proxy-rules:
  - name: first
    pool: RESIDENTIAL
    providers: [ChatGPT-Web]
  - name: second
    pool: fallback
    providers: [chatgpt-web]
`
	if errWrite := os.WriteFile(path, []byte(raw), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	if got := cfg.ProxyPools[0].Entries[0].Ports; got != "3334,3336-3338" {
		t.Fatalf("ports = %q", got)
	}
	if got := cfg.ProxyRules[0].Pool; got != "residential" {
		t.Fatalf("rule pool = %q", got)
	}
	if got, matched := MatchProxyRule(cfg.ProxyRules, "chatgpt-web", 0); !matched || got != "residential" {
		t.Fatalf("normalized first match = %q, %t", got, matched)
	}
}
