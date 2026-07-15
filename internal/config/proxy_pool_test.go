package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeProxyConfiguration(t *testing.T) {
	pools, rules, err := NormalizeProxyConfiguration(
		[]ProxyPoolConfig{{
			Name: " Residential ",
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
	if pool.Name != "Residential" || pool.CheckIntervalSeconds != 300 || pool.BindAttempts != 3 {
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
