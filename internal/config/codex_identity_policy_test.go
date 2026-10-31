package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func identityPtr[T any](v T) *T { return &v }
func TestCodexIdentityPolicyPriorityAndDefaults(t *testing.T) {
	cfg := &Config{Codex: CodexConfig{IdentityConfuse: true}, Routing: RoutingConfig{Strategy: "fill-first", PriorityOverrides: []RoutingPriorityOverride{{Priority: 5, Codex: &CodexIdentityOverride{RequireSessionAffinity: identityPtr(false), RestoreResponse: identityPtr(false), FingerprintMode: identityPtr("full"), UserAgent: identityPtr("custom/1")}}}}}
	policies := CompileCodexIdentityPolicies(cfg)
	base := policies.Resolve(0, "off", "credential/1")
	priority := policies.Resolve(5, "off", "credential/1")
	if base.ConfuseEffective || !base.RequireSessionAffinity || !base.RestoreResponse {
		t.Fatalf("defaults: %+v", base)
	}
	if !priority.ConfuseEffective || priority.RestoreResponse || priority.FingerprintMode != "full" || priority.UserAgent != "custom/1" {
		t.Fatalf("override: %+v", priority)
	}
	*cfg.Routing.PriorityOverrides[0].Codex.FingerprintMode = "device"
	if policies.Resolve(5, "off", "").FingerprintMode != "full" {
		t.Fatal("in-flight snapshot changed")
	}
	if next := CompileCodexIdentityPolicies(cfg); next.Version == policies.Version {
		t.Fatal("changed policy has same version")
	}
	if n := testing.AllocsPerRun(1000, func() { _ = policies.Resolve(5, "device", "") }); n != 0 {
		t.Fatalf("lookup allocated: %f", n)
	}
}
func TestCodexIdentityOptionalFalseAndNormalization(t *testing.T) {
	mode := " FULL "
	raw := []RoutingPriorityOverride{{Priority: 1, Codex: &CodexIdentityOverride{IdentityConfuse: identityPtr(false), FingerprintMode: &mode}}}
	out, err := NormalizeRoutingPriorityOverrides(raw)
	if err != nil {
		t.Fatal(err)
	}
	if *out[0].Codex.IdentityConfuse || *out[0].Codex.FingerprintMode != "full" || mode != " FULL " {
		t.Fatal("normalization changed source or lost false")
	}
	for _, size := range []int{0, 65} {
		o := CodexIdentityOverride{SessionIdentityPoolSize: &size}
		if o.Validate() == nil {
			t.Fatal("invalid size accepted")
		}
	}
}
func BenchmarkCodexIdentityPolicyLookup(b *testing.B) {
	for _, n := range []int{10000, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			cfg := &Config{}
			for i := 0; i < n; i++ {
				cfg.Routing.PriorityOverrides = append(cfg.Routing.PriorityOverrides, RoutingPriorityOverride{Priority: i, Codex: &CodexIdentityOverride{IdentityConfuse: identityPtr(true)}})
			}
			policies := CompileCodexIdentityPolicies(cfg)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = policies.Resolve(n-1, "off", "")
			}
		})
	}
}

func TestCodexIdentityYAMLPreserve(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	source := `codex:
  identity-confuse-require-session-affinity: false # keep global
routing:
  priority-overrides:
    - priority: 5
      codex:
        identity-confuse: false # keep override
        future-field: retained
`
	if err := os.WriteFile(path, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Routing.PriorityOverrides[0].Codex.RestoreResponse = identityPtr(false)
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"# keep global", "# keep override", "future-field: retained", "identity-confuse-restore-response: false"} {
		if !strings.Contains(string(data), part) {
			t.Fatalf("lost %s: %s", part, data)
		}
	}
	cfg.Routing.PriorityOverrides[0].Codex = nil
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	again, _ := os.ReadFile(path)
	if !strings.Contains(string(again), "future-field: retained") || strings.Contains(string(again), "identity-confuse-restore-response") {
		t.Fatalf("inheritance reset failed: %s", again)
	}
	for _, bad := range []string{"yes", "on", "\"false\""} {
		raw := []byte("routing:\n  priority-overrides:\n    - priority: 5\n      codex:\n        identity-confuse: " + bad + "\n")
		if err := validateRequestPolicyBooleansYAML(raw); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}
