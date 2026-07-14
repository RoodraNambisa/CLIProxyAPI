package config

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeUsagePricingCanonicalizesModels(t *testing.T) {
	got, err := NormalizeUsagePricing(UsagePricingConfig{Models: map[string]UsageModelPrice{
		"  GPT-5.4  ": {
			InputPerMillion:       1.25,
			OutputPerMillion:      10,
			CachedInputPerMillion: 0.125,
		},
	}})
	if err != nil {
		t.Fatalf("NormalizeUsagePricing() error = %v", err)
	}
	if len(got.Models) != 1 {
		t.Fatalf("models length = %d, want 1", len(got.Models))
	}
	price, ok := got.Models["gpt-5.4"]
	if !ok {
		t.Fatalf("normalized model missing: %#v", got.Models)
	}
	if price.InputPerMillion != 1.25 || price.OutputPerMillion != 10 || price.CachedInputPerMillion != 0.125 {
		t.Fatalf("price = %#v", price)
	}
}

func TestNormalizeUsagePricingRejectsInvalidEntries(t *testing.T) {
	tests := []struct {
		name   string
		models map[string]UsageModelPrice
	}{
		{name: "empty model", models: map[string]UsageModelPrice{" ": {}}},
		{name: "normalized duplicate", models: map[string]UsageModelPrice{"GPT-5.4": {}, " gpt-5.4 ": {}}},
		{name: "negative input", models: map[string]UsageModelPrice{"gpt-5.4": {InputPerMillion: -1}}},
		{name: "nan output", models: map[string]UsageModelPrice{"gpt-5.4": {OutputPerMillion: math.NaN()}}},
		{name: "infinite cached", models: map[string]UsageModelPrice{"gpt-5.4": {CachedInputPerMillion: math.Inf(1)}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NormalizeUsagePricing(UsagePricingConfig{Models: test.models}); err == nil {
				t.Fatal("NormalizeUsagePricing() error = nil, want validation error")
			}
		})
	}
}

func TestLoadConfigNormalizesAndValidatesUsagePricing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	valid := `usage-pricing:
  models:
    " GPT-5.4 ":
      input-per-million: 1.25
`
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if _, ok := cfg.UsagePricing.Models["gpt-5.4"]; !ok {
		t.Fatalf("loaded models = %#v, want normalized key", cfg.UsagePricing.Models)
	}

	invalid := `usage-pricing:
  models:
    gpt-5.4:
      input-per-million: -.inf
`
	if err := os.WriteFile(path, []byte(invalid), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("LoadConfig() error = nil, want invalid usage price error")
	}
}

func TestSaveConfigCanonicalizesExistingUsagePricingModelKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	raw := `usage-pricing:
  models:
    " gpt-5.4 ": # keep model comment
      input-per-million: 1.25
`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", err)
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if strings.Count(string(saved), `"gpt-5.4":`) != 1 || !strings.Contains(string(saved), "keep model comment") {
		t.Fatalf("saved config did not canonicalize one commented model key:\n%s", saved)
	}
	reloaded, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() after save error = %v", err)
	}
	if len(reloaded.UsagePricing.Models) != 1 || reloaded.UsagePricing.Models["gpt-5.4"].InputPerMillion != 1.25 {
		t.Fatalf("reloaded models = %#v", reloaded.UsagePricing.Models)
	}
}
