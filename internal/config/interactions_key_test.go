package config

import "testing"

func TestSanitizeInteractionsKeys(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		InteractionsKey: []GeminiKey{
			{
				APIKey:         "  shared-key  ",
				Prefix:         " /native/ ",
				BaseURL:        " https://one.example.com ",
				ProxyURL:       " direct ",
				Headers:        map[string]string{" x-test ": " value "},
				ExcludedModels: []string{" GEMINI-2.5-PRO ", "gemini-2.5-pro"},
			},
			{APIKey: "shared-key", BaseURL: "https://one.example.com"},
			{APIKey: "shared-key", BaseURL: "https://two.example.com"},
			{APIKey: "   "},
		},
	}

	cfg.SanitizeInteractionsKeys()

	if got := len(cfg.InteractionsKey); got != 2 {
		t.Fatalf("interactions keys len = %d, want 2", got)
	}
	first := cfg.InteractionsKey[0]
	if first.APIKey != "shared-key" {
		t.Fatalf("api-key = %q, want shared-key", first.APIKey)
	}
	if first.Prefix != "native" {
		t.Fatalf("prefix = %q, want native", first.Prefix)
	}
	if first.BaseURL != "https://one.example.com" {
		t.Fatalf("base-url = %q, want normalized value", first.BaseURL)
	}
	if first.ProxyURL != "direct" {
		t.Fatalf("proxy-url = %q, want direct", first.ProxyURL)
	}
	if got := first.Headers["x-test"]; got != "value" {
		t.Fatalf("header = %q, want value", got)
	}
	if got := len(first.ExcludedModels); got != 1 || first.ExcludedModels[0] != "gemini-2.5-pro" {
		t.Fatalf("excluded-models = %#v, want normalized deduplicated model", first.ExcludedModels)
	}
	if got := cfg.InteractionsKey[1].BaseURL; got != "https://two.example.com" {
		t.Fatalf("second base-url = %q, want https://two.example.com", got)
	}
}

func TestSanitizeGeminiAndInteractionsKeysRemainIndependent(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		GeminiKey:       []GeminiKey{{APIKey: "gemini-key"}},
		InteractionsKey: []GeminiKey{{APIKey: "interactions-key"}},
	}
	cfg.SanitizeGeminiKeys()
	cfg.SanitizeInteractionsKeys()

	if got := cfg.GeminiKey[0].APIKey; got != "gemini-key" {
		t.Fatalf("gemini api-key = %q", got)
	}
	if got := cfg.InteractionsKey[0].APIKey; got != "interactions-key" {
		t.Fatalf("interactions api-key = %q", got)
	}
}
