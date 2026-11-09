package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChatGPTWebImageAggregationConfig(t *testing.T) {
	if got := (ChatGPTWebImageConfig{}).Resolved().NAggregationConcurrency; got != 3 {
		t.Fatalf("default=%d", got)
	}
	for _, value := range []string{"1", "3", "10", "0", "11", "-1", "1.5", "true", "\"3\""} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			err := os.WriteFile(path, []byte("# preserve\nport: 8317\nimages:\n  chatgpt-web:\n    n-aggregation-concurrency: "+value+"\n"), 0600)
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			valid := value == "1" || value == "3" || value == "10"
			if !valid {
				if err == nil {
					t.Fatal("invalid concurrency accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			clone, err := Clone(cfg)
			if err != nil {
				t.Fatal(err)
			}
			*clone.Images.ChatGPTWeb.NAggregationConcurrency = 2
			if *cfg.Images.ChatGPTWeb.NAggregationConcurrency == 2 {
				t.Fatal("clone shared pointer")
			}
			if err := SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatal(err)
			}
			loaded, err := LoadConfig(path)
			if err != nil || *loaded.Images.ChatGPTWeb.NAggregationConcurrency != *cfg.Images.ChatGPTWeb.NAggregationConcurrency {
				t.Fatalf("round trip: %v", err)
			}
			data, _ := os.ReadFile(path)
			if !strings.Contains(string(data), "# preserve") {
				t.Fatal("comment lost")
			}
		})
	}
}

func TestChatGPTWebImageRefillConfig(t *testing.T) {
	defaults := (ChatGPTWebImageConfig{}).Resolved()
	if defaults.NAggregationMaxRetries != 1 || defaults.NAggregationReturnPartial {
		t.Fatal("invalid defaults")
	}
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"n-aggregation-max-retries: 0\n    n-aggregation-return-partial: false", true},
		{"n-aggregation-max-retries: 3\n    n-aggregation-return-partial: true", true},
		{"n-aggregation-max-retries: -1", false}, {"n-aggregation-max-retries: 4", false}, {"n-aggregation-max-retries: 1.5", false}, {"n-aggregation-max-retries: null", false},
		{"n-aggregation-return-partial: 1", false}, {"n-aggregation-return-partial: \"false\"", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(file, []byte("port: 8317\nimages:\n  chatgpt-web:\n    "+tc.value+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(file)
			if !tc.valid {
				if err == nil {
					t.Fatal("accepted invalid config")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			clone, err := Clone(cfg)
			if err != nil {
				t.Fatal(err)
			}
			original := cfg.Images.ChatGPTWeb.Resolved()
			*clone.Images.ChatGPTWeb.NAggregationMaxRetries = 2
			if cfg.Images.ChatGPTWeb.Resolved().NAggregationMaxRetries != original.NAggregationMaxRetries {
				t.Fatal("clone aliases source")
			}
			if err := SaveConfigPreserveComments(file, cfg); err != nil {
				t.Fatal(err)
			}
			restored, err := LoadConfig(file)
			if err != nil {
				t.Fatal(err)
			}
			if restored.Images.ChatGPTWeb.Resolved().NAggregationMaxRetries != original.NAggregationMaxRetries || restored.Images.ChatGPTWeb.NAggregationReturnPartial != original.NAggregationReturnPartial {
				t.Fatal("round trip changed policy")
			}
		})
	}
}
