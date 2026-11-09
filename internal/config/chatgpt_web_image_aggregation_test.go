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
