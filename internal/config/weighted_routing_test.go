package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWeightedRoutingConfigAliasesAndRoundTrip(t *testing.T) {
	for _, strategy := range []string{"weighted-round-robin", "WeightedRoundRobin", " WRR "} {
		t.Run(strategy, func(t *testing.T) {
			normalized, ok := NormalizeRoutingStrategy(strategy)
			if !ok || normalized != "weighted-round-robin" {
				t.Fatal("weighted strategy alias was not recognized")
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte("routing:\n  strategy: round-robin\n  extension: keep\n"), 0600); err != nil {
				t.Fatal(err)
			}
			routing, err := NormalizeRoutingConfig(RoutingConfig{Strategy: strategy, PriorityOverrides: []RoutingPriorityOverride{{Priority: 10, Strategy: strategy}}})
			if err != nil {
				t.Fatal(err)
			}
			if err := SaveConfigPreserveComments(path, &Config{Routing: routing}); err != nil {
				t.Fatal(err)
			}
			loaded, err := LoadConfigOptional(path, false)
			if err != nil {
				t.Fatal(err)
			}
			global, valid := NormalizeRoutingStrategy(loaded.Routing.Strategy)
			if !valid || global != "weighted-round-robin" || len(loaded.Routing.PriorityOverrides) != 1 || loaded.Routing.PriorityOverrides[0].Strategy != "weighted-round-robin" {
				t.Fatalf("weighted strategy did not survive reload: %v", err)
			}
		})
	}
	for _, strategy := range []string{"", "round-robin", "fill-first", "random"} {
		if _, ok := NormalizeRoutingStrategy(strategy); !ok {
			t.Fatal("an existing strategy became invalid")
		}
	}
	if _, ok := NormalizeRoutingStrategy("weighted-unknown"); ok {
		t.Fatal("unknown strategy was accepted")
	}
}
