package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeAuthImagePriorityScope(t *testing.T) {
	base := AuthModelExclusionRule{Models: []string{"text"}, Priorities: []int{3, 5}, DisableImageGeneration: true}
	three, five, outside := base, base, base
	three.DisableImageGenerationPriorities = []int{3, 3}
	five.DisableImageGenerationPriorities = []int{5, 5}
	outside.DisableImageGenerationPriorities = []int{7}
	got := NormalizeAuthModelExclusionRules([]AuthModelExclusionRule{three, five, three, outside, base})
	if len(got) != 4 || !reflect.DeepEqual(got[0].DisableImageGenerationPriorities, []int{3}) ||
		!reflect.DeepEqual(got[1].DisableImageGenerationPriorities, []int{5}) ||
		!reflect.DeepEqual(got[2].DisableImageGenerationPriorities, []int{7}) {
		t.Fatalf("scope was lost, broadened, or incorrectly deduplicated: %#v", got)
	}
}

func TestAuthImagePriorityYAMLRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "auth-model-exclusions:\n  # shared rule\n  - models: [excluded]\n    priorities: [3, 5]\n    disable-image-generation: false # explicit switch\n    disable-image-generation-priorities: [5]\n    future-option: retained\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"# shared rule", "# explicit switch", "disable-image-generation: false", "future-option: retained"} {
		if !strings.Contains(string(saved), marker) {
			t.Fatalf("lost %q: %s", marker, saved)
		}
	}
	cfg.AuthModelExclusions[0].DisableImageGeneration = true
	cfg.AuthModelExclusions[0].DisableImageGenerationPriorities = nil
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.AuthModelExclusions[0].DisableImageGeneration || len(loaded.AuthModelExclusions[0].DisableImageGenerationPriorities) != 0 {
		t.Fatal("clearing the scope did not restore the legacy all-matched behavior")
	}
	cfg.AuthModelExclusions[0].DisableImageGeneration = false
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadConfig(path)
	if err != nil || loaded.AuthModelExclusions[0].DisableImageGeneration {
		t.Fatal("disable switch was not cleared", err)
	}
}
