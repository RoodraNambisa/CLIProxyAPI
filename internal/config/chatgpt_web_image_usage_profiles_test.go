package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChatGPTWebImageUsageProfiles(t *testing.T) {
	c := ChatGPTWebImageUsageConfig{}
	if c.ResolvedEstimationProfile() != ChatGPTWebImageUsageByModel {
		t.Fatal("wrong default")
	}
	for model, want := range map[string]string{"gpt-image-2": "gpt-image-2", "gpt-image-2.5-flare": "gpt-image-2.5", "gpt-image-2.6-sunburst": "gpt-image-2.6", "custom": "gpt-image-2", "gpt-image-2.6-custom": "gpt-image-2"} {
		if got := c.ProfileForModel(model); got != want {
			t.Fatalf("%s: %s", model, got)
		}
	}
	images := ImagesConfig{}
	images.ChatGPTWeb.ImageModels = []string{"custom", "gpt-image-2.6"}
	c.ModelProfiles = map[string]string{" CUSTOM ": "GPT-IMAGE-2.5"}
	if err := c.ValidateModelProfiles(images); err != nil {
		t.Fatal(err)
	}
	c.ModelProfiles = c.ResolvedModelProfiles()
	for _, profile := range append([]string{ChatGPTWebImageUsageByModel}, ChatGPTWebImageUsageProfiles()...) {
		c.EstimationProfile = profile
		want := profile
		if profile == ChatGPTWebImageUsageByModel {
			want = "gpt-image-2.5"
		}
		if got := c.ProfileForModel("CUSTOM"); got != want {
			t.Fatalf("%s: %s", profile, got)
		}
	}
	c.EstimationProfile = ChatGPTWebImageUsageByModel
	if c.ModelProfiles["custom"] != "gpt-image-2.5" {
		t.Fatal("unified mode lost override")
	}
	for _, bad := range []map[string]string{{"custom": "by-model"}, {"custom": "wrong"}, {"missing": "gpt-image-2"}, {"custom": "gpt-image-2", " CUSTOM ": "gpt-image-2.6"}} {
		c.ModelProfiles = bad
		if c.ValidateModelProfiles(images) == nil {
			t.Fatalf("accepted %v", bad)
		}
	}
	c.ModelProfiles = nil
	c.EstimationProfile = "invalid"
	if c.Validate() == nil {
		t.Fatal("accepted invalid mode")
	}
}

func TestChatGPTWebImageUsageProfilesSaveCloneAndRemove(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	source := "# retain\nchatgpt-web:\n  image-usage:\n    future-setting: preserve\n    model-profiles:\n      ' GPT-IMAGE-2.5 ': gpt-image-2.6\n"
	if err := os.WriteFile(path, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	clone, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	clone.ChatGPTWeb.ImageUsage.ModelProfiles["gpt-image-2.5"] = "gpt-image-2"
	if cfg.ChatGPTWeb.ImageUsage.ModelProfiles["gpt-image-2.5"] != "gpt-image-2.6" {
		t.Fatal("clone shares map")
	}
	cfg.ChatGPTWeb.ImageUsage.ModelProfiles = map[string]string{}
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.ChatGPTWeb.ImageUsage.ModelProfiles) != 0 {
		t.Fatal("removed override revived")
	}
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), "future-setting: preserve") || !strings.Contains(string(b), "# retain") {
		t.Fatal("unrelated YAML lost")
	}
	cfg.ChatGPTWeb.ImageUsage.ModelProfiles = map[string]string{"missing": "gpt-image-2"}
	if SaveConfigPreserveComments(path, cfg) == nil {
		t.Fatal("saved unregistered alias")
	}
}
