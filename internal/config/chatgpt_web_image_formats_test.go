package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestChatGPTWebModelOutputFormats(t *testing.T) {
	c := ImagesConfig{}
	for model, formats := range c.ResolvedChatGPTWebModelOutputFormats() {
		want := model == "gpt-image-2.6" || model == "gpt-image-2.6-flare" || model == "gpt-image-2.6-sunburst"
		if slices.Contains(formats, "gif") != want {
			t.Fatalf("default %s: %v", model, formats)
		}
	}
	c.ChatGPTWeb.ImageModels = []string{"custom", "gpt-image-2.6"}
	c.ChatGPTWeb.ModelOutputFormats = map[string][]string{" CUSTOM ": {" JPG ", "gif", "jpeg"}, "gpt-image-2.6": {"png"}}
	normalized, err := c.NormalizedChatGPTWebModelOutputFormats()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(normalized["custom"], []string{"jpeg", "gif"}) {
		t.Fatalf("normalized: %v", normalized)
	}
	resolved := c.ResolvedChatGPTWebModelOutputFormats()
	c.ChatGPTWeb.ModelOutputFormats["gpt-image-2.6"][0] = "gif"
	if !slices.Equal(resolved["gpt-image-2.6"], []string{"png"}) {
		t.Fatal("snapshot mutated")
	}
	for _, bad := range []map[string][]string{{"missing": {"png"}}, {"custom": {}}, {"custom": {"svg"}}, {"custom": {"png"}, " CUSTOM ": {"gif"}}} {
		c.ChatGPTWeb.ModelOutputFormats = bad
		if _, err := c.NormalizedChatGPTWebModelOutputFormats(); err == nil {
			t.Fatalf("accepted invalid policy %v", bad)
		}
	}
}

func TestChatGPTWebModelOutputFormatsLoadClone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	yaml := "images:\n  chatgpt-web:\n    image-models: [custom]\n    model-output-formats:\n      ' CUSTOM ': [PNG, JPG, gif]\n"
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.Images.ChatGPTWeb.ModelOutputFormats["custom"], []string{"png", "jpeg", "gif"}) {
		t.Fatal("load normalization lost")
	}
	cloned, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cloned.Images.ChatGPTWeb.ModelOutputFormats["custom"][0] = "webp"
	if cfg.Images.ChatGPTWeb.ModelOutputFormats["custom"][0] != "png" {
		t.Fatal("clone shares policy")
	}
}

func TestChatGPTWebModelOutputFormatsSaveRestoresDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := "# keep\nimages:\n  chatgpt-web:\n    future-key: keep\n    model-output-formats:\n      gpt-image-2.5: [gif]\n      gpt-image-2.6: [png]\n"
	if err := os.WriteFile(path, []byte(initial), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	delete(cfg.Images.ChatGPTWeb.ModelOutputFormats, "gpt-image-2.5")
	for _, clearAll := range []bool{false, true} {
		if clearAll {
			cfg.Images.ChatGPTWeb.ModelOutputFormats = nil
		}
		if err = SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		reloaded, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(reloaded.Images.ChatGPTWeb.ModelOutputFormats) != len(cfg.Images.ChatGPTWeb.ModelOutputFormats) {
			t.Fatal("removed policy reappeared on reload")
		}
		content, _ := os.ReadFile(path)
		if !strings.Contains(string(content), "# keep") || !strings.Contains(string(content), "future-key: keep") {
			t.Fatal("unrelated config lost")
		}
	}
}
