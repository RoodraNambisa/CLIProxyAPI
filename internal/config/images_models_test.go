package config

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestResolvedImageModelLists(t *testing.T) {
	defaults := []string{"gpt-image-2", "gpt-image-2.5", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst"}
	for _, test := range []struct {
		name      string
		cfg       ImagesConfig
		tool, web []string
	}{
		{name: "defaults", tool: defaults, web: defaults},
		{name: "legacy alias", cfg: ImagesConfig{ImageModel: " old-image "}, tool: []string{"old-image"}, web: []string{"old-image"}},
		{name: "independent lists", cfg: ImagesConfig{
			ImageModels: []string{" gpt-image-2.5 ", "GPT-IMAGE-2.5", "", "custom-tool"},
			ChatGPTWeb:  ChatGPTWebImageConfig{ImageModels: []string{"web-only", " web-only ", ""}},
		}, tool: []string{"gpt-image-2", "gpt-image-2.5", "custom-tool"}, web: []string{"web-only"}},
		{name: "default included", cfg: ImagesConfig{ImageModel: "custom", ImageModels: []string{"gpt-image-2.5"}}, tool: []string{"custom", "gpt-image-2.5"}, web: []string{"custom"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			originalTool := slices.Clone(test.cfg.ImageModels)
			originalWeb := slices.Clone(test.cfg.ChatGPTWeb.ImageModels)
			if got := test.cfg.ResolvedImageModels(); !slices.Equal(got, test.tool) {
				t.Fatalf("tool models = %v, want %v", got, test.tool)
			}
			if got := test.cfg.ResolvedChatGPTWebImageModels(); !slices.Equal(got, test.web) {
				t.Fatalf("Web aliases = %v, want %v", got, test.web)
			}
			if !slices.Equal(originalTool, test.cfg.ImageModels) || !slices.Equal(originalWeb, test.cfg.ChatGPTWeb.ImageModels) {
				t.Fatal("resolving model lists mutated configuration")
			}
		})
	}
}

func TestLoadImageModelListsAndAutoCarrier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	for _, carrier := range []string{"", "    upstream-model: gpt-5-5-custom\n"} {
		data := "images:\n  image-models: [gpt-image-2.5, ' gpt-image-2.5 ', custom]\n  chatgpt-web:\n    image-models: [web-only, web-only]\n" + carrier
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfigOptional(path, false)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(cfg.Images.ImageModels, []string{"gpt-image-2.5", "custom"}) || !slices.Equal(cfg.Images.ChatGPTWeb.ImageModels, []string{"web-only"}) {
			t.Fatalf("model lists were not normalized: %+v", cfg.Images)
		}
		wantCarrier := "auto"
		if carrier != "" {
			wantCarrier = "gpt-5-5-custom"
		}
		if got := cfg.Images.ChatGPTWeb.ResolvedUpstreamModel(); got != wantCarrier {
			t.Fatalf("carrier = %q, want %q", got, wantCarrier)
		}
	}
}
