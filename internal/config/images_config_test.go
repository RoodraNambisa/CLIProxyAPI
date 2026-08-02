package config

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigOptionalImagesStreamFlushSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  enable-stream-flush: false
  stream-flush-interval-ms: 20
  stream-flush-min-bytes: 65536
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Images.StreamFlushIntervalMS != 20 {
		t.Fatalf("StreamFlushIntervalMS = %d, want 20", cfg.Images.StreamFlushIntervalMS)
	}
	if cfg.Images.StreamFlushMinBytes != 65536 {
		t.Fatalf("StreamFlushMinBytes = %d, want 65536", cfg.Images.StreamFlushMinBytes)
	}
	if cfg.Images.EnableStreamFlush == nil || *cfg.Images.EnableStreamFlush {
		t.Fatalf("EnableStreamFlush = %v, want false", cfg.Images.EnableStreamFlush)
	}
}

func TestLoadConfigOptionalChatGPTWebImageSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  chatgpt-web:
    upstream-model: gpt-5-5-custom
    ignore-unsupported-params: true
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if got := cfg.Images.ChatGPTWeb.ResolvedUpstreamModel(); got != "gpt-5-5-custom" {
		t.Fatalf("ChatGPTWeb upstream model = %q, want gpt-5-5-custom", got)
	}
	if !cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams {
		t.Fatal("IgnoreUnsupportedParams = false, want true")
	}
}

func TestLoadConfigOptionalDefaultsChatGPTWebImageUpstreamModel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("images: {}\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if got := cfg.Images.ChatGPTWeb.ResolvedUpstreamModel(); got != DefaultChatGPTWebImageUpstreamModel {
		t.Fatalf("ChatGPTWeb upstream model = %q, want %q", got, DefaultChatGPTWebImageUpstreamModel)
	}
	if cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams {
		t.Fatal("IgnoreUnsupportedParams = true, want false")
	}
}

func TestLoadConfigOptionalChatGPTWebImageAspectSettings(t *testing.T) {
	responseBudget := 64
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  chatgpt-web:
    adapt-size-to-aspect-ratio: true
    strict-size: true
    aspect-ratio-max-error-percent: 0.5
    max-resize-edge-pixels: 2048
    resize-to-requested-size: true
    resize-filter: approx-bilinear
    max-image-response-megabytes: 64
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	resolved := cfg.Images.ChatGPTWeb.Resolved()
	if !resolved.AdaptSizeToAspectRatio || !resolved.StrictSize || resolved.AspectRatioMaxErrorPercent != 0.5 || resolved.MaxResizeEdgePixels != 2048 ||
		!resolved.ResizeToRequestedSize || resolved.ResizeFilter != ChatGPTWebResizeFilterApproxBiLinear ||
		resolved.MaxImageResponseMegabytes != responseBudget {
		t.Fatalf("resolved ChatGPT Web image config = %#v", resolved)
	}
}

func TestChatGPTWebImageAspectSettingsDefaultsAndValidation(t *testing.T) {
	resolved := (ChatGPTWebImageConfig{}).Resolved()
	if resolved.AdaptSizeToAspectRatio ||
		resolved.StrictSize ||
		resolved.AspectRatioMaxErrorPercent != DefaultChatGPTWebAspectRatioMaxErrorPercent ||
		resolved.MaxResizeEdgePixels != DefaultChatGPTWebMaxResizeEdgePixels ||
		resolved.ResizeToRequestedSize || resolved.ResizeFilter != DefaultChatGPTWebResizeFilter ||
		resolved.MaxImageResponseMegabytes != DefaultChatGPTWebMaxImageResponseMegabytes {
		t.Fatalf("resolved defaults = %#v", resolved)
	}

	negativeTolerance := -0.1
	tooLargeTolerance := MaxChatGPTWebAspectRatioMaxErrorPercent + 0.1
	nanTolerance := math.NaN()
	zeroEdge := 0
	tooLargeEdge := MaxChatGPTWebMaxResizeEdgePixels + 1
	zeroResponseBudget := 0
	tooLargeResponseBudget := MaxChatGPTWebMaxImageResponseMegabytes + 1
	tests := []struct {
		name string
		cfg  ChatGPTWebImageConfig
	}{
		{name: "negative tolerance", cfg: ChatGPTWebImageConfig{AspectRatioMaxErrorPercent: &negativeTolerance}},
		{name: "large tolerance", cfg: ChatGPTWebImageConfig{AspectRatioMaxErrorPercent: &tooLargeTolerance}},
		{name: "NaN tolerance", cfg: ChatGPTWebImageConfig{AspectRatioMaxErrorPercent: &nanTolerance}},
		{name: "zero edge", cfg: ChatGPTWebImageConfig{MaxResizeEdgePixels: &zeroEdge}},
		{name: "large edge", cfg: ChatGPTWebImageConfig{MaxResizeEdgePixels: &tooLargeEdge}},
		{name: "resize without adaptation", cfg: ChatGPTWebImageConfig{ResizeToRequestedSize: true}},
		{name: "strict size without adaptation", cfg: ChatGPTWebImageConfig{StrictSize: true}},
		{name: "unknown filter", cfg: ChatGPTWebImageConfig{ResizeFilter: "nearest"}},
		{name: "zero response budget", cfg: ChatGPTWebImageConfig{MaxImageResponseMegabytes: &zeroResponseBudget}},
		{name: "large response budget", cfg: ChatGPTWebImageConfig{MaxImageResponseMegabytes: &tooLargeResponseBudget}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want validation error")
			}
		})
	}
}

func TestChatGPTWebImageResizeSettingsAcceptBothFilters(t *testing.T) {
	for _, filter := range []string{ChatGPTWebResizeFilterCatmullRom, ChatGPTWebResizeFilterApproxBiLinear} {
		t.Run(filter, func(t *testing.T) {
			cfg := ChatGPTWebImageConfig{AdaptSizeToAspectRatio: true, ResizeToRequestedSize: true, ResizeFilter: filter}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestLoadConfigOptionalRejectsInvalidChatGPTWebImageAspectSettings(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "ratio tolerance", body: "aspect-ratio-max-error-percent: 11"},
		{name: "resize dependency", body: "resize-to-requested-size: true"},
		{name: "strict size dependency", body: "strict-size: true"},
		{name: "resize filter", body: "resize-filter: nearest"},
		{name: "response budget", body: "max-image-response-megabytes: 257"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			data := []byte("images:\n  chatgpt-web:\n    " + test.body + "\n")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			if _, err := LoadConfigOptional(path, false); err == nil {
				t.Fatal("LoadConfigOptional() error = nil, want validation error")
			}
		})
	}
}

func TestLoadConfigOptionalDefaultsImagesStreamFlushEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  stream-flush-interval-ms: 20
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Images.EnableStreamFlush == nil || !*cfg.Images.EnableStreamFlush {
		t.Fatalf("EnableStreamFlush = %v, want true", cfg.Images.EnableStreamFlush)
	}
}

func TestLoadConfigOptionalDefaultsNativeImagesDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  image-model: gpt-image-2
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Images.Native.Generations.Enabled {
		t.Fatal("Native.Generations.Enabled = true, want false")
	}
	if cfg.Images.Native.Edits.Enabled {
		t.Fatal("Native.Edits.Enabled = true, want false")
	}
	if got := cfg.Images.Native.Generations.Models; len(got) != 2 || got[0] != "gpt-image-2" || got[1] != "gpt-image-1.5" {
		t.Fatalf("native generation models = %#v", got)
	}
	if cfg.Images.Native.Generations.UnsupportedModelStatusCode != 400 {
		t.Fatalf("native generation status = %d, want 400", cfg.Images.Native.Generations.UnsupportedModelStatusCode)
	}
	if cfg.Images.Native.Edits.UnsupportedModelMessage != "Native image edit is not enabled for model {model}" {
		t.Fatalf("native edit message = %q", cfg.Images.Native.Edits.UnsupportedModelMessage)
	}
}

func TestLoadConfigOptionalNativeImagesSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  native:
    generations:
      enabled: true
      models: ["gpt-image-2", "gpt-image-2", " GPT-IMAGE-2 ", "gpt-image-1.5"]
      param-rules: ["n", " n ", "background=transparent"]
      unsupported-model-status-code: 409
      unsupported-model-message: "no native generation for {model}"
    edits:
      enabled: true
      models: []
      param-rules: ["mask=null"]
      unsupported-model-status-code: 99
      unsupported-model-message: ""
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	gen := cfg.Images.Native.Generations
	if !gen.Enabled {
		t.Fatal("generation native enabled = false, want true")
	}
	if got := gen.Models; len(got) != 2 || got[0] != "gpt-image-2" || got[1] != "gpt-image-1.5" {
		t.Fatalf("generation models = %#v", got)
	}
	if got := gen.ParamRules; len(got) != 2 || got[0] != "n" || got[1] != "background=transparent" {
		t.Fatalf("generation param rules = %#v", got)
	}
	if gen.UnsupportedModelStatusCode != 409 {
		t.Fatalf("generation status = %d, want 409", gen.UnsupportedModelStatusCode)
	}
	if gen.UnsupportedModelMessage != "no native generation for {model}" {
		t.Fatalf("generation message = %q", gen.UnsupportedModelMessage)
	}
	edit := cfg.Images.Native.Edits
	if !edit.Enabled {
		t.Fatal("edit native enabled = false, want true")
	}
	if got := edit.Models; len(got) != 2 || got[0] != "gpt-image-2" || got[1] != "gpt-image-1.5" {
		t.Fatalf("edit default models = %#v", got)
	}
	if edit.UnsupportedModelStatusCode != 400 {
		t.Fatalf("edit status = %d, want 400", edit.UnsupportedModelStatusCode)
	}
	if edit.UnsupportedModelMessage != "Native image edit is not enabled for model {model}" {
		t.Fatalf("edit message = %q", edit.UnsupportedModelMessage)
	}
}

func TestLoadConfigOptionalClampsNegativeImagesStreamFlushSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
  stream-flush-interval-ms: -1
  stream-flush-min-bytes: -1
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Images.StreamFlushIntervalMS != 0 {
		t.Fatalf("StreamFlushIntervalMS = %d, want 0", cfg.Images.StreamFlushIntervalMS)
	}
	if cfg.Images.StreamFlushMinBytes != 0 {
		t.Fatalf("StreamFlushMinBytes = %d, want 0", cfg.Images.StreamFlushMinBytes)
	}
}

func TestLoadConfigOptionalStreamingFlushSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`streaming:
  enable-stream-flush: true
  stream-flush-interval-ms: 25
  stream-flush-min-bytes: 32768
  trust-upstream-sse: true
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if !cfg.Streaming.EnableStreamFlush {
		t.Fatalf("EnableStreamFlush = false, want true")
	}
	if cfg.Streaming.StreamFlushIntervalMS != 25 {
		t.Fatalf("StreamFlushIntervalMS = %d, want 25", cfg.Streaming.StreamFlushIntervalMS)
	}
	if cfg.Streaming.StreamFlushMinBytes != 32768 {
		t.Fatalf("StreamFlushMinBytes = %d, want 32768", cfg.Streaming.StreamFlushMinBytes)
	}
	if !cfg.Streaming.TrustUpstreamSSE {
		t.Fatalf("TrustUpstreamSSE = false, want true")
	}
}

func TestLoadConfigOptionalClampsNegativeStreamingFlushSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`streaming:
  stream-flush-interval-ms: -1
  stream-flush-min-bytes: -1
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Streaming.StreamFlushIntervalMS != 0 {
		t.Fatalf("StreamFlushIntervalMS = %d, want 0", cfg.Streaming.StreamFlushIntervalMS)
	}
	if cfg.Streaming.StreamFlushMinBytes != 0 {
		t.Fatalf("StreamFlushMinBytes = %d, want 0", cfg.Streaming.StreamFlushMinBytes)
	}
}
