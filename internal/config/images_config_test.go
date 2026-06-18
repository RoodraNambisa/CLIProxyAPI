package config

import (
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
