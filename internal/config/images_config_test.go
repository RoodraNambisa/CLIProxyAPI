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
