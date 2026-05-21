package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigOptionalImagesStreamFlushSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`images:
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
