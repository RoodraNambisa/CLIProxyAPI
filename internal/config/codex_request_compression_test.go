package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCodexRequestCompressionConfiguration(t *testing.T) {
	for _, value := range []string{"", "off", " ZSTD ", "gzip", "unknown"} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte("codex:\n  upstream-request-compression: \""+value+"\"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfig(path)
		if value == "gzip" || value == "unknown" {
			if err == nil {
				t.Fatal("unsupported compression accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		want := "off"
		if value == " ZSTD " {
			want = "zstd"
		}
		if cfg.Codex.UpstreamRequestCompression != want {
			t.Fatalf("mode=%q, want %q", cfg.Codex.UpstreamRequestCompression, want)
		}
	}
}
