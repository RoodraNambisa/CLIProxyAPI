package config

import (
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"testing"
)

func TestImageRequestTimeoutConfig(t *testing.T) {
	for _, seconds := range []int{0, 1, 1800, 86400} {
		cfg := ImagesConfig{CodexRequestTimeoutSeconds: seconds, ChatGPTWeb: ChatGPTWebImageConfig{RequestTimeoutSeconds: seconds}}
		if err := cfg.ValidateRequestTimeouts(); err != nil {
			t.Fatal(err)
		}
		data, err := yaml.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var got ImagesConfig
		if err = yaml.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if got.CodexRequestTimeoutSeconds != seconds || got.ChatGPTWeb.RequestTimeoutSeconds != seconds {
			t.Fatalf("roundtrip: %s", data)
		}
	}
	for _, seconds := range []int{-1, 86401} {
		if (ImagesConfig{CodexRequestTimeoutSeconds: seconds}).ValidateRequestTimeouts() == nil {
			t.Fatal("invalid Codex limit accepted")
		}
		if (ImagesConfig{ChatGPTWeb: ChatGPTWebImageConfig{RequestTimeoutSeconds: seconds}}).ValidateRequestTimeouts() == nil {
			t.Fatal("invalid Web limit accepted")
		}
	}
}

func TestImageRequestTimeoutSaveDisable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("images:\n  codex-request-timeout-seconds: 1200 # preserve\n  chatgpt-web:\n    request-timeout-seconds: 1800\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Images.CodexRequestTimeoutSeconds = 0
	cfg.Images.ChatGPTWeb.RequestTimeoutSeconds = 0
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Images.CodexRequestTimeoutSeconds != 0 || loaded.Images.ChatGPTWeb.RequestTimeoutSeconds != 0 {
		t.Fatal("save retained stale enabled timeout")
	}
	cfg.Images.ChatGPTWeb.RequestTimeoutSeconds = -1
	if SaveConfigPreserveComments(path, cfg) == nil {
		t.Fatal("save accepted negative timeout")
	}
}
