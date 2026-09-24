package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestChatGPTWebImageReasoningConfig(t *testing.T) {
	for _, tc := range []struct {
		input   string
		want    string
		invalid bool
	}{
		{input: "{}", want: "auto"},
		{input: "reasoning-mode: ''", want: "auto"},
		{input: "reasoning-mode: instant", want: "instant"},
		{input: "reasoning-mode: ' INSTANT '", want: "instant"},
		{input: "upstream-model: custom\nreasoning-mode: auto", want: "auto"},
		{input: "upstream-model: custom\nreasoning-mode: instant", invalid: true},
		{input: "reasoning-mode: low", want: "low"},
		{input: "reasoning-mode: medium", want: "medium"},
		{input: "reasoning-mode: high", want: "high"},
		{input: "reasoning-mode: xhigh", want: "xhigh"},
		{input: "reasoning-mode: turbo", invalid: true},
		{input: "reasoning-mode: false", invalid: true},
		{input: "reasoning-mode: 0", invalid: true},
		{input: "reasoning-mode: null", invalid: true},
	} {
		t.Run(tc.input, func(t *testing.T) {
			var cfg ChatGPTWebImageConfig
			err := yaml.Unmarshal([]byte(tc.input), &cfg)
			if (err != nil) != tc.invalid {
				t.Fatalf("decode error = %v, invalid = %v", err, tc.invalid)
			}
			if !tc.invalid && cfg.Resolved().ReasoningMode != tc.want {
				t.Fatalf("resolved mode = %q, want %q", cfg.Resolved().ReasoningMode, tc.want)
			}
		})
	}
	var cfg Config
	if err := json.Unmarshal([]byte(`{"images":{"chatgpt-web":{"reasoning-mode":"other"}}}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Images.ChatGPTWeb.ValidateReasoningMode() == nil {
		t.Fatal("invalid JSON config accepted")
	}
}

func TestChatGPTWebImageReasoningSaveReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("images:\n  chatgpt-web:\n    request-timeout-seconds: 120\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"instant", "auto", ""} {
		cfg.Images.ChatGPTWeb.ReasoningMode = mode
		if err = SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		loaded, errLoad := LoadConfig(path)
		if errLoad != nil {
			t.Fatal(errLoad)
		}
		if loaded.Images.ChatGPTWeb.ResolvedReasoningMode() != cfg.Images.ChatGPTWeb.ResolvedReasoningMode() || loaded.Images.ChatGPTWeb.RequestTimeoutSeconds != 120 {
			t.Fatalf("round trip lost policy: %+v", loaded.Images.ChatGPTWeb)
		}
	}
	cfg.Images.ChatGPTWeb.ReasoningMode = "instant"
	cfg.Images.ChatGPTWeb.UpstreamModel = "custom"
	if SaveConfigPreserveComments(path, cfg) == nil {
		t.Fatal("conflicting carrier saved")
	}
}
