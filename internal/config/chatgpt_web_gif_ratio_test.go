package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChatGPTWebGIFAspectRatioExceptionConfig(t *testing.T) {
	if (ChatGPTWebImageConfig{}).Resolved().GIFAllowAspectRatioMismatch {
		t.Fatal("exception must be opt-in")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	for _, raw := range []string{"true", "false", "[]", "invalid", "\"true\""} {
		if err := os.WriteFile(path, []byte("images:\n  chatgpt-web:\n    gif-allow-aspect-ratio-mismatch: "+raw+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfig(path)
		if raw != "true" && raw != "false" {
			if err == nil {
				t.Fatalf("accepted invalid boolean %s", raw)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Images.ChatGPTWeb.Resolved().GIFAllowAspectRatioMismatch != (raw == "true") {
			t.Fatal("wrong resolved value")
		}
		clone, err := Clone(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if clone.Images.ChatGPTWeb.GIFAllowAspectRatioMismatch != (raw == "true") {
			t.Fatal("clone lost value")
		}
	}
	if err := os.WriteFile(path, []byte("# preserved\nimages:\n  chatgpt-web:\n    gif-allow-aspect-ratio-mismatch: true # GIF choice\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Images.ChatGPTWeb.GIFAllowAspectRatioMismatch = false
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Images.ChatGPTWeb.GIFAllowAspectRatioMismatch {
		t.Fatal("false did not survive save/reload")
	}
}

func TestChatGPTWebGIFAspectRatioToleranceConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	for _, raw := range []string{"", "null", "0", "5", "10", "-1", "10.1", ".nan", ".inf", "[]", "true", "\"5\""} {
		body := "images:\n  chatgpt-web:\n    aspect-ratio-max-error-percent: 0.5\n"
		if raw != "" {
			body += "    gif-aspect-ratio-max-error-percent: " + raw + "\n"
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfig(path)
		valid := raw == "" || raw == "null" || raw == "0" || raw == "5" || raw == "10"
		if !valid {
			if err == nil {
				t.Fatalf("accepted %s", raw)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		want := 0.5
		switch raw {
		case "0":
			want = 0
		case "5":
			want = 5
		case "10":
			want = 10
		}
		if got := cfg.Images.ChatGPTWeb.Resolved().GIFAspectRatioMaxErrorPercent; got != want {
			t.Fatalf("%s: %g != %g", raw, got, want)
		}
		cloned, err := Clone(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err = SaveConfigPreserveComments(path, cloned); err != nil {
			t.Fatal(err)
		}
		loaded, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Images.ChatGPTWeb.Resolved().GIFAspectRatioMaxErrorPercent != want {
			t.Fatal("round trip lost tolerance")
		}
		cfg.Images.ChatGPTWeb.GIFAspectRatioMaxErrorPercent = nil
		if err = SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		loaded, err = LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Images.ChatGPTWeb.GIFAspectRatioMaxErrorPercent != nil || loaded.Images.ChatGPTWeb.Resolved().GIFAspectRatioMaxErrorPercent != 0.5 {
			t.Fatal("clear did not restore inheritance")
		}
	}
	if err := os.WriteFile(path, []byte("# keep\nimages:\n  chatgpt-web:\n    aspect-ratio-max-error-percent: 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	zero := 0.0
	cfg.Images.ChatGPTWeb.GIFAspectRatioMaxErrorPercent = &zero
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "gif-aspect-ratio-max-error-percent: 0") || !strings.Contains(string(body), "# keep") {
		t.Fatal("explicit zero or comment lost")
	}
}

func TestChatGPTWebGIFAspectRatioZeroFirstSave(t *testing.T) {
	for _, initial := range []string{"# keep\nport: 8317\n", "# keep\nimages: {}\n", "# keep\nimages:\n  chatgpt-web: {}\n"} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(initial), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		zero := 0.0
		cfg.Images.ChatGPTWeb.GIFAspectRatioMaxErrorPercent = &zero
		if err := SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		loaded, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Images.ChatGPTWeb.GIFAspectRatioMaxErrorPercent == nil || loaded.Images.ChatGPTWeb.Resolved().GIFAspectRatioMaxErrorPercent != 0 {
			t.Fatalf("first save lost explicit zero with original %q", initial)
		}
	}
}
