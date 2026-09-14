package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestImageBootstrapConfig(t *testing.T) {
	for _, input := range []string{"{}", "bootstrap-timeout-seconds: 0\nbootstrap-retries: 0", "bootstrap-timeout-seconds: 3600\nbootstrap-retries: 5", "bootstrap-retries: 1"} {
		var cfg ChatGPTWebImageConfig
		if err := yaml.Unmarshal([]byte(input), &cfg); err != nil {
			t.Fatalf("%s: %v", input, err)
		}
		if err := cfg.ValidateBootstrap(); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"bootstrap-timeout-seconds", "bootstrap-retries"} {
		for _, value := range []string{"-1", "3601", "1.5", "1.0", "\"1\"", "true", "null"} {
			var cfg ChatGPTWebImageConfig
			if err := yaml.Unmarshal([]byte(key+": "+value), &cfg); err == nil {
				t.Errorf("accepted %s: %s", key, value)
			}
		}
		var cfg ChatGPTWebImageConfig
		if err := json.Unmarshal([]byte(`{"`+key+`":1.5}`), &cfg); err == nil {
			t.Errorf("accepted fractional JSON %s", key)
		}
	}
	var cfg ChatGPTWebImageConfig
	if err := yaml.Unmarshal([]byte("defaults: &b {bootstrap-retries: 2.5}\n<<: *b"), &cfg); err == nil {
		t.Fatal("fractional merged value accepted")
	}
}

func TestImageBootstrapConfigSaveReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("images:\n  chatgpt-web:\n    request-timeout-seconds: 120 # keep\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Images.ChatGPTWeb.BootstrapTimeoutSeconds = 10
	cfg.Images.ChatGPTWeb.BootstrapRetries = 1
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Images.ChatGPTWeb.BootstrapTimeoutSeconds != 10 || loaded.Images.ChatGPTWeb.BootstrapRetries != 1 || loaded.Images.ChatGPTWeb.RequestTimeoutSeconds != 120 {
		t.Fatalf("lost fields: %+v", loaded.Images.ChatGPTWeb)
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "# keep") {
		t.Fatal("comment lost")
	}
	cfg.Images.ChatGPTWeb.BootstrapTimeoutSeconds = 0
	cfg.Images.ChatGPTWeb.BootstrapRetries = 0
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadConfig(path)
	if err != nil || loaded.Images.ChatGPTWeb.BootstrapRetries != 0 || loaded.Images.ChatGPTWeb.BootstrapTimeoutSeconds != 0 {
		t.Fatalf("zero not saved: %v", err)
	}
	cfg.Images.ChatGPTWeb.BootstrapRetries = 6
	if err := SaveConfigPreserveComments(path, cfg); err == nil {
		t.Fatal("invalid retry saved")
	}
}
