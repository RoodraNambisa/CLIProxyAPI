package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChatGPTWebAutoReloginDefaultsAndOverrides(t *testing.T) {
	defaults := (ChatGPTWebConfig{}).ResolvedAutoRelogin()
	if defaults.MaxRetries != DefaultChatGPTWebAutoReloginMaxRetries ||
		defaults.JitterPercent != DefaultChatGPTWebAutoReloginJitterPercent {
		t.Fatalf("ResolvedAutoRelogin() = %#v", defaults)
	}

	zero := 0
	explicit := ChatGPTWebConfig{
		AutoReloginMaxRetries:    &zero,
		AutoReloginJitterPercent: &zero,
	}.ResolvedAutoRelogin()
	if explicit.MaxRetries != 0 || explicit.JitterPercent != 0 {
		t.Fatalf("explicit zero ResolvedAutoRelogin() = %#v", explicit)
	}
}

func TestChatGPTWebAutoReloginValidation(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value int
	}{
		{name: "negative retries", field: "retries", value: -1},
		{name: "excess retries", field: "retries", value: MaxChatGPTWebAutoReloginRetries + 1},
		{name: "negative jitter", field: "jitter", value: -1},
		{name: "excess jitter", field: "jitter", value: MaxChatGPTWebAutoReloginJitterPercent + 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := ChatGPTWebConfig{}
			if test.field == "retries" {
				cfg.AutoReloginMaxRetries = &test.value
			} else {
				cfg.AutoReloginJitterPercent = &test.value
			}
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), "chatgpt-web.auto-relogin-") {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestSaveConfigPreservesExplicitZeroChatGPTWebAutoReloginValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  auto-relogin: false\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	zero := 0
	cfg.ChatGPTWeb.AutoReloginMaxRetries = &zero
	cfg.ChatGPTWeb.AutoReloginJitterPercent = &zero
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}

	saved, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved config: %v", errRead)
	}
	for _, expected := range []string{
		"auto-relogin-max-retries: 0",
		"auto-relogin-jitter-percent: 0",
	} {
		if !strings.Contains(string(saved), expected) {
			t.Fatalf("saved config omitted %q:\n%s", expected, saved)
		}
	}
	reloaded, errReload := LoadConfig(path)
	if errReload != nil {
		t.Fatalf("reloaded LoadConfig() error = %v", errReload)
	}
	resolved := reloaded.ChatGPTWeb.ResolvedAutoRelogin()
	if resolved.MaxRetries != 0 || resolved.JitterPercent != 0 {
		t.Fatalf("reloaded ResolvedAutoRelogin() = %#v", resolved)
	}
}
