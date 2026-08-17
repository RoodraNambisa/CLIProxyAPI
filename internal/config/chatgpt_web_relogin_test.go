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
		defaults.JitterPercent != DefaultChatGPTWebAutoReloginJitterPercent ||
		defaults.Workers != DefaultChatGPTWebAutoReloginWorkers ||
		defaults.QueueSize != DefaultChatGPTWebAutoReloginQueueSize {
		t.Fatalf("ResolvedAutoRelogin() = %#v", defaults)
	}

	zero := 0
	one := 1
	explicit := ChatGPTWebConfig{
		AutoReloginMaxRetries:    &zero,
		AutoReloginJitterPercent: &zero,
		AutoReloginWorkers:       &one,
		AutoReloginQueueSize:     &one,
	}.ResolvedAutoRelogin()
	if explicit.MaxRetries != 0 || explicit.JitterPercent != 0 || explicit.Workers != 1 || explicit.QueueSize != 1 {
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
		{name: "zero workers", field: "workers", value: 0},
		{name: "excess workers", field: "workers", value: MaxChatGPTWebAutoReloginWorkers + 1},
		{name: "zero queue", field: "queue", value: 0},
		{name: "excess queue", field: "queue", value: MaxChatGPTWebAutoReloginQueueSize + 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := ChatGPTWebConfig{}
			switch test.field {
			case "retries":
				cfg.AutoReloginMaxRetries = &test.value
			case "jitter":
				cfg.AutoReloginJitterPercent = &test.value
			case "workers":
				cfg.AutoReloginWorkers = &test.value
			case "queue":
				cfg.AutoReloginQueueSize = &test.value
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
	one := 1
	cfg.ChatGPTWeb.AutoReloginMaxRetries = &zero
	cfg.ChatGPTWeb.AutoReloginJitterPercent = &zero
	cfg.ChatGPTWeb.AutoReloginWorkers = &one
	cfg.ChatGPTWeb.AutoReloginQueueSize = &one
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
		"auto-relogin-workers: 1",
		"auto-relogin-queue-size: 1",
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
	if resolved.MaxRetries != 0 || resolved.JitterPercent != 0 || resolved.Workers != 1 || resolved.QueueSize != 1 {
		t.Fatalf("reloaded ResolvedAutoRelogin() = %#v", resolved)
	}
}
