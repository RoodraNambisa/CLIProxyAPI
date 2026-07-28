package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestChatGPTWebResolvedTimezoneDefaultsAndDST(t *testing.T) {
	defaults := (ChatGPTWebConfig{}).ResolvedTimezone(time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC))
	if defaults.Timezone != DefaultChatGPTWebTimezone ||
		defaults.OffsetMinutes != DefaultChatGPTWebTimezoneOffsetMinutes {
		t.Fatalf("default ResolvedTimezone() = %#v", defaults)
	}

	cfg := ChatGPTWebConfig{Timezone: "America/New_York"}
	winter := cfg.ResolvedTimezone(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
	summer := cfg.ResolvedTimezone(time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC))
	if winter.OffsetMinutes != 300 || summer.OffsetMinutes != 240 {
		t.Fatalf("New York offsets winter=%d summer=%d", winter.OffsetMinutes, summer.OffsetMinutes)
	}

	zero := 0
	override := ChatGPTWebConfig{
		Timezone:              "America/New_York",
		TimezoneOffsetMinutes: &zero,
	}.ResolvedTimezone(time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC))
	if override.OffsetMinutes != 0 {
		t.Fatalf("explicit offset ResolvedTimezone() = %#v", override)
	}
}

func TestChatGPTWebTimezoneValidation(t *testing.T) {
	if err := (ChatGPTWebConfig{Timezone: "Not/A_Real_Zone"}).Validate(); err == nil ||
		!strings.Contains(err.Error(), "valid IANA timezone") {
		t.Fatalf("invalid timezone error = %v", err)
	}
	tooLarge := MaxChatGPTWebTimezoneOffsetMinutes + 1
	if err := (ChatGPTWebConfig{TimezoneOffsetMinutes: &tooLarge}).Validate(); err == nil ||
		!strings.Contains(err.Error(), "timezone-offset-minutes") {
		t.Fatalf("invalid offset error = %v", err)
	}
}

func TestSaveConfigPreservesExplicitZeroChatGPTWebTimezoneOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  timezone: UTC\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	zero := 0
	cfg.ChatGPTWeb.TimezoneOffsetMinutes = &zero
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}
	saved, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved config: %v", errRead)
	}
	if !strings.Contains(string(saved), "timezone-offset-minutes: 0") {
		t.Fatalf("saved config omitted explicit zero:\n%s", saved)
	}
	reloaded, errReload := LoadConfig(path)
	if errReload != nil {
		t.Fatalf("reloaded LoadConfig() error = %v", errReload)
	}
	resolved := reloaded.ChatGPTWeb.ResolvedTimezone(time.Now())
	if resolved.Timezone != "UTC" || resolved.OffsetMinutes != 0 {
		t.Fatalf("reloaded ResolvedTimezone() = %#v", resolved)
	}
}
