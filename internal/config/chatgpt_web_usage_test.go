package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChatGPTWebTokenUsageEstimationDefaultsEnabled(t *testing.T) {
	if !(ChatGPTWebConfig{}).TokenUsageEstimationEnabled() {
		t.Fatal("token usage estimation should default to enabled")
	}
}

func TestChatGPTWebUsageCacheDefaults(t *testing.T) {
	resolved := (ChatGPTWebUsageCacheConfig{}).Resolved()
	if resolved.Enabled ||
		resolved.DiskThresholdMB != 1 ||
		resolved.MaxDiskSizeMB != 1024 ||
		!resolved.ResourceGuardEnabled ||
		resolved.MinAvailableDiskMB != 1024 ||
		resolved.MaxFilesystemUsedPercent != 95 ||
		resolved.OrphanRetentionMinutes != 0 ||
		resolved.Path != "" {
		t.Fatalf("Resolved() = %#v", resolved)
	}
	if quality := (ChatGPTWebImageUsageConfig{}).ResolvedAutoOutputQuality(); quality != "medium" {
		t.Fatalf("ResolvedAutoOutputQuality() = %q, want medium", quality)
	}
	fallback := (ChatGPTWebImageFallbackUsageConfig{}).Resolved()
	if fallback.Enabled ||
		fallback.InputTextTokens != 0 ||
		fallback.InputImageTokens != 0 ||
		fallback.OutputTextTokens != 0 ||
		fallback.OutputImageTokens != DefaultChatGPTWebFallbackOutputImageTokens {
		t.Fatalf("fallback Resolved() = %#v", fallback)
	}
}

func TestChatGPTWebUsageConfigValidation(t *testing.T) {
	zero := int64(0)
	negative := int64(-1)
	one := int64(1)
	two := int64(2)
	tooLarge := int64(MaxChatGPTWebUsageCacheMegabytes + 1)
	zeroPercent := 0
	overPercent := 101
	negativeRetention := -1
	overRetention := MaxChatGPTWebUsageCacheOrphanRetention + 1
	negativeTokens := int64(-1)
	for _, test := range []struct {
		name   string
		config ChatGPTWebConfig
	}{
		{name: "zero threshold", config: ChatGPTWebConfig{UsageCache: ChatGPTWebUsageCacheConfig{DiskThresholdMB: &zero}}},
		{name: "zero maximum", config: ChatGPTWebConfig{UsageCache: ChatGPTWebUsageCacheConfig{MaxDiskSizeMB: &zero}}},
		{name: "threshold above maximum", config: ChatGPTWebConfig{UsageCache: ChatGPTWebUsageCacheConfig{DiskThresholdMB: &two, MaxDiskSizeMB: &one}}},
		{name: "byte conversion overflow", config: ChatGPTWebConfig{UsageCache: ChatGPTWebUsageCacheConfig{MinAvailableDiskMB: &tooLarge}}},
		{name: "negative available floor", config: ChatGPTWebConfig{UsageCache: ChatGPTWebUsageCacheConfig{MinAvailableDiskMB: &negative}}},
		{name: "zero used percent", config: ChatGPTWebConfig{UsageCache: ChatGPTWebUsageCacheConfig{MaxFilesystemUsedPercent: &zeroPercent}}},
		{name: "used percent above 100", config: ChatGPTWebConfig{UsageCache: ChatGPTWebUsageCacheConfig{MaxFilesystemUsedPercent: &overPercent}}},
		{name: "negative orphan retention", config: ChatGPTWebConfig{UsageCache: ChatGPTWebUsageCacheConfig{OrphanRetentionMinutes: &negativeRetention}}},
		{name: "orphan retention above maximum", config: ChatGPTWebConfig{UsageCache: ChatGPTWebUsageCacheConfig{OrphanRetentionMinutes: &overRetention}}},
		{name: "invalid quality", config: ChatGPTWebConfig{ImageUsage: ChatGPTWebImageUsageConfig{AutoOutputQuality: "ultra"}}},
		{name: "negative fallback", config: ChatGPTWebConfig{ImageUsage: ChatGPTWebImageUsageConfig{
			FallbackUsage: ChatGPTWebImageFallbackUsageConfig{OutputImageTokens: &negativeTokens},
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.config.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestLoadConfigChatGPTWebTokenUsageEstimation(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   string
		enabled bool
	}{
		{name: "enabled", value: "true", enabled: true},
		{name: "disabled", value: "false", enabled: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte("chatgpt-web:\n  estimate-token-usage: "+test.value+"\n"), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if got := cfg.ChatGPTWeb.TokenUsageEstimationEnabled(); got != test.enabled {
				t.Fatalf("TokenUsageEstimationEnabled() = %t, want %t", got, test.enabled)
			}
		})
	}
}

func TestSaveConfigPreservesDisabledChatGPTWebTokenUsageEstimation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  auto-relogin: false\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	disabled := false
	cfg.ChatGPTWeb.EstimateTokenUsage = &disabled
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}

	saved, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved config: %v", errRead)
	}
	if !strings.Contains(string(saved), "estimate-token-usage: false") {
		t.Fatalf("saved config omitted explicit false:\n%s", saved)
	}
	reloaded, errReload := LoadConfig(path)
	if errReload != nil {
		t.Fatalf("reloaded LoadConfig() error = %v", errReload)
	}
	if reloaded.ChatGPTWeb.TokenUsageEstimationEnabled() {
		t.Fatal("reloaded token usage estimation should remain disabled")
	}
}

func TestSaveConfigDoesNotAddDefaultChatGPTWebUsageSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("port: 8317\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}
	saved, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved config: %v", errRead)
	}
	for _, unexpected := range []string{"estimate-token-usage", "usage-cache", "image-usage"} {
		if strings.Contains(string(saved), unexpected) {
			t.Fatalf("saved config added default %q:\n%s", unexpected, saved)
		}
	}
}

func TestSaveConfigPreservesChatGPTWebUsageCacheSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  auto-relogin: false\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	enabled := false
	threshold := int64(2)
	maximum := int64(16)
	minimumAvailable := int64(4)
	maximumUsedPercent := 90
	orphanRetentionMinutes := 60
	resourceGuardEnabled := false
	cfg.ChatGPTWeb.UsageCache = ChatGPTWebUsageCacheConfig{
		Enabled:                  &enabled,
		DiskThresholdMB:          &threshold,
		MaxDiskSizeMB:            &maximum,
		ResourceGuardEnabled:     &resourceGuardEnabled,
		MinAvailableDiskMB:       &minimumAvailable,
		MaxFilesystemUsedPercent: &maximumUsedPercent,
		OrphanRetentionMinutes:   &orphanRetentionMinutes,
		Path:                     "",
	}
	cfg.ChatGPTWeb.ImageUsage.AutoOutputQuality = "high"
	fallbackEnabled := true
	inputTextTokens := int64(3)
	inputImageTokens := int64(4)
	outputTextTokens := int64(5)
	outputImageTokens := int64(2000)
	cfg.ChatGPTWeb.ImageUsage.FallbackUsage = ChatGPTWebImageFallbackUsageConfig{
		Enabled:           &fallbackEnabled,
		InputTextTokens:   &inputTextTokens,
		InputImageTokens:  &inputImageTokens,
		OutputTextTokens:  &outputTextTokens,
		OutputImageTokens: &outputImageTokens,
	}
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}

	saved, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved config: %v", errRead)
	}
	for _, expected := range []string{
		"usage-cache:", "enabled: false", "disk-threshold-mb: 2", "max-disk-size-mb: 16",
		"resource-guard-enabled: false", "min-available-disk-mb: 4", "max-filesystem-used-percent: 90",
		"orphan-retention-minutes: 60",
		"image-usage:", "auto-output-quality: high", "fallback-usage:", "input-text-tokens: 3",
		"input-image-tokens: 4", "output-text-tokens: 5", "output-image-tokens: 2000",
	} {
		if !strings.Contains(string(saved), expected) {
			t.Fatalf("saved config omitted %q:\n%s", expected, saved)
		}
	}
	reloaded, errReload := LoadConfig(path)
	if errReload != nil {
		t.Fatalf("LoadConfig() after save error = %v", errReload)
	}
	resolved := reloaded.ChatGPTWeb.UsageCache.Resolved()
	fallback := reloaded.ChatGPTWeb.ImageUsage.FallbackUsage.Resolved()
	if resolved.Enabled || resolved.DiskThresholdMB != 2 || resolved.MaxDiskSizeMB != 16 ||
		resolved.ResourceGuardEnabled || resolved.MinAvailableDiskMB != 4 || resolved.MaxFilesystemUsedPercent != 90 ||
		resolved.OrphanRetentionMinutes != 60 ||
		reloaded.ChatGPTWeb.ImageUsage.ResolvedAutoOutputQuality() != "high" ||
		!fallback.Enabled || fallback.InputTextTokens != 3 || fallback.InputImageTokens != 4 ||
		fallback.OutputTextTokens != 5 || fallback.OutputImageTokens != 2000 {
		t.Fatalf("reloaded config = %#v", reloaded.ChatGPTWeb)
	}
}

func TestSaveConfigPreservesZeroChatGPTWebImageFallbackUsage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  auto-relogin: false\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	enabled := true
	zero := int64(0)
	cfg.ChatGPTWeb.ImageUsage.FallbackUsage = ChatGPTWebImageFallbackUsageConfig{
		Enabled:           &enabled,
		InputTextTokens:   &zero,
		InputImageTokens:  &zero,
		OutputTextTokens:  &zero,
		OutputImageTokens: &zero,
	}
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}

	saved, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved config: %v", errRead)
	}
	for _, expected := range []string{
		"fallback-usage:", "enabled: true", "input-text-tokens: 0", "input-image-tokens: 0",
		"output-text-tokens: 0", "output-image-tokens: 0",
	} {
		if !strings.Contains(string(saved), expected) {
			t.Fatalf("saved config omitted %q:\n%s", expected, saved)
		}
	}

	reloaded, errReload := LoadConfig(path)
	if errReload != nil {
		t.Fatalf("LoadConfig() after save error = %v", errReload)
	}
	fallback := reloaded.ChatGPTWeb.ImageUsage.FallbackUsage.Resolved()
	if !fallback.Enabled ||
		fallback.InputTextTokens != 0 ||
		fallback.InputImageTokens != 0 ||
		fallback.OutputTextTokens != 0 ||
		fallback.OutputImageTokens != 0 {
		t.Fatalf("reloaded fallback config = %#v", fallback)
	}
}
