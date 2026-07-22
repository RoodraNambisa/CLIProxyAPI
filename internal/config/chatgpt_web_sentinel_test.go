package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChatGPTWebSentinelConfigDefaults(t *testing.T) {
	resolved := (ChatGPTWebSentinelConfig{}).Resolved()
	if !resolved.SDKRuntimeEnabled {
		t.Fatal("SDKRuntimeEnabled = false, want true")
	}
	if resolved.SDKWorkers != 0 {
		t.Fatalf("SDKWorkers = %d, want 0", resolved.SDKWorkers)
	}
	if resolved.SDKQueueSize != DefaultChatGPTWebSentinelSDKQueueSize {
		t.Fatalf("SDKQueueSize = %d, want %d", resolved.SDKQueueSize, DefaultChatGPTWebSentinelSDKQueueSize)
	}
	if resolved.SDKCacheVersions != DefaultChatGPTWebSentinelSDKCacheVersions {
		t.Fatalf("SDKCacheVersions = %d, want %d", resolved.SDKCacheVersions, DefaultChatGPTWebSentinelSDKCacheVersions)
	}
}

func TestChatGPTWebSentinelConfigPreservesExplicitDisabledAndZeroQueue(t *testing.T) {
	enabled := false
	workers := 4
	queueSize := 0
	cacheVersions := 5
	resolved := (ChatGPTWebSentinelConfig{
		SDKRuntimeEnabled: &enabled,
		SDKWorkers:        &workers,
		SDKQueueSize:      &queueSize,
		SDKCacheVersions:  &cacheVersions,
	}).Resolved()
	if resolved.SDKRuntimeEnabled {
		t.Fatal("SDKRuntimeEnabled = true, want false")
	}
	if resolved.SDKWorkers != workers || resolved.SDKQueueSize != queueSize || resolved.SDKCacheVersions != cacheVersions {
		t.Fatalf("resolved = %+v", resolved)
	}
}

func TestChatGPTWebSentinelConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  ChatGPTWebSentinelConfig
		message string
	}{
		{name: "negative workers", config: ChatGPTWebSentinelConfig{SDKWorkers: intPointer(-1)}, message: "sdk-workers"},
		{name: "too many workers", config: ChatGPTWebSentinelConfig{SDKWorkers: intPointer(MaxChatGPTWebSentinelSDKWorkers + 1)}, message: "sdk-workers"},
		{name: "negative queue", config: ChatGPTWebSentinelConfig{SDKQueueSize: intPointer(-1)}, message: "sdk-queue-size"},
		{name: "too large queue", config: ChatGPTWebSentinelConfig{SDKQueueSize: intPointer(MaxChatGPTWebSentinelSDKQueueSize + 1)}, message: "sdk-queue-size"},
		{name: "zero cache", config: ChatGPTWebSentinelConfig{SDKCacheVersions: intPointer(0)}, message: "sdk-cache-versions"},
		{name: "too many cache versions", config: ChatGPTWebSentinelConfig{SDKCacheVersions: intPointer(MaxChatGPTWebSentinelSDKCacheVersions + 1)}, message: "sdk-cache-versions"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("Validate() error = %v, want %q", err, test.message)
			}
		})
	}
	if err := (ChatGPTWebSentinelConfig{}).Validate(); err != nil {
		t.Fatalf("default Validate() error = %v", err)
	}
}

func TestLoadConfigChatGPTWebSentinelValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("chatgpt-web:\n  sentinel:\n    sdk-runtime-enabled: false\n    sdk-workers: 3\n    sdk-queue-size: 0\n    sdk-cache-versions: 4\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	resolved := cfg.ChatGPTWeb.Sentinel.Resolved()
	if resolved.SDKRuntimeEnabled || resolved.SDKWorkers != 3 || resolved.SDKQueueSize != 0 || resolved.SDKCacheVersions != 4 {
		t.Fatalf("resolved = %+v", resolved)
	}
}

func TestSaveConfigPreserveCommentsAddsExplicitChatGPTWebSentinelZeroValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("port: 8317\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	enabled := false
	workers := 0
	queueSize := 0
	cacheVersions := 3
	cfg.ChatGPTWeb.Sentinel = ChatGPTWebSentinelConfig{
		SDKRuntimeEnabled: &enabled,
		SDKWorkers:        &workers,
		SDKQueueSize:      &queueSize,
		SDKCacheVersions:  &cacheVersions,
	}
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}
	reloaded, errReload := LoadConfig(path)
	if errReload != nil {
		t.Fatalf("reloaded LoadConfig() error = %v", errReload)
	}
	resolved := reloaded.ChatGPTWeb.Sentinel.Resolved()
	if resolved.SDKRuntimeEnabled || resolved.SDKWorkers != 0 || resolved.SDKQueueSize != 0 || resolved.SDKCacheVersions != 3 {
		t.Fatalf("resolved = %+v", resolved)
	}
}

func TestLoadConfigRejectsInvalidChatGPTWebSentinelValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  sentinel:\n    sdk-workers: 17\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "sdk-workers") {
		t.Fatalf("LoadConfig() error = %v", err)
	}
}

func TestLoadConfigRejectsNullChatGPTWebSentinelValues(t *testing.T) {
	fields := []string{
		"sdk-runtime-enabled",
		"sdk-workers",
		"sdk-queue-size",
		"sdk-cache-versions",
	}
	for _, field := range fields {
		t.Run(field, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			data := []byte("chatgpt-web:\n  sentinel:\n    " + field + ": null\n")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), field) {
				t.Fatalf("LoadConfig() error = %v, want %q", err, field)
			}
		})
	}
}

func TestLoadConfigRejectsUnknownChatGPTWebSentinelField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  sentinel:\n    sdk-worker: 4\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "sdk-worker") {
		t.Fatalf("LoadConfig() error = %v", err)
	}
}

func TestLoadConfigRejectsWhitespacePaddedChatGPTWebSentinelField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  sentinel:\n    \" sdk-workers \": 4\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), " sdk-workers ") {
		t.Fatalf("LoadConfig() error = %v", err)
	}
}

func TestLoadOptionalConfigPreservesChatGPTWebSentinelStrictErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "unknown field", yaml: "chatgpt-web:\n  sentinel:\n    sdk-worker: 4\n", want: "sdk-worker"},
		{name: "null value", yaml: "chatgpt-web:\n  sentinel:\n    sdk-workers: null\n", want: "sdk-workers"},
		{name: "wrong type", yaml: "chatgpt-web:\n  sentinel:\n    sdk-workers: enabled\n", want: "chatgpt-web.sentinel"},
		{name: "padded field", yaml: "chatgpt-web:\n  sentinel:\n    \" sdk-workers \": 4\n", want: " sdk-workers "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(test.yaml), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, err := LoadConfigOptional(path, true)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadConfigOptional() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadOptionalConfigIgnoresInvalidChatGPTWebContainerWithoutSentinel(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{name: "sequence", yaml: "chatgpt-web: []\n"},
		{name: "invalid sibling", yaml: "chatgpt-web:\n  auto-relogin: invalid\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(test.yaml), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			cfg, err := LoadConfigOptional(path, true)
			if err != nil {
				t.Fatalf("LoadConfigOptional() error = %v", err)
			}
			if cfg == nil {
				t.Fatal("LoadConfigOptional() returned nil config")
			}
		})
	}
}

func TestLoadOptionalConfigRejectsInvalidChatGPTWebSentinelValues(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "range survives unrelated decode error",
			yaml: "port: invalid\nchatgpt-web:\n  sentinel:\n    sdk-workers: 17\n",
			want: "sdk-workers",
		},
		{
			name: "merged range",
			yaml: "sentinel-defaults: &sentinel-defaults\n  sdk-cache-versions: 0\nchatgpt-web:\n  sentinel:\n    <<: *sentinel-defaults\n",
			want: "sdk-cache-versions",
		},
		{
			name: "merged unknown field",
			yaml: "sentinel-defaults: &sentinel-defaults\n  sdk-worker: 4\nchatgpt-web:\n  sentinel:\n    <<: *sentinel-defaults\n",
			want: "sdk-worker",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(test.yaml), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, err := LoadConfigOptional(path, true)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadConfigOptional() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadConfigChatGPTWebSentinelMergeUsesEffectiveValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("sentinel-defaults: &sentinel-defaults\n  sdk-workers: null\n  sdk-queue-size: 8\nchatgpt-web:\n  sentinel:\n    <<: *sentinel-defaults\n    sdk-workers: 2\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	resolved := cfg.ChatGPTWeb.Sentinel.Resolved()
	if resolved.SDKWorkers != 2 || resolved.SDKQueueSize != 8 {
		t.Fatalf("resolved = %+v", resolved)
	}
}

func TestLoadConfigRejectsRecursiveChatGPTWebSentinelAlias(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("chatgpt-web:\n  sentinel: &sentinel\n    <<: *sentinel\n    sdk-workers: 2\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("LoadConfig() error = nil")
	}
}

func intPointer(value int) *int {
	return &value
}
