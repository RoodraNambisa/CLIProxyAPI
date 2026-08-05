package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestChatGPTWebAccountInfoConfigDefaults(t *testing.T) {
	resolved := (ChatGPTWebAccountInfoConfig{}).Resolved()
	if !resolved.AutoRefreshEnabled ||
		resolved.DiagnosticsEnabled || resolved.RawQuotaResponseEnabled ||
		resolved.RefreshWorkers != 4 || resolved.RefreshQueueSize != 256 ||
		resolved.RefreshTTLMinutes != 15 || resolved.PeriodicRefreshMinutes != 0 ||
		resolved.RecoveryJitterSeconds != 30 ||
		resolved.MaxRetries != 3 {
		t.Fatalf("Resolved() = %+v", resolved)
	}
}

func TestChatGPTWebImportSessionRefreshDefaultsEnabled(t *testing.T) {
	if !(ChatGPTWebConfig{}).ForceSessionRefreshOnImportEnabled() {
		t.Fatal("ForceSessionRefreshOnImportEnabled() = false, want true")
	}
	disabled := false
	if (ChatGPTWebConfig{ForceSessionRefreshOnImport: &disabled}).ForceSessionRefreshOnImportEnabled() {
		t.Fatal("ForceSessionRefreshOnImportEnabled() = true, want false")
	}
}

func TestChatGPTWebImportConfigDefaultsAndValidation(t *testing.T) {
	resolved := (ChatGPTWebImportConfig{}).Resolved()
	if resolved.Workers != 4 || resolved.ValidateModelsAfterUpload || resolved.RefreshAccountInfoAfterUpload {
		t.Fatalf("Resolved() = %+v", resolved)
	}
	for _, workers := range []int{0, 33} {
		cfg := ChatGPTWebImportConfig{Workers: intPointer(workers)}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "workers") {
			t.Fatalf("Validate() workers=%d error = %v", workers, err)
		}
	}
	for _, workers := range []int{1, 32} {
		cfg := ChatGPTWebImportConfig{Workers: intPointer(workers)}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() workers=%d error = %v", workers, err)
		}
	}
}

func TestChatGPTWebAccountInfoConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  ChatGPTWebAccountInfoConfig
		message string
	}{
		{name: "zero workers", config: ChatGPTWebAccountInfoConfig{RefreshWorkers: intPointer(0)}, message: "refresh-workers"},
		{name: "large workers", config: ChatGPTWebAccountInfoConfig{RefreshWorkers: intPointer(33)}, message: "refresh-workers"},
		{name: "negative queue", config: ChatGPTWebAccountInfoConfig{RefreshQueueSize: intPointer(-1)}, message: "refresh-queue-size"},
		{name: "large queue", config: ChatGPTWebAccountInfoConfig{RefreshQueueSize: intPointer(10001)}, message: "refresh-queue-size"},
		{name: "zero ttl", config: ChatGPTWebAccountInfoConfig{RefreshTTLMinutes: intPointer(0)}, message: "refresh-ttl-minutes"},
		{name: "large ttl", config: ChatGPTWebAccountInfoConfig{RefreshTTLMinutes: intPointer(1441)}, message: "refresh-ttl-minutes"},
		{name: "negative periodic refresh", config: ChatGPTWebAccountInfoConfig{PeriodicRefreshMinutes: intPointer(-1)}, message: "periodic-refresh-minutes"},
		{name: "large periodic refresh", config: ChatGPTWebAccountInfoConfig{PeriodicRefreshMinutes: intPointer(10081)}, message: "periodic-refresh-minutes"},
		{name: "negative jitter", config: ChatGPTWebAccountInfoConfig{RecoveryJitterSeconds: intPointer(-1)}, message: "recovery-jitter-seconds"},
		{name: "large jitter", config: ChatGPTWebAccountInfoConfig{RecoveryJitterSeconds: intPointer(301)}, message: "recovery-jitter-seconds"},
		{name: "negative retries", config: ChatGPTWebAccountInfoConfig{MaxRetries: intPointer(-1)}, message: "max-retries"},
		{name: "large retries", config: ChatGPTWebAccountInfoConfig{MaxRetries: intPointer(11)}, message: "max-retries"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("Validate() error = %v, want %q", err, test.message)
			}
		})
	}
}

func TestLoadAndSaveChatGPTWebAccountInfoConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte("chatgpt-web:\n  session-cookie-refresh-on-token-failure: true\n  force-session-refresh-on-import: false\n  import:\n    workers: 6\n    validate-models-after-upload: true\n    refresh-account-info-after-upload: true\n  account-info:\n    auto-refresh-enabled: false\n    diagnostics-enabled: true\n    raw-quota-response-enabled: true\n    refresh-workers: 2\n    refresh-queue-size: 0\n    refresh-ttl-minutes: 30\n    periodic-refresh-minutes: 0\n    recovery-jitter-seconds: 0\n    max-retries: 0\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	resolved := cfg.ChatGPTWeb.AccountInfo.Resolved()
	if resolved.AutoRefreshEnabled || !resolved.DiagnosticsEnabled || !resolved.RawQuotaResponseEnabled ||
		resolved.RefreshWorkers != 2 || resolved.RefreshQueueSize != 0 ||
		resolved.RefreshTTLMinutes != 30 || resolved.PeriodicRefreshMinutes != 0 ||
		resolved.RecoveryJitterSeconds != 0 ||
		resolved.MaxRetries != 0 {
		t.Fatalf("resolved = %+v", resolved)
	}
	if cfg.ChatGPTWeb.ForceSessionRefreshOnImportEnabled() {
		t.Fatal("force-session-refresh-on-import was not loaded")
	}
	if !cfg.ChatGPTWeb.SessionCookieRefreshOnTokenFailure {
		t.Fatal("session-cookie-refresh-on-token-failure was not loaded")
	}
	importConfig := cfg.ChatGPTWeb.Import.Resolved()
	if importConfig.Workers != 6 || !importConfig.ValidateModelsAfterUpload || !importConfig.RefreshAccountInfoAfterUpload {
		t.Fatalf("import config = %+v", importConfig)
	}
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}
	saved, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved config: %v", errRead)
	}
	for _, expected := range []string{
		"session-cookie-refresh-on-token-failure: true",
		"force-session-refresh-on-import: false",
		"import:",
		"workers: 6",
		"validate-models-after-upload: true",
		"refresh-account-info-after-upload: true",
		"account-info:",
		"auto-refresh-enabled: false",
		"diagnostics-enabled: true",
		"raw-quota-response-enabled: true",
		"refresh-queue-size: 0",
		"periodic-refresh-minutes: 0",
		"recovery-jitter-seconds: 0",
		"max-retries: 0",
	} {
		if !strings.Contains(string(saved), expected) {
			t.Fatalf("saved config omitted %q:\n%s", expected, saved)
		}
	}
}

func TestLoadConfigRejectsInvalidChatGPTWebAccountInfoFields(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "unknown", yaml: "chatgpt-web:\n  account-info:\n    refresh-worker: 4\n", want: "refresh-worker"},
		{name: "null", yaml: "chatgpt-web:\n  account-info:\n    refresh-workers: null\n", want: "refresh-workers"},
		{name: "null auto refresh", yaml: "chatgpt-web:\n  account-info:\n    auto-refresh-enabled: null\n", want: "auto-refresh-enabled"},
		{name: "null diagnostics", yaml: "chatgpt-web:\n  account-info:\n    diagnostics-enabled: null\n", want: "diagnostics-enabled"},
		{name: "null raw quota response", yaml: "chatgpt-web:\n  account-info:\n    raw-quota-response-enabled: null\n", want: "raw-quota-response-enabled"},
		{name: "null periodic refresh", yaml: "chatgpt-web:\n  account-info:\n    periodic-refresh-minutes: null\n", want: "periodic-refresh-minutes"},
		{name: "out of range", yaml: "chatgpt-web:\n  account-info:\n    max-retries: 11\n", want: "max-retries"},
		{name: "periodic refresh out of range", yaml: "chatgpt-web:\n  account-info:\n    periodic-refresh-minutes: 10081\n", want: "periodic-refresh-minutes"},
		{name: "unknown import field", yaml: "chatgpt-web:\n  import:\n    worker: 4\n", want: "worker"},
		{name: "null import workers", yaml: "chatgpt-web:\n  import:\n    workers: null\n", want: "workers"},
		{name: "null import flag", yaml: "chatgpt-web:\n  import:\n    validate-models-after-upload: null\n", want: "validate-models-after-upload"},
		{name: "import workers out of range", yaml: "chatgpt-web:\n  import:\n    workers: 33\n", want: "workers"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(test.yaml), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, errLoad := LoadConfig(path)
			if errLoad == nil || !strings.Contains(errLoad.Error(), test.want) {
				t.Fatalf("LoadConfig() error = %v, want %q", errLoad, test.want)
			}
		})
	}
}

func TestLoadOptionalConfigPreservesChatGPTWebAccountInfoStrictErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "unknown field", yaml: "chatgpt-web:\n  account-info:\n    refresh-worker: 4\n", want: "refresh-worker"},
		{name: "null value", yaml: "chatgpt-web:\n  account-info:\n    refresh-workers: null\n", want: "refresh-workers"},
		{name: "wrong type", yaml: "chatgpt-web:\n  account-info:\n    refresh-workers: enabled\n", want: "chatgpt-web.account-info"},
		{
			name: "range survives unrelated decode error",
			yaml: "port: invalid\nchatgpt-web:\n  account-info:\n    max-retries: 11\n",
			want: "max-retries",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(test.yaml), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, errLoad := LoadConfigOptional(path, true)
			if errLoad == nil || !strings.Contains(errLoad.Error(), test.want) {
				t.Fatalf("LoadConfigOptional() error = %v, want %q", errLoad, test.want)
			}
		})
	}
}
