package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigChatGPTWebAutoDeleteDeadAuths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	raw := []byte(`
chatgpt-web:
  auto-delete-dead-auths: true
  auto-delete-dead-priorities: [1, 0, -1]
`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !cfg.ChatGPTWeb.AutoDeleteDeadAuths {
		t.Fatal("AutoDeleteDeadAuths = false, want true")
	}
	want := []int{1, 0, -1}
	if len(cfg.ChatGPTWeb.AutoDeleteDeadPriorities) != len(want) {
		t.Fatalf("AutoDeleteDeadPriorities = %v, want %v", cfg.ChatGPTWeb.AutoDeleteDeadPriorities, want)
	}
	for index := range want {
		if cfg.ChatGPTWeb.AutoDeleteDeadPriorities[index] != want[index] {
			t.Fatalf("AutoDeleteDeadPriorities = %v, want %v", cfg.ChatGPTWeb.AutoDeleteDeadPriorities, want)
		}
	}
}

func TestLoadConfigChatGPTWebInvalidPasskeyResponseAsDead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  invalid-passkey-response-as-dead: true\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !cfg.ChatGPTWeb.InvalidPasskeyResponseAsDead {
		t.Fatal("InvalidPasskeyResponseAsDead = false, want true")
	}
}

func TestLoadConfigRejectsInvalidPasskeyResponseAsDeadNullOrWrongType(t *testing.T) {
	for _, value := range []string{"null", `"true"`, "1"} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			raw := []byte("chatgpt-web:\n  invalid-passkey-response-as-dead: " + value + "\n")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), "invalid-passkey-response-as-dead") {
				t.Fatalf("LoadConfig() error = %v", err)
			}
		})
	}
}

func TestLoadConfigChatGPTWebAPI798AutoLogin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  api798-auto-login-enabled: true\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !cfg.ChatGPTWeb.API798AutoLoginEnabled {
		t.Fatal("API798AutoLoginEnabled = false, want true")
	}
}

func TestLoadConfigRejectsInvalidAPI798AutoLogin(t *testing.T) {
	for _, value := range []string{"null", `"true"`, "1"} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			raw := []byte("chatgpt-web:\n  api798-auto-login-enabled: " + value + "\n")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), "api798-auto-login-enabled") {
				t.Fatalf("LoadConfig() error = %v", err)
			}
		})
	}
}

func TestSaveConfigPreservesChatGPTWebAutoDeleteDeadAuths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("chatgpt-web:\n  auto-relogin: false\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
	cfg.ChatGPTWeb.AutoDeleteDeadPriorities = []int{0, -1}
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", err)
	}

	reloaded, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("reloaded LoadConfig() error = %v", err)
	}
	if !reloaded.ChatGPTWeb.AutoDeleteDeadAuths ||
		len(reloaded.ChatGPTWeb.AutoDeleteDeadPriorities) != 2 ||
		reloaded.ChatGPTWeb.AutoDeleteDeadPriorities[0] != 0 ||
		reloaded.ChatGPTWeb.AutoDeleteDeadPriorities[1] != -1 {
		t.Fatalf("reloaded ChatGPTWeb = %#v", reloaded.ChatGPTWeb)
	}
}
