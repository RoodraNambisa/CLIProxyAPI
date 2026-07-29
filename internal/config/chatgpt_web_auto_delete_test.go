package config

import (
	"os"
	"path/filepath"
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
