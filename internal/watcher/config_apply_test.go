package watcher

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestReloadConfigKeepsPreviousRuntimeAndExternalFileWhenApplyFails(t *testing.T) {
	root := t.TempDir()
	authDir := filepath.Join(root, "auths")
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("MkdirAll() error = %v", errMkdir)
	}
	configPath := filepath.Join(root, "config.yaml")
	newBody := []byte("auth-dir: " + authDir + "\nrequest-retry: 9\n")
	if errWrite := os.WriteFile(configPath, newBody, 0o600); errWrite != nil {
		t.Fatalf("WriteFile() error = %v", errWrite)
	}

	w, errWatcher := NewWatcher(configPath, authDir, nil)
	if errWatcher != nil {
		t.Fatalf("NewWatcher() error = %v", errWatcher)
	}
	defer func() {
		if errClose := w.watcher.Close(); errClose != nil {
			t.Errorf("watcher.Close() error = %v", errClose)
		}
	}()
	previous := &config.Config{AuthDir: authDir, RequestRetry: 1}
	w.SetConfig(previous)
	w.SetConfigApply(func(*config.Config) (*config.Config, error) {
		return nil, errors.New("synthetic apply failure")
	})

	if w.reloadConfig() {
		t.Fatal("reloadConfig() = true, want false")
	}
	w.clientsMutex.RLock()
	gotRuntime := w.config
	w.clientsMutex.RUnlock()
	if gotRuntime == nil || gotRuntime.RequestRetry != 1 {
		t.Fatalf("runtime config changed after failed apply: %#v", gotRuntime)
	}
	gotBody, errRead := os.ReadFile(configPath)
	if errRead != nil {
		t.Fatalf("ReadFile() error = %v", errRead)
	}
	if string(gotBody) != string(newBody) {
		t.Fatalf("external config was overwritten after failed apply:\n%s", gotBody)
	}
}

func TestReloadAPIIdentitiesPersistAndRollbackWithFailedApply(t *testing.T) {
	dir := t.TempDir()
	authDir := filepath.Join(dir, "auths")
	if err := os.MkdirAll(authDir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.yaml")
	body := []byte("auth-dir: " + authDir + "\ncodex-api-key:\n  - api-key: first\n    base-url: https://example.invalid\n    channel-name: Primary\n")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	w, err := NewWatcher(path, authDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.watcher.Close(); err != nil {
			t.Error(err)
		}
	}()
	w.SetConfig(&config.Config{AuthDir: authDir})
	if !w.reloadConfig() {
		t.Fatal("reload failed")
	}
	saved, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if saved.CodexKey[0].CredentialID == "" || saved.CodexKey[0].CredentialIndex == "" {
		t.Fatal("reload did not persist management identity")
	}
	originalID := saved.CodexKey[0].CredentialID
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	if !w.reloadConfig() {
		t.Fatal("legacy rewrite failed")
	}
	saved, err = config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if saved.CodexKey[0].CredentialID != originalID {
		t.Fatal("legacy rewrite changed identity")
	}
	failing := append(append([]byte(nil), body...), []byte("  - api-key: second\n    base-url: https://example.invalid\n")...)
	if err := os.WriteFile(path, failing, 0600); err != nil {
		t.Fatal(err)
	}
	w.SetConfigApply(func(*config.Config) (*config.Config, error) { return nil, errors.New("apply failed") })
	if w.reloadConfig() {
		t.Fatal("failed runtime apply accepted")
	}
	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != string(failing) {
		t.Fatal("failed reload modified external config")
	}
}
