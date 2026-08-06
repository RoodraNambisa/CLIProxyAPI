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
