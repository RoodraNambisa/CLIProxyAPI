package management

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexFingerprintDefaultAppliesOnlyToFutureUploadsWithoutExplicitMode(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{
		AuthDir: authDir,
		CodexFingerprint: config.CodexFingerprintConfig{
			DefaultMode: "session",
		},
	}, manager)

	if err := handler.writeAuthFile(t.Context(), "defaulted.json", []byte(`{"type":"codex","access_token":"one"}`)); err != nil {
		t.Fatalf("write defaulted auth: %v", err)
	}
	assertPersistedCodexFingerprintMode(t, filepath.Join(authDir, "defaulted.json"), "session")
	defaulted, ok := manager.GetByID("defaulted.json")
	if !ok || defaulted == nil {
		t.Fatal("defaulted auth was not registered")
	}
	if got := defaulted.Metadata[codex.FingerprintModeMetadataKey]; got != "session" {
		t.Fatalf("runtime fingerprint mode = %#v, want session", got)
	}

	if err := handler.writeAuthFile(t.Context(), "explicit.json", []byte(`{"type":"codex","access_token":"two","codex_fingerprint_mode":"full"}`)); err != nil {
		t.Fatalf("write explicit auth: %v", err)
	}
	assertPersistedCodexFingerprintMode(t, filepath.Join(authDir, "explicit.json"), "full")

	if err := handler.SetConfig(&config.Config{
		AuthDir: authDir,
		CodexFingerprint: config.CodexFingerprintConfig{
			DefaultMode: "off",
		},
	}); err != nil {
		t.Fatalf("SetConfig() error = %v", err)
	}
	assertPersistedCodexFingerprintMode(t, filepath.Join(authDir, "defaulted.json"), "session")

	if err := handler.writeAuthFile(t.Context(), "after-update.json", []byte(`{"type":"codex","access_token":"three"}`)); err != nil {
		t.Fatalf("write auth after config update: %v", err)
	}
	assertPersistedCodexFingerprintMode(t, filepath.Join(authDir, "after-update.json"), "off")
}

func TestCodexFingerprintDefaultDoesNotModifyOtherProviders(t *testing.T) {
	authDir := t.TempDir()
	handler := NewHandlerWithoutConfigFilePath(&config.Config{
		AuthDir: authDir,
		CodexFingerprint: config.CodexFingerprintConfig{
			DefaultMode: "full",
		},
	}, coreauth.NewManager(nil, nil, nil))
	payload := []byte(`{"type":"claude","access_token":"one"}`)
	if err := handler.writeAuthFile(t.Context(), "claude.json", payload); err != nil {
		t.Fatalf("write Claude auth: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(authDir, "claude.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(payload) {
		t.Fatalf("non-Codex payload changed: got %q, want %q", data, payload)
	}
}

func assertPersistedCodexFingerprintMode(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err = json.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}
	if got := metadata[codex.FingerprintModeMetadataKey]; got != want {
		t.Fatalf("%s = %#v, want %q", codex.FingerprintModeMetadataKey, got, want)
	}
}
