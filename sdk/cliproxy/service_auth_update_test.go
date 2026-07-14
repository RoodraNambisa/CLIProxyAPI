package cliproxy

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestEmitAuthUpdateHonorsWatcherRuntimeDisposition(t *testing.T) {
	t.Run("consumed update is not replayed", func(t *testing.T) {
		manager := coreauth.NewManager(nil, nil, nil)
		active := &coreauth.Auth{ID: "runtime", Provider: "codex", Label: "active"}
		if _, errRegister := manager.Register(t.Context(), active); errRegister != nil {
			t.Fatalf("register active auth: %v", errRegister)
		}
		service := &Service{
			cfg:         &config.Config{},
			coreManager: manager,
			watcher: &WatcherWrapper{dispatchRuntimeUpdate: func(watcher.AuthUpdate) watcher.RuntimeAuthUpdateResult {
				return watcher.RuntimeAuthUpdateResult{Consumed: true}
			}},
		}
		service.emitAuthUpdate(t.Context(), watcher.AuthUpdate{
			Action: watcher.AuthUpdateActionModify,
			Auth:   &coreauth.Auth{ID: active.ID, Provider: active.Provider, Label: "rejected"},
		})
		current, exists := manager.GetByID(active.ID)
		if !exists || current.Label != active.Label {
			t.Fatalf("consumed update changed runtime auth: %#v", current)
		}
	})

	t.Run("translated fallback is applied inline", func(t *testing.T) {
		manager := coreauth.NewManager(nil, nil, nil)
		active := &coreauth.Auth{ID: "runtime", Provider: "codex"}
		if _, errRegister := manager.Register(t.Context(), active); errRegister != nil {
			t.Fatalf("register active auth: %v", errRegister)
		}
		fallback := watcher.AuthUpdate{Action: watcher.AuthUpdateActionDelete, ID: active.ID, Auth: active.Clone()}
		service := &Service{
			cfg:         &config.Config{},
			coreManager: manager,
			watcher: &WatcherWrapper{dispatchRuntimeUpdate: func(watcher.AuthUpdate) watcher.RuntimeAuthUpdateResult {
				return watcher.RuntimeAuthUpdateResult{Fallback: &fallback}
			}},
		}
		service.emitAuthUpdate(t.Context(), watcher.AuthUpdate{
			Action: watcher.AuthUpdateActionModify,
			Auth:   &coreauth.Auth{ID: active.ID, Provider: "gemini-cli"},
		})
		if _, exists := manager.GetByID(active.ID); exists {
			t.Fatal("translated delete fallback left runtime auth registered")
		}
	})
}

func TestAuthFileUpdateStillCurrentRejectsDeletedAndRewrittenSources(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "auth.json")
	initial := []byte(`{"type":"codex","access_token":"first"}`)
	if errWrite := os.WriteFile(path, initial, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	auth := &coreauth.Auth{ID: "auth.json", Provider: "codex", Attributes: map[string]string{"path": path}}
	if errSync := coreauth.SyncPersistedMetadataAndSourceHash(auth, initial); errSync != nil {
		t.Fatalf("SyncPersistedMetadataAndSourceHash() error = %v", errSync)
	}
	if !authFileUpdateStillCurrent(auth, authDir) {
		t.Fatal("current auth file update was rejected")
	}
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"second"}`), 0o600); errWrite != nil {
		t.Fatalf("rewrite auth file: %v", errWrite)
	}
	if authFileUpdateStillCurrent(auth, authDir) {
		t.Fatal("stale auth file update was accepted after rewrite")
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove auth file: %v", errRemove)
	}
	if authFileUpdateStillCurrent(auth, authDir) {
		t.Fatal("stale auth file update was accepted after delete")
	}
}

func TestAuthFileUpdateStillCurrentRejectsRetargetedParentSymlink(t *testing.T) {
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "external")
	initial := []byte(`{"type":"codex","access_token":"first"}`)
	for dir, data := range map[string][]byte{
		firstDir:  initial,
		secondDir: []byte(`{"type":"codex","access_token":"second"}`),
	} {
		if errWrite := os.WriteFile(filepath.Join(dir, "auth.json"), data, 0o600); errWrite != nil {
			t.Fatalf("write auth file: %v", errWrite)
		}
	}
	if errLink := os.Symlink(firstDir, linkDir); errLink != nil {
		t.Skipf("symlink is unavailable: %v", errLink)
	}
	path := filepath.Join(linkDir, "auth.json")
	auth := &coreauth.Auth{ID: "auth.json", Provider: "codex", Attributes: map[string]string{"path": path}}
	if errSync := coreauth.SyncPersistedMetadataAndSourceHash(auth, initial); errSync != nil {
		t.Fatalf("SyncPersistedMetadataAndSourceHash() error = %v", errSync)
	}
	if errRemove := os.Remove(linkDir); errRemove != nil {
		t.Fatalf("remove original parent symlink: %v", errRemove)
	}
	if errLink := os.Symlink(secondDir, linkDir); errLink != nil {
		t.Fatalf("retarget parent symlink: %v", errLink)
	}
	if authFileUpdateStillCurrentAtPath(auth, path) {
		t.Fatal("stale auth update was accepted after parent symlink retarget")
	}
}

func TestApplyCoreAuthAddOrUpdateRevalidatesAfterPathLock(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "auth.json")
	initial := []byte(`{"type":"codex","access_token":"first"}`)
	if errWrite := os.WriteFile(path, initial, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	auth := &coreauth.Auth{ID: "auth.json", Provider: "codex", Attributes: map[string]string{"path": path}}
	if errSync := coreauth.SyncPersistedMetadataAndSourceHash(auth, initial); errSync != nil {
		t.Fatalf("SyncPersistedMetadataAndSourceHash() error = %v", errSync)
	}
	service := &Service{cfg: &config.Config{AuthDir: authDir}, coreManager: coreauth.NewManager(nil, nil, nil)}

	unlockPath := authfileguard.Lock(path)
	done := make(chan struct{})
	go func() {
		service.applyCoreAuthAddOrUpdate(context.Background(), auth)
		close(done)
	}()
	select {
	case <-done:
		unlockPath()
		t.Fatal("auth update bypassed the path lock")
	case <-time.After(50 * time.Millisecond):
	}
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"second"}`), 0o600); errWrite != nil {
		unlockPath()
		t.Fatalf("rewrite auth file: %v", errWrite)
	}
	unlockPath()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for auth update")
	}
	if _, ok := service.coreManager.GetByID(auth.ID); ok {
		t.Fatal("stale auth update was installed after the source changed")
	}
}

func TestAuthFileUpdateStillCurrentRejectsRetiredGeminiCLI(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "legacy.json")
	data := []byte(`{"type":"gemini","access_token":"legacy"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write retired auth file: %v", errWrite)
	}
	auth := &coreauth.Auth{ID: "legacy.json", Provider: "codex", Attributes: map[string]string{"path": path}}
	if authFileUpdateStillCurrent(auth, authDir) {
		t.Fatal("retired Gemini CLI auth file update was accepted")
	}
}
