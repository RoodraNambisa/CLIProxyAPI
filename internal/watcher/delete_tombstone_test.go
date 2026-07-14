package watcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestResumeAuthDeleteTombstonesWaitsForProcessSharedTargetLock(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	const fileName = "codex.json"
	path := filepath.Join(authDir, fileName)
	data := []byte(`{"type":"codex","access_token":"remove"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	generation := authfileguard.NewDeleteGeneration(coreauth.SourceHashFromBytes(data))
	w := &Watcher{
		configPath:          configPath,
		authDir:             authDir,
		config:              &config.Config{AuthDir: authDir},
		lastAuthHashes:      make(map[string]string),
		retiredAuthPaths:    make(map[string]struct{}),
		retiredDeletes:      make(map[string]uint64),
		retiredDeleteHashes: make(map[string]string),
		retiredDeleteStates: make(map[string]*authfileguard.DeleteGeneration),
	}
	if errPersist := w.persistAuthDeleteGenerationTombstone(path, generation); errPersist != nil {
		t.Fatalf("persist tombstone: %v", errPersist)
	}
	normalized := w.normalizeAuthPath(path)
	w.retiredAuthPaths[normalized] = struct{}{}
	w.retiredDeletes[normalized] = 1
	w.retiredDeleteHashes[normalized] = generation.ExpectedHash()
	w.retiredDeleteStates[normalized] = generation
	t.Cleanup(func() {
		authfileguard.ClearQuarantined(path)
		authfileguard.ClearRetired(path)
	})

	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatalf("open auth root: %v", errRoot)
	}
	defer root.Close()
	unlock, errLock := authfileguard.LockRootTarget(root, fileName)
	if errLock != nil {
		t.Fatalf("lock auth target: %v", errLock)
	}
	locked := true
	defer func() {
		if locked {
			_ = unlock()
		}
	}()

	done := make(chan struct{})
	go func() {
		w.resumeAuthDeleteTombstones()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("resume bypassed process-shared target lock")
	case <-time.After(150 * time.Millisecond):
	}
	if _, errStat := os.Stat(path); errStat != nil {
		t.Fatalf("auth changed while target lock was held: %v", errStat)
	}
	if errUnlock := unlock(); errUnlock != nil {
		t.Fatalf("unlock auth target: %v", errUnlock)
	}
	locked = false
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("resume did not finish after target unlock")
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("auth still exists after resumed deletion: %v", errStat)
	}
}

func TestResumeAuthDeleteTombstonesCompletesWhenNestedParentWasRemoved(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	parentDir := filepath.Join(authDir, "nested")
	if errMkdir := os.MkdirAll(parentDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested auth directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	path := filepath.Join(parentDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"remove"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	generation := authfileguard.NewDeleteGeneration(coreauth.SourceHashFromBytes(data))
	w := &Watcher{
		configPath:          configPath,
		authDir:             authDir,
		config:              &config.Config{AuthDir: authDir},
		lastAuthHashes:      make(map[string]string),
		retiredAuthPaths:    make(map[string]struct{}),
		retiredDeletes:      make(map[string]uint64),
		retiredDeleteHashes: make(map[string]string),
		retiredDeleteStates: make(map[string]*authfileguard.DeleteGeneration),
	}
	if errPersist := w.persistAuthDeleteGenerationTombstone(path, generation); errPersist != nil {
		t.Fatalf("persist tombstone: %v", errPersist)
	}
	normalized := w.normalizeAuthPath(path)
	w.retiredAuthPaths[normalized] = struct{}{}
	w.retiredDeletes[normalized] = 1
	w.retiredDeleteHashes[normalized] = generation.ExpectedHash()
	w.retiredDeleteStates[normalized] = generation
	t.Cleanup(func() {
		authfileguard.ClearQuarantined(path)
		authfileguard.ClearRetired(path)
	})
	if errRemove := os.RemoveAll(parentDir); errRemove != nil {
		t.Fatalf("remove nested auth directory: %v", errRemove)
	}

	w.resumeAuthDeleteTombstones()

	if authfileguard.IsQuarantined(path) {
		t.Fatal("missing nested auth remained quarantined")
	}
	w.clientsMutex.RLock()
	_, pending := w.retiredDeleteStates[normalized]
	w.clientsMutex.RUnlock()
	if pending {
		t.Fatal("missing nested auth tombstone remained pending")
	}
}

func TestAuthDeleteTombstoneBlocksCredentialAfterWatcherRestart(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"remote-old"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}

	expectedHash := coreauth.SourceHashFromBytes(data)
	first := &Watcher{configPath: configPath, authDir: authDir}
	if errPersist := first.persistAuthDeleteTombstone(path, expectedHash); errPersist != nil {
		t.Fatalf("persistAuthDeleteTombstone() error = %v", errPersist)
	}
	if errRemove := os.RemoveAll(authDir); errRemove != nil {
		t.Fatalf("reset mirrored auth directory: %v", errRemove)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("recreate mirrored auth directory: %v", errMkdir)
	}
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("restore remote auth: %v", errWrite)
	}

	restarted := &Watcher{
		configPath:       configPath,
		authDir:          authDir,
		config:           &config.Config{AuthDir: authDir},
		lastAuthHashes:   make(map[string]string),
		fileAuthsByPath:  make(map[string]map[string]*coreauth.Auth),
		currentAuths:     make(map[string]*coreauth.Auth),
		retiredAuthPaths: make(map[string]struct{}),
	}
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
	}
	restarted.cacheAuthFileForReload(restarted.config, authDir, path, false, true)

	normalized := restarted.normalizeAuthPath(path)
	restarted.clientsMutex.RLock()
	_, quarantined := restarted.retiredAuthPaths[normalized]
	loadedHash := restarted.retiredDeleteHashes[normalized]
	deleteGeneration := restarted.retiredDeleteStates[normalized]
	authCount := len(restarted.fileAuthsByPath[normalized])
	runtimeCount := len(restarted.currentAuths)
	restarted.clientsMutex.RUnlock()
	if !quarantined || authCount != 0 || runtimeCount != 0 {
		t.Fatalf("restart quarantine = %t, file auths = %d, runtime auths = %d", quarantined, authCount, runtimeCount)
	}
	if loadedHash != expectedHash {
		t.Fatalf("loaded expected hash = %q, want %q", loadedHash, expectedHash)
	}
	if unchanged, errUnchanged := restarted.authFileUnchanged(path); errUnchanged != nil || !unchanged {
		t.Fatalf("restored original auth unchanged = %t, error = %v", unchanged, errUnchanged)
	}

	if errClear := restarted.clearAuthDeleteTombstone(path, deleteGeneration); errClear != nil {
		t.Fatalf("clearAuthDeleteTombstone() error = %v", errClear)
	}
	metadataRoot, authRoot, _, errContext := restarted.authDeleteTombstoneContext()
	if errContext != nil {
		t.Fatalf("authDeleteTombstoneContext() error = %v", errContext)
	}
	tombstonePath := filepath.Join(metadataRoot, authDeleteTombstoneDirectory, authDeleteTombstoneName(authRoot, "codex.json"))
	if _, errStat := os.Stat(tombstonePath); !os.IsNotExist(errStat) {
		t.Fatalf("tombstone still exists: %v", errStat)
	}
}

func TestAuthDeleteTombstoneOldGenerationCannotClearReplacement(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "codex.json")
	w := &Watcher{configPath: configPath, authDir: authDir}
	oldGeneration := authfileguard.NewDeleteGeneration(coreauth.SourceHashFromBytes([]byte("old")))
	newGeneration := authfileguard.NewDeleteGeneration(coreauth.SourceHashFromBytes([]byte("new")))
	if errPersist := w.persistAuthDeleteGenerationTombstone(path, oldGeneration); errPersist != nil {
		t.Fatalf("persist old tombstone: %v", errPersist)
	}
	if errPersist := w.persistAuthDeleteGenerationTombstone(path, newGeneration); errPersist != nil {
		t.Fatalf("persist replacement tombstone: %v", errPersist)
	}

	if errClear := w.clearAuthDeleteTombstone(path, oldGeneration); !errors.Is(errClear, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("clear old generation error = %v, want uncertain", errClear)
	}
	if !authfileguard.IsQuarantined(path) {
		t.Fatal("old generation cleared replacement quarantine")
	}
	if errClear := w.clearAuthDeleteTombstone(path, newGeneration); errClear != nil {
		t.Fatalf("clear replacement generation: %v", errClear)
	}
	if authfileguard.IsQuarantined(path) {
		t.Fatal("replacement generation remained quarantined")
	}
}

func TestAuthDeleteTombstoneResumesRemoteDeletionAfterRestart(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"remote-old"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}

	first := &Watcher{configPath: configPath, authDir: authDir}
	expectedHash := coreauth.SourceHashFromBytes(data)
	if errPersist := first.persistAuthDeleteTombstone(path, expectedHash); errPersist != nil {
		t.Fatalf("persistAuthDeleteTombstone() error = %v", errPersist)
	}

	persister := newControlledFilePersister(t)
	restarted := newFileAdmissionTestWatcher(authDir, persister)
	restarted.configPath = configPath
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
	}
	restarted.resumeAuthDeleteTombstones()
	call := nextControlledPersistCall(t, persister)
	if call.expectedDeleteHash != expectedHash {
		t.Fatalf("resumed delete expected hash = %q, want %q", call.expectedDeleteHash, expectedHash)
	}
	if call.deleteGeneration == nil {
		t.Fatal("resumed delete did not restore its durable generation")
	}
	if call.identityBinding {
		t.Fatal("resumed delete was allowed to bind an unseen backend generation")
	}
	if _, errStat := os.Stat(path); !os.IsNotExist(errStat) {
		t.Fatalf("resumed delete left local auth in place: %v", errStat)
	}
	call.complete(nil)

	normalized := restarted.normalizeAuthPath(path)
	waitForWatcherCondition(t, "resumed auth deletion completion", func() bool {
		restarted.clientsMutex.RLock()
		_, pending := restarted.retiredDeletes[normalized]
		_, quarantined := restarted.retiredAuthPaths[normalized]
		_, tombstoned := restarted.retiredDeleteHashes[normalized]
		restarted.clientsMutex.RUnlock()
		return !pending && !quarantined && !tombstoned
	})
	metadataRoot, authRoot, _, errContext := restarted.authDeleteTombstoneContext()
	if errContext != nil {
		t.Fatalf("authDeleteTombstoneContext() error = %v", errContext)
	}
	tombstonePath := filepath.Join(metadataRoot, authDeleteTombstoneDirectory, authDeleteTombstoneName(authRoot, "codex.json"))
	if _, errStat := os.Stat(tombstonePath); !os.IsNotExist(errStat) {
		t.Fatalf("completed resumed deletion left tombstone: %v", errStat)
	}
}

func TestRetiredAuthDeleteTombstoneResumesWithFinalizerAfterRestart(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "legacy-gemini.json")
	data := []byte(`{"type":"gemini","access_token":"retired"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	authfileguard.MarkRetired(path)
	generation := authfileguard.NewDeleteGeneration(coreauth.SourceHashFromBytes(data))
	first := &Watcher{configPath: configPath, authDir: authDir}
	if errPersist := first.persistAuthDeleteGenerationTombstone(path, generation); errPersist != nil {
		t.Fatalf("persist retired tombstone: %v", errPersist)
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove retired auth: %v", errRemove)
	}
	authfileguard.ClearRetired(path)
	authfileguard.ClearQuarantined(path)
	t.Cleanup(func() {
		authfileguard.ClearRetired(path)
		authfileguard.ClearQuarantined(path)
	})

	persister := newControlledAuthPersister(t)
	restarted := newFileAdmissionTestWatcher(authDir, persister)
	restarted.configPath = configPath
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("load retired tombstone: %v", errLoad)
	}
	if !authfileguard.IsRetired(path) {
		t.Fatal("retired tombstone did not restore the retired path marker")
	}
	restarted.resumeAuthDeleteTombstones()
	select {
	case <-persister.started:
	case <-time.After(time.Second):
		t.Fatal("resumed retired deletion did not call the dedicated finalizer")
	}
	persister.release(nil)

	normalized := restarted.normalizeAuthPath(path)
	waitForWatcherCondition(t, "resumed retired deletion completion", func() bool {
		restarted.clientsMutex.RLock()
		_, pending := restarted.retiredDeletes[normalized]
		_, quarantined := restarted.retiredAuthPaths[normalized]
		_, tombstoned := restarted.retiredDeleteHashes[normalized]
		restarted.clientsMutex.RUnlock()
		return !pending && !quarantined && !tombstoned && !authfileguard.IsRetired(path) && !authfileguard.IsQuarantined(path)
	})
}

func TestAuthDeleteTombstoneClearsCompletedLocalDeletionAfterRestart(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"deleted"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	first := &Watcher{configPath: configPath, authDir: authDir}
	if errPersist := first.persistAuthDeleteTombstone(path, coreauth.SourceHashFromBytes(data)); errPersist != nil {
		t.Fatalf("persistAuthDeleteTombstone() error = %v", errPersist)
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove committed local auth: %v", errRemove)
	}

	restarted := newFileAdmissionTestWatcher(authDir, nil)
	restarted.configPath = configPath
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
	}
	restarted.resumeAuthDeleteTombstones()

	normalized := restarted.normalizeAuthPath(path)
	restarted.clientsMutex.RLock()
	_, pending := restarted.retiredDeletes[normalized]
	_, quarantined := restarted.retiredAuthPaths[normalized]
	_, tombstoned := restarted.retiredDeleteHashes[normalized]
	restarted.clientsMutex.RUnlock()
	if pending || quarantined || tombstoned || authfileguard.IsQuarantined(path) {
		t.Fatal("completed local deletion remained quarantined after restart")
	}
	metadataRoot, authRoot, _, errContext := restarted.authDeleteTombstoneContext()
	if errContext != nil {
		t.Fatalf("authDeleteTombstoneContext() error = %v", errContext)
	}
	tombstonePath := filepath.Join(metadataRoot, authDeleteTombstoneDirectory, authDeleteTombstoneName(authRoot, "codex.json"))
	if _, errStat := os.Stat(tombstonePath); !os.IsNotExist(errStat) {
		t.Fatalf("completed local deletion left tombstone: %v", errStat)
	}
}

func TestAuthDeleteTombstoneAdmitsLocalReplacementAfterRestart(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "codex.json")
	original := []byte(`{"type":"codex","access_token":"original"}`)
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	first := &Watcher{configPath: configPath, authDir: authDir}
	if errPersist := first.persistAuthDeleteTombstone(path, coreauth.SourceHashFromBytes(original)); errPersist != nil {
		t.Fatalf("persistAuthDeleteTombstone() error = %v", errPersist)
	}
	if errWrite := os.WriteFile(path, replacement, 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}

	restarted := newFileAdmissionTestWatcher(authDir, nil)
	restarted.configPath = configPath
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
	}
	restarted.resumeAuthDeleteTombstones()
	if token, count := watcherRuntimeAccessToken(restarted); count != 1 || token != "replacement" {
		t.Fatalf("replacement runtime auth = (%q, %d), want replacement", token, count)
	}
	if authfileguard.IsQuarantined(path) {
		t.Fatal("local replacement remained quarantined after restart")
	}
}

func TestAuthDeleteTombstoneRejectsRetargetedAuthRoot(t *testing.T) {
	rootDir := t.TempDir()
	firstAuthDir := filepath.Join(rootDir, "auth-a")
	secondAuthDir := filepath.Join(rootDir, "auth-b")
	authLink := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config.yaml")
	if errMkdir := os.MkdirAll(firstAuthDir, 0o700); errMkdir != nil {
		t.Fatalf("create first auth directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(secondAuthDir, 0o700); errMkdir != nil {
		t.Fatalf("create second auth directory: %v", errMkdir)
	}
	if errLink := os.Symlink(firstAuthDir, authLink); errLink != nil {
		t.Skipf("symlink not supported: %v", errLink)
	}
	path := filepath.Join(authLink, "codex.json")
	data := []byte(`{"type":"codex","access_token":"old"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	first := &Watcher{configPath: configPath, authDir: authLink}
	deleteGeneration := authfileguard.NewDeleteGeneration(coreauth.SourceHashFromBytes(data))
	if errPersist := first.persistAuthDeleteGenerationTombstone(path, deleteGeneration); errPersist != nil {
		t.Fatalf("persistAuthDeleteTombstone() error = %v", errPersist)
	}
	if errRemove := os.Remove(authLink); errRemove != nil {
		t.Fatalf("remove first auth link: %v", errRemove)
	}
	if errLink := os.Symlink(secondAuthDir, authLink); errLink != nil {
		t.Fatalf("retarget auth link: %v", errLink)
	}

	restarted := newFileAdmissionTestWatcher(authLink, nil)
	restarted.configPath = configPath
	if errClear := restarted.clearAuthDeleteTombstone(path, deleteGeneration); errClear == nil {
		t.Fatal("clearAuthDeleteTombstone() error = nil, want auth root mismatch")
	}
	if !authfileguard.IsQuarantined(path) {
		t.Fatal("auth root mismatch cleared quarantine")
	}
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
	}
}

func TestAuthDeleteTombstoneWithoutConfigPathFollowsPhysicalAuthRoot(t *testing.T) {
	rootDir := t.TempDir()
	firstRoot := filepath.Join(rootDir, "first")
	secondRoot := filepath.Join(rootDir, "second")
	for _, root := range []string{firstRoot, secondRoot} {
		if errMkdir := os.MkdirAll(filepath.Join(root, "auths"), 0o700); errMkdir != nil {
			t.Fatalf("create auth directory: %v", errMkdir)
		}
	}
	rootLink := filepath.Join(rootDir, "current")
	if errLink := os.Symlink(firstRoot, rootLink); errLink != nil {
		t.Skipf("symlink not supported: %v", errLink)
	}
	authDir := filepath.Join(rootLink, "auths")
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"old"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write first auth: %v", errWrite)
	}
	generation := authfileguard.NewDeleteGeneration(coreauth.SourceHashFromBytes(data))
	first := &Watcher{authDir: authDir}
	if errPersist := first.persistAuthDeleteGenerationTombstone(path, generation); errPersist != nil {
		t.Fatalf("persist tombstone: %v", errPersist)
	}
	authfileguard.ClearQuarantined(path)
	authfileguard.ClearQuarantined(filepath.Join(firstRoot, "auths", "codex.json"))

	if errRemove := os.Remove(rootLink); errRemove != nil {
		t.Fatalf("remove first root link: %v", errRemove)
	}
	if errLink := os.Symlink(secondRoot, rootLink); errLink != nil {
		t.Fatalf("retarget root link: %v", errLink)
	}
	second := newFileAdmissionTestWatcher(authDir, nil)
	if errLoad := second.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("load tombstones for second root: %v", errLoad)
	}
	if len(second.retiredAuthPaths) != 0 {
		t.Fatalf("first-root tombstone leaked into second root: %#v", second.retiredAuthPaths)
	}

	if errRemove := os.Remove(rootLink); errRemove != nil {
		t.Fatalf("remove second root link: %v", errRemove)
	}
	if errLink := os.Symlink(firstRoot, rootLink); errLink != nil {
		t.Fatalf("restore first root link: %v", errLink)
	}
	restored := newFileAdmissionTestWatcher(authDir, nil)
	if errLoad := restored.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("reload tombstones for first root: %v", errLoad)
	}
	if _, ok := restored.retiredAuthPaths[restored.normalizeAuthPath(path)]; !ok {
		t.Fatal("first-root tombstone was not restored after link returned")
	}
	if errClear := restored.clearAuthDeleteTombstone(path, generation); errClear != nil {
		t.Fatalf("clear restored tombstone: %v", errClear)
	}
}

func TestAuthDeleteTombstoneSurvivesPhysicalRootAliasChange(t *testing.T) {
	realRoot := t.TempDir()
	aliasRoot := filepath.Join(t.TempDir(), "alias")
	if errLink := os.Symlink(realRoot, aliasRoot); errLink != nil {
		t.Skipf("symlink not supported: %v", errLink)
	}
	realAuthDir := filepath.Join(realRoot, "auths")
	realConfigPath := filepath.Join(realRoot, "config", "config.yaml")
	if errMkdir := os.MkdirAll(realAuthDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(filepath.Dir(realConfigPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errWrite := os.WriteFile(realConfigPath, []byte("auth-dir: auths\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	realPath := filepath.Join(realAuthDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"remote-old"}`)
	if errWrite := os.WriteFile(realPath, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}

	aliasAuthDir := filepath.Join(aliasRoot, "auths")
	aliasConfigPath := filepath.Join(aliasRoot, "config", "config.yaml")
	aliasPath := filepath.Join(aliasAuthDir, "codex.json")
	expectedHash := coreauth.SourceHashFromBytes(data)
	first := &Watcher{configPath: aliasConfigPath, authDir: aliasAuthDir}
	if errPersist := first.persistAuthDeleteTombstone(aliasPath, expectedHash); errPersist != nil {
		t.Fatalf("persistAuthDeleteTombstone() error = %v", errPersist)
	}

	restarted := &Watcher{
		configPath:          realConfigPath,
		authDir:             realAuthDir,
		retiredAuthPaths:    make(map[string]struct{}),
		retiredDeleteHashes: make(map[string]string),
	}
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
	}
	normalized := restarted.normalizeAuthPath(realPath)
	restarted.clientsMutex.RLock()
	_, quarantined := restarted.retiredAuthPaths[normalized]
	loadedHash := restarted.retiredDeleteHashes[normalized]
	restarted.clientsMutex.RUnlock()
	if !quarantined || loadedHash != expectedHash {
		t.Fatalf("physical-root quarantine = %t, hash = %q; want true, %q", quarantined, loadedHash, expectedHash)
	}
}

func TestAuthDeleteTombstoneAcceptsRemoteReplacementDuringRestart(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "codex.json")
	oldData := []byte(`{"type":"codex","access_token":"remote-old"}`)
	replacementData := []byte(`{"type":"codex","access_token":"remote-replacement"}`)
	first := &Watcher{configPath: configPath, authDir: authDir}
	if errPersist := first.persistAuthDeleteTombstone(path, coreauth.SourceHashFromBytes(oldData)); errPersist != nil {
		t.Fatalf("persistAuthDeleteTombstone() error = %v", errPersist)
	}
	if errWrite := os.WriteFile(path, replacementData, 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}

	persister := newControlledFilePersister(t)
	restarted := &Watcher{
		configPath:          configPath,
		authDir:             authDir,
		config:              &config.Config{AuthDir: authDir},
		storePersister:      persister,
		lastAuthHashes:      make(map[string]string),
		fileAuthsByPath:     make(map[string]map[string]*coreauth.Auth),
		currentAuths:        make(map[string]*coreauth.Auth),
		retiredAuthPaths:    make(map[string]struct{}),
		retiredDeleteHashes: make(map[string]string),
	}
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
	}
	restarted.resumeAuthDeleteTombstones()
	call := nextControlledPersistCall(t, persister)
	if string(call.payload) != string(replacementData) {
		t.Fatalf("replacement persistence payload = %q, want %q", call.payload, replacementData)
	}
	call.complete(nil)

	normalized := restarted.normalizeAuthPath(path)
	waitForWatcherCondition(t, "replacement persistence confirmation", func() bool {
		restarted.clientsMutex.RLock()
		_, quarantined := restarted.retiredAuthPaths[normalized]
		_, tombstoned := restarted.retiredDeleteHashes[normalized]
		authCount := len(restarted.fileAuthsByPath[normalized])
		restarted.clientsMutex.RUnlock()
		return !quarantined && !tombstoned && authCount == 1
	})
	metadataRoot, authRoot, _, errContext := restarted.authDeleteTombstoneContext()
	if errContext != nil {
		t.Fatalf("authDeleteTombstoneContext() error = %v", errContext)
	}
	tombstonePath := filepath.Join(metadataRoot, authDeleteTombstoneDirectory, authDeleteTombstoneName(authRoot, "codex.json"))
	if _, errStat := os.Stat(tombstonePath); !os.IsNotExist(errStat) {
		t.Fatalf("replacement tombstone still exists: %v", errStat)
	}
}

type controlledRetiredReplacementPersister struct {
	finalizer *controlledAuthPersister
	files     *controlledFilePersister
}

func (p *controlledRetiredReplacementPersister) PersistConfig(ctx context.Context) error {
	return p.files.PersistConfig(ctx)
}

func (p *controlledRetiredReplacementPersister) PersistAuthFiles(ctx context.Context, message string, paths ...string) error {
	return p.files.PersistAuthFiles(ctx, message, paths...)
}

func (p *controlledRetiredReplacementPersister) FinalizeAuthFileDeletion(ctx context.Context, id string) error {
	return p.finalizer.FinalizeAuthFileDeletion(ctx, id)
}

func TestRetiredReplacementPersistenceQuarantineSurvivesRestart(t *testing.T) {
	finalizer := newControlledAuthPersister(t)
	files := newControlledFilePersister(t)
	persister := &controlledRetiredReplacementPersister{finalizer: finalizer, files: files}
	w, path := newRetiredDeletionTestWatcher(t, persister)
	startRetiredDeletion(t, w, path, finalizer)

	replacementData := []byte(`{"type":"codex","access_token":"replacement"}`)
	if errWrite := os.WriteFile(path, replacementData, 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	w.addOrUpdateClient(path)
	finalizer.release(nil)
	replacementCall := nextControlledPersistCall(t, files)
	if string(replacementCall.payload) != string(replacementData) {
		t.Fatalf("replacement persistence payload = %q, want %q", replacementCall.payload, replacementData)
	}

	metadataRoot, authRoot, _, errContext := w.authDeleteTombstoneContext()
	if errContext != nil {
		t.Fatalf("authDeleteTombstoneContext() error = %v", errContext)
	}
	tombstonePath := filepath.Join(metadataRoot, authDeleteTombstoneDirectory, authDeleteTombstoneName(authRoot, filepath.Base(path)))
	if _, errStat := os.Stat(tombstonePath); errStat != nil {
		t.Fatalf("replacement persistence started without durable tombstone: %v", errStat)
	}

	restartedPersister := newControlledFilePersister(t)
	restarted := newFileAdmissionTestWatcher(filepath.Dir(path), restartedPersister)
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
	}
	restarted.cacheAuthFileForReload(restarted.config, filepath.Dir(path), path, false, true)
	restartedCall := nextControlledPersistCall(t, restartedPersister)
	restarted.clientsMutex.RLock()
	runtimeCount := len(restarted.currentAuths)
	restarted.clientsMutex.RUnlock()
	if runtimeCount != 0 {
		t.Fatalf("restart admitted replacement before persistence: %d auths", runtimeCount)
	}
	if _, errStat := os.Stat(tombstonePath); errStat != nil {
		t.Fatalf("restart lost replacement tombstone: %v", errStat)
	}

	w.stopped.Store(true)
	replacementCall.complete(errors.New("simulated process stop"))
	restartedCall.complete(nil)
	waitForWatcherCondition(t, "replacement admission after restart persistence", func() bool {
		token, count := watcherRuntimeAccessToken(restarted)
		return count == 1 && token == "replacement"
	})
	if _, errStat := os.Stat(tombstonePath); !os.IsNotExist(errStat) {
		t.Fatalf("replacement tombstone still exists after persistence: %v", errStat)
	}
}

func TestAuthDeleteTombstoneWithoutHashPersistsExistingFileAsReplacement(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "replacement.json")
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	if errWrite := os.WriteFile(path, replacement, 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	first := &Watcher{configPath: configPath, authDir: authDir}
	if errPersist := first.persistAuthDeleteTombstone(path, ""); errPersist != nil {
		t.Fatalf("persistAuthDeleteTombstone() error = %v", errPersist)
	}

	persister := newControlledFilePersister(t)
	restarted := newFileAdmissionTestWatcher(authDir, persister)
	restarted.configPath = configPath
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
	}
	restarted.cacheAuthFileForReload(restarted.config, authDir, path, false, true)
	call := nextControlledPersistCall(t, persister)
	if !bytes.Equal(call.payload, replacement) {
		t.Fatalf("persisted replacement = %q, want %q", call.payload, replacement)
	}
	if _, count := watcherRuntimeAccessToken(restarted); count != 0 {
		t.Fatalf("replacement admitted before persistence: %d auths", count)
	}
	call.complete(nil)
	waitForWatcherCondition(t, "replacement admission after hashless tombstone", func() bool {
		token, count := watcherRuntimeAccessToken(restarted)
		return count == 1 && token == "replacement"
	})
}

func TestAuthPersistenceRetriesOnlyTombstoneCleanupAfterRemoteSuccess(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"persisted"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)
	w.configPath = configPath
	w.authRetryBase = 5 * time.Millisecond
	w.addOrUpdateClient(path)
	call := nextControlledPersistCall(t, persister)
	unblock := blockAuthDeleteTombstoneCleanup(t, w)
	call.complete(nil)
	time.Sleep(2 * w.authRetryBase)
	unblock()

	waitForWatcherCondition(t, "auth admission after tombstone cleanup retry", func() bool {
		token, count := watcherRuntimeAccessToken(w)
		return count == 1 && token == "persisted"
	})
	if got := atomic.LoadInt32(&persister.callCount); got != 1 {
		t.Fatalf("remote persistence calls = %d, want 1", got)
	}
}

func TestAuthRemovalRetriesOnlyTombstoneCleanupAfterRemoteSuccess(t *testing.T) {
	rootDir := t.TempDir()
	authDir := filepath.Join(rootDir, "auths")
	configPath := filepath.Join(rootDir, "config", "config.yaml")
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(filepath.Dir(configPath), 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"removed"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)
	w.configPath = configPath
	w.authRetryBase = 5 * time.Millisecond
	normalized := w.normalizeAuthPath(path)
	w.lastAuthHashes[normalized] = coreauth.SourceHashFromBytes(data)
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove auth: %v", errRemove)
	}
	w.removeClientState(path, true)
	call := nextControlledPersistCall(t, persister)
	unblock := blockAuthDeleteTombstoneCleanup(t, w)
	call.complete(nil)
	time.Sleep(2 * w.authRetryBase)
	unblock()

	waitForWatcherCondition(t, "auth removal after tombstone cleanup retry", func() bool {
		w.clientsMutex.RLock()
		_, pending := w.retiredDeletes[normalized]
		_, quarantined := w.retiredAuthPaths[normalized]
		w.clientsMutex.RUnlock()
		return !pending && !quarantined
	})
	if got := atomic.LoadInt32(&persister.callCount); got != 1 {
		t.Fatalf("remote persistence calls = %d, want 1", got)
	}
}

func blockAuthDeleteTombstoneCleanup(t *testing.T, w *Watcher) func() {
	t.Helper()
	metadataRoot, _, _, errContext := w.authDeleteTombstoneContext()
	if errContext != nil {
		t.Fatalf("authDeleteTombstoneContext() error = %v", errContext)
	}
	directory := filepath.Join(metadataRoot, authDeleteTombstoneDirectory)
	if errRemove := os.RemoveAll(directory); errRemove != nil {
		t.Fatalf("remove tombstone directory: %v", errRemove)
	}
	outside := t.TempDir()
	if errLink := os.Symlink(outside, directory); errLink != nil {
		t.Skipf("symlink not supported: %v", errLink)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if errRemove := os.Remove(directory); errRemove != nil && !os.IsNotExist(errRemove) {
				t.Fatalf("remove blocking tombstone symlink: %v", errRemove)
			}
		})
	}
}

func TestAuthDeleteTombstoneRejectsSymlinkedMetadataDirectory(t *testing.T) {
	rootDir := t.TempDir()
	configDir := filepath.Join(rootDir, "config")
	authDir := filepath.Join(rootDir, "auths")
	outsideDir := t.TempDir()
	if errMkdir := os.MkdirAll(configDir, 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	if errLink := os.Symlink(outsideDir, filepath.Join(configDir, authDeleteTombstoneDirectory)); errLink != nil {
		t.Skipf("symlink not supported: %v", errLink)
	}
	path := filepath.Join(authDir, "codex.json")
	w := &Watcher{configPath: filepath.Join(configDir, "config.yaml"), authDir: authDir}
	if errPersist := w.persistAuthDeleteTombstone(path, "expected-hash"); errPersist == nil {
		t.Fatal("persistAuthDeleteTombstone() succeeded through symlinked metadata directory")
	}
	entries, errRead := os.ReadDir(outsideDir)
	if errRead != nil {
		t.Fatalf("read outside directory: %v", errRead)
	}
	if len(entries) != 0 {
		t.Fatalf("outside directory received %d entries", len(entries))
	}
}

func TestLoadAuthDeleteTombstonesIgnoresUnsafeStoredPath(t *testing.T) {
	for _, storedPath := range []string{"/outside.json", "C:outside.json", `..\outside.json`, `\\server\share.json`} {
		t.Run(storedPath, func(t *testing.T) {
			rootDir := t.TempDir()
			configDir := filepath.Join(rootDir, "config")
			authDir := filepath.Join(rootDir, "auths")
			if errMkdir := os.MkdirAll(filepath.Join(configDir, authDeleteTombstoneDirectory), 0o700); errMkdir != nil {
				t.Fatalf("create tombstone directory: %v", errMkdir)
			}
			if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
				t.Fatalf("create auth directory: %v", errMkdir)
			}
			w := &Watcher{
				configPath:       filepath.Join(configDir, "config.yaml"),
				authDir:          authDir,
				retiredAuthPaths: make(map[string]struct{}),
			}
			_, authRoot, _, errContext := w.authDeleteTombstoneContext()
			if errContext != nil {
				t.Fatalf("authDeleteTombstoneContext() error = %v", errContext)
			}
			data, errMarshal := json.Marshal(authDeleteTombstone{AuthRoot: authRoot, Path: storedPath})
			if errMarshal != nil {
				t.Fatalf("marshal unsafe tombstone: %v", errMarshal)
			}
			tombstonePath := filepath.Join(configDir, authDeleteTombstoneDirectory, "unsafe.tombstone")
			if errWrite := os.WriteFile(tombstonePath, data, 0o600); errWrite != nil {
				t.Fatalf("write unsafe tombstone: %v", errWrite)
			}

			if errLoad := w.loadAuthDeleteTombstones(); errLoad != nil {
				t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
			}
			w.clientsMutex.RLock()
			count := len(w.retiredAuthPaths)
			w.clientsMutex.RUnlock()
			if count != 0 {
				t.Fatalf("unsafe tombstone quarantined %d paths", count)
			}
		})
	}
}

func TestLoadAuthDeleteTombstonesContinuesAfterMalformedEntry(t *testing.T) {
	rootDir := t.TempDir()
	configDir := filepath.Join(rootDir, "config")
	authDir := filepath.Join(rootDir, "auths")
	if errMkdir := os.MkdirAll(configDir, 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	configPath := filepath.Join(configDir, "config.yaml")
	authPath := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"pending-delete"}`)
	if errWrite := os.WriteFile(authPath, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	first := &Watcher{configPath: configPath, authDir: authDir}
	if errPersist := first.persistAuthDeleteTombstone(authPath, coreauth.SourceHashFromBytes(data)); errPersist != nil {
		t.Fatalf("persist valid tombstone: %v", errPersist)
	}
	malformedPath := filepath.Join(configDir, authDeleteTombstoneDirectory, "malformed.tombstone")
	if errWrite := os.WriteFile(malformedPath, []byte(`{"auth_root":`), 0o600); errWrite != nil {
		t.Fatalf("write malformed tombstone: %v", errWrite)
	}

	restarted := newFileAdmissionTestWatcher(authDir, nil)
	restarted.configPath = configPath
	if errLoad := restarted.loadAuthDeleteTombstones(); errLoad != nil {
		t.Fatalf("loadAuthDeleteTombstones() error = %v", errLoad)
	}
	normalized := restarted.normalizeAuthPath(authPath)
	restarted.clientsMutex.RLock()
	_, quarantined := restarted.retiredAuthPaths[normalized]
	restarted.clientsMutex.RUnlock()
	if !quarantined || !authfileguard.IsQuarantined(authPath) {
		t.Fatal("valid tombstone was not loaded after malformed neighbor")
	}
	t.Cleanup(func() { authfileguard.ClearQuarantined(authPath) })
}

func TestWatcherStartIgnoresInvalidAuthDeleteTombstone(t *testing.T) {
	tests := []struct {
		name  string
		write func(*testing.T, string)
	}{
		{
			name: "malformed json",
			write: func(t *testing.T, path string) {
				t.Helper()
				if errWrite := os.WriteFile(path, []byte(`{"auth_root":`), 0o600); errWrite != nil {
					t.Fatalf("write malformed tombstone: %v", errWrite)
				}
			},
		},
		{
			name: "unreadable symlink",
			write: func(t *testing.T, path string) {
				t.Helper()
				outside := filepath.Join(t.TempDir(), "outside.tombstone")
				if errWrite := os.WriteFile(outside, []byte(`{}`), 0o600); errWrite != nil {
					t.Fatalf("write outside tombstone: %v", errWrite)
				}
				if errLink := os.Symlink(outside, path); errLink != nil {
					t.Skipf("symlink not supported: %v", errLink)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootDir := t.TempDir()
			configDir := filepath.Join(rootDir, "config")
			authDir := filepath.Join(rootDir, "auths")
			if errMkdir := os.MkdirAll(filepath.Join(configDir, authDeleteTombstoneDirectory), 0o700); errMkdir != nil {
				t.Fatalf("create tombstone directory: %v", errMkdir)
			}
			if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
				t.Fatalf("create auth directory: %v", errMkdir)
			}
			configPath := filepath.Join(configDir, "config.yaml")
			if errWrite := os.WriteFile(configPath, []byte("auth-dir: "+authDir+"\n"), 0o600); errWrite != nil {
				t.Fatalf("write config: %v", errWrite)
			}
			authPath := filepath.Join(authDir, "codex.json")
			if errWrite := os.WriteFile(authPath, []byte(`{"type":"codex","access_token":"blocked"}`), 0o600); errWrite != nil {
				t.Fatalf("write auth: %v", errWrite)
			}
			tt.write(t, filepath.Join(configDir, authDeleteTombstoneDirectory, "invalid.tombstone"))

			w, errWatcher := NewWatcher(configPath, authDir, nil)
			if errWatcher != nil {
				t.Fatalf("NewWatcher() error = %v", errWatcher)
			}
			t.Cleanup(func() { _ = w.Stop() })
			w.SetConfig(&config.Config{AuthDir: authDir})
			if errStart := w.Start(context.Background()); errStart != nil {
				t.Fatalf("Start() error = %v", errStart)
			}
			w.clientsMutex.RLock()
			runtimeCount := len(w.currentAuths)
			w.clientsMutex.RUnlock()
			if runtimeCount != 1 {
				t.Fatalf("invalid tombstone startup loaded %d auths, want 1", runtimeCount)
			}
		})
	}
}

func TestLoadAuthDeleteTombstonesRejectsUnstableDirectory(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{
			name: "symlink",
			setup: func(t *testing.T, path string) {
				t.Helper()
				target := t.TempDir()
				if errLink := os.Symlink(target, path); errLink != nil {
					t.Skipf("symlink not supported: %v", errLink)
				}
			},
		},
		{
			name: "regular file",
			setup: func(t *testing.T, path string) {
				t.Helper()
				if errWrite := os.WriteFile(path, []byte("not a directory"), 0o600); errWrite != nil {
					t.Fatalf("write tombstone directory placeholder: %v", errWrite)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configDir := t.TempDir()
			authDir := t.TempDir()
			tt.setup(t, filepath.Join(configDir, authDeleteTombstoneDirectory))
			w := &Watcher{configPath: filepath.Join(configDir, "config.yaml"), authDir: authDir}
			if errLoad := w.loadAuthDeleteTombstones(); errLoad == nil {
				t.Fatal("loadAuthDeleteTombstones() succeeded for unstable directory")
			}
		})
	}
}

func TestAuthDeleteTombstoneRetryPreservesFirstRemoteAttempt(t *testing.T) {
	rootDir := t.TempDir()
	configDir := filepath.Join(rootDir, "config")
	authDir := filepath.Join(rootDir, "auths")
	outsideDir := t.TempDir()
	if errMkdir := os.MkdirAll(configDir, 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	if errLink := os.Symlink(outsideDir, filepath.Join(configDir, authDeleteTombstoneDirectory)); errLink != nil {
		t.Skipf("symlink not supported: %v", errLink)
	}
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"removed"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)
	w.configPath = filepath.Join(configDir, "config.yaml")
	w.authRetryBase = 20 * time.Millisecond
	normalized := w.normalizeAuthPath(path)
	w.lastAuthHashes[normalized] = coreauth.SourceHashFromBytes(data)
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove auth: %v", errRemove)
	}

	w.removeClientState(path, true)
	select {
	case call := <-persister.calls:
		call.complete(nil)
		t.Fatal("remote deletion started before tombstone persistence recovered")
	default:
	}
	if errRemove := os.Remove(filepath.Join(configDir, authDeleteTombstoneDirectory)); errRemove != nil {
		t.Fatalf("remove blocking tombstone symlink: %v", errRemove)
	}
	call := nextControlledPersistCall(t, persister)
	if call.deleteAttempt != 0 {
		t.Fatalf("first remote delete attempt = %d, want 0", call.deleteAttempt)
	}
	if !call.identityBinding {
		t.Fatal("live first delete attempt cannot bind its inspected backend generation")
	}
	call.complete(nil)
	waitForWatcherCondition(t, "deletion after tombstone retry", func() bool {
		w.clientsMutex.RLock()
		_, pending := w.retiredDeletes[normalized]
		_, quarantined := w.retiredAuthPaths[normalized]
		w.clientsMutex.RUnlock()
		return !pending && !quarantined
	})
}

func TestAuthReplacementRetryDoesNotPersistTombstonedOriginal(t *testing.T) {
	rootDir := t.TempDir()
	configDir := filepath.Join(rootDir, "config")
	authDir := filepath.Join(rootDir, "auths")
	if errMkdir := os.MkdirAll(configDir, 0o700); errMkdir != nil {
		t.Fatalf("create config directory: %v", errMkdir)
	}
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth directory: %v", errMkdir)
	}
	path := filepath.Join(authDir, "codex.json")
	originalData := []byte(`{"type":"codex","access_token":"original"}`)
	replacementData := []byte(`{"type":"codex","access_token":"replacement"}`)
	if errWrite := os.WriteFile(path, replacementData, 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)
	w.configPath = filepath.Join(configDir, "config.yaml")
	w.authRetryBase = 20 * time.Millisecond
	normalized := w.normalizeAuthPath(path)
	originalHash := coreauth.SourceHashFromBytes(originalData)
	w.lastAuthHashes[normalized] = originalHash
	w.retiredAuthPaths[normalized] = struct{}{}
	w.retiredDeleteHashes = map[string]string{normalized: originalHash}
	if errPersist := w.persistAuthDeleteTombstone(path, originalHash); errPersist != nil {
		t.Fatalf("persistAuthDeleteTombstone() error = %v", errPersist)
	}

	w.addOrUpdateClient(path)
	first := nextControlledPersistCall(t, persister)
	replacementHash := coreauth.SourceHashFromBytes(replacementData)
	if first.expectedPersistHash != replacementHash {
		t.Fatalf("replacement expected hash = %q, want %q", first.expectedPersistHash, replacementHash)
	}
	first.complete(errors.New("temporary persistence failure"))
	if errWrite := os.WriteFile(path, originalData, 0o600); errWrite != nil {
		t.Fatalf("restore original auth: %v", errWrite)
	}
	w.addOrUpdateClient(path)

	waitForWatcherCondition(t, "stale replacement retry cancellation", func() bool {
		w.clientsMutex.RLock()
		_, pending := w.retiredDeletes[normalized]
		_, quarantined := w.retiredAuthPaths[normalized]
		deleteHash := w.retiredDeleteHashes[normalized]
		runtimeCount := len(w.currentAuths)
		w.clientsMutex.RUnlock()
		return !pending && quarantined && deleteHash == originalHash && runtimeCount == 0
	})
	if got := atomic.LoadInt32(&persister.callCount); got != 1 {
		t.Fatalf("remote persistence calls = %d, want 1", got)
	}
	select {
	case retry := <-persister.calls:
		retry.complete(nil)
		t.Fatalf("stale replacement reached remote persister with payload %q", retry.payload)
	default:
	}
}
