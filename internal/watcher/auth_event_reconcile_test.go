package watcher

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func newAuthEventFixture(t *testing.T) (*Watcher, string, []byte) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "chatgpt-web.json")
	data := []byte(`{"type":"chatgpt-web","email":"fixture@example.com","session_token":"initial"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	w := &Watcher{authDir: dir, config: &config.Config{AuthDir: dir}}
	w.addOrUpdateClient(path)
	if len(w.currentAuths) != 1 {
		t.Fatal("initial credential was not loaded")
	}
	return w, path, data
}

func captureAuthEventLogs(t *testing.T) *logtest.Hook {
	t.Helper()
	logger := log.StandardLogger()
	previousHooks := logger.ReplaceHooks(log.LevelHooks{})
	previousLevel := logger.GetLevel()
	logger.SetLevel(log.DebugLevel)
	hook := logtest.NewGlobal()
	t.Cleanup(func() {
		logger.ReplaceHooks(previousHooks)
		logger.SetLevel(previousLevel)
	})
	return hook
}

func TestAuthEventRechecksRemovalAfterSaveLock(t *testing.T) {
	for _, mode := range []string{"rename", "remove", "initial-sync"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w, path, _ := newAuthEventFixture(t)
				unlock := authfileguard.Lock(path)
				defer unlock()
				if errMove := os.Rename(path, path+".old"); errMove != nil {
					t.Fatal(errMove)
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					if mode == "initial-sync" {
						w.replayInitialSyncAuthPath(path)
						return
					}
					op := fsnotify.Rename
					if mode == "remove" {
						op = fsnotify.Remove
					}
					w.handleEvent(fsnotify.Event{Name: path, Op: op})
				}()
				// Advance the old replacement delay while the save still owns the path.
				time.Sleep(2 * replaceCheckDelay)
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("event completed while the save owned the path")
				default:
				}
				rotated := []byte(`{"type":"chatgpt-web","email":"fixture@example.com","session_token":"rotated"}`)
				if errWrite := os.WriteFile(path, rotated, 0o600); errWrite != nil {
					t.Fatal(errWrite)
				}
				authfileguard.MarkManagerPersistedGeneration(path, coreauth.SourceHashFromBytes(rotated))
				unlock()
				synctest.Wait()
				<-done
				if !w.isKnownAuthFile(path) || len(w.currentAuths) != 1 {
					t.Fatal("completed save was mistaken for a credential deletion")
				}
				for _, auth := range w.currentAuths {
					if auth.Metadata["session_token"] != "rotated" {
						t.Fatal("completed save was not adopted")
					}
				}
			})
		})
	}
}

func TestAuthEventIgnoresQueuedCreateAfterConfirmedDeletion(t *testing.T) {
	w, path, data := newAuthEventFixture(t)
	store := sdkAuth.NewFileTokenStore()
	store.SetBaseDir(filepath.Dir(path))
	if errDelete := store.DeleteIfSourceHashMatches(t.Context(), filepath.Base(path), coreauth.SourceHashFromBytes(data)); errDelete != nil {
		t.Fatal(errDelete)
	}
	hook := captureAuthEventLogs(t)
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Rename})
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Create})
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
	if w.isKnownAuthFile(path) || len(w.currentAuths) != 0 {
		t.Fatal("deleted credential was restored by a stale event")
	}
	for _, entry := range hook.AllEntries() {
		if entry.Level <= log.WarnLevel {
			t.Fatalf("confirmed deletion produced a spurious failure: %s", entry.Message)
		}
	}
}

func TestAuthEventAtomicReplacementDoesNotSleepOrHideNextDeletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, path, _ := newAuthEventFixture(t)
		started := time.Now()
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Rename})
		if time.Since(started) != 0 {
			t.Fatal("already present replacement blocked the event loop")
		}
		if errRemove := os.Remove(path); errRemove != nil {
			t.Fatal(errRemove)
		}
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Remove})
		if w.isKnownAuthFile(path) {
			t.Fatal("replacement debounce hid a subsequent real deletion")
		}
	})
}

func TestAuthEventInspectionErrorDoesNotDeleteCredential(t *testing.T) {
	w, path, _ := newAuthEventFixture(t)
	// ENOTDIR is an inspection failure, not proof that this auth was deleted.
	if errMove := os.Rename(w.authDir, w.authDir+".old"); errMove != nil {
		t.Fatal(errMove)
	}
	t.Cleanup(func() {
		if errRemove := os.Remove(w.authDir); errRemove != nil {
			t.Error(errRemove)
		}
		if errRestore := os.Rename(w.authDir+".old", w.authDir); errRestore != nil {
			t.Error(errRestore)
		}
	})
	if errWrite := os.WriteFile(w.authDir, []byte("not a directory"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	hook := captureAuthEventLogs(t)
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Rename})
	if !w.isKnownAuthFile(path) || len(w.currentAuths) != 1 {
		t.Fatal("inspection failure was mistaken for a credential deletion")
	}
	found := false
	for _, entry := range hook.AllEntries() {
		if entry.Level <= log.WarnLevel && strings.Contains(entry.Message, filepath.Base(path)) {
			found = true
		}
	}
	if !found {
		t.Fatal("inspection failure was hidden")
	}
}

func BenchmarkAuthEventAtomicReplacement(b *testing.B) {
	for _, size := range []int{1, 100000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "codex.json")
			if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"fixture"}`), 0o600); errWrite != nil {
				b.Fatal(errWrite)
			}
			w := &Watcher{authDir: dir, config: &config.Config{AuthDir: dir}}
			w.addOrUpdateClient(path)
			for i := 1; i < size; i++ {
				w.lastAuthHashes[filepath.Join(dir, fmt.Sprintf("other-%d.json", i))] = "unchanged"
			}
			event := fsnotify.Event{Name: path, Op: fsnotify.Rename}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				w.handleEvent(event)
			}
		})
	}
}
