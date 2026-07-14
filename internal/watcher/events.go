// events.go implements fsnotify event handling for config and auth file changes.
// It normalizes paths, debounces noisy events, and triggers reload/update logic.
package watcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	log "github.com/sirupsen/logrus"
)

func matchProvider(provider string, targets []string) (string, bool) {
	p := strings.ToLower(strings.TrimSpace(provider))
	for _, t := range targets {
		if strings.EqualFold(p, strings.TrimSpace(t)) {
			return p, true
		}
	}
	return p, false
}

func (w *Watcher) start(ctx context.Context) (err error) {
	w.eventMu.Lock()
	defer w.eventMu.Unlock()
	addedPaths := make([]string, 0, 2)
	defer func() {
		if err == nil || len(addedPaths) == 0 {
			return
		}
		for index := len(addedPaths) - 1; index >= 0; index-- {
			if errRemove := w.watcher.Remove(addedPaths[index]); errRemove != nil {
				err = errors.Join(err, fmt.Errorf("remove partial watcher registration %s: %w", addedPaths[index], errRemove))
			}
		}
		w.eventPathsAdded = false
	}()
	if ctx == nil {
		return fmt.Errorf("watcher context is nil")
	}
	if w.stopped.Load() {
		return fmt.Errorf("watcher is stopped")
	}
	if w.eventCancel != nil {
		return fmt.Errorf("watcher event loop is already running")
	}
	if !w.eventPathsAdded {
		if errAddConfig := w.watcher.Add(w.configPath); errAddConfig != nil {
			log.Errorf("failed to watch config file %s: %v", w.configPath, errAddConfig)
			return errAddConfig
		}
		addedPaths = append(addedPaths, w.configPath)
		log.Debugf("watching config file: %s", w.configPath)
		if errAddAuthDir := w.watcher.Add(w.authDir); errAddAuthDir != nil {
			log.Errorf("failed to watch auth directory %s: %v", w.authDir, errAddAuthDir)
			return errAddAuthDir
		}
		addedPaths = append(addedPaths, w.authDir)
		w.eventPathsAdded = true
		log.Debugf("watching auth directory: %s", w.authDir)
	}
	if !w.eventInitialized {
		if errLoad := w.loadAuthDeleteTombstones(); errLoad != nil {
			return fmt.Errorf("load auth deletion tombstones: %w", errLoad)
		}
		w.resumeAuthDeleteTombstones()
		w.eventInitialized = true
	}
	eventCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	w.eventCancel = cancel
	w.eventDone = done
	go func() {
		w.processEvents(eventCtx)
		w.eventMu.Lock()
		if w.eventDone == done {
			w.eventCancel = nil
			w.eventDone = nil
		}
		w.eventMu.Unlock()
		close(done)
	}()

	w.reloadClientsWithOptions(true, nil, false, true)
	return nil
}

func (w *Watcher) stopEventLoop() {
	if w == nil {
		return
	}
	w.eventMu.Lock()
	w.stopped.Store(true)
	cancel := w.eventCancel
	done := w.eventDone
	w.eventCancel = nil
	w.eventDone = nil
	w.eventMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (w *Watcher) processEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			w.handleEvent(event)
		case errWatch, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			log.Errorf("file watcher error: %v", errWatch)
		}
	}
}

func (w *Watcher) handleEvent(event fsnotify.Event) {
	// Filter only relevant events: config file or auth-dir JSON files.
	configOps := fsnotify.Write | fsnotify.Create | fsnotify.Rename
	normalizedName := w.normalizeAuthPath(event.Name)
	normalizedConfigPath := w.normalizeAuthPath(w.configPath)
	normalizedAuthDir := w.normalizeAuthPath(w.authDir)
	isConfigEvent := normalizedName == normalizedConfigPath && event.Op&configOps != 0
	authOps := fsnotify.Create | fsnotify.Write | fsnotify.Remove | fsnotify.Rename
	isAuthJSON := filepath.Dir(normalizedName) == normalizedAuthDir && strings.HasSuffix(normalizedName, ".json") && event.Op&authOps != 0
	if !isConfigEvent && !isAuthJSON {
		// Ignore unrelated files (e.g., cookie snapshots *.cookie) and other noise.
		return
	}

	now := time.Now()
	log.Debugf("file system event detected: %s %s", event.Op.String(), event.Name)

	// Handle config file changes
	if isConfigEvent {
		log.Debugf("config file change details - operation: %s, timestamp: %s", event.Op.String(), now.Format("2006-01-02 15:04:05.000"))
		w.scheduleConfigReload()
		return
	}
	if event.Op&(fsnotify.Create|fsnotify.Write) != 0 {
		defer w.clearRemoveDebounce(normalizedName)
	}
	if info, errInfo := os.Lstat(event.Name); errInfo == nil && info.Mode()&os.ModeSymlink != 0 {
		log.Warnf("ignoring symlink auth file event: %s", filepath.Base(event.Name))
		w.removeClientState(event.Name, false)
		return
	}

	// Handle auth directory changes incrementally (.json only)
	if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		if w.shouldDebounceRemove(normalizedName, now) {
			log.Debugf("debouncing remove event for %s", filepath.Base(event.Name))
			return
		}
		// Atomic replace on some platforms may surface as Rename (or Remove) before the new file is ready.
		// Wait briefly; if the path exists again, treat it as an update unless the previous
		// credential was retired and still requires confirmed deletion.
		time.Sleep(replaceCheckDelay)
		if _, statErr := os.Stat(event.Name); statErr == nil {
			if w.finalizeRetiredAuthReplacement(event.Name) {
				log.Infof("auth file changed (%s): %s, finalizing retired deletion before replacement", event.Op.String(), filepath.Base(event.Name))
				if !w.requiresRetiredAuthDeletion(event.Name) {
					w.addOrUpdateClient(event.Name)
				}
				return
			}
			if unchanged, errSame := w.authFileUnchanged(event.Name); errSame == nil && unchanged {
				log.Debugf("auth file unchanged (hash match), skipping reload: %s", filepath.Base(event.Name))
				return
			}
			log.Infof("auth file changed (%s): %s, processing incrementally", event.Op.String(), filepath.Base(event.Name))
			w.addOrUpdateClient(event.Name)
			return
		}
		if !w.isKnownAuthFile(event.Name) {
			log.Debugf("ignoring remove for unknown auth file: %s", filepath.Base(event.Name))
			return
		}
		log.Infof("auth file changed (%s): %s, processing incrementally", event.Op.String(), filepath.Base(event.Name))
		w.removeClient(event.Name)
		return
	}
	if event.Op&(fsnotify.Create|fsnotify.Write) != 0 {
		if w.finalizeRetiredAuthReplacement(event.Name) {
			log.Infof("auth file changed (%s): %s, finalizing retired deletion before replacement", event.Op.String(), filepath.Base(event.Name))
			if !w.requiresRetiredAuthDeletion(event.Name) {
				w.addOrUpdateClient(event.Name)
			}
			return
		}
		if unchanged, errSame := w.authFileUnchanged(event.Name); errSame == nil && unchanged {
			log.Debugf("auth file unchanged (hash match), skipping reload: %s", filepath.Base(event.Name))
			return
		}
		log.Infof("auth file changed (%s): %s, processing incrementally", event.Op.String(), filepath.Base(event.Name))
		w.addOrUpdateClient(event.Name)
	}
}

func (w *Watcher) authFileUnchanged(path string) (bool, error) {
	if info, errInfo := os.Lstat(path); errInfo == nil && info.Mode()&os.ModeSymlink != 0 {
		return false, os.ErrInvalid
	}
	data, errRead := readAuthFileUnderRoot(w.authRootDir(), path)
	if errRead != nil {
		return false, errRead
	}
	if len(data) == 0 {
		return false, nil
	}
	sum := sha256.Sum256(data)
	curHash := hex.EncodeToString(sum[:])

	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	prevHash, ok := w.lastAuthHashes[normalized]
	_, quarantined := w.retiredAuthPaths[normalized]
	_, persistencePending := w.retiredDeletes[normalized]
	deleteHash, tombstoned := w.retiredDeleteHashes[normalized]
	w.clientsMutex.RUnlock()
	if tombstoned && (w.storePersister == nil || (deleteHash != "" && deleteHash == curHash)) {
		return true, nil
	}
	if ok && prevHash == curHash {
		return !quarantined || persistencePending || authfileguard.IsRetired(path), nil
	}
	return false, nil
}

func (w *Watcher) isKnownAuthFile(path string) bool {
	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	_, ok := w.lastAuthHashes[normalized]
	return ok
}

func (w *Watcher) normalizeAuthPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}
	cleaned := filepath.Clean(trimmed)
	if runtime.GOOS == "windows" {
		cleaned = strings.TrimPrefix(cleaned, `\\?\`)
		cleaned = strings.ToLower(cleaned)
	}
	return cleaned
}

func (w *Watcher) shouldDebounceRemove(normalizedPath string, now time.Time) bool {
	if normalizedPath == "" {
		return false
	}
	w.clientsMutex.Lock()
	if w.lastRemoveTimes == nil {
		w.lastRemoveTimes = make(map[string]time.Time)
	}
	if last, ok := w.lastRemoveTimes[normalizedPath]; ok {
		if now.Sub(last) < authRemoveDebounceWindow {
			w.clientsMutex.Unlock()
			return true
		}
	}
	w.lastRemoveTimes[normalizedPath] = now
	if len(w.lastRemoveTimes) > 128 {
		cutoff := now.Add(-2 * authRemoveDebounceWindow)
		for p, t := range w.lastRemoveTimes {
			if t.Before(cutoff) {
				delete(w.lastRemoveTimes, p)
			}
		}
	}
	w.clientsMutex.Unlock()
	return false
}

func (w *Watcher) clearRemoveDebounce(normalizedPath string) {
	if normalizedPath == "" {
		return
	}
	w.clientsMutex.Lock()
	delete(w.lastRemoveTimes, normalizedPath)
	w.clientsMutex.Unlock()
}
