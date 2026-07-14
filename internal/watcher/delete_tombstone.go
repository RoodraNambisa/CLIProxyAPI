package watcher

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const authDeleteTombstoneDirectory = ".cliproxy-delete-quarantine"

type authDeleteTombstone struct {
	AuthRoot     string                                         `json:"auth_root"`
	Path         string                                         `json:"path"`
	Retired      bool                                           `json:"retired,omitempty"`
	Generation   string                                         `json:"generation,omitempty"`
	ExpectedHash string                                         `json:"expected_hash,omitempty"`
	Identities   map[string]authfileguard.DeleteBackendIdentity `json:"identities,omitempty"`
	CreatedAt    time.Time                                      `json:"created_at"`
}

func (w *Watcher) persistAuthDeleteTombstone(path, expectedHash string) error {
	return w.persistAuthDeleteGenerationTombstone(path, authfileguard.NewDeleteGeneration(expectedHash))
}

func (w *Watcher) persistAuthDeleteGenerationTombstone(path string, generation *authfileguard.DeleteGeneration) error {
	if generation == nil {
		generation = authfileguard.NewDeleteGeneration("")
	}
	return w.persistAuthDeleteTombstoneSnapshot(path, generation.Snapshot())
}

func (w *Watcher) persistAuthDeleteTombstoneSnapshot(path string, snapshot authfileguard.DeleteGenerationSnapshot) (err error) {
	metadataRoot, authRoot, authDir, errContext := w.authDeleteTombstoneContext()
	if errContext != nil {
		return errContext
	}
	relativePath, ok := authDeleteRelativePath(authDir, path)
	if !ok {
		return fmt.Errorf("auth delete tombstone path is outside auth directory")
	}
	tombstone := authDeleteTombstone{
		AuthRoot:     authRoot,
		Path:         filepath.ToSlash(relativePath),
		Retired:      authfileguard.IsRetired(path),
		Generation:   strings.TrimSpace(snapshot.Generation),
		ExpectedHash: normalizeAuthDeleteExpectedHash(snapshot.ExpectedHash),
		Identities:   snapshot.Identities,
		CreatedAt:    time.Now().UTC(),
	}
	data, errMarshal := json.Marshal(tombstone)
	if errMarshal != nil {
		return fmt.Errorf("marshal auth delete tombstone: %w", errMarshal)
	}
	root, errRoot := os.OpenRoot(metadataRoot)
	if errRoot != nil {
		return fmt.Errorf("open auth delete tombstone root: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close auth delete tombstone root: %w", errClose))
		}
	}()
	if errMkdir := root.MkdirAll(authDeleteTombstoneDirectory, 0o700); errMkdir != nil {
		return fmt.Errorf("create auth delete tombstone directory: %w", errMkdir)
	}
	if errSync := syncRootDirectory(root, "."); errSync != nil {
		return fmt.Errorf("sync auth delete tombstone root: %w", errSync)
	}
	name := filepath.Join(authDeleteTombstoneDirectory, authDeleteTombstoneName(authRoot, relativePath))
	unlock, errLock := authfileguard.LockRootTarget(root, name)
	if errLock != nil {
		return fmt.Errorf("lock auth delete tombstone: %w", errLock)
	}
	defer func() { err = errors.Join(err, unlock()) }()
	if errWrite := writeRootFileAtomically(root, name, data, 0o600); errWrite != nil {
		return fmt.Errorf("write auth delete tombstone: %w", errWrite)
	}
	authfileguard.MarkQuarantined(path)
	return nil
}

// PersistAuthDeleteQuarantine durably records a pending auth deletion before
// either the local mirror or remote backing record is removed.
func PersistAuthDeleteQuarantine(configPath, authDir, path string, generation *authfileguard.DeleteGeneration) error {
	if generation == nil {
		generation = authfileguard.NewDeleteGeneration("")
	}
	w := &Watcher{configPath: configPath, authDir: authDir}
	if errPersist := w.persistAuthDeleteGenerationTombstone(path, generation); errPersist != nil {
		return errPersist
	}
	generation.SetPersistHook(func(snapshot authfileguard.DeleteGenerationSnapshot) error {
		return w.persistAuthDeleteTombstoneSnapshot(path, snapshot)
	})
	return nil
}

// ClearAuthDeleteQuarantine removes a completed auth deletion tombstone.
func ClearAuthDeleteQuarantine(configPath, authDir, path string, generation *authfileguard.DeleteGeneration) error {
	w := &Watcher{configPath: configPath, authDir: authDir}
	return w.clearAuthDeleteTombstone(path, generation)
}

// LoadAuthDeleteQuarantine marks durable auth deletion tombstones before the
// runtime manager admits credentials from its backing store.
func LoadAuthDeleteQuarantine(configPath, authDir string) error {
	w := &Watcher{
		configPath:          configPath,
		authDir:             authDir,
		retiredAuthPaths:    make(map[string]struct{}),
		retiredDeleteHashes: make(map[string]string),
		retiredDeleteStates: make(map[string]*authfileguard.DeleteGeneration),
	}
	return w.loadAuthDeleteTombstones()
}

func (w *Watcher) clearAuthDeleteTombstone(path string, generation *authfileguard.DeleteGeneration) (err error) {
	if generation == nil {
		return authfileguard.ErrDeleteGenerationUncertain
	}
	metadataRoot, authRoot, authDir, errContext := w.authDeleteTombstoneContext()
	if errContext != nil {
		return errContext
	}
	relativePath, ok := authDeleteRelativePath(authDir, path)
	if !ok {
		return fmt.Errorf("auth delete tombstone path is outside auth directory")
	}
	root, errRoot := os.OpenRoot(metadataRoot)
	if errRoot != nil {
		return fmt.Errorf("open auth delete tombstone root: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close auth delete tombstone root: %w", errClose))
		}
	}()
	if errMkdir := root.MkdirAll(authDeleteTombstoneDirectory, 0o700); errMkdir != nil {
		return fmt.Errorf("create auth delete tombstone directory: %w", errMkdir)
	}
	name := filepath.Join(authDeleteTombstoneDirectory, authDeleteTombstoneName(authRoot, relativePath))
	unlock, errLock := authfileguard.LockRootTarget(root, name)
	if errLock != nil {
		return fmt.Errorf("lock auth delete tombstone: %w", errLock)
	}
	defer func() { err = errors.Join(err, unlock()) }()
	data, errRead := root.ReadFile(name)
	if errors.Is(errRead, os.ErrNotExist) {
		if errConflict := rejectConflictingAuthDeleteTombstone(root, authRoot, relativePath); errConflict != nil {
			return errConflict
		}
		authfileguard.ClearQuarantined(path)
		return nil
	}
	if errRead != nil {
		return fmt.Errorf("read auth delete tombstone before removal: %w", errRead)
	}
	var tombstone authDeleteTombstone
	if errJSON := json.Unmarshal(data, &tombstone); errJSON != nil {
		return fmt.Errorf("parse auth delete tombstone before removal: %w", errJSON)
	}
	if !authDeleteTombstoneMatchesGeneration(tombstone, generation.Snapshot()) {
		return authfileguard.ErrDeleteGenerationUncertain
	}
	errRemove := root.Remove(name)
	if errors.Is(errRemove, os.ErrNotExist) {
		if errConflict := rejectConflictingAuthDeleteTombstone(root, authRoot, relativePath); errConflict != nil {
			return errConflict
		}
	} else if errRemove != nil {
		return fmt.Errorf("remove auth delete tombstone: %w", errRemove)
	}
	if errRemove == nil {
		if errSync := syncRootDirectory(root, authDeleteTombstoneDirectory); errSync != nil {
			return fmt.Errorf("sync removed auth delete tombstone: %w", errSync)
		}
	}
	authfileguard.ClearQuarantined(path)
	return nil
}

func authDeleteTombstoneMatchesGeneration(tombstone authDeleteTombstone, snapshot authfileguard.DeleteGenerationSnapshot) bool {
	if strings.TrimSpace(tombstone.Generation) != strings.TrimSpace(snapshot.Generation) {
		return false
	}
	if normalizeAuthDeleteExpectedHash(tombstone.ExpectedHash) != normalizeAuthDeleteExpectedHash(snapshot.ExpectedHash) {
		return false
	}
	return deleteBackendIdentitiesEqual(tombstone.Identities, snapshot.Identities)
}

func deleteBackendIdentitiesEqual(left, right map[string]authfileguard.DeleteBackendIdentity) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftIdentity := range left {
		rightIdentity, ok := right[key]
		if !ok || strings.TrimSpace(leftIdentity.Value) != strings.TrimSpace(rightIdentity.Value) || leftIdentity.RetrySafe != rightIdentity.RetrySafe {
			return false
		}
	}
	return true
}

func rejectConflictingAuthDeleteTombstone(root *os.Root, authRoot, relativePath string) error {
	directory, errOpen := root.Open(authDeleteTombstoneDirectory)
	if errors.Is(errOpen, os.ErrNotExist) {
		return nil
	}
	if errOpen != nil {
		return fmt.Errorf("inspect auth delete tombstone directory: %w", errOpen)
	}
	entries, errRead := directory.ReadDir(-1)
	errClose := directory.Close()
	if errRead != nil || errClose != nil {
		return errors.Join(errRead, errClose)
	}
	wantPath := filepath.Clean(relativePath)
	for _, entry := range entries {
		if entry == nil || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tombstone") {
			continue
		}
		data, errFile := root.ReadFile(filepath.Join(authDeleteTombstoneDirectory, entry.Name()))
		if errFile != nil {
			return fmt.Errorf("inspect auth delete tombstone %s: %w", entry.Name(), errFile)
		}
		var tombstone authDeleteTombstone
		if errJSON := json.Unmarshal(data, &tombstone); errJSON != nil {
			return fmt.Errorf("inspect auth delete tombstone %s: %w", entry.Name(), errJSON)
		}
		if filepath.Clean(filepath.FromSlash(strings.TrimSpace(tombstone.Path))) != wantPath {
			continue
		}
		storedRoot := strings.TrimSpace(tombstone.AuthRoot)
		if storedRoot != authRoot {
			return fmt.Errorf("auth delete tombstone %s belongs to auth root %q, current root is %q", entry.Name(), storedRoot, authRoot)
		}
		return fmt.Errorf("auth delete tombstone %s could not be removed by its expected name", entry.Name())
	}
	return nil
}

func (w *Watcher) loadAuthDeleteTombstones() (err error) {
	metadataRoot, authRoot, authDir, errContext := w.authDeleteTombstoneContext()
	if errContext != nil {
		return fmt.Errorf("resolve auth delete tombstone directory: %w", errContext)
	}
	root, errRoot := os.OpenRoot(metadataRoot)
	if errRoot != nil {
		return fmt.Errorf("open auth delete tombstone root: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close auth delete tombstone root: %w", errClose))
		}
	}()
	directoryBefore, errDirectoryBefore := root.Lstat(authDeleteTombstoneDirectory)
	if errors.Is(errDirectoryBefore, os.ErrNotExist) {
		return nil
	}
	if errDirectoryBefore != nil {
		return fmt.Errorf("inspect auth delete tombstone directory: %w", errDirectoryBefore)
	}
	if directoryBefore.Mode()&os.ModeSymlink != 0 || !directoryBefore.IsDir() {
		return errors.New("auth delete tombstone path is not a stable directory")
	}
	directoryRoot, errOpenRoot := root.OpenRoot(authDeleteTombstoneDirectory)
	if errOpenRoot != nil {
		return fmt.Errorf("open auth delete tombstone directory: %w", errOpenRoot)
	}
	defer func() {
		if errClose := directoryRoot.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close auth delete tombstone root: %w", errClose))
		}
	}()
	directoryOpened, errDirectoryOpened := directoryRoot.Stat(".")
	directoryAfter, errDirectoryAfter := root.Lstat(authDeleteTombstoneDirectory)
	if errDirectoryOpened != nil || errDirectoryAfter != nil || directoryAfter.Mode()&os.ModeSymlink != 0 || !directoryAfter.IsDir() || !os.SameFile(directoryBefore, directoryOpened) || !os.SameFile(directoryAfter, directoryOpened) {
		return errors.Join(errDirectoryOpened, errDirectoryAfter, errors.New("auth delete tombstone directory changed while opening"))
	}
	directory, errOpen := directoryRoot.Open(".")
	if errOpen != nil {
		return fmt.Errorf("open auth delete tombstone directory for listing: %w", errOpen)
	}
	entries, errRead := directory.ReadDir(-1)
	errCloseDirectory := directory.Close()
	if errRead != nil || errCloseDirectory != nil {
		if errRead != nil {
			errRead = fmt.Errorf("read auth delete tombstone directory: %w", errRead)
		}
		if errCloseDirectory != nil {
			errCloseDirectory = fmt.Errorf("close auth delete tombstone directory: %w", errCloseDirectory)
		}
		return errors.Join(errRead, errCloseDirectory)
	}
	for _, entry := range entries {
		if entry == nil || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tombstone") {
			continue
		}
		info, errInfo := directoryRoot.Lstat(entry.Name())
		if errors.Is(errInfo, os.ErrNotExist) {
			continue
		}
		if errInfo != nil {
			return fmt.Errorf("inspect auth delete tombstone %s: %w", entry.Name(), errInfo)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			log.Warnf("ignoring non-regular auth delete tombstone %s", entry.Name())
			continue
		}
		data, errFile := directoryRoot.ReadFile(entry.Name())
		if errors.Is(errFile, os.ErrNotExist) {
			continue
		}
		if errFile != nil {
			return fmt.Errorf("read auth delete tombstone %s: %w", entry.Name(), errFile)
		}
		var tombstone authDeleteTombstone
		if errJSON := json.Unmarshal(data, &tombstone); errJSON != nil {
			log.WithError(errJSON).Warnf("ignoring malformed auth delete tombstone %s", entry.Name())
			continue
		}
		if strings.TrimSpace(tombstone.AuthRoot) != authRoot {
			log.Warnf("ignoring stale auth delete tombstone %s for a different auth root", entry.Name())
			continue
		}
		relativePath, ok := validAuthDeleteRelativePath(filepath.FromSlash(strings.TrimSpace(tombstone.Path)))
		if !ok {
			log.Warnf("ignoring auth delete tombstone %s with an unsafe path", entry.Name())
			continue
		}
		normalized := w.normalizeAuthPath(filepath.Join(authDir, relativePath))
		path := filepath.Join(authDir, relativePath)
		snapshot := authfileguard.DeleteGenerationSnapshot{
			Generation:   strings.TrimSpace(tombstone.Generation),
			ExpectedHash: normalizeAuthDeleteExpectedHash(tombstone.ExpectedHash),
			Identities:   tombstone.Identities,
		}
		generation := authfileguard.NewDeleteGenerationFromSnapshot(snapshot)
		authfileguard.MarkQuarantined(path)
		if tombstone.Retired {
			authfileguard.MarkRetired(path)
		}
		w.clientsMutex.Lock()
		if w.retiredAuthPaths == nil {
			w.retiredAuthPaths = make(map[string]struct{})
		}
		if w.retiredDeleteHashes == nil {
			w.retiredDeleteHashes = make(map[string]string)
		}
		if w.retiredDeleteStates == nil {
			w.retiredDeleteStates = make(map[string]*authfileguard.DeleteGeneration)
		}
		w.retiredAuthPaths[normalized] = struct{}{}
		w.retiredDeleteHashes[normalized] = snapshot.ExpectedHash
		w.retiredDeleteStates[normalized] = generation
		w.clientsMutex.Unlock()
	}
	return nil
}

func (w *Watcher) resumeAuthDeleteTombstones() {
	if w == nil {
		return
	}
	type pendingDelete struct {
		path       string
		normalized string
		generation *authfileguard.DeleteGeneration
		sequence   uint64
	}
	w.clientsMutex.Lock()
	pending := make([]pendingDelete, 0, len(w.retiredDeleteStates))
	for normalized, generation := range w.retiredDeleteStates {
		if generation == nil {
			continue
		}
		sequence := w.retiredDeletes[normalized]
		if sequence == 0 {
			sequence = w.nextAuthPersistenceGenerationLocked(normalized)
		}
		pending = append(pending, pendingDelete{
			path:       normalized,
			normalized: normalized,
			generation: generation,
			sequence:   sequence,
		})
	}
	w.clientsMutex.Unlock()

	for _, item := range pending {
		func() {
			unlockPath := authfileguard.Lock(item.path)
			defer unlockPath()

			unlockTarget, errLock := lockAuthDeleteTarget(w, item.path)
			if errLock != nil {
				log.WithError(errLock).Warnf("auth deletion remains quarantined for %s", item.normalized)
				return
			}
			defer func() {
				if errUnlock := unlockTarget(); errUnlock != nil {
					log.WithError(errUnlock).Warnf("failed to unlock auth deletion target for %s", item.normalized)
				}
			}()

			data, _, errRead := readAuthFileVersionUnderRoot(w.authRootDir(), item.path)
			switch {
			case errRead == nil:
				expectedHash := item.generation.ExpectedHash()
				if expectedHash == "" || !coreauth.SourceHashMatchesBytes(expectedHash, data) {
					if w.storePersister == nil {
						if errClear := w.completeLocalAuthDeleteTombstone(item.path, item.normalized, item.sequence); errClear != nil {
							log.WithError(errClear).Warnf("local auth replacement remains quarantined for %s", item.normalized)
						} else {
							w.addOrUpdateClientLocked(item.path)
						}
					} else {
						// Keep the durable tombstone and quarantine until the replacement has
						// been persisted. The admission path clears both only after confirmation.
						authfileguard.ClearRetired(item.path)
						authfileguard.MarkQuarantined(item.path)
						w.addOrUpdateClientLocked(item.path)
					}
					return
				}
				if errRemove := removeAuthDeletePath(w.authRootDir(), item.path); errRemove != nil {
					log.WithError(errRemove).Warnf("auth deletion remains quarantined for %s", item.normalized)
					return
				}
			case errors.Is(errRead, os.ErrNotExist):
			case errRead != nil:
				log.WithError(errRead).Warnf("auth deletion remains quarantined for %s", item.normalized)
				return
			}
			if w.storePersister == nil {
				if errClear := w.completeLocalAuthDeleteTombstone(item.path, item.normalized, item.sequence); errClear != nil {
					log.WithError(errClear).Warnf("local auth deletion remains quarantined for %s", item.normalized)
				}
			} else {
				item.generation.SetPersistHook(func(snapshot authfileguard.DeleteGenerationSnapshot) error {
					return w.persistAuthDeleteTombstoneSnapshot(item.path, snapshot)
				})
				message := fmt.Sprintf("Resume removing auth %s", filepath.Base(item.path))
				if authfileguard.IsRetired(item.path) {
					w.startRetiredAuthRemovalPersistenceAttemptLocked(item.path, item.normalized, message, item.sequence, item.generation, authfileguard.CaptureRetired(item.path), 0, 0)
				} else {
					w.startAuthRemovalPersistenceAttemptLocked(item.path, item.normalized, message, item.sequence, item.generation, 0, 0, false)
				}
			}
		}()
	}
}

func lockAuthDeleteTarget(w *Watcher, path string) (func() error, error) {
	if w == nil {
		return nil, errors.New("auth delete watcher is nil")
	}
	_, _, authDir, errContext := w.authDeleteTombstoneContext()
	if errContext != nil {
		return nil, errContext
	}
	relativePath, ok := authDeleteRelativePath(authDir, path)
	if !ok {
		return nil, errors.New("auth delete target is outside auth directory")
	}
	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		return nil, errRoot
	}
	parentPath := filepath.Dir(relativePath)
	if parentPath != "." {
		if errMkdir := root.MkdirAll(parentPath, 0o700); errMkdir != nil {
			return nil, errors.Join(errMkdir, root.Close())
		}
	}
	parentRoot, leaf, closeParent, errParent := openAuthDeleteTargetParent(root, relativePath)
	if errParent != nil {
		return nil, errors.Join(errParent, root.Close())
	}
	unlock, errLock := authfileguard.LockRootTarget(parentRoot, leaf)
	if errLock != nil {
		return nil, errors.Join(errLock, closeParent(), root.Close())
	}
	return func() error {
		return errors.Join(unlock(), closeParent(), root.Close())
	}, nil
}

func openAuthDeleteTargetParent(root *os.Root, relativePath string) (*os.Root, string, func() error, error) {
	parts := strings.Split(filepath.Clean(relativePath), string(filepath.Separator))
	current := root
	owned := false
	closeCurrent := func() error {
		if !owned {
			return nil
		}
		return current.Close()
	}
	for _, component := range parts[:len(parts)-1] {
		before, errBefore := current.Lstat(component)
		if errBefore != nil {
			return nil, "", nil, errors.Join(errBefore, closeCurrent())
		}
		if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
			return nil, "", nil, errors.Join(errors.New("auth delete target parent is not a stable directory"), closeCurrent())
		}
		next, errOpen := current.OpenRoot(component)
		if errOpen != nil {
			return nil, "", nil, errors.Join(errOpen, closeCurrent())
		}
		opened, errOpened := next.Stat(".")
		after, errAfter := current.Lstat(component)
		if errOpened != nil || errAfter != nil || after.Mode()&os.ModeSymlink != 0 || !after.IsDir() || !os.SameFile(before, opened) || !os.SameFile(after, opened) {
			return nil, "", nil, errors.Join(errOpened, errAfter, next.Close(), closeCurrent(), errors.New("auth delete target parent changed while opening"))
		}
		if errClose := closeCurrent(); errClose != nil {
			return nil, "", nil, errors.Join(errClose, next.Close())
		}
		current = next
		owned = true
	}
	return current, parts[len(parts)-1], closeCurrent, nil
}

func (w *Watcher) completeLocalAuthDeleteTombstone(path, normalized string, sequence uint64) error {
	w.clientsMutex.Lock()
	if w.retiredDeletes[normalized] != sequence {
		w.clientsMutex.Unlock()
		return nil
	}
	deleteGeneration := w.retiredDeleteStates[normalized]
	if errClear := w.clearAuthDeleteTombstone(path, deleteGeneration); errClear != nil {
		w.clientsMutex.Unlock()
		return errClear
	}
	delete(w.retiredDeletes, normalized)
	delete(w.retiredDeleteHashes, normalized)
	delete(w.retiredDeleteStates, normalized)
	delete(w.retiredAuthPaths, normalized)
	delete(w.lastAuthHashes, normalized)
	w.clientsMutex.Unlock()
	authfileguard.ClearRetired(path)
	return nil
}

func removeAuthDeletePath(authDir, path string) error {
	authDir = filepath.Clean(strings.TrimSpace(authDir))
	path = filepath.Clean(strings.TrimSpace(path))
	relativePath, ok := authDeleteRelativePath(authDir, path)
	if !ok {
		return fmt.Errorf("auth delete path is outside auth directory")
	}
	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		return errRoot
	}
	defer func() { _ = root.Close() }()
	if errRemove := root.Remove(relativePath); errRemove != nil {
		if !errors.Is(errRemove, os.ErrNotExist) {
			return errRemove
		}
	} else if errSync := syncRootDirectory(root, filepath.Dir(relativePath)); errSync != nil {
		return errSync
	}
	return nil
}

func (w *Watcher) authDeleteTombstoneContext() (metadataRoot, authRoot, authDir string, err error) {
	authDir = strings.TrimSpace(w.authRootDir())
	if authDir == "" {
		return "", "", "", fmt.Errorf("auth delete tombstone auth directory is empty")
	}
	authDir, err = filepath.Abs(authDir)
	if err != nil {
		return "", "", "", fmt.Errorf("resolve auth delete tombstone auth directory: %w", err)
	}
	authDir = filepath.Clean(authDir)
	physicalAuthDir, errResolveAuth := filepath.EvalSymlinks(authDir)
	if errResolveAuth != nil {
		return "", "", "", fmt.Errorf("resolve physical auth delete tombstone directory: %w", errResolveAuth)
	}
	authRoot = filepath.ToSlash(w.normalizeAuthPath(physicalAuthDir))

	configPath := strings.TrimSpace(w.configPath)
	if configPath == "" {
		metadataRoot, err = filepath.EvalSymlinks(filepath.Dir(authDir))
		if err != nil {
			return "", "", "", fmt.Errorf("resolve auth delete tombstone metadata directory: %w", err)
		}
	} else {
		configPath, err = filepath.Abs(configPath)
		if err != nil {
			return "", "", "", fmt.Errorf("resolve auth delete tombstone config path: %w", err)
		}
		configPath = filepath.Clean(configPath)
		physicalConfigPath, errResolveConfig := filepath.EvalSymlinks(configPath)
		if errResolveConfig == nil {
			metadataRoot = filepath.Dir(physicalConfigPath)
		} else if errors.Is(errResolveConfig, os.ErrNotExist) {
			metadataRoot, err = filepath.EvalSymlinks(filepath.Dir(configPath))
			if err != nil {
				return "", "", "", fmt.Errorf("resolve auth delete tombstone metadata directory: %w", err)
			}
		} else {
			return "", "", "", fmt.Errorf("resolve physical auth delete tombstone config path: %w", errResolveConfig)
		}
	}
	metadataRoot = filepath.Clean(metadataRoot)
	return metadataRoot, authRoot, authDir, nil
}

func authDeleteRelativePath(authDir, path string) (string, bool) {
	relativePath, errRel := filepath.Rel(filepath.Clean(authDir), filepath.Clean(path))
	if errRel != nil {
		return "", false
	}
	return validAuthDeleteRelativePath(relativePath)
}

func validAuthDeleteRelativePath(relativePath string) (string, bool) {
	portablePath := strings.ReplaceAll(strings.TrimSpace(relativePath), `\`, "/")
	if portablePath == "" || strings.HasPrefix(portablePath, "/") || hasWindowsAuthDeleteVolumeShape(portablePath) {
		return "", false
	}
	portablePath = pathpkg.Clean(portablePath)
	if portablePath == "." || portablePath == ".." || strings.HasPrefix(portablePath, "../") || !strings.HasSuffix(strings.ToLower(portablePath), ".json") {
		return "", false
	}
	relativePath = filepath.Clean(filepath.FromSlash(portablePath))
	if filepath.IsAbs(relativePath) || filepath.VolumeName(relativePath) != "" {
		return "", false
	}
	return relativePath, true
}

func hasWindowsAuthDeleteVolumeShape(path string) bool {
	if len(path) < 2 || path[1] != ':' {
		return false
	}
	first := path[0]
	return first >= 'a' && first <= 'z' || first >= 'A' && first <= 'Z'
}

func authDeleteTombstoneName(authRoot, relativePath string) string {
	key := authRoot + "\x00" + filepath.ToSlash(filepath.Clean(relativePath))
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:]) + ".tombstone"
}

func normalizeAuthDeleteExpectedHash(expectedHash string) string {
	expectedHash = strings.ToLower(strings.TrimSpace(expectedHash))
	if len(expectedHash) != sha256.Size*2 {
		return ""
	}
	if _, errDecode := hex.DecodeString(expectedHash); errDecode != nil {
		return ""
	}
	return expectedHash
}

func writeRootFileAtomically(root *os.Root, path string, data []byte, mode os.FileMode) error {
	directory := filepath.Dir(path)
	for range 10 {
		random := make([]byte, 16)
		if _, errRandom := rand.Read(random); errRandom != nil {
			return errRandom
		}
		temporaryPath := filepath.Join(directory, ".tombstone-"+hex.EncodeToString(random))
		temporary, errCreate := root.OpenFile(temporaryPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if errors.Is(errCreate, os.ErrExist) {
			continue
		}
		if errCreate != nil {
			return errCreate
		}
		removeTemporary := true
		defer func() {
			if removeTemporary {
				_ = root.Remove(temporaryPath)
			}
		}()
		if _, errWrite := temporary.Write(data); errWrite != nil {
			_ = temporary.Close()
			return errWrite
		}
		if errSync := temporary.Sync(); errSync != nil {
			_ = temporary.Close()
			return errSync
		}
		if errClose := temporary.Close(); errClose != nil {
			return errClose
		}
		if errRename := root.Rename(temporaryPath, path); errRename != nil {
			return errRename
		}
		if errSync := syncRootDirectory(root, directory); errSync != nil {
			return errSync
		}
		removeTemporary = false
		return nil
	}
	return fmt.Errorf("create temporary auth delete tombstone: too many collisions")
}
