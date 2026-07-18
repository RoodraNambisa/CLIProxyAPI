// clients.go implements watcher client lifecycle logic and persistence helpers.
// It reloads clients, handles incremental auth file changes, and persists updates when supported.
package watcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/diff"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

var errSymlinkAuthFile = errors.New("symlink auth file")

type authFileVersion struct {
	hash string
	info fs.FileInfo
}

func readAuthFileUnderRoot(authDir, path string) ([]byte, error) {
	data, _, err := readAuthFileVersionUnderRoot(authDir, path)
	return data, err
}

func readAuthFileVersionUnderRoot(authDir, path string) ([]byte, authFileVersion, error) {
	authDir = strings.TrimSpace(authDir)
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, authFileVersion{}, os.ErrInvalid
	}
	if authDir == "" {
		authDir = filepath.Dir(path)
	}
	authDir, errAbs := filepath.Abs(authDir)
	if errAbs != nil {
		return nil, authFileVersion{}, errAbs
	}
	lexicalAuthDir := filepath.Clean(authDir)
	authDir = lexicalAuthDir
	if resolved, errEval := filepath.EvalSymlinks(authDir); errEval == nil {
		authDir = filepath.Clean(resolved)
	} else if !os.IsNotExist(errEval) {
		return nil, authFileVersion{}, errEval
	}
	path, errAbs = filepath.Abs(path)
	if errAbs != nil {
		return nil, authFileVersion{}, errAbs
	}
	path = filepath.Clean(path)
	relativePath, errRel := filepath.Rel(lexicalAuthDir, path)
	if errRel != nil || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		relativePath, errRel = filepath.Rel(authDir, path)
	}
	if errRel != nil || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		return nil, authFileVersion{}, os.ErrPermission
	}
	root, errOpen := os.OpenRoot(authDir)
	if errOpen != nil {
		return nil, authFileVersion{}, errOpen
	}
	defer func() { _ = root.Close() }()
	before, errBefore := root.Lstat(relativePath)
	if errBefore != nil {
		return nil, authFileVersion{}, errBefore
	}
	if before.Mode()&os.ModeSymlink != 0 {
		return nil, authFileVersion{}, errSymlinkAuthFile
	}
	if !before.Mode().IsRegular() {
		return nil, authFileVersion{}, fmt.Errorf("auth file is not regular")
	}
	file, errFile := root.Open(relativePath)
	if errFile != nil {
		return nil, authFileVersion{}, errFile
	}
	defer func() { _ = file.Close() }()
	opened, errOpened := file.Stat()
	if errOpened != nil {
		return nil, authFileVersion{}, errOpened
	}
	after, errAfter := root.Lstat(relativePath)
	if errAfter != nil {
		return nil, authFileVersion{}, errAfter
	}
	if after.Mode()&os.ModeSymlink != 0 || !after.Mode().IsRegular() || !os.SameFile(before, opened) || !os.SameFile(after, opened) {
		return nil, authFileVersion{}, fmt.Errorf("auth file changed while opening")
	}
	data, errRead := io.ReadAll(file)
	if errRead != nil {
		return nil, authFileVersion{}, errRead
	}
	opened, errOpened = authfileguard.HardenChatGPTWebCredentialFile(file, opened, data)
	if errOpened != nil {
		return nil, authFileVersion{}, errOpened
	}
	sum := sha256.Sum256(data)
	return data, authFileVersion{hash: hex.EncodeToString(sum[:]), info: opened}, nil
}

func sameAuthFileVersion(expected, current authFileVersion) bool {
	if expected.hash == "" || expected.hash != current.hash {
		return false
	}
	if expected.info == nil {
		return true
	}
	return current.info != nil &&
		os.SameFile(expected.info, current.info) &&
		expected.info.Size() == current.info.Size() &&
		expected.info.ModTime().Equal(current.info.ModTime())
}

func (w *Watcher) authRootDir() string {
	if w == nil {
		return ""
	}
	if authDir := strings.TrimSpace(w.authDir); authDir != "" {
		return authDir
	}
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if w.config == nil {
		return ""
	}
	return strings.TrimSpace(w.config.AuthDir)
}

func (w *Watcher) reloadClients(rescanAuth bool, affectedOAuthProviders []string, forceAuthRefresh bool) {
	w.reloadClientsWithOptions(rescanAuth, affectedOAuthProviders, forceAuthRefresh, false)
}

func (w *Watcher) reloadClientsWithOptions(rescanAuth bool, affectedOAuthProviders []string, forceAuthRefresh, resolveTombstoneReplacements bool) {
	log.Debugf("starting full client load process")

	w.clientsMutex.RLock()
	cfg := w.config
	w.clientsMutex.RUnlock()

	if cfg == nil {
		log.Error("config is nil, cannot reload clients")
		return
	}

	if len(affectedOAuthProviders) > 0 {
		w.clientsMutex.Lock()
		if w.currentAuths != nil {
			filtered := make(map[string]*coreauth.Auth, len(w.currentAuths))
			for id, auth := range w.currentAuths {
				if auth == nil {
					continue
				}
				provider := strings.ToLower(strings.TrimSpace(auth.Provider))
				if _, match := matchProvider(provider, affectedOAuthProviders); match {
					continue
				}
				filtered[id] = auth
			}
			w.currentAuths = filtered
			log.Debugf("applying oauth-excluded-models to providers %v", affectedOAuthProviders)
		} else {
			w.currentAuths = nil
		}
		w.clientsMutex.Unlock()
	}

	geminiAPIKeyCount, vertexCompatAPIKeyCount, claudeAPIKeyCount, codexAPIKeyCount, openAICompatCount := BuildAPIKeyClients(cfg)
	totalAPIKeyClients := geminiAPIKeyCount + vertexCompatAPIKeyCount + claudeAPIKeyCount + codexAPIKeyCount + openAICompatCount
	log.Debugf("loaded %d API key clients", totalAPIKeyClients)

	var authFileCount int
	if rescanAuth {
		authFileCount = w.loadFileClients(cfg)
		log.Debugf("loaded %d file-based clients", authFileCount)
	} else {
		w.clientsMutex.RLock()
		authFileCount = len(w.lastAuthHashes)
		w.clientsMutex.RUnlock()
		log.Debugf("skipping auth directory rescan; retaining %d existing auth files", authFileCount)
	}

	if rescanAuth {
		cacheAuthContents := log.IsLevelEnabled(log.DebugLevel)
		w.clientsMutex.Lock()
		w.lastAuthHashes = make(map[string]string)
		if cacheAuthContents {
			w.lastAuthContents = make(map[string]*coreauth.Auth)
		} else {
			w.lastAuthContents = nil
		}
		w.fileAuthsByPath = make(map[string]map[string]*coreauth.Auth)
		if w.retiredAuthPaths == nil {
			w.retiredAuthPaths = make(map[string]struct{})
		}
		w.clientsMutex.Unlock()

		if resolvedAuthDir, errResolveAuthDir := util.ResolveAuthDir(cfg.AuthDir); errResolveAuthDir != nil {
			log.Errorf("failed to resolve auth directory for hash cache: %v", errResolveAuthDir)
		} else if resolvedAuthDir != "" {
			entries, errReadDir := os.ReadDir(resolvedAuthDir)
			if errReadDir != nil {
				log.Errorf("failed to read auth directory for hash cache: %v", errReadDir)
			} else {
				for _, entry := range entries {
					if entry == nil || entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
						continue
					}
					name := entry.Name()
					if !strings.HasSuffix(strings.ToLower(name), ".json") {
						continue
					}
					fullPath := filepath.Join(resolvedAuthDir, name)
					w.cacheAuthFileForReload(cfg, resolvedAuthDir, fullPath, cacheAuthContents, resolveTombstoneReplacements)
				}
			}
		}
	}

	totalNewClients := authFileCount + geminiAPIKeyCount + vertexCompatAPIKeyCount + claudeAPIKeyCount + codexAPIKeyCount + openAICompatCount

	if w.reloadCallback != nil {
		log.Debugf("triggering server update callback before auth refresh")
		w.reloadCallback(cfg)
	}

	w.refreshAuthState(forceAuthRefresh)

	log.Infof("full client load complete - %d clients (%d auth files + %d Gemini API keys + %d Vertex API keys + %d Claude API keys + %d Codex keys + %d OpenAI-compat)",
		totalNewClients,
		authFileCount,
		geminiAPIKeyCount,
		vertexCompatAPIKeyCount,
		claudeAPIKeyCount,
		codexAPIKeyCount,
		openAICompatCount,
	)
}

func (w *Watcher) cacheAuthFileForReload(cfg *config.Config, authDir, path string, cacheAuthContents, resolveTombstoneReplacement bool) {
	// Event handlers already hold the path lock when taking clientsMutex. Keep
	// this order consistent so a reload cannot deadlock with a file event.
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()

	data, errReadFile := readAuthFileUnderRoot(authDir, path)
	if errReadFile != nil || len(data) == 0 {
		return
	}

	retiredFile := coreauth.IsRetiredGeminiCLIAuthFileData(data)
	if retiredFile {
		authfileguard.MarkRetired(path)
	}
	sum := sha256.Sum256(data)
	normalizedPath := w.normalizeAuthPath(path)
	currentHash := hex.EncodeToString(sum[:])
	confirmReplacement := resolveTombstoneReplacement && w.shouldConfirmAuthDeleteTombstoneReplacement(normalizedPath, data)

	var cachedAuth *coreauth.Auth
	if cacheAuthContents {
		var auth coreauth.Auth
		if errParse := json.Unmarshal(data, &auth); errParse == nil {
			cachedAuth = &auth
		}
	}

	var pathAuths map[string]*coreauth.Auth
	if !retiredFile {
		ctx := &synthesizer.SynthesisContext{
			Config:      cfg,
			AuthDir:     authDir,
			Now:         time.Now(),
			IDGenerator: synthesizer.NewStableIDGenerator(),
		}
		pathAuths = authSliceToMap(synthesizer.SynthesizeAuthFile(ctx, path, data))
	}

	w.clientsMutex.Lock()
	if w.lastAuthHashes == nil {
		w.lastAuthHashes = make(map[string]string)
	}
	if w.fileAuthsByPath == nil {
		w.fileAuthsByPath = make(map[string]map[string]*coreauth.Auth)
	}
	if w.retiredAuthPaths == nil {
		w.retiredAuthPaths = make(map[string]struct{})
	}
	w.lastAuthHashes[normalizedPath] = currentHash
	if retiredFile {
		w.retiredAuthPaths[normalizedPath] = struct{}{}
	}
	if cachedAuth != nil {
		if w.lastAuthContents == nil {
			w.lastAuthContents = make(map[string]*coreauth.Auth)
		}
		w.lastAuthContents[normalizedPath] = cachedAuth
	}
	if _, retiredPath := w.retiredAuthPaths[normalizedPath]; retiredPath {
		delete(w.fileAuthsByPath, normalizedPath)
		w.clientsMutex.Unlock()
		if confirmReplacement {
			w.addOrUpdateClientLocked(path)
		}
		return
	}
	if len(pathAuths) > 0 {
		w.fileAuthsByPath[normalizedPath] = cloneAuthMap(pathAuths)
	} else {
		delete(w.fileAuthsByPath, normalizedPath)
	}
	w.clientsMutex.Unlock()
}

func (w *Watcher) shouldConfirmAuthDeleteTombstoneReplacement(normalized string, data []byte) bool {
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	expectedHash, tombstoned := w.retiredDeleteHashes[normalized]
	_, pending := w.retiredDeletes[normalized]
	return tombstoned && !pending && w.storePersister != nil && (expectedHash == "" || !coreauth.SourceHashMatchesBytes(expectedHash, data))
}

func (w *Watcher) addOrUpdateClient(path string) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	w.addOrUpdateClientLocked(path)
}

func (w *Watcher) addOrUpdateClientLocked(path string) {
	w.addOrUpdateClientWithPersistedHashLocked(path, "")
}

func (w *Watcher) addOrUpdateClientWithPersistedHashLocked(path, persistedHash string) {
	if info, errInfo := os.Lstat(path); errInfo == nil && info.Mode()&os.ModeSymlink != 0 {
		log.Warnf("ignoring symlink auth file: %s", filepath.Base(path))
		w.removeClientStateLocked(path, false)
		return
	}
	data, fileVersion, errRead := readAuthFileVersionUnderRoot(w.authRootDir(), path)
	if errRead != nil {
		log.Errorf("failed to read auth file %s: %v", filepath.Base(path), errRead)
		return
	}
	if len(data) == 0 {
		log.Debugf("ignoring empty auth file: %s", filepath.Base(path))
		return
	}

	curHash := fileVersion.hash
	normalized := w.normalizeAuthPath(path)

	// Parse new auth content for diff comparison
	var newAuth coreauth.Auth
	if errParse := json.Unmarshal(data, &newAuth); errParse != nil {
		log.Errorf("failed to parse auth file %s: %v", filepath.Base(path), errParse)
		return
	}

	w.clientsMutex.Lock()
	if w.config == nil {
		log.Error("config is nil, cannot add or update client")
		w.clientsMutex.Unlock()
		return
	}
	if w.fileAuthsByPath == nil {
		w.fileAuthsByPath = make(map[string]map[string]*coreauth.Auth)
	}
	if w.lastAuthHashes == nil {
		w.lastAuthHashes = make(map[string]string)
	}
	if w.retiredAuthPaths == nil {
		w.retiredAuthPaths = make(map[string]struct{})
	}
	if w.retiredDeletes == nil {
		w.retiredDeletes = make(map[string]uint64)
	}
	if w.retiredDeleteHashes == nil {
		w.retiredDeleteHashes = make(map[string]string)
	}
	if w.retiredDeleteStates == nil {
		w.retiredDeleteStates = make(map[string]*authfileguard.DeleteGeneration)
	}
	if expectedHash, tombstoned := w.retiredDeleteHashes[normalized]; tombstoned && (w.storePersister == nil || (expectedHash != "" && coreauth.SourceHashMatchesBytes(expectedHash, data))) {
		w.lastAuthHashes[normalized] = curHash
		delete(w.fileAuthsByPath, normalized)
		log.Debugf("auth file remains quarantined by deletion tombstone: %s", filepath.Base(path))
		w.clientsMutex.Unlock()
		return
	}
	if prev, ok := w.lastAuthHashes[normalized]; ok && prev == curHash {
		_, quarantined := w.retiredAuthPaths[normalized]
		_, persistencePending := w.retiredDeletes[normalized]
		if !quarantined || persistencePending || authfileguard.IsRetired(path) {
			log.Debugf("auth file unchanged (hash match), skipping reload: %s", filepath.Base(path))
			w.clientsMutex.Unlock()
			return
		}
	}

	// Get old auth for diff comparison
	cacheAuthContents := log.IsLevelEnabled(log.DebugLevel)
	var oldAuth *coreauth.Auth
	if cacheAuthContents && w.lastAuthContents != nil {
		oldAuth = w.lastAuthContents[normalized]
	}

	// Compute and log field changes
	if cacheAuthContents {
		if changes := diff.BuildAuthChangeDetails(oldAuth, &newAuth); len(changes) > 0 {
			log.Debugf("auth field changes for %s:", filepath.Base(path))
			for _, c := range changes {
				log.Debugf("  %s", c)
			}
		}
	}

	// Update caches
	w.lastAuthHashes[normalized] = curHash
	if cacheAuthContents {
		if w.lastAuthContents == nil {
			w.lastAuthContents = make(map[string]*coreauth.Auth)
		}
		w.lastAuthContents[normalized] = &newAuth
	}

	oldByID := make(map[string]*coreauth.Auth, len(w.fileAuthsByPath[normalized]))
	for id, a := range w.fileAuthsByPath[normalized] {
		oldByID[id] = a
	}

	if coreauth.IsRetiredGeminiCLIAuthFileData(data) {
		authfileguard.MarkRetired(path)
		w.retiredAuthPaths[normalized] = struct{}{}
		delete(w.retiredDeletes, normalized)
	}
	retiredPath := authfileguard.IsRetired(path)
	if retiredPath {
		w.retiredAuthPaths[normalized] = struct{}{}
	}

	if retiredPath {
		delete(w.fileAuthsByPath, normalized)
		updates := w.computePerPathUpdatesLocked(oldByID, map[string]*coreauth.Auth{})
		w.clientsMutex.Unlock()
		coreauth.WarnRetiredGeminiCLIAuthIgnored()
		w.dispatchAuthUpdates(updates)
		return
	}

	persistenceConfirmed := persistedHash != "" && persistedHash == curHash
	if w.storePersister == nil || persistenceConfirmed {
		delete(w.retiredAuthPaths, normalized)
		delete(w.retiredDeletes, normalized)
		sctx := &synthesizer.SynthesisContext{
			Config:      w.config,
			AuthDir:     w.authDir,
			Now:         time.Now(),
			IDGenerator: synthesizer.NewStableIDGenerator(),
		}
		newByID := authSliceToMap(synthesizer.SynthesizeAuthFile(sctx, path, data))
		if len(newByID) > 0 {
			w.fileAuthsByPath[normalized] = cloneAuthMap(newByID)
		} else {
			delete(w.fileAuthsByPath, normalized)
		}
		updates := w.computePerPathUpdatesLocked(oldByID, newByID)
		w.clientsMutex.Unlock()
		w.dispatchAuthUpdates(updates)
		return
	}

	deleteState := w.retiredDeleteStates[normalized]
	if deleteState == nil {
		deleteState = authfileguard.NewDeleteGeneration(w.retiredDeleteHashes[normalized])
	}
	previousGeneration := w.retiredDeletes[normalized]
	if previousGeneration == 0 {
		previousGeneration = w.nextAuthPersistenceGenerationLocked(normalized)
	}
	w.clientsMutex.Unlock()
	deleteState.SetPersistHook(func(snapshot authfileguard.DeleteGenerationSnapshot) error {
		return w.persistAuthDeleteTombstoneSnapshot(path, snapshot)
	})
	if errTombstone := w.persistAuthDeleteGenerationTombstone(path, deleteState); errTombstone != nil {
		w.scheduleAuthPersistenceRetry(normalized, previousGeneration, 0, fmt.Errorf("persist auth replacement quarantine: %w", errTombstone), func(int) {
			w.addOrUpdateClient(path)
		})
		return
	}
	w.clientsMutex.Lock()
	if w.retiredDeletes[normalized] != previousGeneration {
		w.clientsMutex.Unlock()
		return
	}
	persistenceGeneration := w.nextAuthPersistenceGenerationLocked(normalized)
	w.retiredDeleteStates[normalized] = deleteState
	delete(w.fileAuthsByPath, normalized)
	updates := w.computePerPathUpdatesLocked(oldByID, map[string]*coreauth.Auth{})
	w.clientsMutex.Unlock()

	w.dispatchAuthUpdates(updates)
	started := w.persistAuthAsyncWithCompletionContext(fmt.Sprintf("Sync auth %s", filepath.Base(path)), func(ctx context.Context) context.Context {
		return authfileguard.WithExpectedPersistHash(ctx, fileVersion.hash)
	}, func(errPersist error) {
		w.completeAuthPersistenceAttempt(path, normalized, fileVersion, persistenceGeneration, 0, errPersist)
	}, path)
	if !started {
		w.completeAuthPersistenceAttempt(path, normalized, fileVersion, persistenceGeneration, 0, errors.New("auth persistence was not started"))
	}
}

func (w *Watcher) completeAuthPersistence(path, normalized, expectedHash string, persistenceGeneration uint64, errPersist error) {
	w.completeAuthPersistenceAttempt(path, normalized, authFileVersion{hash: expectedHash}, persistenceGeneration, 0, errPersist)
}

func (w *Watcher) completeAuthPersistenceAttempt(path, normalized string, expectedVersion authFileVersion, persistenceGeneration uint64, attempt int, errPersist error) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()

	w.clientsMutex.Lock()
	if w.retiredDeletes[normalized] != persistenceGeneration {
		w.clientsMutex.Unlock()
		return
	}
	if errPersist != nil {
		if errors.Is(errPersist, authfileguard.ErrPersistGenerationStale) {
			delete(w.retiredDeletes, normalized)
			w.clientsMutex.Unlock()
			w.addOrUpdateClientLocked(path)
			return
		}
		w.clientsMutex.Unlock()
		w.scheduleAuthPersistenceRetry(normalized, persistenceGeneration, attempt, errPersist, func(nextAttempt int) {
			started := w.persistAuthAsyncWithCompletionContext(fmt.Sprintf("Retry auth %s", filepath.Base(path)), func(ctx context.Context) context.Context {
				return authfileguard.WithExpectedPersistHash(ctx, expectedVersion.hash)
			}, func(errRetry error) {
				w.completeAuthPersistenceAttempt(path, normalized, expectedVersion, persistenceGeneration, nextAttempt, errRetry)
			}, path)
			if !started {
				w.completeAuthPersistenceAttempt(path, normalized, expectedVersion, persistenceGeneration, nextAttempt, errors.New("auth persistence retry was not started"))
			}
		})
		return
	}
	w.clientsMutex.Unlock()

	_, currentVersion, errRead := readAuthFileVersionUnderRoot(w.authRootDir(), path)
	versionMatches := errRead == nil && sameAuthFileVersion(expectedVersion, currentVersion)
	if !versionMatches || authfileguard.IsRetired(path) {
		w.clientsMutex.Lock()
		if w.retiredDeletes[normalized] == persistenceGeneration {
			delete(w.retiredDeletes, normalized)
		}
		w.clientsMutex.Unlock()
		if currentVersion.hash != "" && !authfileguard.IsRetired(path) {
			w.addOrUpdateClientLocked(path)
		}
		return
	}

	w.clientsMutex.Lock()
	if w.retiredDeletes[normalized] != persistenceGeneration {
		w.clientsMutex.Unlock()
		return
	}
	deleteGeneration := w.retiredDeleteStates[normalized]
	w.clientsMutex.Unlock()
	errTombstone := w.clearAuthDeleteTombstone(path, deleteGeneration)
	w.clientsMutex.Lock()
	if w.retiredDeletes[normalized] != persistenceGeneration {
		w.clientsMutex.Unlock()
		return
	}
	if errTombstone != nil {
		w.clientsMutex.Unlock()
		log.WithError(errTombstone).Warnf("auth persistence completed but deletion tombstone remains for %s", normalized)
		w.scheduleAuthPersistenceRetry(normalized, persistenceGeneration, attempt, errTombstone, func(nextAttempt int) {
			w.completeAuthPersistenceAttempt(path, normalized, expectedVersion, persistenceGeneration, nextAttempt, nil)
		})
		return
	}
	delete(w.retiredDeletes, normalized)
	delete(w.retiredAuthPaths, normalized)
	delete(w.retiredDeleteHashes, normalized)
	delete(w.retiredDeleteStates, normalized)
	delete(w.lastAuthHashes, normalized)
	w.clientsMutex.Unlock()

	w.addOrUpdateClientWithPersistedHashLocked(path, expectedVersion.hash)
}

func (w *Watcher) nextAuthPersistenceGenerationLocked(normalized string) uint64 {
	if w.retiredAuthPaths == nil {
		w.retiredAuthPaths = make(map[string]struct{})
	}
	if w.retiredDeletes == nil {
		w.retiredDeletes = make(map[string]uint64)
	}
	if w.retiredDeleteHashes == nil {
		w.retiredDeleteHashes = make(map[string]string)
	}
	if w.retiredDeleteStates == nil {
		w.retiredDeleteStates = make(map[string]*authfileguard.DeleteGeneration)
	}
	w.retiredAuthPaths[normalized] = struct{}{}
	w.retiredDeleteSeq++
	w.retiredDeletes[normalized] = w.retiredDeleteSeq
	return w.retiredDeleteSeq
}

func (w *Watcher) completeAuthRemoval(path, normalized string, persistenceGeneration uint64, errPersist error) {
	w.completeAuthRemovalAttempt(path, normalized, persistenceGeneration, nil, 0, errPersist)
}

func (w *Watcher) completeAuthRemovalAttempt(path, normalized string, persistenceGeneration uint64, deleteGeneration *authfileguard.DeleteGeneration, attempt int, errPersist error) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()

	w.clientsMutex.Lock()
	if w.retiredDeletes[normalized] != persistenceGeneration {
		w.clientsMutex.Unlock()
		return
	}
	if deleteGeneration == nil {
		deleteGeneration = w.retiredDeleteStates[normalized]
	}
	if errPersist != nil {
		w.clientsMutex.Unlock()
		if errors.Is(errPersist, authfileguard.ErrDeleteGenerationUncertain) {
			log.WithError(errPersist).Warnf("auth removal remains quarantined for %s", normalized)
			return
		}
		w.scheduleAuthPersistenceRetry(normalized, persistenceGeneration, attempt, errPersist, func(nextAttempt int) {
			w.retryAuthRemovalPersistence(path, normalized, fmt.Sprintf("Retry removing auth %s", filepath.Base(path)), persistenceGeneration, deleteGeneration, nextAttempt, 0, false)
		})
		return
	}
	w.clientsMutex.Unlock()
	_, errInfo := os.Lstat(path)
	if errInfo == nil {
		w.addOrUpdateClientLocked(path)
		return
	}
	if !os.IsNotExist(errInfo) {
		w.scheduleAuthPersistenceRetry(normalized, persistenceGeneration, attempt, fmt.Errorf("inspect auth replacement: %w", errInfo), func(nextAttempt int) {
			w.completeAuthRemovalAttempt(path, normalized, persistenceGeneration, deleteGeneration, nextAttempt, nil)
		})
		return
	}
	errTombstone := w.clearAuthDeleteTombstone(path, deleteGeneration)
	w.clientsMutex.Lock()
	if w.retiredDeletes[normalized] != persistenceGeneration {
		w.clientsMutex.Unlock()
		return
	}
	if errTombstone != nil {
		w.clientsMutex.Unlock()
		log.WithError(errTombstone).Warnf("auth removal completed but deletion tombstone remains for %s", normalized)
		w.scheduleAuthPersistenceRetry(normalized, persistenceGeneration, attempt, errTombstone, func(nextAttempt int) {
			w.completeAuthRemovalAttempt(path, normalized, persistenceGeneration, deleteGeneration, nextAttempt, nil)
		})
		return
	}
	delete(w.retiredDeletes, normalized)
	delete(w.retiredDeleteHashes, normalized)
	delete(w.retiredDeleteStates, normalized)
	delete(w.retiredAuthPaths, normalized)
	w.clientsMutex.Unlock()
	authfileguard.ClearRetired(path)
}

func (w *Watcher) startAuthRemovalPersistence(path, normalized, message string, persistenceGeneration uint64, deleteGeneration *authfileguard.DeleteGeneration) {
	w.startAuthRemovalPersistenceAttemptLocked(path, normalized, message, persistenceGeneration, deleteGeneration, 0, 0, true)
}

func (w *Watcher) retryAuthRemovalPersistence(path, normalized, message string, persistenceGeneration uint64, deleteGeneration *authfileguard.DeleteGeneration, remoteAttempt, tombstoneAttempt int, allowIdentityBinding bool) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	w.clientsMutex.RLock()
	current := w.retiredDeletes[normalized] == persistenceGeneration
	w.clientsMutex.RUnlock()
	if !current {
		return
	}
	w.startAuthRemovalPersistenceAttemptLocked(path, normalized, message, persistenceGeneration, deleteGeneration, remoteAttempt, tombstoneAttempt, allowIdentityBinding)
}

func (w *Watcher) startAuthRemovalPersistenceAttemptLocked(path, normalized, message string, persistenceGeneration uint64, deleteGeneration *authfileguard.DeleteGeneration, remoteAttempt, tombstoneAttempt int, allowIdentityBinding bool) {
	if deleteGeneration == nil {
		deleteGeneration = authfileguard.NewDeleteGeneration("")
	}
	deleteGeneration.SetPersistHook(func(snapshot authfileguard.DeleteGenerationSnapshot) error {
		return w.persistAuthDeleteTombstoneSnapshot(path, snapshot)
	})
	if errTombstone := w.persistAuthDeleteGenerationTombstone(path, deleteGeneration); errTombstone != nil {
		w.scheduleAuthPersistenceRetry(normalized, persistenceGeneration, tombstoneAttempt, fmt.Errorf("persist auth removal quarantine: %w", errTombstone), func(nextTombstoneAttempt int) {
			w.retryAuthRemovalPersistence(path, normalized, message, persistenceGeneration, deleteGeneration, remoteAttempt, nextTombstoneAttempt, allowIdentityBinding)
		})
		return
	}
	started := w.persistAuthAsyncWithCompletionContext(message, func(ctx context.Context) context.Context {
		ctx = authfileguard.WithDeleteGeneration(ctx, deleteGeneration)
		ctx = authfileguard.WithDeleteAttempt(ctx, remoteAttempt)
		if allowIdentityBinding {
			ctx = authfileguard.WithDeleteIdentityBinding(ctx)
		}
		return ctx
	}, func(errPersist error) {
		w.completeAuthRemovalAttempt(path, normalized, persistenceGeneration, deleteGeneration, remoteAttempt, errPersist)
	}, path)
	if !started {
		w.startAuthPersistenceTask(func(context.Context) {
			w.completeAuthRemovalAttempt(path, normalized, persistenceGeneration, deleteGeneration, remoteAttempt, errors.New("auth persistence was not started"))
		})
	}
}

func (w *Watcher) startRetiredAuthRemovalPersistence(path, normalized, message string, persistenceGeneration uint64, deleteGeneration *authfileguard.DeleteGeneration, retiredSnapshot authfileguard.RetiredSnapshot) {
	w.startRetiredAuthRemovalPersistenceAttemptLocked(path, normalized, message, persistenceGeneration, deleteGeneration, retiredSnapshot, 0, 0)
}

func (w *Watcher) retryRetiredAuthRemovalPersistence(path, normalized, message string, persistenceGeneration uint64, deleteGeneration *authfileguard.DeleteGeneration, retiredSnapshot authfileguard.RetiredSnapshot, remoteAttempt, tombstoneAttempt int) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	w.clientsMutex.RLock()
	current := w.retiredDeletes[normalized] == persistenceGeneration
	w.clientsMutex.RUnlock()
	if !current {
		return
	}
	w.startRetiredAuthRemovalPersistenceAttemptLocked(path, normalized, message, persistenceGeneration, deleteGeneration, retiredSnapshot, remoteAttempt, tombstoneAttempt)
}

func (w *Watcher) startRetiredAuthRemovalPersistenceAttemptLocked(path, normalized, message string, persistenceGeneration uint64, deleteGeneration *authfileguard.DeleteGeneration, retiredSnapshot authfileguard.RetiredSnapshot, remoteAttempt, tombstoneAttempt int) {
	if deleteGeneration == nil {
		deleteGeneration = authfileguard.NewDeleteGeneration("")
	}
	deleteGeneration.SetPersistHook(func(snapshot authfileguard.DeleteGenerationSnapshot) error {
		return w.persistAuthDeleteTombstoneSnapshot(path, snapshot)
	})
	if errTombstone := w.persistAuthDeleteGenerationTombstone(path, deleteGeneration); errTombstone != nil {
		w.scheduleAuthPersistenceRetry(normalized, persistenceGeneration, tombstoneAttempt, fmt.Errorf("persist retired auth removal quarantine: %w", errTombstone), func(nextTombstoneAttempt int) {
			w.retryRetiredAuthRemovalPersistence(path, normalized, message, persistenceGeneration, deleteGeneration, retiredSnapshot, remoteAttempt, nextTombstoneAttempt)
		})
		return
	}
	started := w.deleteRetiredAuthAsyncWithCompletion(path, message, deleteGeneration, remoteAttempt, func(errPersist error) {
		w.completeRetiredDeleteAttempt(path, normalized, persistenceGeneration, deleteGeneration, retiredSnapshot, remoteAttempt, errPersist)
	})
	if !started {
		w.startAuthPersistenceTask(func(context.Context) {
			w.completeRetiredDeleteAttempt(path, normalized, persistenceGeneration, deleteGeneration, retiredSnapshot, remoteAttempt, errors.New("auth persistence was not started"))
		})
	}
}

func (w *Watcher) removeClient(path string) {
	w.removeClientState(path, true)
}

func (w *Watcher) removeClientState(path string, persist bool) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	w.removeClientStateLocked(path, persist)
}

func (w *Watcher) removeClientStateLocked(path string, persist bool) {
	normalized := w.normalizeAuthPath(path)
	retiredPath := authfileguard.IsRetired(path)
	retiredSnapshot := authfileguard.CaptureRetired(path)

	w.clientsMutex.Lock()
	oldByID := make(map[string]*coreauth.Auth, len(w.fileAuthsByPath[normalized]))
	for id, a := range w.fileAuthsByPath[normalized] {
		oldByID[id] = a
	}
	expectedHash := authDeleteExpectedHash(w.lastAuthHashes[normalized], oldByID)
	deleteGeneration := authfileguard.NewDeleteGeneration(expectedHash)
	delete(w.lastAuthHashes, normalized)
	delete(w.lastAuthContents, normalized)
	delete(w.fileAuthsByPath, normalized)

	persistenceGeneration := uint64(0)
	deferPersistence := persist && w.storePersister != nil
	_, tombstoned := w.retiredDeleteHashes[normalized]
	_, persistencePending := w.retiredDeletes[normalized]
	preserveQuarantine := !persist && (tombstoned || persistencePending)
	if deferPersistence {
		persistenceGeneration = w.nextAuthPersistenceGenerationLocked(normalized)
		w.retiredDeleteHashes[normalized] = normalizeAuthDeleteExpectedHash(deleteGeneration.ExpectedHash())
		w.retiredDeleteStates[normalized] = deleteGeneration
	} else if !preserveQuarantine {
		delete(w.retiredDeletes, normalized)
		delete(w.retiredDeleteHashes, normalized)
		delete(w.retiredDeleteStates, normalized)
		if persist || !retiredPath {
			delete(w.retiredAuthPaths, normalized)
		}
	}

	updates := w.computePerPathUpdatesLocked(oldByID, map[string]*coreauth.Auth{})
	w.clientsMutex.Unlock()

	if persist && !deferPersistence && retiredPath {
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	}
	w.dispatchAuthUpdates(updates)
	if !deferPersistence {
		return
	}

	message := fmt.Sprintf("Remove auth %s", filepath.Base(path))
	if retiredPath {
		w.startRetiredAuthRemovalPersistence(path, normalized, message, persistenceGeneration, deleteGeneration, retiredSnapshot)
	} else {
		w.startAuthRemovalPersistence(path, normalized, message, persistenceGeneration, deleteGeneration)
	}
}

func authDeleteExpectedHash(rawHash string, auths map[string]*coreauth.Auth) string {
	canonicalHash := ""
	for _, auth := range auths {
		if auth == nil || auth.Attributes == nil {
			continue
		}
		hash := normalizeAuthDeleteExpectedHash(auth.Attributes[coreauth.SourceHashAttributeKey])
		if hash == "" {
			continue
		}
		if canonicalHash != "" && canonicalHash != hash {
			return normalizeAuthDeleteExpectedHash(rawHash)
		}
		canonicalHash = hash
	}
	if canonicalHash != "" {
		return canonicalHash
	}
	return normalizeAuthDeleteExpectedHash(rawHash)
}

func (w *Watcher) requiresRetiredAuthDeletion(path string) bool {
	if w == nil || !authfileguard.IsRetired(path) {
		return false
	}
	data, errRead := readAuthFileUnderRoot(w.authRootDir(), path)
	if errRead != nil || len(data) == 0 || coreauth.IsRetiredGeminiCLIAuthFileData(data) {
		return false
	}
	sctx := &synthesizer.SynthesisContext{
		Config:      w.config,
		AuthDir:     w.authDir,
		Now:         time.Now(),
		IDGenerator: synthesizer.NewStableIDGenerator(),
	}
	return len(synthesizer.SynthesizeAuthFile(sctx, path, data)) > 0
}

func (w *Watcher) finalizeRetiredAuthReplacement(path string) bool {
	if !w.requiresRetiredAuthDeletion(path) {
		return false
	}
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	if !w.requiresRetiredAuthDeletion(path) {
		return false
	}
	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	_, pending := w.retiredDeletes[normalized]
	w.clientsMutex.RUnlock()
	if !pending {
		w.removeClientStateLocked(path, true)
	}
	return true
}

func (w *Watcher) deleteRetiredAuthAsyncWithCompletion(path, message string, deleteGeneration *authfileguard.DeleteGeneration, deleteAttempt int, complete func(error)) bool {
	if w == nil || w.storePersister == nil || strings.TrimSpace(path) == "" {
		return false
	}
	authDir := w.authRootDir()
	var finalize func(context.Context) error
	if finalizer, ok := w.storePersister.(authDeleteAtBaseDirFinalizer); ok && finalizer != nil {
		relativePath, errRel := filepath.Rel(authDir, path)
		if errRel != nil || relativePath == "." || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
			return false
		}
		finalize = func(ctx context.Context) error {
			return finalizer.FinalizeAuthFileDeletionAtBaseDir(ctx, authDir, relativePath)
		}
	} else if finalizer, ok := w.storePersister.(authDeleteFinalizer); ok && finalizer != nil {
		relativePath, errRel := filepath.Rel(authDir, path)
		if errRel != nil || relativePath == "." || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
			return false
		}
		finalize = func(ctx context.Context) error {
			return finalizer.FinalizeAuthFileDeletion(ctx, relativePath)
		}
	} else {
		if _, errInfo := os.Lstat(path); errInfo == nil || !os.IsNotExist(errInfo) {
			return false
		}
		finalize = func(ctx context.Context) error {
			return w.storePersister.PersistAuthFiles(ctx, message, path)
		}
	}
	return w.startAuthPersistenceTask(func(ctx context.Context) {
		ctx = authfileguard.WithDeleteGeneration(ctx, deleteGeneration)
		ctx = authfileguard.WithDeleteAttempt(ctx, deleteAttempt)
		if deleteAttempt == 0 {
			ctx = authfileguard.WithDeleteIdentityBinding(ctx)
		}
		errDelete := finalize(ctx)
		if errDelete != nil {
			log.Errorf("failed to delete retired auth: %v", errDelete)
		}
		if complete != nil {
			complete(errDelete)
		}
	})
}

func (w *Watcher) completeRetiredDelete(path, normalized string, deleteSeq uint64, retiredSnapshot authfileguard.RetiredSnapshot, errPersist error) {
	w.clientsMutex.RLock()
	expectedHash := w.retiredDeleteHashes[normalized]
	deleteGeneration := w.retiredDeleteStates[normalized]
	w.clientsMutex.RUnlock()
	if deleteGeneration == nil {
		deleteGeneration = authfileguard.NewDeleteGeneration(expectedHash)
	}
	w.completeRetiredDeleteAttempt(path, normalized, deleteSeq, deleteGeneration, retiredSnapshot, 0, errPersist)
}

func (w *Watcher) completeRetiredDeleteAttempt(path, normalized string, deleteSeq uint64, deleteGeneration *authfileguard.DeleteGeneration, retiredSnapshot authfileguard.RetiredSnapshot, attempt int, errPersist error) {
	if outcome, ok := coreauth.DeleteOutcomeFromError(errPersist); ok && outcome == coreauth.DeleteOutcomeCommitted {
		errPersist = nil
	}
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	w.clientsMutex.Lock()
	if w.retiredDeletes[normalized] != deleteSeq {
		w.clientsMutex.Unlock()
		return
	}
	if errPersist != nil {
		w.clientsMutex.Unlock()
		if errors.Is(errPersist, authfileguard.ErrDeleteGenerationUncertain) {
			log.WithError(errPersist).Warnf("retired auth removal remains quarantined for %s", normalized)
			return
		}
		w.scheduleAuthPersistenceRetry(normalized, deleteSeq, attempt, errPersist, func(nextAttempt int) {
			w.retryRetiredAuthRemovalPersistence(path, normalized, fmt.Sprintf("Retry removing retired auth %s", filepath.Base(path)), deleteSeq, deleteGeneration, retiredSnapshot, nextAttempt, 0)
		})
		return
	}
	_, errInfo := os.Lstat(path)
	if errInfo == nil {
		delete(w.retiredDeletes, normalized)
		delete(w.lastAuthHashes, normalized)
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
		w.clientsMutex.Unlock()
		w.addOrUpdateClientLocked(path)
		return
	}
	if !os.IsNotExist(errInfo) {
		w.clientsMutex.Unlock()
		w.scheduleAuthPersistenceRetry(normalized, deleteSeq, attempt, fmt.Errorf("inspect retired auth replacement: %w", errInfo), func(nextAttempt int) {
			w.completeRetiredDeleteAttempt(path, normalized, deleteSeq, deleteGeneration, retiredSnapshot, nextAttempt, nil)
		})
		return
	}
	w.clientsMutex.Unlock()
	errTombstone := w.clearAuthDeleteTombstone(path, deleteGeneration)
	w.clientsMutex.Lock()
	if w.retiredDeletes[normalized] != deleteSeq {
		w.clientsMutex.Unlock()
		return
	}
	if errTombstone != nil {
		w.clientsMutex.Unlock()
		log.WithError(errTombstone).Warnf("retired auth removal completed but deletion tombstone remains for %s", normalized)
		w.scheduleAuthPersistenceRetry(normalized, deleteSeq, attempt, errTombstone, func(nextAttempt int) {
			w.completeRetiredDeleteAttempt(path, normalized, deleteSeq, deleteGeneration, retiredSnapshot, nextAttempt, nil)
		})
		return
	}
	delete(w.retiredDeletes, normalized)
	delete(w.retiredDeleteHashes, normalized)
	delete(w.retiredDeleteStates, normalized)
	delete(w.retiredAuthPaths, normalized)
	delete(w.lastAuthHashes, normalized)
	authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	w.clientsMutex.Unlock()
}

func (w *Watcher) scheduleAuthPersistenceRetry(normalized string, generation uint64, attempt int, errPersist error, retry func(int)) {
	if w == nil || retry == nil || w.stopped.Load() {
		return
	}
	nextAttempt := attempt + 1
	baseDelay := w.authRetryBase
	if baseDelay <= 0 {
		baseDelay = authPersistenceRetryBase
	}
	delay := baseDelay << min(nextAttempt-1, 7)
	log.WithError(errPersist).Warnf("auth persistence failed for %s; retrying in %s", normalized, delay)
	ready := make(chan struct{})
	var timer *time.Timer
	w.authRetryMu.Lock()
	if w.stopped.Load() {
		w.authRetryMu.Unlock()
		return
	}
	if w.authRetryTimers == nil {
		w.authRetryTimers = make(map[*time.Timer]struct{})
	}
	w.authRetryWG.Add(1)
	timer = time.AfterFunc(delay, func() {
		<-ready
		w.authRetryMu.Lock()
		delete(w.authRetryTimers, timer)
		w.authRetryMu.Unlock()
		defer w.authRetryWG.Done()
		if w.stopped.Load() {
			return
		}
		w.clientsMutex.RLock()
		current := w.retiredDeletes[normalized] == generation
		w.clientsMutex.RUnlock()
		if current {
			retry(nextAttempt)
		}
	})
	w.authRetryTimers[timer] = struct{}{}
	close(ready)
	w.authRetryMu.Unlock()
}

func (w *Watcher) stopAuthPersistenceRetryTimers() {
	if w == nil {
		return
	}
	w.authRetryMu.Lock()
	for timer := range w.authRetryTimers {
		if timer.Stop() {
			delete(w.authRetryTimers, timer)
			w.authRetryWG.Done()
		}
	}
	w.authRetryMu.Unlock()
	w.authRetryWG.Wait()
}

func (w *Watcher) startAuthPersistenceTask(task func(context.Context)) bool {
	if w == nil || task == nil {
		return false
	}
	w.authWorkMu.Lock()
	if w.stopped.Load() {
		w.authWorkMu.Unlock()
		return false
	}
	if w.authWorkContext == nil {
		w.authWorkContext, w.authWorkCancel = context.WithCancel(context.Background())
	}
	ctx := w.authWorkContext
	w.authWorkWG.Add(1)
	w.authWorkMu.Unlock()
	go func() {
		defer w.authWorkWG.Done()
		task(ctx)
	}()
	return true
}

func (w *Watcher) stopAuthPersistenceTasks() {
	if w == nil {
		return
	}
	w.authWorkMu.Lock()
	cancel := w.authWorkCancel
	w.authWorkCancel = nil
	w.authWorkContext = nil
	w.authWorkMu.Unlock()
	if cancel != nil {
		cancel()
	}
	done := make(chan struct{})
	go func() {
		w.authWorkWG.Wait()
		close(done)
	}()
	wait := authPersistenceShutdownWait
	if wait <= 0 {
		wait = 5 * time.Second
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		log.Warnf("auth persistence tasks did not stop within %s", wait)
	}
}

func (w *Watcher) computePerPathUpdatesLocked(oldByID, newByID map[string]*coreauth.Auth) []AuthUpdate {
	if w.currentAuths == nil {
		w.currentAuths = make(map[string]*coreauth.Auth)
	}
	updates := make([]AuthUpdate, 0, len(oldByID)+len(newByID))
	for id, newAuth := range newByID {
		existing, ok := w.currentAuths[id]
		if !ok {
			w.currentAuths[id] = newAuth.Clone()
			updates = append(updates, AuthUpdate{Action: AuthUpdateActionAdd, ID: id, Auth: newAuth.Clone()})
			continue
		}
		if !authEqual(existing, newAuth) {
			w.currentAuths[id] = newAuth.Clone()
			updates = append(updates, AuthUpdate{Action: AuthUpdateActionModify, ID: id, Auth: newAuth.Clone()})
		}
	}
	for id := range oldByID {
		if _, stillExists := newByID[id]; stillExists {
			continue
		}
		var deletedAuth *coreauth.Auth
		if previous := oldByID[id]; previous != nil {
			deletedAuth = previous.Clone()
		}
		delete(w.currentAuths, id)
		updates = append(updates, AuthUpdate{Action: AuthUpdateActionDelete, ID: id, Auth: deletedAuth})
	}
	return updates
}

func authSliceToMap(auths []*coreauth.Auth) map[string]*coreauth.Auth {
	byID := make(map[string]*coreauth.Auth, len(auths))
	for _, a := range auths {
		if a == nil || strings.TrimSpace(a.ID) == "" {
			continue
		}
		byID[a.ID] = a
	}
	return byID
}

func cloneAuthMap(auths map[string]*coreauth.Auth) map[string]*coreauth.Auth {
	set := make(map[string]*coreauth.Auth, len(auths))
	for id, auth := range auths {
		if auth == nil {
			set[id] = nil
			continue
		}
		set[id] = auth.Clone()
	}
	return set
}

func (w *Watcher) loadFileClients(cfg *config.Config) int {
	authFileCount := 0
	successfulAuthCount := 0

	authDir, errResolveAuthDir := util.ResolveAuthDir(cfg.AuthDir)
	if errResolveAuthDir != nil {
		log.Errorf("failed to resolve auth directory: %v", errResolveAuthDir)
		return 0
	}
	if authDir == "" {
		return 0
	}

	entries, errReadDir := os.ReadDir(authDir)
	if errReadDir != nil {
		log.Errorf("error reading auth directory: %v", errReadDir)
		return 0
	}
	for _, entry := range entries {
		if entry == nil || entry.IsDir() {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 {
			log.Warnf("ignoring symlink auth file: %s", entry.Name())
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		authFileCount++
		log.Debugf("processing auth file %d: %s", authFileCount, name)
		fullPath := filepath.Join(authDir, name)
		if data, errReadFile := readAuthFileUnderRoot(authDir, fullPath); errReadFile == nil && len(data) > 0 {
			successfulAuthCount++
		}
	}
	log.Debugf("auth directory scan complete - found %d .json files, %d readable", authFileCount, successfulAuthCount)
	return authFileCount
}

func BuildAPIKeyClients(cfg *config.Config) (int, int, int, int, int) {
	geminiAPIKeyCount := 0
	vertexCompatAPIKeyCount := 0
	claudeAPIKeyCount := 0
	codexAPIKeyCount := 0
	openAICompatCount := 0

	if len(cfg.GeminiKey) > 0 {
		geminiAPIKeyCount += len(cfg.GeminiKey)
	}
	if len(cfg.InteractionsKey) > 0 {
		geminiAPIKeyCount += len(cfg.InteractionsKey)
	}
	if len(cfg.VertexCompatAPIKey) > 0 {
		vertexCompatAPIKeyCount += len(cfg.VertexCompatAPIKey)
	}
	if len(cfg.ClaudeKey) > 0 {
		claudeAPIKeyCount += len(cfg.ClaudeKey)
	}
	if len(cfg.CodexKey) > 0 {
		codexAPIKeyCount += len(cfg.CodexKey)
	}
	if len(cfg.OpenAICompatibility) > 0 {
		for _, compatConfig := range cfg.OpenAICompatibility {
			if compatConfig.Disabled {
				continue
			}
			openAICompatCount += len(compatConfig.APIKeyEntries)
		}
	}
	return geminiAPIKeyCount, vertexCompatAPIKeyCount, claudeAPIKeyCount, codexAPIKeyCount, openAICompatCount
}

func (w *Watcher) persistConfigAsync() {
	if w == nil || w.storePersister == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := w.storePersister.PersistConfig(ctx); err != nil {
			log.Errorf("failed to persist config change: %v", err)
		}
	}()
}

func (w *Watcher) persistAuthAsync(message string, paths ...string) {
	w.persistAuthAsyncWithCompletion(message, nil, paths...)
}

func (w *Watcher) persistAuthAsyncWithCompletion(message string, complete func(error), paths ...string) bool {
	return w.persistAuthAsyncWithCompletionContext(message, nil, complete, paths...)
}

func (w *Watcher) persistAuthAsyncWithCompletionContext(message string, decorateContext func(context.Context) context.Context, complete func(error), paths ...string) bool {
	if w == nil || w.storePersister == nil {
		return false
	}
	filtered := make([]string, 0, len(paths))
	for _, p := range paths {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			filtered = append(filtered, trimmed)
		}
	}
	if len(filtered) == 0 {
		return false
	}
	return w.startAuthPersistenceTask(func(parent context.Context) {
		ctx, cancel := context.WithTimeout(parent, 30*time.Second)
		defer cancel()
		if decorateContext != nil {
			ctx = decorateContext(ctx)
		}
		errPersist := w.storePersister.PersistAuthFiles(ctx, message, filtered...)
		if errPersist != nil {
			log.Errorf("failed to persist auth changes: %v", errPersist)
		}
		if complete != nil {
			complete(errPersist)
		}
	})
}

func (w *Watcher) stopServerUpdateTimer() {
	w.serverUpdateMu.Lock()
	defer w.serverUpdateMu.Unlock()
	if w.serverUpdateTimer != nil {
		w.serverUpdateTimer.Stop()
		w.serverUpdateTimer = nil
	}
	w.serverUpdatePend = false
}

func (w *Watcher) triggerServerUpdate(cfg *config.Config) {
	if w == nil || w.reloadCallback == nil || cfg == nil {
		return
	}
	if w.stopped.Load() {
		return
	}

	now := time.Now()

	w.serverUpdateMu.Lock()
	if w.serverUpdateLast.IsZero() || now.Sub(w.serverUpdateLast) >= serverUpdateDebounce {
		w.serverUpdateLast = now
		if w.serverUpdateTimer != nil {
			w.serverUpdateTimer.Stop()
			w.serverUpdateTimer = nil
		}
		w.serverUpdatePend = false
		w.serverUpdateMu.Unlock()
		w.reloadCallback(cfg)
		return
	}

	if w.serverUpdatePend {
		w.serverUpdateMu.Unlock()
		return
	}

	delay := serverUpdateDebounce - now.Sub(w.serverUpdateLast)
	if delay < 10*time.Millisecond {
		delay = 10 * time.Millisecond
	}
	w.serverUpdatePend = true
	if w.serverUpdateTimer != nil {
		w.serverUpdateTimer.Stop()
		w.serverUpdateTimer = nil
	}
	var timer *time.Timer
	timer = time.AfterFunc(delay, func() {
		if w.stopped.Load() {
			return
		}
		w.clientsMutex.RLock()
		latestCfg := w.config
		w.clientsMutex.RUnlock()

		w.serverUpdateMu.Lock()
		if w.serverUpdateTimer != timer || !w.serverUpdatePend {
			w.serverUpdateMu.Unlock()
			return
		}
		w.serverUpdateTimer = nil
		w.serverUpdatePend = false
		if latestCfg == nil || w.reloadCallback == nil || w.stopped.Load() {
			w.serverUpdateMu.Unlock()
			return
		}

		w.serverUpdateLast = time.Now()
		w.serverUpdateMu.Unlock()
		w.reloadCallback(latestCfg)
	})
	w.serverUpdateTimer = timer
	w.serverUpdateMu.Unlock()
}
