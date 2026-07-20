package management

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

type retainedCodexAuthResult struct {
	Name           string   `json:"name"`
	DependentCount int      `json:"dependent_count"`
	DependentNames []string `json:"dependent_names"`
}

type authDependencyDeleteResult struct {
	deleted  []string
	retained []retainedCodexAuthResult
	failed   []gin.H
}

type authDependencyDeleteContext struct {
	h                 *Handler
	ctx               context.Context
	root              *os.Root
	lexicalAuthDir    string
	authDir           string
	requested         map[string]struct{}
	forceCascade      bool
	suppressCleanup   map[string]struct{}
	processed         map[string]struct{}
	result            authDependencyDeleteResult
	ignoreMissingFile bool
}

func (h *Handler) deleteAuthFilesWithDependencies(ctx context.Context, root *os.Root, lexicalAuthDir, authDir string, names []string, forceCascade, ignoreMissing bool) authDependencyDeleteResult {
	operation := &authDependencyDeleteContext{
		h:                 h,
		ctx:               ctx,
		root:              root,
		lexicalAuthDir:    lexicalAuthDir,
		authDir:           authDir,
		requested:         make(map[string]struct{}, len(names)),
		forceCascade:      forceCascade,
		suppressCleanup:   make(map[string]struct{}),
		processed:         make(map[string]struct{}),
		ignoreMissingFile: ignoreMissing,
	}
	run := func(lockedCtx context.Context) error {
		operation.ctx = lockedCtx
		for _, name := range names {
			operation.requested[managedAuthNameKey(name)] = struct{}{}
			if auth := h.managedAuthForNameAtRoot(root, lexicalAuthDir, authDir, name); auth != nil && strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
				if uid := coreauth.ChatGPTWebCredentialUID(auth); uid != "" {
					operation.suppressCleanup[uid] = struct{}{}
				}
			}
		}
		ordered := append([]string(nil), names...)
		sort.SliceStable(ordered, func(i, j int) bool {
			return operation.deleteOrder(ordered[i]) < operation.deleteOrder(ordered[j])
		})
		for _, name := range ordered {
			operation.delete(name, false)
		}
		return nil
	}
	if h == nil {
		for _, name := range names {
			operation.fail(name, http.StatusServiceUnavailable, errors.New("management handler is unavailable"))
		}
		return operation.result
	}
	if h.authManager == nil {
		for _, name := range names {
			provider := managedAuthFileProviderAtRoot(root, authDir, name)
			if provider == "codex" || provider == "chatgpt-web" {
				operation.processed[managedAuthNameKey(name)] = struct{}{}
				operation.fail(name, http.StatusServiceUnavailable, errors.New("dependency-aware auth manager is unavailable"))
			}
		}
		_ = run(ctx)
	} else if errMutation := h.authManager.WithChatGPTWebDependencyMutation(ctx, run); errMutation != nil {
		operation.fail("", http.StatusInternalServerError, errMutation)
	}
	return operation.result
}

func (operation *authDependencyDeleteContext) deleteOrder(name string) int {
	auth := operation.h.managedAuthForNameAtRoot(operation.root, operation.lexicalAuthDir, operation.authDir, name)
	if auth == nil {
		return 1
	}
	switch strings.ToLower(strings.TrimSpace(auth.Provider)) {
	case "chatgpt-web":
		return 0
	case "codex":
		return 2
	default:
		return 1
	}
}

func (operation *authDependencyDeleteContext) delete(name string, dependencyDelete bool) {
	key := managedAuthNameKey(name)
	if _, done := operation.processed[key]; done {
		return
	}
	operation.processed[key] = struct{}{}
	auth := operation.h.managedAuthForNameAtRoot(operation.root, operation.lexicalAuthDir, operation.authDir, name)
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		operation.deletePhysical(name, auth, dependencyDelete)
		return
	}

	graph, errGraph := operation.dependencyGraph()
	if errGraph != nil {
		operation.fail(name, http.StatusConflict, fmt.Errorf("cannot verify credential dependencies: %w", errGraph))
		return
	}
	dependents, ambiguous := graph.DependentsForSource(auth)
	if ambiguous {
		operation.fail(name, http.StatusConflict, errors.New("codex credential UID is duplicated; dependency deletion is ambiguous"))
		return
	}
	reservationSource := auth
	if uid := coreauth.ChatGPTWebCredentialUID(auth); uid != "" {
		persistedSource, sourceAmbiguous := graph.SourceByUID(uid)
		if sourceAmbiguous {
			operation.fail(name, http.StatusConflict, errors.New("codex credential UID is duplicated; dependency deletion is ambiguous"))
			return
		}
		if persistedSource != nil && persistedSource.ID == auth.ID {
			reservationSource = persistedSource
		}
	}
	if len(coreauth.ChatGPTWebActiveDependencyReservations(reservationSource, time.Now())) > 0 {
		operation.fail(name, http.StatusConflict, errors.New("Codex credential has an in-flight Web conversion; retry deletion after it finishes"))
		return
	}
	if len(dependents) == 0 {
		operation.deletePhysicalExpected(name, auth, dependencyDelete, coreauth.ChatGPTWebCredentialUID(auth), dependencySourceHash(auth))
		return
	}
	cascade := operation.forceCascade || operation.allDependentsRequested(dependents)
	if !cascade {
		operation.retain(name, auth, dependents)
		return
	}
	uid := coreauth.ChatGPTWebCredentialUID(auth)
	expectedHash := dependencySourceHash(auth)
	if uid == "" || expectedHash == "" {
		operation.fail(name, http.StatusConflict, errors.New("Codex credential has no stable dependency generation"))
		return
	}
	if errCurrent := operation.validateSourceDecision(name, uid, expectedHash); errCurrent != nil {
		operation.fail(name, http.StatusConflict, errCurrent)
		return
	}
	if uid != "" {
		operation.suppressCleanup[uid] = struct{}{}
	}
	failuresBeforeCascade := len(operation.result.failed)
	for _, dependent := range dependents {
		dependentName, ok := operation.h.managedAuthNameForAuthAtRoot(operation.root, operation.lexicalAuthDir, operation.authDir, dependent)
		if !ok {
			operation.fail(dependent.ID, http.StatusConflict, errors.New("linked Web credential is not backed by a managed auth file"))
			continue
		}
		operation.deleteExpected(dependentName, dependent, true, dependencySourceHash(dependent))
	}

	current, ok := operation.h.authManager.GetByID(auth.ID)
	if !ok || current == nil {
		if len(operation.result.failed) > failuresBeforeCascade {
			operation.fail(name, http.StatusConflict, errors.New("Codex credential disappeared after a dependent deletion failed"))
		}
		return
	}
	if coreauth.ChatGPTWebCredentialUID(current) != uid || dependencySourceHash(current) != expectedHash {
		operation.fail(name, http.StatusConflict, errors.New("Codex credential changed during cascade deletion"))
		return
	}
	if len(operation.result.failed) > failuresBeforeCascade {
		currentGraph, errGraph := operation.dependencyGraph()
		if errGraph != nil {
			operation.fail(name, http.StatusConflict, fmt.Errorf("cannot verify remaining dependencies: %w", errGraph))
			return
		}
		remaining, stillAmbiguous := currentGraph.DependentsForSource(current)
		if stillAmbiguous {
			operation.fail(name, http.StatusConflict, errors.New("codex credential UID became ambiguous during cascade deletion"))
			return
		}
		operation.retain(name, current, remaining)
		return
	}
	currentGraph, errGraph := operation.dependencyGraph()
	if errGraph != nil {
		operation.fail(name, http.StatusConflict, fmt.Errorf("cannot verify remaining dependencies: %w", errGraph))
		return
	}
	remaining, stillAmbiguous := currentGraph.DependentsForSource(current)
	if stillAmbiguous || len(remaining) > 0 {
		operation.retain(name, current, remaining)
		return
	}
	operation.deletePhysicalExpected(name, current, dependencyDelete, uid, expectedHash)
}

func (operation *authDependencyDeleteContext) allDependentsRequested(dependents []*coreauth.Auth) bool {
	if len(dependents) == 0 {
		return true
	}
	for _, dependent := range dependents {
		name, ok := operation.h.managedAuthNameForAuthAtRoot(operation.root, operation.lexicalAuthDir, operation.authDir, dependent)
		if !ok {
			return false
		}
		if _, requested := operation.requested[managedAuthNameKey(name)]; !requested {
			return false
		}
	}
	return true
}

func (operation *authDependencyDeleteContext) deletePhysical(name string, auth *coreauth.Auth, dependencyDelete bool) {
	operation.deletePhysicalExpected(name, auth, dependencyDelete, "", "")
}

func (operation *authDependencyDeleteContext) deleteExpected(name string, auth *coreauth.Auth, dependencyDelete bool, expectedSourceHash string) {
	key := managedAuthNameKey(name)
	if _, done := operation.processed[key]; done {
		return
	}
	operation.processed[key] = struct{}{}
	operation.deletePhysicalExpected(name, auth, dependencyDelete, "", expectedSourceHash)
}

func (operation *authDependencyDeleteContext) deletePhysicalExpected(name string, auth *coreauth.Auth, dependencyDelete bool, expectedSourceUID, expectedSourceHash string) {
	linkedSourceUID := coreauth.ChatGPTWebLinkedSourceUID(auth)
	deletedName, status, errDelete := operation.h.deleteAuthFileByNameAtRootExpected(
		operation.ctx,
		operation.root,
		operation.lexicalAuthDir,
		operation.authDir,
		name,
		expectedSourceHash,
		expectedSourceUID,
	)
	if errDelete != nil {
		if operation.ignoreMissingFile && (errors.Is(errDelete, errAuthFileNotFound) || errors.Is(errDelete, fs.ErrNotExist)) {
			return
		}
		operation.fail(name, status, errDelete)
		return
	}
	operation.result.deleted = append(operation.result.deleted, deletedName)
	if linkedSourceUID == "" {
		return
	}
	if _, suppressed := operation.suppressCleanup[linkedSourceUID]; suppressed || dependencyDelete {
		return
	}
	operation.cleanupRetainedSource(linkedSourceUID)
}

func (operation *authDependencyDeleteContext) validateSourceDecision(name, expectedSourceUID, expectedSourceHash string) error {
	_, displayName, errResolve := resolveManagedAuthFilePathAtRoot(operation.root, operation.authDir, name)
	if errResolve != nil {
		return errResolve
	}
	actualName, errActual := actualManagedAuthFileNameAtRoot(operation.root, displayName)
	if errActual != nil {
		return errActual
	}
	snapshot, errRead := captureManagedAuthFileSnapshotAtRoot(operation.root, filepath.FromSlash(actualName))
	if errRead != nil {
		return errRead
	}
	if !coreauth.SourceHashMatchesBytes(expectedSourceHash, snapshot.data) || !managedCodexSourceUIDMatches(snapshot.data, expectedSourceUID) {
		return errors.New("Codex credential changed before cascade deletion")
	}
	return nil
}

func (operation *authDependencyDeleteContext) cleanupRetainedSource(sourceUID string) {
	graph, errGraph := operation.dependencyGraph()
	if errGraph != nil {
		operation.fail("", http.StatusConflict, fmt.Errorf("cannot verify credential dependencies: %w", errGraph))
		return
	}
	source, ambiguous := graph.SourceByUID(sourceUID)
	if ambiguous || source == nil || !coreauth.ChatGPTWebAuthRetainedForDependents(source) {
		return
	}
	dependents, dependentAmbiguous := graph.DependentsForSource(source)
	if dependentAmbiguous || len(dependents) != 0 {
		return
	}
	name, ok := operation.h.managedAuthNameForAuthAtRoot(operation.root, operation.lexicalAuthDir, operation.authDir, source)
	if !ok {
		operation.fail(source.ID, http.StatusConflict, errors.New("retained Codex credential is not backed by a managed auth file"))
		return
	}
	deletedIDs, errReconcile := operation.h.reconcileChatGPTWebDependencies(operation.ctx, "management-delete")
	for _, deletedID := range deletedIDs {
		if deletedID == source.ID {
			operation.result.deleted = append(operation.result.deleted, name)
			return
		}
	}
	if errReconcile != nil {
		operation.fail(name, http.StatusInternalServerError, errReconcile)
	}
}

func (operation *authDependencyDeleteContext) dependencyGraph() (*coreauth.ChatGPTWebDependencyGraph, error) {
	if operation == nil || operation.h == nil || operation.root == nil {
		return nil, errors.New("managed auth root is unavailable")
	}
	byID := make(map[string]*coreauth.Auth)
	if operation.h.authManager != nil {
		auths, errList := operation.h.authManager.PersistedAuthSnapshot(operation.ctx)
		if errList != nil {
			return nil, errList
		}
		for _, auth := range auths {
			if auth != nil && strings.TrimSpace(auth.ID) != "" {
				byID[auth.ID] = auth
			}
		}
		for _, auth := range operation.h.authManager.List() {
			if auth == nil || strings.TrimSpace(auth.ID) == "" {
				continue
			}
			if _, persisted := byID[auth.ID]; !persisted {
				byID[auth.ID] = auth
			}
		}
	}
	names, errList := listAllManageableAuthFileNamesAtRoot(operation.root, operation.authDir)
	if errList != nil {
		return nil, errList
	}
	for _, name := range names {
		data, _, path, errRead := readManagedAuthFileAtRoot(operation.root, operation.authDir, name)
		if errRead != nil {
			return nil, errRead
		}
		if coreauth.IsRetiredGeminiCLIAuthFileData(data) {
			continue
		}
		auth, errBuild := operation.h.buildAuthFromFileData(path, data)
		if errBuild != nil {
			return nil, errBuild
		}
		if _, authoritative := byID[auth.ID]; !authoritative {
			byID[auth.ID] = auth
		}
	}
	auths := make([]*coreauth.Auth, 0, len(byID))
	for _, auth := range byID {
		auths = append(auths, auth)
	}
	return coreauth.BuildChatGPTWebDependencyGraph(auths), nil
}

func (operation *authDependencyDeleteContext) retain(name string, source *coreauth.Auth, dependents []*coreauth.Auth) {
	dependentNames := make([]string, 0, len(dependents))
	for _, dependent := range dependents {
		if dependentName, ok := operation.h.managedAuthNameForAuthAtRoot(operation.root, operation.lexicalAuthDir, operation.authDir, dependent); ok {
			dependentNames = append(dependentNames, dependentName)
		} else if dependent != nil {
			dependentNames = append(dependentNames, dependent.ID)
		}
	}
	sort.Strings(dependentNames)
	retainedName, status, errRetain := operation.h.retainCodexAuthAtRoot(operation.ctx, operation.root, operation.authDir, name, source)
	if errRetain != nil {
		operation.fail(name, status, errRetain)
		return
	}
	operation.result.retained = append(operation.result.retained, retainedCodexAuthResult{
		Name:           retainedName,
		DependentCount: len(dependentNames),
		DependentNames: dependentNames,
	})
}

func (operation *authDependencyDeleteContext) fail(name string, status int, err error) {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	operation.result.failed = append(operation.result.failed, gin.H{"name": name, "status": status, "error": err.Error()})
}

func (h *Handler) retainCodexAuthAtRoot(ctx context.Context, root *os.Root, authDir, name string, expected *coreauth.Auth) (string, int, error) {
	targetPath, displayName, errResolve := resolveManagedAuthFilePathAtRoot(root, authDir, name)
	if errResolve != nil {
		return "", managedAuthPathErrorStatus(errResolve), errResolve
	}
	actualName, errActual := actualManagedAuthFileNameAtRoot(root, displayName)
	if errActual != nil {
		return "", http.StatusInternalServerError, errActual
	}
	displayName = actualName
	targetPath = filepath.Join(authDir, filepath.FromSlash(displayName))
	lockedCtx, unlockAuthMutation, errLockAuth := h.authManager.LockAuthMutation(ctx, expected)
	if errLockAuth != nil {
		return displayName, http.StatusInternalServerError, errLockAuth
	}
	defer unlockAuthMutation()
	unlockOperation := lockManagedAuthFileOperation(targetPath)
	defer unlockOperation()
	snapshot, errRead := captureManagedAuthFileSnapshotAtRoot(root, filepath.FromSlash(displayName))
	if errRead != nil {
		if errors.Is(errRead, fs.ErrNotExist) {
			return displayName, http.StatusNotFound, errAuthFileNotFound
		}
		return displayName, http.StatusInternalServerError, errRead
	}
	if errAccess := validateExplicitManagedAuthFileAccess(displayName, snapshot.data); errAccess != nil {
		return displayName, http.StatusBadRequest, errAccess
	}
	if expected == nil || !strings.EqualFold(strings.TrimSpace(expected.Provider), "codex") {
		return displayName, http.StatusBadRequest, errors.New("only Codex credentials can be retained for Web dependents")
	}
	if expectedHash := strings.TrimSpace(expected.Attributes[coreauth.SourceHashAttributeKey]); expectedHash != "" && !coreauth.SourceHashMatchesBytes(expectedHash, snapshot.data) {
		return displayName, http.StatusConflict, errors.New("credential changed before retention")
	}
	updated := coreauth.RetainCodexAuthForChatGPTWebDependents(expected, time.Now().UTC())
	installed, current, errUpdate := h.authManager.UpdateIfCurrentSourceHash(coreauth.WithSkipStateCarryForward(lockedCtx), expected, updated)
	if errUpdate != nil {
		if errors.Is(errUpdate, authfileguard.ErrPersistGenerationStale) {
			return displayName, http.StatusConflict, errors.New("credential changed before retention")
		}
		return displayName, http.StatusInternalServerError, errUpdate
	}
	if !current || installed == nil {
		return displayName, http.StatusConflict, errors.New("credential changed before retention")
	}
	return displayName, http.StatusAccepted, nil
}

func (h *Handler) managedAuthForNameAtRoot(root *os.Root, lexicalAuthDir, authDir, name string) *coreauth.Auth {
	if h == nil {
		return nil
	}
	if h.authManager != nil {
		for _, auth := range h.authManager.List() {
			if auth == nil || isRuntimeOnlyAuth(auth) {
				continue
			}
			managedName, managed := managedAuthBackingFileNameAtRoot(root, lexicalAuthDir, authDir, auth)
			if managed && managedAuthNameEqual(managedName, name) {
				return auth
			}
		}
	}
	data, _, path, errRead := readManagedAuthFileAtRoot(root, authDir, name)
	if errRead == nil && !coreauth.IsRetiredGeminiCLIAuthFileData(data) {
		if auth, errBuild := h.buildAuthFromFileData(path, data); errBuild == nil {
			coreauth.SetSourceHashAttribute(auth, data)
			return auth
		}
	}
	return nil
}

func (h *Handler) managedAuthNameForAuthAtRoot(root *os.Root, lexicalAuthDir, authDir string, auth *coreauth.Auth) (string, bool) {
	if auth == nil || isRuntimeOnlyAuth(auth) {
		return "", false
	}
	name, managed := managedAuthBackingFileNameAtRoot(root, lexicalAuthDir, authDir, auth)
	return name, managed && name != ""
}

// RestoreAuthFile restores a Codex credential retained for linked Web credentials.
func (h *Handler) RestoreAuthFile(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	var request struct {
		Name string `json:"name"`
	}
	if errBind := c.ShouldBindJSON(&request); errBind != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(errBind, &maxBytesError) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body is too large"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	if strings.TrimSpace(request.Name) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth manager unavailable"})
		return
	}
	h.chatGPTWebDependencyMu.Lock()
	defer h.chatGPTWebDependencyMu.Unlock()
	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		status := http.StatusInternalServerError
		if errors.Is(errRoot, fs.ErrNotExist) {
			status = http.StatusNotFound
		}
		c.JSON(status, gin.H{"error": errRoot.Error()})
		return
	}
	defer closeManagedAuthRoot(root)
	names, errNames := h.canonicalAuthFileNamesAtRoot(root, lexicalAuthDir, authDir, []string{request.Name})
	if errNames != nil || len(names) != 1 {
		c.JSON(managedAuthPathErrorStatus(errNames), gin.H{"error": "invalid auth file name"})
		return
	}
	name := names[0]
	auth := h.managedAuthForNameAtRoot(root, lexicalAuthDir, authDir, name)
	if auth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth file not found"})
		return
	}
	installed, status, errRestore := h.restoreCodexAuthAtRoot(c.Request.Context(), root, authDir, name, auth)
	if errRestore != nil {
		c.JSON(status, gin.H{"error": errRestore.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "name": name, "disabled": installed.Disabled})
}

func (h *Handler) restoreCodexAuthAtRoot(ctx context.Context, root *os.Root, authDir, name string, expected *coreauth.Auth) (*coreauth.Auth, int, error) {
	targetPath, displayName, errResolve := resolveManagedAuthFilePathAtRoot(root, authDir, name)
	if errResolve != nil {
		return nil, managedAuthPathErrorStatus(errResolve), errResolve
	}
	actualName, errActual := actualManagedAuthFileNameAtRoot(root, displayName)
	if errActual != nil {
		return nil, http.StatusInternalServerError, errActual
	}
	displayName = actualName
	targetPath = filepath.Join(authDir, filepath.FromSlash(displayName))
	lockedCtx, unlockAuthMutation, errLockAuth := h.authManager.LockAuthMutation(ctx, expected)
	if errLockAuth != nil {
		return nil, http.StatusInternalServerError, errLockAuth
	}
	defer unlockAuthMutation()
	unlockOperation := lockManagedAuthFileOperation(targetPath)
	defer unlockOperation()
	snapshot, errRead := captureManagedAuthFileSnapshotAtRoot(root, filepath.FromSlash(displayName))
	if errRead != nil {
		if errors.Is(errRead, fs.ErrNotExist) {
			return nil, http.StatusNotFound, errAuthFileNotFound
		}
		return nil, http.StatusInternalServerError, errRead
	}
	if errAccess := validateExplicitManagedAuthFileAccess(displayName, snapshot.data); errAccess != nil {
		return nil, http.StatusBadRequest, errAccess
	}
	if expected == nil || !coreauth.ChatGPTWebAuthRetainedForDependents(expected) {
		return nil, http.StatusConflict, errors.New("credential is not retained for Web dependents")
	}
	if expectedHash := strings.TrimSpace(expected.Attributes[coreauth.SourceHashAttributeKey]); expectedHash != "" && !coreauth.SourceHashMatchesBytes(expectedHash, snapshot.data) {
		return nil, http.StatusConflict, errors.New("credential changed before restore")
	}
	updated, retained := coreauth.RestoreCodexAuthFromChatGPTWebRetention(expected, time.Now().UTC())
	if !retained {
		return nil, http.StatusConflict, errors.New("credential is not retained for Web dependents")
	}
	installed, current, errUpdate := h.authManager.UpdateIfCurrentSourceHash(coreauth.WithSkipStateCarryForward(lockedCtx), expected, updated)
	if errUpdate != nil {
		if errors.Is(errUpdate, authfileguard.ErrPersistGenerationStale) {
			return nil, http.StatusConflict, errors.New("credential changed before restore")
		}
		return nil, http.StatusInternalServerError, errUpdate
	}
	if !current || installed == nil {
		return nil, http.StatusConflict, errors.New("credential changed before restore")
	}
	return installed, http.StatusOK, nil
}

func (h *Handler) cleanupRetainedCodexSource(ctx context.Context, sourceUID string) {
	sourceUID = strings.TrimSpace(sourceUID)
	if h == nil || h.authManager == nil || sourceUID == "" {
		return
	}
	h.chatGPTWebDependencyMu.Lock()
	defer h.chatGPTWebDependencyMu.Unlock()
	deleted, errReconcile := h.reconcileChatGPTWebDependencies(ctx, "management")
	if errReconcile != nil {
		log.WithError(errReconcile).WithField("source_uid", sourceUID).Warn("failed to reconcile retained Codex source")
		return
	}
	if len(deleted) > 0 {
		log.WithField("source_uid", sourceUID).Debug("removed retained Codex source without dependents")
	}
}

func (h *Handler) reconcileChatGPTWebDependencies(ctx context.Context, reason string) ([]string, error) {
	if h == nil || h.authManager == nil {
		return nil, nil
	}
	if h.dependencyReconcileHook != nil {
		return h.dependencyReconcileHook(ctx, reason)
	}
	return h.authManager.ReconcileChatGPTWebDependencies(ctx)
}

func linkedSourceUID(auth *coreauth.Auth) string {
	return coreauth.ChatGPTWebLinkedSourceUID(auth)
}

func dependencySourceHash(auth *coreauth.Auth) string {
	if auth == nil || auth.Attributes == nil {
		return ""
	}
	return strings.TrimSpace(auth.Attributes[coreauth.SourceHashAttributeKey])
}

func managedCodexSourceUIDMatches(data []byte, expectedSourceUID string) bool {
	expectedSourceUID = strings.TrimSpace(expectedSourceUID)
	if expectedSourceUID == "" {
		return false
	}
	var envelope struct {
		Type          string `json:"type"`
		CredentialUID string `json:"credential_uid"`
	}
	if errDecode := json.Unmarshal(data, &envelope); errDecode != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(envelope.Type), "codex") && strings.TrimSpace(envelope.CredentialUID) == expectedSourceUID
}

func retainedDeletionRequestedAt(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	return strings.TrimSpace(stringValue(auth.Metadata, "deletion_requested_at"))
}

func retainedSourceMissing(auth *coreauth.Auth, graph *coreauth.ChatGPTWebDependencyGraph) bool {
	uid := coreauth.ChatGPTWebLinkedSourceUID(auth)
	if uid == "" || graph == nil {
		return false
	}
	source, ambiguous := graph.SourceByUID(uid)
	return source == nil || ambiguous || !coreauth.ChatGPTWebLinkedSourceMatches(auth, source)
}

func dependencyNames(dependents []*coreauth.Auth) []string {
	names := make([]string, 0, len(dependents))
	for _, dependent := range dependents {
		if dependent == nil {
			continue
		}
		name := strings.TrimSpace(dependent.FileName)
		if name == "" {
			name = dependent.ID
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func retainedDependencySummary(auth *coreauth.Auth, graph *coreauth.ChatGPTWebDependencyGraph) (int, []string, bool) {
	if auth == nil || graph == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return 0, nil, false
	}
	dependents, ambiguous := graph.DependentsForSource(auth)
	return len(dependents), dependencyNames(dependents), ambiguous
}

func dependencyAction(c *gin.Context, all bool) (string, error) {
	if all {
		return "cascade", nil
	}
	action := strings.ToLower(strings.TrimSpace(c.Query("dependency_action")))
	if action == "" {
		action = "retain"
	}
	if action != "retain" && action != "cascade" {
		return "", fmt.Errorf("dependency_action must be retain or cascade")
	}
	return action, nil
}
