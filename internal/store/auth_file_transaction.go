package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	internalauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type authFileSnapshot struct {
	data   []byte
	mode   fs.FileMode
	info   fs.FileInfo
	exists bool
}

type authRuntimeSnapshot struct {
	metadata        map[string]any
	attributes      map[string]string
	disabled        bool
	fileName        string
	storageSetter   internalauth.MetadataSetter
	storageMetadata map[string]any
	storageSnapshot bool
}

var errReservedAuthLockPath = errors.New("auth path uses a reserved lock name")

func rejectReservedAuthLockPath(path string) error {
	if authfileguard.IsPersistentLockPath(path) {
		return errReservedAuthLockPath
	}
	return nil
}

func captureAuthRuntimeSnapshot(auth *cliproxyauth.Auth) authRuntimeSnapshot {
	snapshot := authRuntimeSnapshot{}
	if auth == nil {
		return snapshot
	}
	if auth.Metadata != nil {
		snapshot.metadata = make(map[string]any, len(auth.Metadata))
		for key, value := range auth.Metadata {
			snapshot.metadata[key] = value
		}
	}
	if auth.Attributes != nil {
		snapshot.attributes = make(map[string]string, len(auth.Attributes))
		for key, value := range auth.Attributes {
			snapshot.attributes[key] = value
		}
	}
	snapshot.disabled = auth.Disabled
	snapshot.fileName = auth.FileName
	if setter, ok := auth.Storage.(internalauth.MetadataSetter); ok {
		if snapshotter, okSnapshot := auth.Storage.(internalauth.MetadataSnapshotter); okSnapshot {
			snapshot.storageSetter = setter
			snapshot.storageMetadata = cloneAuthMetadata(snapshotter.MetadataSnapshot())
			snapshot.storageSnapshot = true
		}
	}
	return snapshot
}

func (s authRuntimeSnapshot) restore(auth *cliproxyauth.Auth) {
	if auth == nil {
		return
	}
	auth.Metadata = s.metadata
	auth.Attributes = s.attributes
	auth.Disabled = s.disabled
	auth.FileName = s.fileName
	if s.storageSetter != nil {
		s.storageSetter.SetMetadata(cloneAuthMetadata(s.storageMetadata))
	}
}

func cloneAuthMetadata(metadata map[string]any) map[string]any {
	if metadata == nil {
		return nil
	}
	cloned := make(map[string]any, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func authSaveVerificationContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func authRollbackContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

type authDeleteProbeState uint8

const (
	authDeleteProbeAbsent authDeleteProbeState = iota
	authDeleteProbeOriginal
	authDeleteProbeReplaced
)

func removeAuthFileAtRoot(root *os.Root, name string) (err error) {
	return removeAuthFileAtRootContext(context.Background(), root, name)
}

func removeAuthFileAtRootContext(ctx context.Context, root *os.Root, name string) error {
	return removeAuthFileAtRootWithLocks(ctx, root, name, false, false, false)
}

func removeAuthFileAtRootTransactionLockedContext(ctx context.Context, root *os.Root, name string) error {
	return removeAuthFileAtRootWithLocks(ctx, root, name, true, true, false)
}

func removeAuthFileAtRootTransactionTargetLockedContext(ctx context.Context, root *os.Root, name string) error {
	return removeAuthFileAtRootWithLocks(ctx, root, name, true, true, true)
}

func removeAuthFileAtRootWithLocks(ctx context.Context, root *os.Root, name string, rootMutationLocked, pathLocked, targetLocked bool) (err error) {
	if root == nil {
		return errors.New("auth store: remove auth file: root is nil")
	}
	if !rootMutationLocked {
		unlockRootMutation, errMutationLock := authfileguard.LockRootMutationContext(ctx, root)
		if errMutationLock != nil {
			return fmt.Errorf("auth store: lock auth root for removal: %w", errMutationLock)
		}
		defer func() { err = errors.Join(err, unlockRootMutation()) }()
	}
	if !targetLocked {
		unlockTarget, errLock := lockAuthSnapshotTargetContext(ctx, root, name, pathLocked)
		if errLock != nil {
			return errLock
		}
		defer func() { err = errors.Join(err, unlockTarget()) }()
	}
	parent, leaf, closeParent, errParent := openAuthSnapshotParent(root, name)
	if errParent != nil {
		if errors.Is(errParent, fs.ErrNotExist) {
			return nil
		}
		return errParent
	}
	if closeParent != nil {
		defer func() { err = errors.Join(err, closeParent()) }()
	}
	if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
		return errParentIdentity
	}
	if errRemove := parent.Remove(leaf); errRemove != nil {
		if errors.Is(errRemove, fs.ErrNotExist) {
			return nil
		}
		return errRemove
	}
	if errSync := syncAuthSnapshotDirectory(parent); errSync != nil {
		return errSync
	}
	return revalidateAuthSnapshotParent(root, name, parent)
}

func mkdirAuthDirectoriesAtRoot(root *os.Root, relativePath string, mode fs.FileMode) error {
	if errMkdir := root.MkdirAll(relativePath, mode); errMkdir != nil {
		return errMkdir
	}
	clean := filepath.Clean(relativePath)
	if clean == "." {
		return nil
	}
	parent := "."
	for _, component := range strings.Split(clean, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		if errSync := syncAuthSnapshotDirectoryAt(root, parent); errSync != nil {
			return errSync
		}
		parent = filepath.Join(parent, component)
	}
	return nil
}

func clearAuthDirectoryAtRoot(root *os.Root) (err error) {
	if root == nil {
		return errors.New("auth store: clear auth directory: root is nil")
	}
	unlockRoot, errLock := authfileguard.LockRootRebuild(root)
	if errLock != nil {
		return fmt.Errorf("auth store: lock auth directory for reset: %w", errLock)
	}
	defer func() { err = errors.Join(err, unlockRoot()) }()
	return clearAuthDirectoryAtRootLocked(root)
}

func clearAuthDirectoryAtRootLocked(root *os.Root) error {
	_, err := clearAuthDirectoryContentsAtRoot(root)
	return err
}

func clearAuthDirectoryContentsAtRoot(root *os.Root) (keptLock bool, err error) {
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		return false, fmt.Errorf("auth store: open auth directory for reset: %w", errOpen)
	}
	entries, errRead := directory.ReadDir(-1)
	errClose := directory.Close()
	if errRead != nil || errClose != nil {
		return false, errors.Join(errRead, errClose)
	}
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		name := entry.Name()
		info, errInfo := root.Lstat(name)
		if errInfo != nil {
			return keptLock, fmt.Errorf("auth store: inspect auth mirror entry %s: %w", name, errInfo)
		}
		if authfileguard.IsPersistentLockFileName(name) && info.Mode()&os.ModeSymlink == 0 && info.Mode().IsRegular() {
			keptLock = true
			continue
		}
		if info.IsDir() {
			child, errChild := root.OpenRoot(name)
			if errChild != nil {
				return keptLock, fmt.Errorf("auth store: open auth mirror directory %s: %w", name, errChild)
			}
			childKeptLock, errClear := clearAuthDirectoryContentsAtRoot(child)
			errCloseChild := child.Close()
			if errClear != nil || errCloseChild != nil {
				return keptLock, errors.Join(errClear, errCloseChild)
			}
			if childKeptLock {
				keptLock = true
				continue
			}
		}
		if errRemove := root.Remove(name); errRemove != nil {
			return keptLock, fmt.Errorf("auth store: remove auth mirror entry %s: %w", name, errRemove)
		}
	}
	return keptLock, syncAuthSnapshotDirectory(root)
}

func syncAuthSnapshotDirectoryAt(root *os.Root, relativePath string) (err error) {
	directoryRoot := root
	if relativePath != "." {
		opened, errOpen := root.OpenRoot(relativePath)
		if errOpen != nil {
			return errOpen
		}
		directoryRoot = opened
		defer func() { err = errors.Join(err, closeAuthSnapshotRoot(opened)) }()
	}
	return syncAuthSnapshotDirectory(directoryRoot)
}

func deleteAuthFileTransaction(root *os.Root, name string, prepareDelete func(authFileSnapshot) error, deleteRemote func() error, probeRemote func() (authDeleteProbeState, error)) (resultErr error) {
	return deleteAuthFileTransactionContext(context.Background(), root, name, prepareDelete, deleteRemote, probeRemote)
}

func deleteAuthFileTransactionContext(ctx context.Context, root *os.Root, name string, prepareDelete func(authFileSnapshot) error, deleteRemote func() error, probeRemote func() (authDeleteProbeState, error)) error {
	return deleteAuthFileTransactionWithLocks(ctx, root, name, prepareDelete, deleteRemote, probeRemote, false, false, false)
}

func deleteAuthFileTransactionLockedContext(ctx context.Context, root *os.Root, name string, prepareDelete func(authFileSnapshot) error, deleteRemote func() error, probeRemote func() (authDeleteProbeState, error)) error {
	return deleteAuthFileTransactionWithLocks(ctx, root, name, prepareDelete, deleteRemote, probeRemote, true, true, false)
}

func deleteAuthFileTransactionTargetLockedContext(ctx context.Context, root *os.Root, name string, prepareDelete func(authFileSnapshot) error, deleteRemote func() error, probeRemote func() (authDeleteProbeState, error)) error {
	return deleteAuthFileTransactionWithLocks(ctx, root, name, prepareDelete, deleteRemote, probeRemote, true, true, true)
}

func deleteAuthFileTransactionWithLocks(ctx context.Context, root *os.Root, name string, prepareDelete func(authFileSnapshot) error, deleteRemote func() error, probeRemote func() (authDeleteProbeState, error), rootMutationLocked, pathLocked, targetLocked bool) (resultErr error) {
	if root == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errors.New("auth store: root is nil"))
	}
	if prepareDelete == nil || deleteRemote == nil || probeRemote == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errors.New("auth store: delete transaction is incomplete"))
	}
	if !rootMutationLocked {
		unlockRootMutation, errMutationLock := authfileguard.LockRootMutationContext(ctx, root)
		if errMutationLock != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth store: lock auth root for deletion: %w", errMutationLock))
		}
		defer func() {
			resultErr = joinAuthDeleteParentClose(resultErr, unlockRootMutation())
		}()
	}
	if !targetLocked {
		unlockTarget, errLock := lockAuthSnapshotTargetContext(ctx, root, name, pathLocked)
		if errLock != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth store: lock auth file before deletion: %w", errLock))
		}
		defer func() {
			resultErr = joinAuthDeleteParentClose(resultErr, unlockTarget())
		}()
	}
	parent, leaf, closeParent, errParent := openAuthSnapshotParent(root, name)
	if errParent != nil && !errors.Is(errParent, fs.ErrNotExist) {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth store: open auth parent: %w", errParent))
	}
	if closeParent != nil {
		defer func() {
			resultErr = joinAuthDeleteParentClose(resultErr, closeParent())
		}()
	}
	var original authFileSnapshot
	if parent != nil {
		if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errParentIdentity)
		}
		var errSnapshot error
		original, errSnapshot = captureAuthFileSnapshot(parent, leaf)
		if errSnapshot != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth store: inspect auth file before deletion: %w", errSnapshot))
		}
	}
	if errPrepare := prepareDelete(original); errPrepare != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth store: persist delete quarantine: %w", errPrepare))
	}
	if parent == nil {
		preparedParent, preparedLeaf, closePreparedParent, errPreparedParent := openAuthSnapshotParent(root, name)
		if errPreparedParent != nil && !errors.Is(errPreparedParent, fs.ErrNotExist) {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth store: reopen auth parent after persisting quarantine: %w", errPreparedParent))
		}
		if preparedParent != nil {
			parent = preparedParent
			leaf = preparedLeaf
			if closePreparedParent != nil {
				defer func() {
					resultErr = joinAuthDeleteParentClose(resultErr, closePreparedParent())
				}()
			}
		}
	}
	if parent != nil {
		if original.exists {
			if errRemove := removeExpectedAuthSnapshot(parent, leaf, original, syncAuthSnapshotDirectory); errRemove != nil {
				return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth store: delete auth file after persisting quarantine: %w", errRemove))
			}
		} else {
			current, errCurrent := captureAuthFileSnapshot(parent, leaf)
			if errCurrent != nil {
				return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth store: revalidate missing auth after persisting quarantine: %w", errCurrent))
			}
			if !sameAuthFileGeneration(current, original) {
				return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, authfileguard.ErrPersistGenerationStale)
			}
		}
		if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, errParentIdentity)
		}
	}

	errDelete := deleteRemote()
	remoteState, errProbe := probeRemote()
	if errProbe != nil {
		return cliproxyauth.NewDeleteOutcomeError(
			cliproxyauth.DeleteOutcomeUncertain,
			errors.Join(errDelete, fmt.Errorf("auth store: probe remote deletion: %w", errProbe)),
		)
	}
	switch remoteState {
	case authDeleteProbeAbsent:
		if errDelete == nil {
			return nil
		}
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeCommitted, errDelete)
	case authDeleteProbeReplaced:
		if errDelete == nil {
			errDelete = errors.New("auth store: remote auth was replaced while confirming deletion")
		}
		return cliproxyauth.NewDeleteOutcomeError(
			cliproxyauth.DeleteOutcomeUncertain,
			fmt.Errorf("auth store: remote auth was replaced while confirming deletion: %w", errDelete),
		)
	default:
		if errDelete == nil {
			errDelete = errors.New("auth store: remote auth still exists after deletion")
		}
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, errDelete)
	}
}

func durableAuthDelete(ctx context.Context, configPath, authDir, path string, expectedData []byte, backendKey, backendIdentity string, retrySafe, remoteExists bool, remoteData []byte) (context.Context, func() error, func() error) {
	if ctx == nil {
		ctx = context.Background()
	}
	generation := authfileguard.DeleteGenerationFromContext(ctx)
	if generation == nil {
		generation = authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(expectedData))
	}
	deleteCtx := authfileguard.WithDeleteGeneration(ctx, generation)
	prepare := func() error {
		if remoteExists {
			expectedHash := generation.ExpectedHash()
			if expectedHash != "" && !cliproxyauth.SourceHashMatchesBytes(expectedHash, remoteData) {
				return authfileguard.ErrPersistGenerationStale
			}
		}
		if generation.CheckBackendIdentity(backendKey, backendIdentity, retrySafe, authfileguard.DeleteAttempt(ctx) == 0) != authfileguard.DeleteIdentityMatched {
			return authfileguard.ErrDeleteGenerationUncertain
		}
		return watcher.PersistAuthDeleteQuarantine(configPath, authDir, path, generation)
	}
	clear := func() error {
		return watcher.ClearAuthDeleteQuarantine(configPath, authDir, path, generation)
	}
	return deleteCtx, prepare, clear
}

func finishDurableAuthDelete(errDelete error, clear func() error) error {
	if !deleteOutcomeIsCommitted(errDelete) || clear == nil {
		return errDelete
	}
	if errClear := clear(); errClear != nil {
		return cliproxyauth.NewDeleteOutcomeError(
			cliproxyauth.DeleteOutcomeCommitted,
			errors.Join(errDelete, fmt.Errorf("auth store: clear delete quarantine: %w", errClear)),
		)
	}
	return errDelete
}

func joinAuthDeleteParentClose(resultErr, errClose error) error {
	if errClose == nil {
		return resultErr
	}
	if deleteOutcomeIsCommitted(resultErr) {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeCommitted, errors.Join(resultErr, errClose))
	}
	return errors.Join(resultErr, errClose)
}

func deleteOutcomeIsCommitted(err error) bool {
	if err == nil {
		return true
	}
	outcome, ok := cliproxyauth.DeleteOutcomeFromError(err)
	return ok && outcome == cliproxyauth.DeleteOutcomeCommitted
}

func probeAuthDeleteResult(ctx context.Context, probe func(context.Context) (authDeleteProbeState, error)) (authDeleteProbeState, error) {
	return probe(authRollbackContext(ctx))
}

type authDeleteGenerationResult uint8

const (
	authDeleteGenerationUncertain authDeleteGenerationResult = iota
	authDeleteGenerationMatched
	authDeleteGenerationReplaced
)

func matchExpectedAuthDeleteGeneration(ctx context.Context, backendKey, backendIdentity string, retrySafe bool, remoteData []byte) authDeleteGenerationResult {
	generation := authfileguard.DeleteGenerationFromContext(ctx)
	if generation == nil {
		return authDeleteGenerationMatched
	}
	expectedHash := generation.ExpectedHash()
	if expectedHash != "" && !cliproxyauth.SourceHashMatchesBytes(expectedHash, remoteData) {
		return authDeleteGenerationReplaced
	}
	if generation.CheckBackendIdentity(backendKey, backendIdentity, retrySafe, authfileguard.DeleteIdentityBindingAllowed(ctx)) != authfileguard.DeleteIdentityMatched {
		return authDeleteGenerationUncertain
	}
	return authDeleteGenerationMatched
}

func captureAuthFileSnapshot(root *os.Root, name string) (snapshot authFileSnapshot, err error) {
	if root == nil {
		return authFileSnapshot{}, errors.New("auth store: capture auth file before persistence: root is nil")
	}
	parent, leaf, closeParent, errParent := openAuthSnapshotParent(root, name)
	if errParent != nil {
		if errors.Is(errParent, fs.ErrNotExist) {
			return authFileSnapshot{}, nil
		}
		return authFileSnapshot{}, fmt.Errorf("auth store: capture auth file before persistence: %w", errParent)
	}
	if closeParent != nil {
		defer func() {
			if errClose := closeParent(); errClose != nil {
				err = errors.Join(err, errClose)
			}
		}()
	}

	before, errBefore := parent.Lstat(leaf)
	if errBefore != nil {
		if errors.Is(errBefore, fs.ErrNotExist) {
			return authFileSnapshot{}, nil
		}
		return authFileSnapshot{}, fmt.Errorf("auth store: capture auth file before persistence: inspect %s: %w", name, errBefore)
	}
	if errValidate := validateAuthSnapshotRegularFile(name, before); errValidate != nil {
		return authFileSnapshot{}, errValidate
	}

	file, errOpen := openAuthSnapshotFile(parent, leaf)
	if errOpen != nil {
		return authFileSnapshot{}, fmt.Errorf("auth store: capture auth file before persistence: open %s: %w", name, errOpen)
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("auth store: close snapshot file %s: %w", name, errClose))
		}
	}()
	opened, errOpened := file.Stat()
	if errOpened != nil {
		return authFileSnapshot{}, fmt.Errorf("auth store: capture auth file before persistence: inspect opened %s: %w", name, errOpened)
	}
	if errValidate := validateAuthSnapshotRegularFile(name, opened); errValidate != nil {
		return authFileSnapshot{}, errValidate
	}
	after, errAfter := parent.Lstat(leaf)
	if errAfter != nil {
		return authFileSnapshot{}, fmt.Errorf("auth store: capture auth file before persistence: re-inspect %s: %w", name, errAfter)
	}
	if errValidate := validateAuthSnapshotRegularFile(name, after); errValidate != nil {
		return authFileSnapshot{}, errValidate
	}
	if !os.SameFile(before, opened) || !os.SameFile(after, opened) {
		return authFileSnapshot{}, fmt.Errorf("auth store: capture auth file before persistence: %s changed while opening", name)
	}

	data, errRead := io.ReadAll(file)
	if errRead != nil {
		return authFileSnapshot{}, fmt.Errorf("auth store: capture auth file before persistence: read %s: %w", name, errRead)
	}
	return authFileSnapshot{data: data, mode: opened.Mode().Perm(), info: opened, exists: true}, nil
}

func sameAuthFileGeneration(current, expected authFileSnapshot) bool {
	if current.exists != expected.exists {
		return false
	}
	if !current.exists {
		return true
	}
	if current.info == nil || expected.info == nil || !os.SameFile(current.info, expected.info) {
		return false
	}
	return current.mode.Perm() == expected.mode.Perm() && bytes.Equal(current.data, expected.data)
}

func lockAuthSnapshotTarget(root *os.Root, relativePath string) (func() error, error) {
	return lockAuthSnapshotTargetContext(context.Background(), root, relativePath, false)
}

func lockAuthSnapshotTargetContext(ctx context.Context, root *os.Root, relativePath string, pathLocked bool) (func() error, error) {
	var unlockPath func()
	if !pathLocked {
		var errPath error
		unlockPath, errPath = authfileguard.LockContext(ctx, filepath.Join(root.Name(), filepath.FromSlash(relativePath)))
		if errPath != nil {
			return nil, errPath
		}
	}
	unlockTarget, errTarget := authfileguard.LockRootTargetContext(ctx, root, relativePath)
	if errTarget != nil {
		if unlockPath != nil {
			unlockPath()
		}
		return nil, errTarget
	}
	return func() error {
		errUnlock := unlockTarget()
		if unlockPath != nil {
			unlockPath()
		}
		return errUnlock
	}, nil
}

func writeAuthFileAtomicallyAtRoot(root *os.Root, name string, data []byte, expected *authFileSnapshot) error {
	return writeAuthFileAtomicallyAtRootContext(context.Background(), root, name, data, expected)
}

func writeAuthFileAtomicallyAtRootContext(ctx context.Context, root *os.Root, name string, data []byte, expected *authFileSnapshot) error {
	return writeAuthFileAtomicallyAtRootWithPolicyContext(ctx, root, name, data, expected, false, 0o600, syncAuthSnapshotDirectory, false, false, false)
}

func writeAuthFileAtomicallyAtRootTransactionLockedContext(ctx context.Context, root *os.Root, name string, data []byte, expected *authFileSnapshot) error {
	return writeAuthFileAtomicallyAtRootWithPolicyContext(ctx, root, name, data, expected, false, 0o600, syncAuthSnapshotDirectory, true, true, false)
}

func writeAuthFileAtomicallyAtRootTransactionTargetLockedContext(ctx context.Context, root *os.Root, name string, data []byte, expected *authFileSnapshot) error {
	return writeAuthFileAtomicallyAtRootWithPolicyContext(ctx, root, name, data, expected, false, 0o600, syncAuthSnapshotDirectory, true, true, true)
}

func writeAuthFileAtomicallyAtRootWithReceipt(root *os.Root, name string, data []byte, expected *authFileSnapshot) (authFileSnapshot, error) {
	return writeAuthFileAtomicallyAtRootWithReceiptContext(context.Background(), root, name, data, expected)
}

func writeAuthFileAtomicallyAtRootWithReceiptContext(ctx context.Context, root *os.Root, name string, data []byte, expected *authFileSnapshot) (authFileSnapshot, error) {
	var installed authFileSnapshot
	err := writeAuthFileAtomicallyAtRootWithPolicyAndReceipt(ctx, root, name, data, expected, false, 0o600, syncAuthSnapshotDirectory, false, false, false, &installed)
	return installed, err
}

func writeAuthFileAtomicallyAtRootWithReceiptTransactionLockedContext(ctx context.Context, root *os.Root, name string, data []byte, expected *authFileSnapshot) (authFileSnapshot, error) {
	var installed authFileSnapshot
	err := writeAuthFileAtomicallyAtRootWithPolicyAndReceipt(ctx, root, name, data, expected, false, 0o600, syncAuthSnapshotDirectory, true, true, false, &installed)
	return installed, err
}

func writeAuthFileAtomicallyAtRootWithReceiptTransactionTargetLockedContext(ctx context.Context, root *os.Root, name string, data []byte, expected *authFileSnapshot) (authFileSnapshot, error) {
	var installed authFileSnapshot
	err := writeAuthFileAtomicallyAtRootWithPolicyAndReceipt(ctx, root, name, data, expected, false, 0o600, syncAuthSnapshotDirectory, true, true, true, &installed)
	return installed, err
}

func writeAuthMirrorFileAtomicallyAtRoot(root *os.Root, name string, data []byte, expected *authFileSnapshot) error {
	return writeAuthMirrorFileAtomicallyAtRootContext(context.Background(), root, name, data, expected)
}

func writeAuthMirrorFileAtomicallyAtRootContext(ctx context.Context, root *os.Root, name string, data []byte, expected *authFileSnapshot) error {
	return writeAuthFileAtomicallyAtRootWithPolicyContext(ctx, root, name, data, expected, true, 0o600, syncAuthSnapshotDirectory, false, false, false)
}

func writeAuthMirrorFileAtomicallyAtRootLocked(root *os.Root, name string, data []byte, expected *authFileSnapshot) error {
	return writeAuthMirrorFileAtomicallyAtRootLockedContext(context.Background(), root, name, data, expected)
}

func writeAuthMirrorFileAtomicallyAtRootLockedContext(ctx context.Context, root *os.Root, name string, data []byte, expected *authFileSnapshot) error {
	return writeAuthFileAtomicallyAtRootWithPolicyAndReceipt(ctx, root, name, data, expected, true, 0o600, syncAuthSnapshotDirectory, true, false, false, nil)
}

func writeAuthFileAtomicallyAtRootWithPolicy(root *os.Root, name string, data []byte, expected *authFileSnapshot, allowRetired bool, mode fs.FileMode, syncDirectory func(*os.Root) error) error {
	return writeAuthFileAtomicallyAtRootWithPolicyContext(context.Background(), root, name, data, expected, allowRetired, mode, syncDirectory, false, false, false)
}

func writeAuthFileAtomicallyAtRootWithPolicyContext(ctx context.Context, root *os.Root, name string, data []byte, expected *authFileSnapshot, allowRetired bool, mode fs.FileMode, syncDirectory func(*os.Root) error, rootMutationLocked, pathLocked, targetLocked bool) (err error) {
	return writeAuthFileAtomicallyAtRootWithPolicyAndReceipt(ctx, root, name, data, expected, allowRetired, mode, syncDirectory, rootMutationLocked, pathLocked, targetLocked, nil)
}

func writeAuthFileAtomicallyAtRootWithPolicyAndReceipt(ctx context.Context, root *os.Root, name string, data []byte, expected *authFileSnapshot, allowRetired bool, mode fs.FileMode, syncDirectory func(*os.Root) error, rootMutationLocked, pathLocked, targetLocked bool, installedOut *authFileSnapshot) (err error) {
	if root == nil {
		return errors.New("auth store: write auth file: root is nil")
	}
	if syncDirectory == nil {
		syncDirectory = syncAuthSnapshotDirectory
	}
	committed := false
	if !rootMutationLocked {
		unlockRootMutation, errMutationLock := authfileguard.LockRootMutationContext(ctx, root)
		if errMutationLock != nil {
			return fmt.Errorf("auth store: lock auth root for write: %w", errMutationLock)
		}
		defer func() { err = joinAuthSaveCleanupError(err, unlockRootMutation(), committed) }()
	}
	var installCleanupWarning error
	parent, leaf, closeParent, errParent := openAuthSnapshotParent(root, name)
	if errParent != nil {
		return fmt.Errorf("auth store: open auth parent for write: %w", errParent)
	}
	if closeParent != nil {
		defer func() { err = joinAuthSaveCleanupError(err, closeParent(), committed) }()
	}
	tempName := ".auth-write-" + uuid.NewString()
	stagingOwned := true
	file, errOpen := parent.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errOpen != nil {
		return fmt.Errorf("auth store: create temporary auth file: %w", errOpen)
	}
	defer func() {
		if !stagingOwned {
			return
		}
		if errRemove := parent.Remove(tempName); errRemove == nil {
			err = joinAuthSaveCleanupError(err, syncDirectory(parent), committed)
		} else if !errors.Is(errRemove, fs.ErrNotExist) {
			err = joinAuthSaveCleanupError(err, fmt.Errorf("auth store: remove temporary auth file: %w", errRemove), committed)
		}
	}()
	if errStage := stageAuthSnapshotFile(file, data, mode); errStage != nil {
		return errStage
	}
	stagedSnapshot, errStaged := captureAuthFileSnapshot(parent, tempName)
	if errStaged != nil {
		return fmt.Errorf("auth store: inspect staged auth file: %w", errStaged)
	}
	if !targetLocked {
		unlockTarget, errLock := lockAuthSnapshotTargetContext(ctx, root, name, pathLocked)
		if errLock != nil {
			return fmt.Errorf("auth store: lock auth target for write: %w", errLock)
		}
		defer func() { err = joinAuthSaveCleanupError(err, unlockTarget(), committed) }()
	}
	if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
		return errParentIdentity
	}
	if expected != nil {
		current, errCapture := captureAuthFileSnapshot(parent, leaf)
		if errCapture != nil {
			return errCapture
		}
		if !allowRetired {
			if errRetired := current.rejectRetiredGeminiCLIAuthPersistence(); errRetired != nil {
				return errRetired
			}
		}
		if !sameAuthFileGeneration(current, *expected) {
			return authfileguard.ErrPersistGenerationStale
		}
	}
	var movedOriginal string
	if expected != nil && !expected.exists {
		cleanupWarning, errInstall := authfileguard.InstallStagedFileNoReplace(parent, tempName, leaf)
		if errInstall != nil {
			return fmt.Errorf("auth store: install auth file: %w", errInstall)
		}
		if cleanupWarning != nil {
			installCleanupWarning = fmt.Errorf("auth store: clean up staged auth file: %w", cleanupWarning)
		}
		stagingOwned = errors.Is(cleanupWarning, authfileguard.ErrStagedFileCleanupRequired)
	} else if expected != nil {
		var errExchange error
		movedOriginal, errExchange = exchangeExpectedAuthSnapshot(parent, tempName, leaf, *expected, syncDirectory)
		rollbackConfirmed := authExchangeRollbackConfirmed(errExchange)
		if movedOriginal != "" && !rollbackConfirmed {
			stagingOwned = false
		}
		if errExchange != nil {
			if rollbackConfirmed {
				errCleanup := discardMovedAuthSnapshot(parent, movedOriginal, syncDirectory)
				return cliproxyauth.NewSaveOutcomeError(
					cliproxyauth.SaveOutcomeRolledBack,
					errors.Join(
						fmt.Errorf("auth store: exchange auth file generation: %w", errExchange),
						wrapOptionalError("auth store: remove linked previous auth generation", errCleanup),
					),
				)
			}
			if movedOriginal != "" || errors.Is(errExchange, authfileguard.ErrExchangeOutcomeUncertain) {
				return cliproxyauth.NewSaveOutcomeError(
					cliproxyauth.SaveOutcomeUncertain,
					fmt.Errorf("auth store: exchange auth file generation: %w", errExchange),
				)
			}
			return fmt.Errorf("auth store: exchange auth file generation: %w", errExchange)
		}
	} else if errRename := parent.Rename(tempName, leaf); errRename != nil {
		return fmt.Errorf("auth store: replace auth file: %w", errRename)
	} else {
		stagingOwned = false
	}
	installedSnapshot, errInstalled := captureAuthFileSnapshot(parent, leaf)
	if errInstalled != nil || !sameAuthFileGeneration(installedSnapshot, stagedSnapshot) {
		return cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			errors.Join(errInstalled, errors.New("auth store: installed auth generation changed before directory sync")),
		)
	}
	if installedOut != nil {
		*installedOut = installedSnapshot
	}
	if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
		if movedOriginal != "" {
			errParentIdentity = errors.Join(
				errParentIdentity,
				restoreAuthSnapshotFromMovedOriginal(parent, leaf, movedOriginal, installedSnapshot, syncDirectory),
			)
		}
		return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeUncertain, errParentIdentity)
	}
	if errSync := syncDirectory(parent); errSync != nil {
		errParentIdentity := revalidateAuthSnapshotParent(root, name, parent)
		if expected == nil {
			return cliproxyauth.NewSaveOutcomeError(
				cliproxyauth.SaveOutcomeUncertain,
				errors.Join(errSync, errParentIdentity, installCleanupWarning),
			)
		}
		var errRollback error
		if movedOriginal != "" {
			errRollback = restoreAuthSnapshotFromMovedOriginal(parent, leaf, movedOriginal, installedSnapshot, syncDirectory)
		} else {
			errRollback = restoreAuthSnapshotAfterSyncFailure(parent, leaf, expected, &installedSnapshot, syncDirectory)
		}
		errResult := errors.Join(errSync, installCleanupWarning)
		if errRollback != nil || errParentIdentity != nil {
			errResult = errors.Join(
				errResult,
				wrapOptionalError("auth store: restore auth after directory sync failure", errRollback),
				wrapOptionalError("auth store: auth parent changed during failed directory sync", errParentIdentity),
			)
			return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeUncertain, errResult)
		}
		return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, errResult)
	}
	if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
		var errRollback error
		if expected != nil {
			if movedOriginal != "" {
				errRollback = restoreAuthSnapshotFromMovedOriginal(parent, leaf, movedOriginal, installedSnapshot, syncDirectory)
			} else {
				errRollback = restoreAuthSnapshotAfterSyncFailure(parent, leaf, expected, &installedSnapshot, syncDirectory)
			}
		}
		return cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			errors.Join(
				fmt.Errorf("auth store: auth parent changed during directory sync: %w", errParentIdentity),
				wrapOptionalError("auth store: restore auth after parent replacement", errRollback),
				installCleanupWarning,
			),
		)
	}
	committed = true
	if movedOriginal != "" {
		if errCleanup := discardMovedAuthSnapshot(parent, movedOriginal, syncDirectory); errCleanup != nil {
			return cliproxyauth.NewSaveOutcomeError(
				cliproxyauth.SaveOutcomeCommitted,
				fmt.Errorf("auth store: remove previous auth generation: %w", errCleanup),
			)
		}
	}
	if installCleanupWarning != nil {
		return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeCommitted, installCleanupWarning)
	}
	return nil
}

func authExchangeRollbackConfirmed(err error) bool {
	if outcome, explicit := cliproxyauth.SaveOutcomeFromError(err); explicit {
		return outcome == cliproxyauth.SaveOutcomeRolledBack
	}
	return errors.Is(err, authfileguard.ErrExchangeCleanupRequired) &&
		!errors.Is(err, authfileguard.ErrExchangeOutcomeUncertain)
}

func joinAuthSaveCleanupError(resultErr, cleanupErr error, committed bool) error {
	if cleanupErr == nil {
		return resultErr
	}
	joined := errors.Join(resultErr, cleanupErr)
	if committed {
		return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeCommitted, joined)
	}
	if outcome, explicit := cliproxyauth.SaveOutcomeFromError(resultErr); explicit {
		return cliproxyauth.NewSaveOutcomeError(outcome, joined)
	}
	return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, joined)
}

func stageAuthSnapshotFile(file *os.File, data []byte, mode fs.FileMode) (err error) {
	return stageAuthSnapshotFileWithMode(file, data, mode, true)
}

func stageExactAuthSnapshotFile(file *os.File, data []byte, mode fs.FileMode) (err error) {
	return stageAuthSnapshotFileWithMode(file, data, mode, false)
}

func stageAuthSnapshotFileWithMode(file *os.File, data []byte, mode fs.FileMode, defaultZeroMode bool) (err error) {
	if file == nil {
		return errors.New("auth store: temporary auth file is nil")
	}
	if defaultZeroMode && mode.Perm() == 0 {
		mode = 0o600
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, file.Close())
		}
	}()
	if _, errWrite := file.Write(data); errWrite != nil {
		return fmt.Errorf("auth store: write temporary auth file: %w", errWrite)
	}
	if errChmod := file.Chmod(mode.Perm()); errChmod != nil {
		return fmt.Errorf("auth store: chmod temporary auth file: %w", errChmod)
	}
	if errSync := file.Sync(); errSync != nil {
		return fmt.Errorf("auth store: sync temporary auth file: %w", errSync)
	}
	if errClose := file.Close(); errClose != nil {
		closed = true
		return fmt.Errorf("auth store: close temporary auth file: %w", errClose)
	}
	closed = true
	return nil
}

func moveExpectedAuthSnapshotAside(parent *os.Root, leaf string, expected authFileSnapshot, syncDirectory func(*os.Root) error) (string, error) {
	movedName := ".auth-displaced-" + uuid.NewString()
	if errMove := parent.Rename(leaf, movedName); errMove != nil {
		return "", errMove
	}
	moved, errMoved := captureAuthFileSnapshot(parent, movedName)
	if errMoved == nil && sameAuthFileGeneration(moved, expected) {
		return movedName, nil
	}
	errRestore := restoreMovedAuthSnapshot(parent, movedName, leaf, syncDirectory)
	return "", errors.Join(errMoved, authfileguard.ErrPersistGenerationStale, errRestore)
}

func exchangeExpectedAuthSnapshot(parent *os.Root, stagedName, leaf string, expected authFileSnapshot, syncDirectory func(*os.Root) error) (string, error) {
	stagedInfo, errStaged := parent.Lstat(stagedName)
	if errStaged != nil {
		return "", errStaged
	}
	if stagedInfo.Mode()&os.ModeSymlink != 0 || !stagedInfo.Mode().IsRegular() {
		return "", errors.New("auth store: staged auth generation is not a regular file")
	}
	displacedName, errExchange := authfileguard.ExchangeStagedFile(parent, stagedName, leaf)
	if errExchange != nil {
		return displacedName, errExchange
	}
	displaced, errDisplaced := captureAuthFileSnapshot(parent, displacedName)
	if errDisplaced == nil && sameAuthFileGeneration(displaced, expected) {
		return displacedName, nil
	}
	restoredDisplaced, errRestore := authfileguard.ExchangeStagedFile(parent, displacedName, leaf)
	if errRestore != nil {
		return displacedName, cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			errors.Join(errDisplaced, authfileguard.ErrPersistGenerationStale, errRestore),
		)
	}
	restoredInfo, errRestored := parent.Lstat(restoredDisplaced)
	if errRestored != nil || !os.SameFile(restoredInfo, stagedInfo) {
		return restoredDisplaced, cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			errors.Join(errDisplaced, errRestored, authfileguard.ErrPersistGenerationStale),
		)
	}
	errCleanup := discardMovedAuthSnapshot(parent, restoredDisplaced, syncDirectory)
	if errCleanup != nil {
		return restoredDisplaced, cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			errors.Join(errDisplaced, authfileguard.ErrPersistGenerationStale, errCleanup),
		)
	}
	return "", errors.Join(errDisplaced, authfileguard.ErrPersistGenerationStale)
}

func restoreMovedAuthSnapshot(parent *os.Root, movedName, leaf string, syncDirectory func(*os.Root) error) error {
	cleanupWarning, errRestore := authfileguard.InstallStagedFileNoReplace(parent, movedName, leaf)
	if errRestore != nil {
		if errors.Is(errRestore, authfileguard.ErrPersistGenerationStale) {
			return errors.Join(errRestore, syncDirectory(parent))
		}
		return errRestore
	}
	if errors.Is(cleanupWarning, authfileguard.ErrStagedFileCleanupRequired) {
		return errors.Join(cleanupWarning, discardMovedAuthSnapshot(parent, movedName, syncDirectory))
	}
	return errors.Join(cleanupWarning, syncDirectory(parent))
}

func discardMovedAuthSnapshot(parent *os.Root, movedName string, syncDirectory func(*os.Root) error) error {
	errRemove := parent.Remove(movedName)
	if errors.Is(errRemove, fs.ErrNotExist) {
		errRemove = nil
	}
	return errors.Join(errRemove, syncDirectory(parent))
}

func removeExpectedAuthSnapshot(parent *os.Root, leaf string, expected authFileSnapshot, syncDirectory func(*os.Root) error) error {
	movedName, errMove := moveExpectedAuthSnapshotAside(parent, leaf, expected, syncDirectory)
	if errMove != nil {
		return errMove
	}
	return discardMovedAuthSnapshot(parent, movedName, syncDirectory)
}

func restoreAuthSnapshotFromMovedOriginal(parent *os.Root, leaf, movedOriginal string, installed authFileSnapshot, syncDirectory func(*os.Root) error) error {
	movedInstalled, errExchange := exchangeExpectedAuthSnapshot(parent, movedOriginal, leaf, installed, syncDirectory)
	if errExchange != nil {
		return errExchange
	}
	return discardMovedAuthSnapshot(parent, movedInstalled, syncDirectory)
}

func restoreAuthSnapshotAfterSyncFailure(parent *os.Root, leaf string, expected, installed *authFileSnapshot, syncDirectory func(*os.Root) error) (err error) {
	if expected == nil || installed == nil {
		return nil
	}
	if !expected.exists {
		movedInstalled, errMove := moveExpectedAuthSnapshotAside(parent, leaf, *installed, syncDirectory)
		if errMove != nil {
			return errMove
		}
		return discardMovedAuthSnapshot(parent, movedInstalled, syncDirectory)
	}
	tempName := ".auth-restore-" + uuid.NewString()
	file, errOpen := parent.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errOpen != nil {
		return errOpen
	}
	defer func() {
		if errRemove := parent.Remove(tempName); errRemove == nil {
			err = errors.Join(err, syncDirectory(parent))
		} else if !errors.Is(errRemove, fs.ErrNotExist) {
			err = errors.Join(err, errRemove)
		}
	}()
	if errStage := stageExactAuthSnapshotFile(file, expected.data, expected.mode); errStage != nil {
		return errStage
	}
	movedInstalled, errExchange := exchangeExpectedAuthSnapshot(parent, tempName, leaf, *installed, syncDirectory)
	if errExchange != nil {
		return errExchange
	}
	return discardMovedAuthSnapshot(parent, movedInstalled, syncDirectory)
}

func restoreAuthFileSnapshotAtRoot(root *os.Root, name string, installed, original authFileSnapshot) error {
	return restoreAuthFileSnapshotAtRootContext(context.Background(), root, name, installed, original)
}

func restoreAuthFileSnapshotAtRootContext(ctx context.Context, root *os.Root, name string, installed, original authFileSnapshot) error {
	return restoreAuthFileSnapshotAtRootWithLocks(ctx, root, name, installed, original, false, false, false)
}

func restoreAuthFileSnapshotAtRootTransactionLockedContext(ctx context.Context, root *os.Root, name string, installed, original authFileSnapshot) error {
	return restoreAuthFileSnapshotAtRootWithLocks(ctx, root, name, installed, original, true, true, false)
}

func restoreAuthFileSnapshotAtRootTransactionTargetLockedContext(ctx context.Context, root *os.Root, name string, installed, original authFileSnapshot) error {
	return restoreAuthFileSnapshotAtRootWithLocks(ctx, root, name, installed, original, true, true, true)
}

func restoreAuthFileSnapshotAtRootWithLocks(ctx context.Context, root *os.Root, name string, installed, original authFileSnapshot, rootMutationLocked, pathLocked, targetLocked bool) (err error) {
	if root == nil {
		return errors.New("auth store: restore auth snapshot: root is nil")
	}
	committed := false
	if !rootMutationLocked {
		unlockRootMutation, errMutationLock := authfileguard.LockRootMutationContext(ctx, root)
		if errMutationLock != nil {
			return fmt.Errorf("auth store: lock auth root for rollback: %w", errMutationLock)
		}
		defer func() { err = joinAuthSaveCleanupError(err, unlockRootMutation(), committed) }()
	}
	parent, leaf, closeParent, errParent := openAuthSnapshotParent(root, name)
	if errParent != nil {
		return fmt.Errorf("auth store: open auth parent for rollback: %w", errParent)
	}
	if closeParent != nil {
		defer func() { err = joinAuthSaveCleanupError(err, closeParent(), committed) }()
	}
	if !targetLocked {
		unlockTarget, errLock := lockAuthSnapshotTargetContext(ctx, root, name, pathLocked)
		if errLock != nil {
			return fmt.Errorf("auth store: lock local auth during rollback: %w", errLock)
		}
		defer func() { err = joinAuthSaveCleanupError(err, unlockTarget(), committed) }()
	}
	if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
		return errParentIdentity
	}
	current, errCurrent := captureAuthFileSnapshot(parent, leaf)
	if errCurrent != nil {
		return fmt.Errorf("auth store: inspect local auth before rollback: %w", errCurrent)
	}
	if !sameAuthFileGeneration(current, installed) {
		return authfileguard.ErrPersistGenerationStale
	}
	if original.exists {
		if bytes.Equal(current.data, original.data) && current.mode.Perm() == original.mode.Perm() {
			committed = true
			return nil
		}
		if errRetired := current.rejectRetiredGeminiCLIAuthPersistence(); errRetired != nil {
			return errRetired
		}
		if errWrite := restoreAuthSnapshotAfterSyncFailure(parent, leaf, &original, &current, syncAuthSnapshotDirectory); errWrite != nil {
			return fmt.Errorf("auth store: restore local auth snapshot: %w", errWrite)
		}
		if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
			return errParentIdentity
		}
		committed = true
		return nil
	}
	if errRemove := removeExpectedAuthSnapshot(parent, leaf, current, syncAuthSnapshotDirectory); errRemove != nil {
		return fmt.Errorf("auth store: remove local auth during rollback: %w", errRemove)
	}
	if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
		return errParentIdentity
	}
	committed = true
	return nil
}

func rollBackCommittedAuthFileWriteAtRoot(root *os.Root, name string, installed, original authFileSnapshot, writeErr error) error {
	return rollBackCommittedAuthFileWriteAtRootContext(context.Background(), root, name, installed, original, writeErr)
}

func rollBackCommittedAuthFileWriteAtRootContext(ctx context.Context, root *os.Root, name string, installed, original authFileSnapshot, writeErr error) error {
	return rollBackCommittedAuthFileWriteAtRootWithLocks(ctx, root, name, installed, original, writeErr, false, false, false)
}

func rollBackCommittedAuthFileWriteAtRootTransactionLockedContext(ctx context.Context, root *os.Root, name string, installed, original authFileSnapshot, writeErr error) error {
	return rollBackCommittedAuthFileWriteAtRootWithLocks(ctx, root, name, installed, original, writeErr, true, true, false)
}

func rollBackCommittedAuthFileWriteAtRootTransactionTargetLockedContext(ctx context.Context, root *os.Root, name string, installed, original authFileSnapshot, writeErr error) error {
	return rollBackCommittedAuthFileWriteAtRootWithLocks(ctx, root, name, installed, original, writeErr, true, true, true)
}

func rollBackCommittedAuthFileWriteAtRootWithLocks(ctx context.Context, root *os.Root, name string, installed, original authFileSnapshot, writeErr error, rootMutationLocked, pathLocked, targetLocked bool) error {
	outcome, explicit := cliproxyauth.SaveOutcomeFromError(writeErr)
	if !explicit || outcome != cliproxyauth.SaveOutcomeCommitted {
		return writeErr
	}
	errRollback := restoreAuthFileSnapshotAtRootWithLocks(ctx, root, name, installed, original, rootMutationLocked, pathLocked, targetLocked)
	errResult := errors.Join(writeErr, wrapOptionalError("auth store: roll back committed local auth write", errRollback))
	if authFileRollbackCompleted(errRollback) {
		return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, errResult)
	}
	return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeUncertain, errResult)
}

func authFileRollbackCompleted(err error) bool {
	if err == nil {
		return true
	}
	outcome, explicit := cliproxyauth.SaveOutcomeFromError(err)
	return explicit && outcome == cliproxyauth.SaveOutcomeCommitted
}

func mapAuthCreateGenerationConflict(requireAbsent bool, err error) error {
	if !requireAbsent || !errors.Is(err, authfileguard.ErrPersistGenerationStale) {
		return err
	}
	outcome, explicit := cliproxyauth.SaveOutcomeFromError(err)
	if explicit && outcome != cliproxyauth.SaveOutcomeRolledBack {
		return err
	}
	mapped := errors.Join(cliproxyauth.ErrAuthAlreadyExists, err)
	if explicit {
		return cliproxyauth.NewSaveOutcomeError(outcome, mapped)
	}
	return mapped
}

func removeAuthFileAtRootForSnapshot(root *os.Root, name string, expected authFileSnapshot) (err error) {
	return removeAuthFileAtRootForSnapshotContext(context.Background(), root, name, expected)
}

func removeAuthFileAtRootForSnapshotContext(ctx context.Context, root *os.Root, name string, expected authFileSnapshot) error {
	return removeAuthFileAtRootForSnapshotWithLocks(ctx, root, name, expected, false, false)
}

func removeAuthFileAtRootForSnapshotWithLocks(ctx context.Context, root *os.Root, name string, expected authFileSnapshot, rootMutationLocked, pathLocked bool) (err error) {
	committed := false
	if root == nil {
		return errors.New("auth store: remove auth snapshot: root is nil")
	}
	if !rootMutationLocked {
		unlockRootMutation, errMutationLock := authfileguard.LockRootMutationContext(ctx, root)
		if errMutationLock != nil {
			return fmt.Errorf("auth store: lock auth root for rollback: %w", errMutationLock)
		}
		defer func() { err = joinAuthSaveCleanupError(err, unlockRootMutation(), committed) }()
	}
	parent, leaf, closeParent, errParent := openAuthSnapshotParent(root, name)
	if errParent != nil {
		return fmt.Errorf("auth store: open auth parent for rollback: %w", errParent)
	}
	if closeParent != nil {
		defer func() { err = joinAuthSaveCleanupError(err, closeParent(), committed) }()
	}
	unlockTarget, errLock := lockAuthSnapshotTargetContext(ctx, root, name, pathLocked)
	if errLock != nil {
		return fmt.Errorf("auth store: lock local auth during rollback: %w", errLock)
	}
	defer func() { err = joinAuthSaveCleanupError(err, unlockTarget(), committed) }()
	if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
		return errParentIdentity
	}
	current, errCurrent := captureAuthFileSnapshot(parent, leaf)
	if errCurrent != nil {
		return fmt.Errorf("auth store: inspect local auth during rollback: %w", errCurrent)
	}
	if !sameAuthFileGeneration(current, expected) {
		return authfileguard.ErrPersistGenerationStale
	}
	if errRemove := removeExpectedAuthSnapshot(parent, leaf, expected, syncAuthSnapshotDirectory); errRemove != nil {
		return fmt.Errorf("auth store: remove local auth during rollback: %w", errRemove)
	}
	if errParentIdentity := revalidateAuthSnapshotParent(root, name, parent); errParentIdentity != nil {
		return errParentIdentity
	}
	committed = true
	return nil
}

func restoreAuthFileSnapshotAtPath(path string, installed, original authFileSnapshot) (err error) {
	return restoreAuthFileSnapshotAtPathContext(context.Background(), path, installed, original)
}

func restoreAuthFileSnapshotAtPathContext(ctx context.Context, path string, installed, original authFileSnapshot) (err error) {
	root, errRoot := os.OpenRoot(filepath.Dir(path))
	if errRoot != nil {
		return fmt.Errorf("auth store: open auth directory for rollback: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("auth store: close auth directory after rollback: %w", errClose))
		}
	}()
	return restoreAuthFileSnapshotAtRootContext(ctx, root, filepath.Base(path), installed, original)
}

func openAuthSnapshotParent(root *os.Root, name string) (*os.Root, string, func() error, error) {
	clean := filepath.Clean(filepath.FromSlash(strings.TrimSpace(name)))
	if clean == "." || clean == ".." || filepath.IsAbs(clean) || filepath.VolumeName(clean) != "" || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return nil, "", nil, fmt.Errorf("invalid root-relative auth path %q", name)
	}
	parts := strings.Split(clean, string(os.PathSeparator))
	current := root
	owned := false
	closeCurrent := func() error {
		if owned {
			return closeAuthSnapshotRoot(current)
		}
		return nil
	}
	for _, component := range parts[:len(parts)-1] {
		before, errBefore := current.Lstat(component)
		if errBefore != nil {
			return nil, "", nil, errors.Join(errBefore, closeCurrent())
		}
		if errValidate := validateAuthSnapshotDirectory(component, before); errValidate != nil {
			return nil, "", nil, errors.Join(errValidate, closeCurrent())
		}
		next, errOpen := current.OpenRoot(component)
		if errOpen != nil {
			return nil, "", nil, errors.Join(errOpen, closeCurrent())
		}
		opened, errOpened := next.Stat(".")
		if errOpened != nil {
			return nil, "", nil, errors.Join(errOpened, closeAuthSnapshotRoot(next), closeCurrent())
		}
		after, errAfter := current.Lstat(component)
		if errAfter != nil {
			return nil, "", nil, errors.Join(errAfter, closeAuthSnapshotRoot(next), closeCurrent())
		}
		if errValidate := validateAuthSnapshotDirectory(component, after); errValidate != nil {
			return nil, "", nil, errors.Join(errValidate, closeAuthSnapshotRoot(next), closeCurrent())
		}
		if !os.SameFile(before, opened) || !os.SameFile(after, opened) {
			return nil, "", nil, errors.Join(
				fmt.Errorf("auth path component %s changed while opening", component),
				closeAuthSnapshotRoot(next),
				closeCurrent(),
			)
		}
		if errClose := closeCurrent(); errClose != nil {
			return nil, "", nil, errors.Join(errClose, closeAuthSnapshotRoot(next))
		}
		current = next
		owned = true
	}
	if !owned {
		return current, parts[len(parts)-1], nil, nil
	}
	return current, parts[len(parts)-1], func() error { return closeAuthSnapshotRoot(current) }, nil
}

func revalidateAuthSnapshotParent(root *os.Root, name string, opened *os.Root) (err error) {
	if root == nil || opened == nil {
		return authfileguard.ErrPersistGenerationStale
	}
	if errRootIdentity := revalidateAuthSnapshotRoot(root); errRootIdentity != nil {
		return errRootIdentity
	}
	live, _, closeLive, errLive := openAuthSnapshotParent(root, name)
	if errLive != nil {
		return errors.Join(authfileguard.ErrPersistGenerationStale, errLive)
	}
	if closeLive != nil {
		defer func() { err = errors.Join(err, closeLive()) }()
	}
	openedInfo, errOpened := opened.Stat(".")
	liveInfo, errLiveInfo := live.Stat(".")
	if errOpened != nil || errLiveInfo != nil || !os.SameFile(openedInfo, liveInfo) {
		return errors.Join(errOpened, errLiveInfo, authfileguard.ErrPersistGenerationStale)
	}
	return revalidateAuthSnapshotRoot(root)
}

func revalidateAuthSnapshotRoot(root *os.Root) (err error) {
	if root == nil {
		return authfileguard.ErrPersistGenerationStale
	}
	openedInfo, errOpened := root.Stat(".")
	live, errLive := os.OpenRoot(root.Name())
	if errOpened != nil || errLive != nil {
		return errors.Join(errOpened, errLive, authfileguard.ErrPersistGenerationStale)
	}
	defer func() { err = errors.Join(err, live.Close()) }()
	liveInfo, errLiveInfo := live.Stat(".")
	if errLiveInfo != nil || !os.SameFile(openedInfo, liveInfo) {
		return errors.Join(errLiveInfo, authfileguard.ErrPersistGenerationStale)
	}
	return nil
}

func closeAuthSnapshotRoot(root *os.Root) error {
	if errClose := root.Close(); errClose != nil {
		return fmt.Errorf("auth store: close snapshot root: %w", errClose)
	}
	return nil
}

func validateAuthSnapshotDirectory(name string, info fs.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("auth path component %s is a symbolic link", name)
	}
	if !info.IsDir() {
		return fmt.Errorf("auth path component %s is not a directory", name)
	}
	return nil
}

func validateAuthSnapshotRegularFile(name string, info fs.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("auth store: capture auth file before persistence: %s is a symbolic link", name)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("auth store: capture auth file before persistence: %s is not a regular file", name)
	}
	return nil
}

func (s authFileSnapshot) rejectRetiredGeminiCLIAuthPersistence() error {
	if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(s.data); errRetired != nil {
		return fmt.Errorf("auth store: %w", errRetired)
	}
	return nil
}

func captureAuthFileSnapshotAtPath(path string) (snapshot authFileSnapshot, err error) {
	root, errRoot := os.OpenRoot(filepath.Dir(path))
	if errRoot != nil {
		return authFileSnapshot{}, fmt.Errorf("auth store: open auth snapshot directory: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("auth store: close auth snapshot directory: %w", errClose))
		}
	}()
	return captureAuthFileSnapshot(root, filepath.Base(path))
}

func produceAuthStorageData(storage internalauth.TokenStorage) (data []byte, err error) {
	if storage == nil {
		return nil, errors.New("auth store: token storage is nil")
	}
	if marshaler, ok := storage.(internalauth.TokenDataMarshaler); ok {
		data, err = marshaler.MarshalTokenData()
	} else {
		sandbox, errTemp := os.MkdirTemp("", ".cli-proxy-auth-storage-")
		if errTemp != nil {
			return nil, fmt.Errorf("auth store: create storage sandbox: %w", errTemp)
		}
		defer func() {
			if errRemove := os.RemoveAll(sandbox); errRemove != nil {
				err = errors.Join(err, fmt.Errorf("auth store: remove storage sandbox: %w", errRemove))
			}
		}()
		outputPath := filepath.Join(sandbox, "token-"+uuid.NewString()+".json")
		if errSave := storage.SaveTokenToFile(outputPath); errSave != nil {
			return nil, errSave
		}
		info, errInfo := os.Lstat(outputPath)
		if errInfo != nil {
			return nil, fmt.Errorf("auth store: inspect storage output: %w", errInfo)
		}
		if !info.Mode().IsRegular() {
			return nil, errors.New("auth store: storage output is not a regular file")
		}
		data, err = os.ReadFile(outputPath)
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, errors.New("auth store: persisted auth is empty")
	}
	var metadata map[string]any
	if errJSON := json.Unmarshal(data, &metadata); errJSON != nil {
		return nil, fmt.Errorf("auth store: persisted auth is invalid JSON: %w", errJSON)
	}
	if metadata == nil {
		return nil, errors.New("auth store: persisted auth must be a JSON object")
	}
	return data, nil
}

func prepareAuthStorageData(auth *cliproxyauth.Auth, snapshot authRuntimeSnapshot) ([]byte, error) {
	if auth == nil || auth.Storage == nil {
		return nil, errors.New("auth store: token storage is nil")
	}
	if setter, ok := auth.Storage.(internalauth.MetadataSetter); ok {
		setter.SetMetadata(cliproxyauth.MetadataWithDisabled(auth))
	}
	data, err := produceAuthStorageData(auth.Storage)
	if err == nil {
		if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(data); errRetired != nil {
			err = fmt.Errorf("auth store: %w", errRetired)
		}
	}
	if err != nil {
		snapshot.restore(auth)
	}
	return data, err
}
