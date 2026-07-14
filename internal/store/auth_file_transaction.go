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
		snapshot.storageSetter = setter
		if snapshotter, okSnapshot := auth.Storage.(internalauth.MetadataSnapshotter); okSnapshot {
			snapshot.storageMetadata = cloneAuthMetadata(snapshotter.MetadataSnapshot())
		} else {
			snapshot.storageMetadata = cliproxyauth.MetadataWithDisabled(auth)
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

type authDeleteProbeState uint8

const (
	authDeleteProbeAbsent authDeleteProbeState = iota
	authDeleteProbeOriginal
	authDeleteProbeReplaced
)

func removeAuthFileAtRoot(root *os.Root, name string) (err error) {
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
	unlockTarget, errLock := lockAuthSnapshotTarget(parent, leaf)
	if errLock != nil {
		return errLock
	}
	defer func() { err = errors.Join(err, unlockTarget()) }()
	if errRemove := parent.Remove(leaf); errRemove != nil {
		if errors.Is(errRemove, fs.ErrNotExist) {
			return nil
		}
		return errRemove
	}
	return syncAuthSnapshotDirectory(parent)
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
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		return fmt.Errorf("auth store: open auth directory for reset: %w", errOpen)
	}
	entries, errRead := directory.ReadDir(-1)
	errClose := directory.Close()
	if errRead != nil || errClose != nil {
		return errors.Join(errRead, errClose)
	}
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		if errRemove := root.RemoveAll(entry.Name()); errRemove != nil {
			return fmt.Errorf("auth store: remove auth mirror entry %s: %w", entry.Name(), errRemove)
		}
	}
	return syncAuthSnapshotDirectory(root)
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
	if root == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errors.New("auth store: root is nil"))
	}
	if prepareDelete == nil || deleteRemote == nil || probeRemote == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errors.New("auth store: delete transaction is incomplete"))
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
		unlockTarget, errLock := lockAuthSnapshotTarget(parent, leaf)
		if errLock != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth store: lock auth file before deletion: %w", errLock))
		}
		defer func() {
			resultErr = joinAuthDeleteParentClose(resultErr, unlockTarget())
		}()
		var errSnapshot error
		original, errSnapshot = captureAuthFileSnapshot(parent, leaf)
		if errSnapshot != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth store: inspect auth file before deletion: %w", errSnapshot))
		}
	}
	if errPrepare := prepareDelete(original); errPrepare != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth store: persist delete quarantine: %w", errPrepare))
	}
	if parent != nil {
		current, errCurrent := captureAuthFileSnapshot(parent, leaf)
		if errCurrent != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth store: revalidate auth file after persisting quarantine: %w", errCurrent))
		}
		if !sameAuthFileGeneration(current, original) {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, authfileguard.ErrPersistGenerationStale)
		}
		if errRemove := parent.Remove(leaf); errRemove != nil {
			if !errors.Is(errRemove, fs.ErrNotExist) {
				return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth store: delete auth file after persisting quarantine: %w", errRemove))
			}
		} else if errSync := syncAuthSnapshotDirectory(parent); errSync != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth store: sync deleted auth file: %w", errSync))
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
	if err := ctx.Err(); err != nil {
		return authDeleteProbeOriginal, err
	}
	return probe(ctx)
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

	file, errOpen := parent.Open(leaf)
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
	mode := opened.Mode().Perm()
	if mode == 0 {
		mode = 0o600
	}
	return authFileSnapshot{data: data, mode: mode, info: opened, exists: true}, nil
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
	return bytes.Equal(current.data, expected.data)
}

func lockAuthSnapshotTarget(root *os.Root, relativePath string) (func() error, error) {
	return authfileguard.LockRootTarget(root, relativePath)
}

func writeAuthFileAtomicallyAtRoot(root *os.Root, name string, data []byte, expected *authFileSnapshot) error {
	return writeAuthFileAtomicallyAtRootWithPolicy(root, name, data, expected, false)
}

func writeAuthMirrorFileAtomicallyAtRoot(root *os.Root, name string, data []byte, expected *authFileSnapshot) error {
	return writeAuthFileAtomicallyAtRootWithPolicy(root, name, data, expected, true)
}

func writeAuthFileAtomicallyAtRootWithPolicy(root *os.Root, name string, data []byte, expected *authFileSnapshot, allowRetired bool) (err error) {
	if root == nil {
		return errors.New("auth store: write auth file: root is nil")
	}
	parent, leaf, closeParent, errParent := openAuthSnapshotParent(root, name)
	if errParent != nil {
		return fmt.Errorf("auth store: open auth parent for write: %w", errParent)
	}
	if closeParent != nil {
		defer func() { err = errors.Join(err, closeParent()) }()
	}
	tempName := ".auth-write-" + uuid.NewString()
	file, errOpen := parent.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errOpen != nil {
		return fmt.Errorf("auth store: create temporary auth file: %w", errOpen)
	}
	defer func() {
		if errRemove := parent.Remove(tempName); errRemove != nil && !errors.Is(errRemove, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("auth store: remove temporary auth file: %w", errRemove))
		}
	}()
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, file.Close())
		}
	}()
	if _, errWrite := file.Write(data); errWrite != nil {
		return fmt.Errorf("auth store: write temporary auth file: %w", errWrite)
	}
	if errSync := file.Sync(); errSync != nil {
		return fmt.Errorf("auth store: sync temporary auth file: %w", errSync)
	}
	if errClose := file.Close(); errClose != nil {
		closed = true
		return fmt.Errorf("auth store: close temporary auth file: %w", errClose)
	}
	closed = true
	unlockTarget, errLock := lockAuthSnapshotTarget(parent, leaf)
	if errLock != nil {
		return fmt.Errorf("auth store: lock auth target for write: %w", errLock)
	}
	defer func() { err = errors.Join(err, unlockTarget()) }()
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
	if errRename := parent.Rename(tempName, leaf); errRename != nil {
		return fmt.Errorf("auth store: replace auth file: %w", errRename)
	}
	return syncAuthSnapshotDirectory(parent)
}

func restoreAuthFileSnapshotAtRoot(root *os.Root, name string, written []byte, original authFileSnapshot) error {
	current, errCurrent := captureAuthFileSnapshot(root, name)
	if errCurrent != nil {
		return fmt.Errorf("auth store: inspect local auth before rollback: %w", errCurrent)
	}
	if !current.exists || !bytes.Equal(current.data, written) {
		return authfileguard.ErrPersistGenerationStale
	}
	if original.exists {
		if bytes.Equal(current.data, original.data) {
			return nil
		}
		if errWrite := writeAuthFileAtomicallyAtRoot(root, name, original.data, &current); errWrite != nil {
			return fmt.Errorf("auth store: restore local auth snapshot: %w", errWrite)
		}
		return nil
	}
	return removeAuthFileAtRootForSnapshot(root, name, current)
}

func removeAuthFileAtRootForSnapshot(root *os.Root, name string, expected authFileSnapshot) (err error) {
	parent, leaf, closeParent, errParent := openAuthSnapshotParent(root, name)
	if errParent != nil {
		return fmt.Errorf("auth store: open auth parent for rollback: %w", errParent)
	}
	if closeParent != nil {
		defer func() { err = errors.Join(err, closeParent()) }()
	}
	unlockTarget, errLock := lockAuthSnapshotTarget(parent, leaf)
	if errLock != nil {
		return fmt.Errorf("auth store: lock local auth during rollback: %w", errLock)
	}
	defer func() { err = errors.Join(err, unlockTarget()) }()
	current, errCurrent := captureAuthFileSnapshot(parent, leaf)
	if errCurrent != nil {
		return fmt.Errorf("auth store: inspect local auth during rollback: %w", errCurrent)
	}
	if !sameAuthFileGeneration(current, expected) {
		return authfileguard.ErrPersistGenerationStale
	}
	if errRemove := parent.Remove(leaf); errRemove != nil {
		return fmt.Errorf("auth store: remove local auth during rollback: %w", errRemove)
	}
	return syncAuthSnapshotDirectory(parent)
}

func restoreAuthFileSnapshotAtPath(path string, written []byte, original authFileSnapshot) (err error) {
	root, errRoot := os.OpenRoot(filepath.Dir(path))
	if errRoot != nil {
		return fmt.Errorf("auth store: open auth directory for rollback: %w", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("auth store: close auth directory after rollback: %w", errClose))
		}
	}()
	return restoreAuthFileSnapshotAtRoot(root, filepath.Base(path), written, original)
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
	if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(data); errRetired != nil {
		return nil, fmt.Errorf("auth store: %w", errRetired)
	}
	return data, nil
}
