package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	internalauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth"
	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/jsonsemantic"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/sirupsen/logrus"
)

// FileTokenStore persists token records and auth metadata using the filesystem as backing storage.
type FileTokenStore struct {
	operationOnce       sync.Once
	operationToken      chan struct{}
	dirLock             sync.RWMutex
	baseDir             string
	lastResolvedBaseDir string
	syncDirectory       func(*os.Root, string) error
	lockTarget          func(context.Context, *os.Root, string) (func() error, error)
}

type fileTokenSnapshot struct {
	data   []byte
	mode   fs.FileMode
	info   fs.FileInfo
	exists bool
}

var fetchAntigravityProjectID = FetchAntigravityProjectID

func resolveFileTokenBaseDir(baseDir string, create bool) (string, string, error) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		return "", "", nil
	}
	lexicalBaseDir, errAbs := filepath.Abs(baseDir)
	if errAbs != nil {
		return "", "", fmt.Errorf("auth filestore: resolve base dir: %w", errAbs)
	}
	lexicalBaseDir = filepath.Clean(lexicalBaseDir)
	if create {
		if errMkdir := createFileTokenBaseDir(lexicalBaseDir); errMkdir != nil {
			return "", "", fmt.Errorf("auth filestore: create base dir: %w", errMkdir)
		}
	}
	resolvedBaseDir := lexicalBaseDir
	if resolved, errEval := filepath.EvalSymlinks(lexicalBaseDir); errEval == nil {
		resolvedBaseDir = filepath.Clean(resolved)
	} else if !os.IsNotExist(errEval) || !create {
		return lexicalBaseDir, resolvedBaseDir, fmt.Errorf("auth filestore: resolve base dir: %w", errEval)
	}
	return lexicalBaseDir, resolvedBaseDir, nil
}

func createFileTokenBaseDir(path string) error {
	if info, errStat := os.Stat(path); errStat == nil {
		if !info.IsDir() {
			return fmt.Errorf("path is not a directory")
		}
		return nil
	} else if !os.IsNotExist(errStat) {
		return errStat
	}
	root, relativePath, _, errRoot := openRootForPath(path, "")
	if errRoot != nil {
		return errRoot
	}
	defer closeFileTokenRoot(root)
	return mkdirAllRootAndSync(root, relativePath, 0o700)
}

func mkdirAllRootAndSync(root *os.Root, relativePath string, mode fs.FileMode) error {
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
		if errSync := syncAuthRootDirectory(root, parent); errSync != nil {
			return errSync
		}
		parent = filepath.Join(parent, component)
	}
	return nil
}

func openRootForPath(path, resolvedBaseDir string) (*os.Root, string, bool, error) {
	if resolvedBaseDir != "" {
		if relativePath, inside := relativePathWithin(resolvedBaseDir, path); inside {
			root, errOpen := os.OpenRoot(resolvedBaseDir)
			if errOpen != nil {
				return nil, "", false, fmt.Errorf("auth filestore: open base dir: %w", errOpen)
			}
			return root, relativePath, true, nil
		}
		if relativePath, inside := physicalRelativePathWithin(resolvedBaseDir, path); inside {
			root, errOpen := os.OpenRoot(resolvedBaseDir)
			if errOpen != nil {
				return nil, "", false, fmt.Errorf("auth filestore: open base dir: %w", errOpen)
			}
			return root, relativePath, true, nil
		}
	}

	existingAncestor := filepath.Dir(path)
	for {
		info, errStat := os.Stat(existingAncestor)
		if errStat == nil {
			if !info.IsDir() {
				return nil, "", false, fmt.Errorf("auth filestore: path ancestor is not a directory: %s", existingAncestor)
			}
			break
		}
		if !os.IsNotExist(errStat) {
			return nil, "", false, fmt.Errorf("auth filestore: inspect path ancestor: %w", errStat)
		}
		parent := filepath.Dir(existingAncestor)
		if parent == existingAncestor {
			return nil, "", false, fmt.Errorf("auth filestore: no existing path ancestor for %s", path)
		}
		existingAncestor = parent
	}
	resolvedAncestor, errEval := filepath.EvalSymlinks(existingAncestor)
	if errEval != nil {
		return nil, "", false, fmt.Errorf("auth filestore: resolve path ancestor: %w", errEval)
	}
	root, errOpen := os.OpenRoot(resolvedAncestor)
	if errOpen != nil {
		return nil, "", false, fmt.Errorf("auth filestore: open path ancestor: %w", errOpen)
	}
	relativePath, errRel := filepath.Rel(existingAncestor, path)
	if errRel != nil || relativePath == "." || filepath.IsAbs(relativePath) || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		closeFileTokenRoot(root)
		return nil, "", false, fmt.Errorf("auth filestore: invalid path below ancestor")
	}
	return root, relativePath, false, nil
}

func closeFileTokenRoot(root *os.Root) {
	if root != nil {
		if errClose := root.Close(); errClose != nil {
			logrus.WithError(errClose).Error("auth filestore: close filesystem root failed")
		}
	}
}

func writeRootFileAtomically(ctx context.Context, root *os.Root, relativePath string, data []byte) (err error) {
	return writeRootFileAtomicallyForSnapshot(ctx, root, relativePath, data, nil, "", nil)
}

func lockRootAuthTarget(ctx context.Context, root *os.Root, relativePath string) (func() error, error) {
	return authfileguard.LockRootTargetContext(ctx, root, relativePath)
}

func writeRootFileAtomicallyForSnapshot(ctx context.Context, root *os.Root, relativePath string, data []byte, expected *fileTokenSnapshot, absolutePath string, syncDirectory func(*os.Root, string) error) (err error) {
	unlockTarget, errLock := lockRootAuthTarget(ctx, root, relativePath)
	if errLock != nil {
		return fmt.Errorf("lock auth target: %w", errLock)
	}
	err = writeRootFileAtomicallyForSnapshotTargetLocked(ctx, root, relativePath, data, expected, absolutePath, syncDirectory)
	committed := err == nil || fileTokenSaveCommitted(err)
	return joinFileTokenSaveCleanupError(err, unlockTarget(), committed)
}

func writeRootFileAtomicallyForSnapshotTargetLocked(ctx context.Context, root *os.Root, relativePath string, data []byte, expected *fileTokenSnapshot, absolutePath string, syncDirectory func(*os.Root, string) error) (err error) {
	if root == nil {
		return fmt.Errorf("auth filestore: root is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return errContext
	}
	if syncDirectory == nil {
		syncDirectory = syncAuthRootDirectory
	}
	committed := false
	var installCleanupWarning error
	tempPath := filepath.Join(filepath.Dir(relativePath), ".auth-save-"+uuid.NewString())
	stagingOwned := true
	defer func() {
		if !stagingOwned {
			return
		}
		if errRemove := root.Remove(tempPath); errRemove == nil {
			err = joinFileTokenSaveCleanupError(err, syncDirectory(root, filepath.Dir(relativePath)), committed)
		} else if !os.IsNotExist(errRemove) {
			err = joinFileTokenSaveCleanupError(err, fmt.Errorf("remove temporary auth file: %w", errRemove), committed)
		}
	}()
	if errStage := stageRootAuthFile(root, tempPath, data, 0o600); errStage != nil {
		return errStage
	}
	stagedSnapshot, errStaged := captureFileTokenSnapshot(root, tempPath)
	if errStaged != nil {
		return fmt.Errorf("inspect staged auth file: %w", errStaged)
	}
	if expected != nil {
		if errValidate := expected.validate(root, relativePath, absolutePath); errValidate != nil {
			return errValidate
		}
	}
	var displacedOriginal string
	if expected != nil && !expected.exists {
		cleanupWarning, errInstall := authfileguard.InstallStagedFileNoReplace(root, tempPath, relativePath)
		if errInstall != nil {
			return errInstall
		}
		if cleanupWarning != nil {
			installCleanupWarning = cleanupWarning
		}
		stagingOwned = errors.Is(cleanupWarning, authfileguard.ErrStagedFileCleanupRequired)
	} else if expected != nil {
		var errExchange error
		displacedOriginal, errExchange = exchangeExpectedFileTokenSnapshot(root, tempPath, relativePath, *expected, syncDirectory)
		rollbackConfirmed := fileTokenExchangeRollbackConfirmed(errExchange)
		if displacedOriginal != "" && !rollbackConfirmed {
			stagingOwned = false
		}
		if errExchange != nil {
			if rollbackConfirmed {
				// Cleanup-required does not prove that the displaced path still names
				// the expected generation. Keep it and only remove our staged path.
				return cliproxyauth.NewSaveOutcomeError(
					cliproxyauth.SaveOutcomeRolledBack,
					fmt.Errorf("auth filestore: exchange auth generation: %w", errExchange),
				)
			}
			if displacedOriginal != "" || errors.Is(errExchange, authfileguard.ErrExchangeOutcomeUncertain) {
				return cliproxyauth.NewSaveOutcomeError(
					cliproxyauth.SaveOutcomeUncertain,
					fmt.Errorf("auth filestore: exchange auth generation: %w", errExchange),
				)
			}
			return fmt.Errorf("auth filestore: exchange auth generation: %w", errExchange)
		}
	} else if errReplace := replaceRootFile(root, tempPath, relativePath); errReplace != nil {
		return errReplace
	} else {
		stagingOwned = false
	}
	installedSnapshot, errInstalled := captureFileTokenSnapshot(root, relativePath)
	if errInstalled != nil || !sameFileTokenGeneration(installedSnapshot, stagedSnapshot) {
		return cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			errors.Join(errInstalled, errors.New("auth filestore: installed auth generation changed before directory sync")),
		)
	}
	if errSync := syncDirectory(root, filepath.Dir(relativePath)); errSync != nil {
		errRootIdentity := revalidateFileTokenRootIdentity(root)
		if displacedOriginal != "" {
			errSync = errors.Join(errSync, discardFileTokenSnapshot(root, displacedOriginal, syncDirectory))
		}
		return cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			errors.Join(errSync, errRootIdentity),
		)
	}
	if errRootIdentity := revalidateFileTokenRootIdentity(root); errRootIdentity != nil {
		if displacedOriginal != "" {
			errRootIdentity = errors.Join(errRootIdentity, discardFileTokenSnapshot(root, displacedOriginal, syncDirectory))
		}
		return cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			fmt.Errorf("auth filestore: auth parent changed during directory sync: %w", errRootIdentity),
		)
	}
	committed = true
	if displacedOriginal != "" {
		if errCleanup := discardFileTokenSnapshot(root, displacedOriginal, syncDirectory); errCleanup != nil {
			return cliproxyauth.NewSaveOutcomeError(
				cliproxyauth.SaveOutcomeCommitted,
				fmt.Errorf("auth filestore: remove previous auth generation: %w", errCleanup),
			)
		}
	}
	if installCleanupWarning != nil {
		return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeCommitted, installCleanupWarning)
	}
	return nil
}

func revalidateFileTokenRootIdentity(root *os.Root) (err error) {
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

func revalidateFileTokenParent(root *os.Root, relativePath string, openedParent *os.Root) (err error) {
	if root == nil || openedParent == nil {
		return authfileguard.ErrPersistGenerationStale
	}
	if errRoot := revalidateFileTokenRootIdentity(root); errRoot != nil {
		return errRoot
	}
	liveParent, _, closeLiveParent, errOpen := openFileTokenParent(root, relativePath)
	if errOpen != nil {
		return errors.Join(errOpen, authfileguard.ErrPersistGenerationStale)
	}
	defer closeLiveParent()
	openedInfo, errOpened := openedParent.Stat(".")
	liveInfo, errLive := liveParent.Stat(".")
	if errOpened != nil || errLive != nil || !os.SameFile(openedInfo, liveInfo) {
		return errors.Join(errOpened, errLive, authfileguard.ErrPersistGenerationStale)
	}
	return revalidateFileTokenRootIdentity(root)
}

func joinFileTokenSaveCleanupError(resultErr, cleanupErr error, committed bool) error {
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

func stageRootAuthFile(root *os.Root, relativePath string, data []byte, mode fs.FileMode) (err error) {
	file, errOpen := root.OpenFile(relativePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode.Perm())
	if errOpen != nil {
		return errOpen
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, file.Close())
		}
	}()
	if _, errWrite := file.Write(data); errWrite != nil {
		return errWrite
	}
	if errChmod := file.Chmod(mode.Perm()); errChmod != nil {
		return errChmod
	}
	if errSync := file.Sync(); errSync != nil {
		return errSync
	}
	if errClose := file.Close(); errClose != nil {
		closed = true
		return errClose
	}
	closed = true
	return nil
}

func sameFileTokenGeneration(current, expected fileTokenSnapshot) bool {
	if current.exists != expected.exists {
		return false
	}
	if !current.exists {
		return true
	}
	return current.info != nil &&
		expected.info != nil &&
		os.SameFile(current.info, expected.info) &&
		current.mode.Perm() == expected.mode.Perm() &&
		bytes.Equal(current.data, expected.data)
}

func exchangeExpectedFileTokenSnapshot(root *os.Root, stagedPath, targetPath string, expected fileTokenSnapshot, syncDirectory func(*os.Root, string) error) (string, error) {
	stagedInfo, errStaged := root.Lstat(stagedPath)
	if errStaged != nil {
		return "", errStaged
	}
	if stagedInfo.Mode()&os.ModeSymlink != 0 || !stagedInfo.Mode().IsRegular() {
		return "", errors.New("auth filestore: staged auth generation is not a regular file")
	}
	displacedPath, errExchange := authfileguard.ExchangeStagedFile(root, stagedPath, targetPath)
	if errExchange != nil {
		return displacedPath, errExchange
	}
	displaced, errDisplaced := captureFileTokenSnapshot(root, displacedPath)
	if errDisplaced == nil && sameFileTokenGeneration(displaced, expected) {
		return displacedPath, nil
	}
	restoredPath, errRestore := authfileguard.ExchangeStagedFile(root, displacedPath, targetPath)
	if errRestore != nil {
		return displacedPath, cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			errors.Join(errDisplaced, authfileguard.ErrPersistGenerationStale, errRestore),
		)
	}
	restoredInfo, errRestored := root.Lstat(restoredPath)
	if errRestored != nil || !os.SameFile(restoredInfo, stagedInfo) {
		return restoredPath, cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			errors.Join(errDisplaced, errRestored, authfileguard.ErrPersistGenerationStale),
		)
	}
	errCleanup := discardFileTokenSnapshot(root, restoredPath, syncDirectory)
	if errCleanup != nil {
		return restoredPath, cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeUncertain,
			errors.Join(errDisplaced, authfileguard.ErrPersistGenerationStale, errCleanup),
		)
	}
	return "", errors.Join(errDisplaced, authfileguard.ErrPersistGenerationStale)
}

func discardFileTokenSnapshot(root *os.Root, relativePath string, syncDirectory func(*os.Root, string) error) error {
	errRemove := root.Remove(relativePath)
	if os.IsNotExist(errRemove) {
		errRemove = nil
	}
	return errors.Join(errRemove, syncDirectory(root, filepath.Dir(relativePath)))
}

func (s fileTokenSnapshot) validate(root *os.Root, leaf, absolutePath string) error {
	current, errRead := captureFileTokenSnapshot(root, leaf)
	currentExists := current.exists
	if errRead != nil && !os.IsNotExist(errRead) {
		return errRead
	}
	if currentExists {
		if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(current.data); errRetired != nil {
			authfileguard.MarkRetired(absolutePath)
			return errRetired
		}
	}
	if currentExists != s.exists || !bytes.Equal(current.data, s.data) {
		return authfileguard.ErrPersistGenerationStale
	}
	if currentExists && (current.info == nil || s.info == nil || !os.SameFile(current.info, s.info)) {
		return authfileguard.ErrPersistGenerationStale
	}
	if currentExists && current.mode.Perm() != s.mode.Perm() {
		return authfileguard.ErrPersistGenerationStale
	}
	return nil
}

func validatePersistedAuthData(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("persisted auth is empty")
	}
	var metadata map[string]any
	if errUnmarshal := json.Unmarshal(data, &metadata); errUnmarshal != nil {
		return errUnmarshal
	}
	if metadata == nil {
		return fmt.Errorf("persisted auth must be a JSON object")
	}
	return nil
}

func openFileTokenParent(root *os.Root, relativePath string) (*os.Root, string, func(), error) {
	clean := filepath.Clean(filepath.FromSlash(strings.TrimSpace(relativePath)))
	if clean == "." || clean == ".." || filepath.IsAbs(clean) || filepath.VolumeName(clean) != "" || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return nil, "", nil, fmt.Errorf("auth filestore: invalid root-relative auth path %q", relativePath)
	}
	parts := strings.Split(clean, string(os.PathSeparator))
	current := root
	owned := false
	closeCurrent := func() {
		if owned {
			closeFileTokenRoot(current)
		}
	}
	for _, component := range parts[:len(parts)-1] {
		before, errBefore := current.Lstat(component)
		if errBefore != nil {
			closeCurrent()
			return nil, "", nil, errBefore
		}
		if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
			closeCurrent()
			return nil, "", nil, fmt.Errorf("auth filestore: auth path component %s is not a stable directory", component)
		}
		next, errOpen := current.OpenRoot(component)
		if errOpen != nil {
			closeCurrent()
			return nil, "", nil, errOpen
		}
		opened, errOpened := next.Stat(".")
		after, errAfter := current.Lstat(component)
		if errOpened != nil || errAfter != nil || after.Mode()&os.ModeSymlink != 0 || !after.IsDir() || !os.SameFile(before, opened) || !os.SameFile(after, opened) {
			closeFileTokenRoot(next)
			closeCurrent()
			return nil, "", nil, errors.Join(errOpened, errAfter, fmt.Errorf("auth filestore: auth path component %s changed while opening", component))
		}
		closeCurrent()
		current = next
		owned = true
	}
	closeParent := func() {}
	if owned {
		closeParent = func() { closeFileTokenRoot(current) }
	}
	return current, parts[len(parts)-1], closeParent, nil
}

func readFileTokenData(root *os.Root, leaf string) ([]byte, error) {
	snapshot, errSnapshot := captureFileTokenSnapshot(root, leaf)
	return snapshot.data, errSnapshot
}

func captureFileTokenSnapshot(root *os.Root, leaf string) (fileTokenSnapshot, error) {
	before, errBefore := root.Lstat(leaf)
	if errBefore != nil {
		return fileTokenSnapshot{}, errBefore
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return fileTokenSnapshot{}, fmt.Errorf("auth filestore: auth path is not a regular file")
	}
	file, errOpen := openFileTokenSnapshotFile(root, leaf)
	if errOpen != nil {
		return fileTokenSnapshot{}, errOpen
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			logrus.WithError(errClose).Error("auth filestore: close auth file failed")
		}
	}()
	opened, errOpened := file.Stat()
	after, errAfter := root.Lstat(leaf)
	if errOpened != nil || errAfter != nil || after.Mode()&os.ModeSymlink != 0 || !after.Mode().IsRegular() || !os.SameFile(before, opened) || !os.SameFile(after, opened) {
		return fileTokenSnapshot{}, errors.Join(errOpened, errAfter, fmt.Errorf("auth filestore: auth path changed while opening"))
	}
	data, errRead := io.ReadAll(file)
	if errRead != nil {
		return fileTokenSnapshot{}, errRead
	}
	opened, errOpened = authfileguard.HardenChatGPTWebCredentialFile(file, opened, data)
	if errOpened != nil {
		return fileTokenSnapshot{}, errOpened
	}
	return fileTokenSnapshot{data: data, mode: opened.Mode().Perm(), info: opened, exists: true}, nil
}

// ReadAuthFileSnapshot reads an auth file through a stable parent and file
// handle, rejecting symlink and replacement races at the leaf path.
func ReadAuthFileSnapshot(path string) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("auth filestore: auth path is empty")
	}
	path = filepath.Clean(path)
	root, errRoot := os.OpenRoot(filepath.Dir(path))
	if errRoot != nil {
		return nil, errRoot
	}
	defer closeFileTokenRoot(root)
	return readFileTokenData(root, filepath.Base(path))
}

func marshalTokenStorageData(storage internalauth.TokenStorage) ([]byte, error) {
	if marshaler, ok := storage.(internalauth.TokenDataMarshaler); ok {
		data, errMarshal := marshaler.MarshalTokenData()
		if errMarshal != nil {
			return nil, errMarshal
		}
		return data, nil
	}
	return marshalLegacyTokenStorageData(storage)
}

func marshalLegacyTokenStorageData(storage internalauth.TokenStorage) (data []byte, err error) {
	tempDir := os.TempDir()
	resolvedTempDir, errEval := filepath.EvalSymlinks(tempDir)
	if errEval != nil {
		return nil, fmt.Errorf("resolve storage sandbox root: %w", errEval)
	}
	tempDir = filepath.Clean(resolvedTempDir)
	tempRoot, errOpen := os.OpenRoot(tempDir)
	if errOpen != nil {
		return nil, fmt.Errorf("open storage sandbox root: %w", errOpen)
	}
	defer func() {
		if errClose := tempRoot.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close storage sandbox root: %w", errClose))
		}
	}()
	sandboxName := ".cli-proxy-auth-storage-" + uuid.NewString()
	if errMkdir := tempRoot.Mkdir(sandboxName, 0o700); errMkdir != nil {
		return nil, fmt.Errorf("create storage sandbox: %w", errMkdir)
	}
	defer func() {
		if errRemove := tempRoot.RemoveAll(sandboxName); errRemove != nil && !os.IsNotExist(errRemove) {
			err = errors.Join(err, fmt.Errorf("remove storage sandbox: %w", errRemove))
		}
	}()
	sandboxRoot, errSandbox := tempRoot.OpenRoot(sandboxName)
	if errSandbox != nil {
		return nil, fmt.Errorf("open storage sandbox: %w", errSandbox)
	}
	defer func() {
		if errClose := sandboxRoot.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close storage sandbox: %w", errClose))
		}
	}()

	const outputName = "token.json"
	outputFile, errCreate := sandboxRoot.OpenFile(outputName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errCreate != nil {
		return nil, fmt.Errorf("create storage sandbox output: %w", errCreate)
	}
	if errClose := outputFile.Close(); errClose != nil {
		return nil, fmt.Errorf("close storage sandbox output: %w", errClose)
	}
	outputPath := filepath.Join(tempDir, sandboxName, outputName)
	if errSave := storage.SaveTokenToFile(outputPath); errSave != nil {
		return nil, errSave
	}
	data, errRead := readFileTokenData(sandboxRoot, outputName)
	if errRead != nil {
		return nil, fmt.Errorf("read storage sandbox output: %w", errRead)
	}
	return data, nil
}

// NewFileTokenStore creates a token store that saves credentials to disk through the
// TokenStorage implementation embedded in the token record.
func NewFileTokenStore() *FileTokenStore {
	return &FileTokenStore{}
}

func (s *FileTokenStore) lockOperation(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.operationOnce.Do(func() {
		s.operationToken = make(chan struct{}, 1)
		s.operationToken <- struct{}{}
	})
	select {
	case <-s.operationToken:
		var once sync.Once
		return func() {
			once.Do(func() { s.operationToken <- struct{}{} })
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *FileTokenStore) syncRootDirectory(root *os.Root, relativePath string) error {
	if s != nil && s.syncDirectory != nil {
		return s.syncDirectory(root, relativePath)
	}
	return syncAuthRootDirectory(root, relativePath)
}

// SetBaseDir updates the default directory used for auth JSON persistence when no explicit path is provided.
func (s *FileTokenStore) SetBaseDir(dir string) {
	dir = strings.TrimSpace(dir)
	s.dirLock.Lock()
	if s.baseDir != dir {
		s.lastResolvedBaseDir = ""
	}
	s.baseDir = dir
	s.dirLock.Unlock()
}

// Save persists token storage and metadata to the resolved auth file path.
func (s *FileTokenStore) Save(ctx context.Context, auth *cliproxyauth.Auth) (string, error) {
	return s.save(ctx, auth, false)
}

// SaveIfAbsent persists auth only when the target file does not already exist.
func (s *FileTokenStore) SaveIfAbsent(ctx context.Context, auth *cliproxyauth.Auth) (string, error) {
	return s.save(ctx, auth, true)
}

func (s *FileTokenStore) save(ctx context.Context, auth *cliproxyauth.Auth, requireAbsent bool) (savedPath string, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return "", errContext
	}
	if auth == nil {
		return "", fmt.Errorf("auth filestore: auth is nil")
	}
	createChatGPTWeb := requireAbsent && strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web")
	if cliproxyauth.IsRetiredGeminiCLIAuth(auth) {
		cliproxyauth.WarnRetiredGeminiCLIAuthIgnored()
		return "", fmt.Errorf("auth filestore: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}

	lexicalBaseDir, resolvedBaseDir, errBase := resolveFileTokenBaseDir(s.baseDirSnapshot(), true)
	if errBase != nil {
		return "", errBase
	}
	s.rememberResolvedBaseDir(resolvedBaseDir)
	path, allowOutsideBaseDir, err := s.resolveAuthPath(auth, lexicalBaseDir, resolvedBaseDir)
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", fmt.Errorf("auth filestore: missing file path attribute for %s", auth.ID)
	}
	persistedPath := path
	path, err = s.secureAuthPath(path, allowOutsideBaseDir, lexicalBaseDir, resolvedBaseDir)
	if err != nil {
		return "", err
	}
	root, relativePath, managed, errRoot := openRootForPath(path, resolvedBaseDir)
	if errRoot != nil {
		return "", errRoot
	}
	defer closeFileTokenRoot(root)
	if !managed {
		path = filepath.Join(root.Name(), relativePath)
	}
	if authfileguard.IsPersistentLockPath(relativePath) {
		return "", fmt.Errorf("auth filestore: auth path uses a reserved lock name")
	}
	if !managed && !allowOutsideBaseDir {
		return "", fmt.Errorf("auth filestore: managed auth path is outside base dir")
	}
	if createChatGPTWeb && !managed {
		return "", cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeRolledBack,
			errors.New("auth filestore: new chatgpt web credentials must be stored in the configured auth directory"),
		)
	}

	unlockOperation, errOperationLock := s.lockOperation(ctx)
	if errOperationLock != nil {
		return "", errOperationLock
	}
	defer unlockOperation()
	if errContext := ctx.Err(); errContext != nil {
		return "", errContext
	}
	if managed || createChatGPTWeb {
		var unlockRootMutation func() error
		var errMutationLock error
		if createChatGPTWeb {
			unlockRootMutation, errMutationLock = authfileguard.LockRootRebuildContext(ctx, root)
		} else {
			unlockRootMutation, errMutationLock = authfileguard.LockRootMutationContext(ctx, root)
		}
		if errMutationLock != nil {
			return "", fmt.Errorf("auth filestore: lock auth root for save: %w", errMutationLock)
		}
		defer func() {
			if errUnlock := unlockRootMutation(); errUnlock != nil {
				logrus.WithError(errUnlock).Error("auth filestore: unlock auth root after save")
			}
		}()
	}
	unlockPath, errPathLock := authfileguard.LockContext(ctx, path)
	if errPathLock != nil {
		return "", fmt.Errorf("auth filestore: lock auth path for save: %w", errPathLock)
	}
	defer unlockPath()
	if errRootIdentity := revalidateFileTokenRootIdentity(root); errRootIdentity != nil {
		return "", fmt.Errorf("auth filestore: auth root changed while locking save path: %w", errRootIdentity)
	}
	if authfileguard.IsRetired(path) {
		return "", fmt.Errorf("auth filestore: %w", cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly)
	}
	if authfileguard.IsQuarantined(path) {
		return "", fmt.Errorf("auth filestore: auth deletion is still pending: %w", authfileguard.ErrDeleteGenerationUncertain)
	}
	parentPath := filepath.Dir(relativePath)
	if (!auth.Disabled || requireAbsent) && parentPath != "." {
		if errMkdir := mkdirAllRootAndSync(root, parentPath, 0o700); errMkdir != nil {
			return "", fmt.Errorf("auth filestore: create dir failed: %w", errMkdir)
		}
	}
	persistenceCommitted := false
	lockTarget := lockRootAuthTarget
	if s.lockTarget != nil {
		lockTarget = s.lockTarget
	}
	unlockTarget, errLock := lockTarget(ctx, root, relativePath)
	if errLock != nil {
		return "", fmt.Errorf("auth filestore: lock auth target for save: %w", errLock)
	}
	defer func() {
		resultErr = joinFileTokenSaveCleanupError(resultErr, unlockTarget(), persistenceCommitted)
	}()
	parentRoot, leaf, closeParent, errParent := openFileTokenParent(root, relativePath)
	if errParent != nil {
		if auth.Disabled && !requireAbsent && os.IsNotExist(errParent) {
			return "", nil
		}
		return "", fmt.Errorf("auth filestore: open auth parent: %w", errParent)
	}
	defer closeParent()

	initialSnapshot, errExisting := captureFileTokenSnapshot(parentRoot, leaf)
	existingData := initialSnapshot.data
	if requireAbsent && errExisting == nil {
		return "", cliproxyauth.ErrAuthAlreadyExists
	}
	if errExisting == nil {
		if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(existingData); errRetired != nil {
			authfileguard.MarkRetired(path)
			return "", fmt.Errorf("auth filestore: %w", errRetired)
		}
	} else if !os.IsNotExist(errExisting) {
		return "", fmt.Errorf("auth filestore: read existing failed: %w", errExisting)
	}
	if auth.Disabled && !requireAbsent && os.IsNotExist(errExisting) {
		return "", nil
	}

	var committedWarning error
	switch {
	case auth.Storage != nil:
		setter, previousStorageMetadata, storageSnapshot := captureFileTokenStorageMetadata(auth)
		storagePersisted := false
		if setter != nil {
			setter.SetMetadata(cliproxyauth.MetadataWithDisabled(auth))
			if storageSnapshot {
				defer func() {
					if !storagePersisted {
						setter.SetMetadata(cloneFileTokenMetadata(previousStorageMetadata))
					}
				}()
			}
		}
		data, errData := marshalTokenStorageData(auth.Storage)
		if errData != nil {
			return "", fmt.Errorf("auth filestore: produce storage auth: %w", errData)
		}
		if errValidate := validatePersistedAuthData(data); errValidate != nil {
			return "", fmt.Errorf("auth filestore: invalid persisted storage auth: %w", errValidate)
		}
		if errRetired := cliproxyauth.RejectRetiredGeminiCLIAuthFileMutation(data); errRetired != nil {
			return "", fmt.Errorf("auth filestore: %w", errRetired)
		}
		if createChatGPTWeb {
			if errUnique := validateFileTokenChatGPTWebCreate(ctx, root, relativePath, data); errUnique != nil {
				return "", errUnique
			}
		}
		err = writeRootFileAtomicallyForSnapshotTargetLocked(ctx, parentRoot, leaf, data, &initialSnapshot, path, s.syncRootDirectory)
		persistenceCommitted = err == nil || fileTokenSaveCommitted(err)
		if err != nil {
			storagePersisted = fileTokenSaveMayHavePersisted(err)
			if err = mapFileTokenCreateGenerationConflict(requireAbsent, err); errors.Is(err, cliproxyauth.ErrAuthAlreadyExists) {
				return "", err
			}
			if !fileTokenSaveCommitted(err) {
				return "", fmt.Errorf("auth filestore: persist storage auth failed: %w", err)
			}
			committedWarning = fmt.Errorf("auth filestore: persist storage auth cleanup failed: %w", err)
		}
		storagePersisted = true
		if errSync := cliproxyauth.SyncPersistedMetadataAndSourceHash(auth, data); errSync != nil {
			committedWarning = joinCommittedFileTokenSaveWarning(
				committedWarning,
				fmt.Errorf("auth filestore: sync persisted storage auth failed: %w", errSync),
			)
		}
	case auth.Metadata != nil:
		raw, errMarshal := cliproxyauth.CanonicalMetadataBytes(auth)
		if errMarshal != nil {
			return "", fmt.Errorf("auth filestore: canonicalize metadata failed: %w", errMarshal)
		}
		if createChatGPTWeb {
			if errUnique := validateFileTokenChatGPTWebCreate(ctx, root, relativePath, raw); errUnique != nil {
				return "", errUnique
			}
		}
		if errValidate := initialSnapshot.validate(parentRoot, leaf, path); errValidate != nil {
			if errValidate = mapFileTokenCreateGenerationConflict(requireAbsent, errValidate); errors.Is(errValidate, cliproxyauth.ErrAuthAlreadyExists) {
				return "", errValidate
			}
			return "", fmt.Errorf("auth filestore: local auth changed before replacement: %w", errValidate)
		}
		if errExisting == nil {
			if jsonEqual(existingData, raw) {
				persistenceCommitted = true
				cliproxyauth.SetSourceHashAttribute(auth, raw)
				return finishFileTokenSave(auth, persistedPath, nil)
			}
			if errWrite := writeRootFileAtomicallyForSnapshotTargetLocked(ctx, parentRoot, leaf, raw, &initialSnapshot, path, s.syncRootDirectory); errWrite != nil {
				if errWrite = mapFileTokenCreateGenerationConflict(requireAbsent, errWrite); errors.Is(errWrite, cliproxyauth.ErrAuthAlreadyExists) {
					return "", errWrite
				}
				if !fileTokenSaveCommitted(errWrite) {
					return "", fmt.Errorf("auth filestore: write existing failed: %w", errWrite)
				}
				persistenceCommitted = true
				committedWarning = fmt.Errorf("auth filestore: write existing cleanup failed: %w", errWrite)
			} else {
				persistenceCommitted = true
			}
			cliproxyauth.SetSourceHashAttribute(auth, raw)
			return finishFileTokenSave(auth, persistedPath, committedWarning)
		}
		if errWrite := writeRootFileAtomicallyForSnapshotTargetLocked(ctx, parentRoot, leaf, raw, &initialSnapshot, path, s.syncRootDirectory); errWrite != nil {
			if errWrite = mapFileTokenCreateGenerationConflict(requireAbsent, errWrite); errors.Is(errWrite, cliproxyauth.ErrAuthAlreadyExists) {
				return "", errWrite
			}
			if !fileTokenSaveCommitted(errWrite) {
				return "", fmt.Errorf("auth filestore: write file failed: %w", errWrite)
			}
			persistenceCommitted = true
			committedWarning = fmt.Errorf("auth filestore: write file cleanup failed: %w", errWrite)
		} else {
			persistenceCommitted = true
		}
		cliproxyauth.SetSourceHashAttribute(auth, raw)
	default:
		return "", fmt.Errorf("auth filestore: nothing to persist for %s", auth.ID)
	}

	return finishFileTokenSave(auth, persistedPath, committedWarning)
}

func validateFileTokenChatGPTWebCreate(ctx context.Context, root *os.Root, relativePath string, data []byte) error {
	var envelope struct {
		Type  string `json:"type"`
		Email string `json:"email"`
	}
	if errJSON := json.Unmarshal(data, &envelope); errJSON != nil {
		return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, fmt.Errorf("auth filestore: decode chatgpt web credential: %w", errJSON))
	}
	if !strings.EqualFold(strings.TrimSpace(envelope.Type), "chatgpt-web") {
		return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, errors.New("auth filestore: chatgpt web credential has an invalid type"))
	}
	email := strings.ToLower(strings.TrimSpace(envelope.Email))
	if email == "" {
		return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, errors.New("auth filestore: chatgpt web credential email is empty"))
	}
	conflict, errConflict := fileTokenRootHasChatGPTWebEmail(ctx, root, relativePath, email)
	if errConflict != nil {
		return cliproxyauth.NewSaveOutcomeError(
			cliproxyauth.SaveOutcomeRolledBack,
			fmt.Errorf("auth filestore: inspect chatgpt web email uniqueness: %w", errConflict),
		)
	}
	if conflict {
		return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, cliproxyauth.ErrChatGPTWebEmailAlreadyExists)
	}
	return nil
}

func fileTokenRootHasChatGPTWebEmail(ctx context.Context, root *os.Root, skipRelativePath, email string) (bool, error) {
	if root == nil || email == "" {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	skipRelativePath = filepath.Clean(skipRelativePath)
	conflict := false
	errWalk := fs.WalkDir(root.FS(), ".", func(walkPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if errContext := ctx.Err(); errContext != nil {
			return errContext
		}
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			return nil
		}
		info, errInfo := entry.Info()
		if errInfo != nil {
			return errInfo
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		relativePath := filepath.Clean(filepath.FromSlash(walkPath))
		if sameFileTokenRelativePath(root, relativePath, skipRelativePath) {
			return nil
		}
		snapshot, errSnapshot := captureFileTokenSnapshot(root, relativePath)
		if errSnapshot != nil {
			return errSnapshot
		}
		var envelope struct {
			Type  string `json:"type"`
			Email string `json:"email"`
		}
		if errJSON := json.Unmarshal(snapshot.data, &envelope); errJSON != nil || !strings.EqualFold(strings.TrimSpace(envelope.Type), "chatgpt-web") {
			return nil
		}
		if strings.EqualFold(strings.TrimSpace(envelope.Email), email) {
			conflict = true
			return fs.SkipAll
		}
		return nil
	})
	return conflict, errWalk
}

func sameFileTokenRelativePath(root *os.Root, left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if left == right {
		return true
	}
	if root == nil {
		return false
	}
	leftInfo, errLeft := root.Lstat(left)
	rightInfo, errRight := root.Lstat(right)
	return errLeft == nil && errRight == nil && os.SameFile(leftInfo, rightInfo)
}

func captureFileTokenStorageMetadata(auth *cliproxyauth.Auth) (internalauth.MetadataSetter, map[string]any, bool) {
	if auth == nil {
		return nil, nil, false
	}
	setter, ok := auth.Storage.(internalauth.MetadataSetter)
	if !ok {
		return nil, nil, false
	}
	if snapshotter, okSnapshot := auth.Storage.(internalauth.MetadataSnapshotter); okSnapshot {
		return setter, cloneFileTokenMetadata(snapshotter.MetadataSnapshot()), true
	}
	return setter, nil, false
}

func cloneFileTokenMetadata(metadata map[string]any) map[string]any {
	if metadata == nil {
		return nil
	}
	cloned := make(map[string]any, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func fileTokenSaveCommitted(err error) bool {
	outcome, explicit := cliproxyauth.SaveOutcomeFromError(err)
	return explicit && outcome == cliproxyauth.SaveOutcomeCommitted
}

func fileTokenSaveMayHavePersisted(err error) bool {
	outcome, explicit := cliproxyauth.SaveOutcomeFromError(err)
	return explicit && outcome != cliproxyauth.SaveOutcomeRolledBack
}

func fileTokenExchangeRollbackConfirmed(err error) bool {
	if outcome, explicit := cliproxyauth.SaveOutcomeFromError(err); explicit {
		return outcome == cliproxyauth.SaveOutcomeRolledBack
	}
	return errors.Is(err, authfileguard.ErrExchangeCleanupRequired) &&
		!errors.Is(err, authfileguard.ErrExchangeOutcomeUncertain)
}

func mapFileTokenCreateGenerationConflict(requireAbsent bool, err error) error {
	if requireAbsent && errors.Is(err, authfileguard.ErrPersistGenerationStale) {
		if outcome, explicit := cliproxyauth.SaveOutcomeFromError(err); explicit && outcome != cliproxyauth.SaveOutcomeRolledBack {
			return err
		}
		return errors.Join(cliproxyauth.ErrAuthAlreadyExists, err)
	}
	return err
}

func joinCommittedFileTokenSaveWarning(current, next error) error {
	joined := errors.Join(current, next)
	if joined == nil {
		return nil
	}
	return cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeCommitted, joined)
}

func finishFileTokenSave(auth *cliproxyauth.Auth, persistedPath string, committedWarning error) (string, error) {
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["path"] = persistedPath

	if strings.TrimSpace(auth.FileName) == "" {
		auth.FileName = auth.ID
	}

	return persistedPath, committedWarning
}

// List enumerates all auth JSON files under the configured directory.
func (s *FileTokenStore) List(ctx context.Context) ([]*cliproxyauth.Auth, error) {
	_, dir, errBase := resolveFileTokenBaseDir(s.baseDirSnapshot(), false)
	if errBase != nil {
		return nil, errBase
	}
	s.rememberResolvedBaseDir(dir)
	if dir == "" {
		return nil, fmt.Errorf("auth filestore: directory not configured")
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		return nil, fmt.Errorf("auth filestore: open directory: %w", errRoot)
	}
	defer closeFileTokenRoot(root)
	entries := make([]*cliproxyauth.Auth, 0)
	err := fs.WalkDir(root.FS(), ".", func(walkPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if errContext := ctx.Err(); errContext != nil {
			return errContext
		}
		if d.IsDir() {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			return nil
		}
		info, errInfo := d.Info()
		if errInfo != nil || !info.Mode().IsRegular() {
			return errInfo
		}
		relativePath := filepath.FromSlash(walkPath)
		path := filepath.Join(dir, relativePath)
		auth, err := s.readAuthFileFromRoot(ctx, root, relativePath, path, dir)
		if err != nil {
			if errContext := ctx.Err(); errContext != nil {
				return errContext
			}
			return nil
		}
		if auth != nil {
			entries = append(entries, auth)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// Delete removes the auth file.
func (s *FileTokenStore) Delete(ctx context.Context, id string) (resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return errContext
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("auth filestore: id is empty")
	}
	if authfileguard.IsPersistentLockPath(filepath.FromSlash(id)) {
		return fmt.Errorf("auth filestore: auth path uses a reserved lock name")
	}
	lexicalBaseDir, resolvedBaseDir, errBase := resolveFileTokenBaseDir(s.baseDirSnapshot(), false)
	if errBase != nil {
		if errors.Is(errBase, os.ErrNotExist) {
			path := filepath.FromSlash(id)
			if !filepath.IsAbs(path) {
				guardBaseDir := s.resolvedBaseDirSnapshot()
				if guardBaseDir == "" {
					guardBaseDir = lexicalBaseDir
				}
				guardPath := filepath.Join(guardBaseDir, path)
				if _, inside := relativePathWithin(guardBaseDir, guardPath); inside {
					unlockOperation, errOperationLock := s.lockOperation(ctx)
					if errOperationLock != nil {
						return errOperationLock
					}
					defer unlockOperation()
					unlockPath, errPathLock := authfileguard.LockContext(ctx, guardPath)
					if errPathLock != nil {
						return fmt.Errorf("auth filestore: lock auth path for deletion: %w", errPathLock)
					}
					defer unlockPath()
					retiredSnapshot := authfileguard.CaptureRetired(guardPath)
					authfileguard.ClearRetiredSnapshot(retiredSnapshot)
				}
				return nil
			}
			if !isExplicitOutsideBaseDir(path, lexicalBaseDir, resolvedBaseDir) {
				return nil
			}
		} else {
			return errBase
		}
	}
	s.rememberResolvedBaseDir(resolvedBaseDir)
	path, guardPath, allowOutsideBaseDir, err := s.resolveDeletePath(id, lexicalBaseDir, resolvedBaseDir)
	if err != nil {
		return err
	}
	unlockOperation, errOperationLock := s.lockOperation(ctx)
	if errOperationLock != nil {
		return errOperationLock
	}
	defer unlockOperation()
	if errContext := ctx.Err(); errContext != nil {
		return errContext
	}
	root, relativePath, managed, errRoot := openRootForPath(path, resolvedBaseDir)
	if errRoot != nil {
		if os.IsNotExist(errRoot) {
			unlockPath, errPathLock := authfileguard.LockContext(ctx, guardPath)
			if errPathLock != nil {
				return fmt.Errorf("auth filestore: lock auth path for deletion: %w", errPathLock)
			}
			defer unlockPath()
			retiredSnapshot := authfileguard.CaptureRetired(guardPath)
			authfileguard.ClearRetiredSnapshot(retiredSnapshot)
			return nil
		}
		return errRoot
	}
	defer closeFileTokenRoot(root)
	if !managed && !allowOutsideBaseDir {
		return fmt.Errorf("auth filestore: managed auth path is outside base dir")
	}
	if !managed {
		guardPath = filepath.Join(root.Name(), relativePath)
	}
	if managed {
		unlockRootMutation, errMutationLock := authfileguard.LockRootMutationContext(ctx, root)
		if errMutationLock != nil {
			return fmt.Errorf("auth filestore: lock auth root for deletion: %w", errMutationLock)
		}
		defer func() {
			resultErr = joinFileTokenDeleteUnlock(resultErr, unlockRootMutation())
		}()
	}
	unlockPath, errPathLock := authfileguard.LockContext(ctx, guardPath)
	if errPathLock != nil {
		return fmt.Errorf("auth filestore: lock auth path for deletion: %w", errPathLock)
	}
	defer unlockPath()
	if errRootIdentity := revalidateFileTokenRootIdentity(root); errRootIdentity != nil {
		return cliproxyauth.NewDeleteOutcomeError(
			cliproxyauth.DeleteOutcomeRolledBack,
			fmt.Errorf("auth filestore: auth root changed while locking delete path: %w", errRootIdentity),
		)
	}
	retiredSnapshot := authfileguard.CaptureRetired(guardPath)
	unlockTarget, errTargetLock := lockRootAuthTarget(ctx, root, relativePath)
	if errTargetLock != nil {
		return fmt.Errorf("auth filestore: lock auth target for deletion: %w", errTargetLock)
	}
	defer func() {
		resultErr = joinFileTokenDeleteUnlock(resultErr, unlockTarget())
	}()
	parentRoot, leaf, closeParent, errParent := openFileTokenParent(root, relativePath)
	if errParent != nil {
		if errors.Is(errParent, fs.ErrNotExist) {
			authfileguard.ClearRetiredSnapshot(retiredSnapshot)
			return nil
		}
		return fmt.Errorf("auth filestore: open auth parent: %w", errParent)
	}
	defer closeParent()
	if errParentIdentity := revalidateFileTokenParent(root, relativePath, parentRoot); errParentIdentity != nil {
		return cliproxyauth.NewDeleteOutcomeError(
			cliproxyauth.DeleteOutcomeRolledBack,
			fmt.Errorf("auth filestore: auth parent changed before deletion: %w", errParentIdentity),
		)
	}
	err = parentRoot.Remove(leaf)
	if err != nil && !os.IsNotExist(err) {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: delete failed: %w", err))
	}
	var errSync error
	if err == nil {
		errSync = s.syncRootDirectory(parentRoot, ".")
	}
	errParentIdentity := revalidateFileTokenParent(root, relativePath, parentRoot)
	if errSync != nil || errParentIdentity != nil {
		return cliproxyauth.NewDeleteOutcomeError(
			cliproxyauth.DeleteOutcomeUncertain,
			errors.Join(
				wrapFileTokenError("auth filestore: sync deleted auth", errSync),
				wrapFileTokenError("auth filestore: auth parent changed after deletion", errParentIdentity),
			),
		)
	}
	authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	return nil
}

// DeleteAuthFileAtRoot removes one managed auth file through a stable root while
// serializing against concurrent saves to the same store and path.
func (s *FileTokenStore) DeleteAuthFileAtRoot(baseDir string, root *os.Root, id string) (resultErr error) {
	return s.DeleteAuthFileAtRootContext(context.Background(), baseDir, root, id)
}

// DeleteAuthFileAtRootContext is DeleteAuthFileAtRoot with cancellable lock waiting.
func (s *FileTokenStore) DeleteAuthFileAtRootContext(ctx context.Context, baseDir string, root *os.Root, id string) (resultErr error) {
	return s.DeleteAuthFileAtRootPreparedContext(ctx, baseDir, root, id, nil)
}

// DeleteAuthFileAtRootPrepared runs prepare after locking and reading the
// current file, then removes that exact managed path.
func (s *FileTokenStore) DeleteAuthFileAtRootPrepared(baseDir string, root *os.Root, id string, prepare func(path string, data []byte) error) (resultErr error) {
	return s.DeleteAuthFileAtRootPreparedContext(context.Background(), baseDir, root, id, prepare)
}

// DeleteAuthFileAtRootPreparedContext is DeleteAuthFileAtRootPrepared with cancellable lock waiting.
func (s *FileTokenStore) DeleteAuthFileAtRootPreparedContext(ctx context.Context, baseDir string, root *os.Root, id string, prepare func(path string, data []byte) error) (resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errContext)
	}
	if root == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: root is nil"))
	}
	id = strings.TrimSpace(id)
	cleanID := filepath.Clean(filepath.FromSlash(id))
	if cleanID == "." || cleanID == ".." || strings.HasPrefix(cleanID, ".."+string(os.PathSeparator)) || filepath.IsAbs(cleanID) {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: invalid auth identifier %s", id))
	}
	if authfileguard.IsPersistentLockPath(cleanID) {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errors.New("auth filestore: auth path uses a reserved lock name"))
	}
	_, resolvedBaseDir, errBase := resolveFileTokenBaseDir(baseDir, false)
	if errBase != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errBase)
	}
	guardBaseDir := resolvedBaseDir
	if guardBaseDir == "" {
		guardBaseDir = filepath.Clean(root.Name())
	}
	guardPath := filepath.Join(guardBaseDir, cleanID)

	unlockOperation, errOperationLock := s.lockOperation(ctx)
	if errOperationLock != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, errOperationLock)
	}
	defer unlockOperation()
	unlockRootMutation, errMutationLock := authfileguard.LockRootMutationContext(ctx, root)
	if errMutationLock != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: lock auth root for deletion: %w", errMutationLock))
	}
	defer func() {
		resultErr = joinFileTokenDeleteUnlock(resultErr, unlockRootMutation())
	}()
	// Keep the same path-before-target order used by Save and Delete so
	// different FileTokenStore instances cannot form a cross-lock cycle.
	unlockPath, errPathLock := authfileguard.LockContext(ctx, guardPath)
	if errPathLock != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: lock auth path for deletion: %w", errPathLock))
	}
	defer unlockPath()
	unlockTarget, errLock := lockRootAuthTarget(ctx, root, cleanID)
	if errLock != nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: lock auth target for deletion: %w", errLock))
	}
	defer func() {
		resultErr = joinFileTokenDeleteUnlock(resultErr, unlockTarget())
	}()
	retiredSnapshot := authfileguard.CaptureRetired(guardPath)
	parentRoot, leaf, closeParent, errParent := openFileTokenParent(root, cleanID)
	if errParent != nil {
		if errors.Is(errParent, fs.ErrNotExist) {
			prepared := false
			if prepare != nil {
				if errPrepare := prepare(guardPath, nil); errPrepare != nil {
					return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: prepare missing deletion: %w", errPrepare))
				}
				prepared = true
			}
			checkRoot, checkLeaf, closeCheck, errCheck := openFileTokenParent(root, cleanID)
			if errCheck == nil {
				defer closeCheck()
				if _, errSnapshot := captureFileAuthSnapshot(checkRoot, checkLeaf); errSnapshot == nil {
					return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, authfileguard.ErrPersistGenerationStale)
				} else if !errors.Is(errSnapshot, fs.ErrNotExist) {
					return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeUncertain, fmt.Errorf("auth filestore: recheck missing auth: %w", errSnapshot))
				}
			} else if !errors.Is(errCheck, fs.ErrNotExist) {
				outcome := cliproxyauth.DeleteOutcomeRolledBack
				if prepared {
					outcome = cliproxyauth.DeleteOutcomeUncertain
				}
				return cliproxyauth.NewDeleteOutcomeError(outcome, fmt.Errorf("auth filestore: recheck missing auth parent: %w", errCheck))
			}
			authfileguard.ClearRetiredSnapshot(retiredSnapshot)
			return nil
		}
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: open auth parent: %w", errParent))
	}
	defer closeParent()
	if errParentIdentity := revalidateFileTokenParent(root, cleanID, parentRoot); errParentIdentity != nil {
		return cliproxyauth.NewDeleteOutcomeError(
			cliproxyauth.DeleteOutcomeRolledBack,
			fmt.Errorf("auth filestore: auth parent changed before deletion: %w", errParentIdentity),
		)
	}
	prepared := false
	var preparedSnapshot *fileAuthSnapshot
	if prepare != nil {
		snapshot, errRead := captureFileAuthSnapshot(parentRoot, leaf)
		if errRead != nil && !errors.Is(errRead, fs.ErrNotExist) {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: read auth before deletion: %w", errRead))
		}
		if errRead == nil {
			if errPrepare := prepare(guardPath, snapshot.data); errPrepare != nil {
				return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: prepare deletion: %w", errPrepare))
			}
			preparedSnapshot = snapshot
			prepared = true
		} else if errPrepare := prepare(guardPath, nil); errPrepare != nil {
			return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeRolledBack, fmt.Errorf("auth filestore: prepare missing deletion: %w", errPrepare))
		} else {
			prepared = true
		}
	}
	if preparedSnapshot != nil {
		errRemove := removePreparedFileAuthSnapshot(parentRoot, leaf, *preparedSnapshot, s.syncRootDirectory)
		errParentIdentity := revalidateFileTokenParent(root, cleanID, parentRoot)
		if errRemove != nil || errParentIdentity != nil {
			return cliproxyauth.NewDeleteOutcomeError(
				cliproxyauth.DeleteOutcomeUncertain,
				errors.Join(errRemove, wrapFileTokenError("auth filestore: auth parent changed after deletion", errParentIdentity)),
			)
		}
		authfileguard.ClearRetiredSnapshot(retiredSnapshot)
		return nil
	}
	errRemove := parentRoot.Remove(leaf)
	if errRemove != nil {
		if !os.IsNotExist(errRemove) {
			outcome := cliproxyauth.DeleteOutcomeRolledBack
			if prepared {
				outcome = cliproxyauth.DeleteOutcomeUncertain
			}
			return cliproxyauth.NewDeleteOutcomeError(outcome, fmt.Errorf("auth filestore: delete failed: %w", errRemove))
		}
	}
	var errSync error
	if errRemove == nil {
		errSync = s.syncRootDirectory(parentRoot, ".")
	}
	errParentIdentity := revalidateFileTokenParent(root, cleanID, parentRoot)
	if errSync != nil || errParentIdentity != nil {
		return cliproxyauth.NewDeleteOutcomeError(
			cliproxyauth.DeleteOutcomeUncertain,
			errors.Join(
				wrapFileTokenError("auth filestore: sync deleted auth", errSync),
				wrapFileTokenError("auth filestore: auth parent changed after deletion", errParentIdentity),
			),
		)
	}
	authfileguard.ClearRetiredSnapshot(retiredSnapshot)
	return nil
}

func removePreparedFileAuthSnapshot(root *os.Root, relativePath string, expected fileAuthSnapshot, syncDirectory func(*os.Root, string) error) error {
	displacedPath := filepath.Join(filepath.Dir(relativePath), ".auth-delete-"+uuid.NewString())
	if errMove := root.Rename(relativePath, displacedPath); errMove != nil {
		return fmt.Errorf("auth filestore: move prepared auth for deletion: %w", errMove)
	}
	displaced, errDisplaced := captureFileAuthSnapshot(root, displacedPath)
	if errDisplaced == nil && os.SameFile(expected.info, displaced.info) && bytes.Equal(expected.data, displaced.data) {
		return discardFileTokenSnapshot(root, displacedPath, syncDirectory)
	}
	errRestore := restoreDisplacedFileTokenSnapshot(root, displacedPath, relativePath, syncDirectory)
	return errors.Join(errDisplaced, authfileguard.ErrPersistGenerationStale, errRestore)
}

func restoreDisplacedFileTokenSnapshot(root *os.Root, displacedPath, relativePath string, syncDirectory func(*os.Root, string) error) error {
	cleanupWarning, errRestore := authfileguard.InstallStagedFileNoReplace(root, displacedPath, relativePath)
	if errRestore != nil {
		if errors.Is(errRestore, authfileguard.ErrPersistGenerationStale) {
			return errors.Join(errRestore, syncDirectory(root, filepath.Dir(relativePath)))
		}
		return errRestore
	}
	if errRestore == nil {
		if errors.Is(cleanupWarning, authfileguard.ErrStagedFileCleanupRequired) {
			return errors.Join(cleanupWarning, discardFileTokenSnapshot(root, displacedPath, syncDirectory))
		}
	}
	return errors.Join(cleanupWarning, syncDirectory(root, filepath.Dir(relativePath)))
}

func wrapFileTokenError(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}

func joinFileTokenDeleteUnlock(resultErr, errUnlock error) error {
	if errUnlock == nil {
		return resultErr
	}
	if resultErr == nil {
		return cliproxyauth.NewDeleteOutcomeError(cliproxyauth.DeleteOutcomeCommitted, errUnlock)
	}
	outcome, explicit := cliproxyauth.DeleteOutcomeFromError(resultErr)
	if !explicit {
		return errors.Join(resultErr, errUnlock)
	}
	return cliproxyauth.NewDeleteOutcomeError(outcome, errors.Join(resultErr, errUnlock))
}

func (s *FileTokenStore) resolveDeletePath(id, lexicalBaseDir, resolvedBaseDir string) (string, string, bool, error) {
	path := filepath.FromSlash(id)
	allowOutsideBaseDir := isExplicitOutsideBaseDir(path, lexicalBaseDir, resolvedBaseDir)
	if !filepath.IsAbs(path) {
		if lexicalBaseDir == "" {
			absolutePath, errAbs := filepath.Abs(path)
			if errAbs != nil {
				return "", "", false, fmt.Errorf("auth filestore: resolve delete path: %w", errAbs)
			}
			path = absolutePath
			allowOutsideBaseDir = true
		} else {
			path = filepath.Join(lexicalBaseDir, path)
		}
	}
	resolved, err := s.secureAuthPath(path, allowOutsideBaseDir, lexicalBaseDir, resolvedBaseDir)
	return resolved, resolved, allowOutsideBaseDir, err
}

func (s *FileTokenStore) secureAuthPath(path string, allowOutsideBaseDir bool, lexicalBaseDir, resolvedBaseDir string) (string, error) {
	path, errAbs := filepath.Abs(path)
	if errAbs != nil {
		return "", fmt.Errorf("auth filestore: resolve auth path: %w", errAbs)
	}
	path = filepath.Clean(path)
	if lexicalBaseDir != "" {
		if rel, inside := relativePathWithin(lexicalBaseDir, path); inside {
			path = filepath.Join(resolvedBaseDir, rel)
		}
		if _, inside := relativePathWithin(resolvedBaseDir, path); inside {
			if hasSymlink, errSymlink := pathHasSymlinkBelow(resolvedBaseDir, path); errSymlink != nil {
				return "", fmt.Errorf("auth filestore: inspect auth path: %w", errSymlink)
			} else if hasSymlink {
				return "", fmt.Errorf("auth filestore: refusing symlink auth path")
			}
			return path, nil
		}
		if rel, inside := physicalRelativePathWithin(resolvedBaseDir, path); inside {
			path = filepath.Join(resolvedBaseDir, rel)
			if hasSymlink, errSymlink := pathHasSymlinkBelow(resolvedBaseDir, path); errSymlink != nil {
				return "", fmt.Errorf("auth filestore: inspect auth path: %w", errSymlink)
			} else if hasSymlink {
				return "", fmt.Errorf("auth filestore: refusing symlink auth path")
			}
			return path, nil
		}
		if allowOutsideBaseDir {
			if info, errInfo := os.Lstat(path); errInfo == nil && info.Mode()&os.ModeSymlink != 0 {
				return "", fmt.Errorf("auth filestore: refusing symlink auth path")
			}
			return path, nil
		}
		return "", fmt.Errorf("auth filestore: auth path is outside base dir")
	}
	if info, errInfo := os.Lstat(path); errInfo == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("auth filestore: refusing symlink auth path")
	}
	return path, nil
}

func isExplicitOutsideBaseDir(path, lexicalBaseDir, resolvedBaseDir string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	path = filepath.Clean(path)
	if lexicalBaseDir != "" {
		if _, inside := relativePathWithin(lexicalBaseDir, path); inside {
			return false
		}
		if _, inside := relativePathWithin(resolvedBaseDir, path); inside {
			return false
		}
		if _, inside := physicalRelativePathWithin(resolvedBaseDir, path); inside {
			return false
		}
	}
	return true
}

func relativePathWithin(root, path string) (string, bool) {
	rel, errRel := filepath.Rel(root, path)
	if errRel != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return rel, true
}

func physicalRelativePathWithin(root, path string) (string, bool) {
	rootInfo, errRoot := os.Stat(root)
	if errRoot != nil || !rootInfo.IsDir() {
		return "", false
	}
	current := filepath.Clean(path)
	components := make([]string, 0, 2)
	for {
		info, errInfo := os.Stat(current)
		if errInfo == nil {
			if os.SameFile(rootInfo, info) {
				for left, right := 0, len(components)-1; left < right; left, right = left+1, right-1 {
					components[left], components[right] = components[right], components[left]
				}
				if len(components) == 0 {
					return ".", true
				}
				return filepath.Join(components...), true
			}
		} else if !os.IsNotExist(errInfo) {
			return "", false
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", false
		}
		components = append(components, filepath.Base(current))
		current = parent
	}
}

func pathHasSymlinkBelow(root, path string) (bool, error) {
	rel, inside := relativePathWithin(root, path)
	if !inside {
		return false, fmt.Errorf("path is outside base dir")
	}
	current := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, errInfo := os.Lstat(current)
		if errInfo != nil {
			if os.IsNotExist(errInfo) {
				return false, nil
			}
			return false, errInfo
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return true, nil
		}
	}
	return false, nil
}

func (s *FileTokenStore) readAuthFile(ctx context.Context, path, baseDir string) (*cliproxyauth.Auth, error) {
	root, errOpen := os.OpenRoot(baseDir)
	if errOpen != nil {
		return nil, fmt.Errorf("open auth root: %w", errOpen)
	}
	defer closeFileTokenRoot(root)
	relativePath, errRel := filepath.Rel(baseDir, path)
	if errRel != nil || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("read file: path is outside base dir")
	}
	return s.readAuthFileFromRoot(ctx, root, relativePath, path, baseDir)
}

type fileAuthSnapshot struct {
	data []byte
	info fs.FileInfo
}

func captureFileAuthSnapshot(root *os.Root, relativePath string) (snapshot *fileAuthSnapshot, err error) {
	parentRoot, leaf, closeParent, errParent := openFileTokenParent(root, relativePath)
	if errParent != nil {
		return nil, errParent
	}
	defer closeParent()
	before, errBefore := parentRoot.Lstat(leaf)
	if errBefore != nil {
		return nil, errBefore
	}
	if errValidate := validateFileAuthSnapshotInfo(before); errValidate != nil {
		return nil, errValidate
	}
	file, errOpen := openFileTokenSnapshotFile(parentRoot, leaf)
	if errOpen != nil {
		return nil, errOpen
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			err = errors.Join(err, fmt.Errorf("close auth file: %w", errClose))
		}
	}()
	opened, errOpened := file.Stat()
	after, errAfter := parentRoot.Lstat(leaf)
	if errOpened != nil || errAfter != nil {
		return nil, errors.Join(errOpened, errAfter)
	}
	if errValidate := validateFileAuthSnapshotInfo(opened); errValidate != nil {
		return nil, errValidate
	}
	if errValidate := validateFileAuthSnapshotInfo(after); errValidate != nil {
		return nil, errValidate
	}
	if !os.SameFile(before, opened) || !os.SameFile(after, opened) {
		return nil, errors.New("auth filestore: auth path changed while opening")
	}
	data, errRead := io.ReadAll(file)
	if errRead != nil {
		return nil, errRead
	}
	opened, errOpened = authfileguard.HardenChatGPTWebCredentialFile(file, opened, data)
	if errOpened != nil {
		return nil, errOpened
	}
	return &fileAuthSnapshot{data: data, info: opened}, nil
}

func validateFileAuthSnapshotInfo(info fs.FileInfo) error {
	if info == nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("auth filestore: auth path is not a regular file")
	}
	return nil
}

func captureLockedFileAuthSnapshot(root *os.Root, relativePath, path string) (*fileAuthSnapshot, error) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	snapshot, err := captureFileAuthSnapshot(root, relativePath)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	if cliproxyauth.IsRetiredGeminiCLIAuthFileData(snapshot.data) {
		authfileguard.MarkRetired(path)
	}
	return snapshot, nil
}

func fileAuthSnapshotIsCurrent(root *os.Root, relativePath, path string, snapshot *fileAuthSnapshot) (bool, error) {
	unlockPath := authfileguard.Lock(path)
	defer unlockPath()
	current, err := captureFileAuthSnapshot(root, relativePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("revalidate auth file: %w", err)
	}
	return os.SameFile(snapshot.info, current.info) && bytes.Equal(snapshot.data, current.data), nil
}

func (s *FileTokenStore) readAuthFileFromRoot(ctx context.Context, root *os.Root, relativePath, path, baseDir string) (*cliproxyauth.Auth, error) {
	snapshot, err := captureLockedFileAuthSnapshot(root, relativePath, path)
	if err != nil {
		return nil, err
	}
	if len(snapshot.data) == 0 {
		return nil, nil
	}
	data := snapshot.data
	metadata := make(map[string]any)
	if err = json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshal auth json: %w", err)
	}
	provider, _ := metadata["type"].(string)
	provider = strings.TrimSpace(provider)
	if provider == "" {
		provider = "unknown"
	}
	diskMetadata := make(map[string]any, len(metadata))
	for key, value := range metadata {
		diskMetadata[key] = value
	}
	diskDisabled, _ := diskMetadata["disabled"].(bool)
	diskAuth := &cliproxyauth.Auth{Metadata: diskMetadata, Disabled: diskDisabled}
	diskCanonical, errCanonical := cliproxyauth.CanonicalMetadataBytes(diskAuth)
	if errCanonical != nil {
		return nil, fmt.Errorf("canonicalize auth metadata: %w", errCanonical)
	}
	fetchedProjectID := ""
	revalidateSnapshot := false
	if provider == "antigravity" {
		projectID := ""
		if pid, ok := metadata["project_id"].(string); ok {
			projectID = strings.TrimSpace(pid)
		}
		if projectID == "" {
			accessToken := extractAccessToken(metadata)
			if accessToken != "" {
				revalidateSnapshot = true
				projectIDFromUpstream, errFetch := fetchAntigravityProjectID(ctx, accessToken, http.DefaultClient)
				if errContext := ctx.Err(); errContext != nil {
					return nil, errContext
				}
				if errFetch == nil {
					fetchedProjectID = strings.TrimSpace(projectIDFromUpstream)
				}
			}
		}
	}
	if revalidateSnapshot {
		current, errCurrent := fileAuthSnapshotIsCurrent(root, relativePath, path, snapshot)
		if errCurrent != nil {
			return nil, errCurrent
		}
		if !current {
			return nil, nil
		}
	}
	if fetchedProjectID != "" {
		metadata["project_id"] = fetchedProjectID
	}
	id := s.idFor(path, baseDir)
	disabled, _ := metadata["disabled"].(bool)
	status := cliproxyauth.StatusActive
	if disabled {
		status = cliproxyauth.StatusDisabled
	}
	auth := &cliproxyauth.Auth{
		ID:               id,
		Provider:         provider,
		FileName:         id,
		Label:            s.labelFor(metadata),
		Status:           status,
		Disabled:         disabled,
		Attributes:       map[string]string{"path": path},
		Metadata:         metadata,
		CreatedAt:        snapshot.info.ModTime(),
		UpdatedAt:        snapshot.info.ModTime(),
		LastRefreshedAt:  time.Time{},
		NextRefreshAfter: time.Time{},
	}
	cliproxyauth.ApplyFileBackedGeminiAPIKey(auth)
	if strings.EqualFold(strings.TrimSpace(provider), "codex") {
		if planType := internalcodex.EffectivePlanType(metadata); planType != "" {
			auth.Attributes["plan_type"] = planType
		}
	}
	cliproxyauth.SetSourceHashAttribute(auth, diskCanonical)
	if email, ok := metadata["email"].(string); ok && email != "" {
		auth.Attributes["email"] = email
	}
	cliproxyauth.ApplyCustomHeadersFromMetadata(auth)
	return auth, nil
}

func (s *FileTokenStore) idFor(path, baseDir string) string {
	id := path
	if baseDir != "" {
		if rel, errRel := filepath.Rel(baseDir, path); errRel == nil && rel != "" {
			id = rel
		}
	}
	// On Windows, normalize ID casing to avoid duplicate auth entries caused by case-insensitive paths.
	if runtime.GOOS == "windows" {
		id = strings.ToLower(id)
	}
	return id
}

func (s *FileTokenStore) resolveAuthPath(auth *cliproxyauth.Auth, lexicalBaseDir, resolvedBaseDir string) (string, bool, error) {
	if auth == nil {
		return "", false, fmt.Errorf("auth filestore: auth is nil")
	}
	if auth.Attributes != nil {
		if p := strings.TrimSpace(auth.Attributes["path"]); p != "" {
			return p, lexicalBaseDir == "" || isExplicitOutsideBaseDir(p, lexicalBaseDir, resolvedBaseDir), nil
		}
	}
	if fileName := strings.TrimSpace(auth.FileName); fileName != "" {
		if filepath.IsAbs(fileName) {
			return fileName, isExplicitOutsideBaseDir(fileName, lexicalBaseDir, resolvedBaseDir), nil
		}
		if lexicalBaseDir != "" {
			return filepath.Join(lexicalBaseDir, fileName), false, nil
		}
		return fileName, true, nil
	}
	if auth.ID == "" {
		return "", false, fmt.Errorf("auth filestore: missing id")
	}
	if filepath.IsAbs(auth.ID) {
		return auth.ID, isExplicitOutsideBaseDir(auth.ID, lexicalBaseDir, resolvedBaseDir), nil
	}
	if lexicalBaseDir == "" {
		return "", false, fmt.Errorf("auth filestore: directory not configured")
	}
	return filepath.Join(lexicalBaseDir, auth.ID), false, nil
}

func (s *FileTokenStore) labelFor(metadata map[string]any) string {
	if metadata == nil {
		return ""
	}
	if v, ok := metadata["label"].(string); ok && v != "" {
		return v
	}
	if v, ok := metadata["email"].(string); ok && v != "" {
		return v
	}
	if project, ok := metadata["project_id"].(string); ok && project != "" {
		return project
	}
	return ""
}

func (s *FileTokenStore) baseDirSnapshot() string {
	s.dirLock.RLock()
	defer s.dirLock.RUnlock()
	return s.baseDir
}

func (s *FileTokenStore) rememberResolvedBaseDir(dir string) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return
	}
	s.dirLock.Lock()
	s.lastResolvedBaseDir = dir
	s.dirLock.Unlock()
}

func (s *FileTokenStore) resolvedBaseDirSnapshot() string {
	s.dirLock.RLock()
	defer s.dirLock.RUnlock()
	return s.lastResolvedBaseDir
}

func extractAccessToken(metadata map[string]any) string {
	if at, ok := metadata["access_token"].(string); ok {
		if v := strings.TrimSpace(at); v != "" {
			return v
		}
	}
	if tokenMap, ok := metadata["token"].(map[string]any); ok {
		if at, ok := tokenMap["access_token"].(string); ok {
			if v := strings.TrimSpace(at); v != "" {
				return v
			}
		}
	}
	return ""
}

// jsonEqual compares two JSON blobs by parsing them into Go objects and deep comparing.
func jsonEqual(a, b []byte) bool {
	objA, okA := decodeJSONForComparison(a)
	if !okA {
		return false
	}
	objB, okB := decodeJSONForComparison(b)
	if !okB {
		return false
	}
	return deepEqualJSON(objA, objB)
}

func decodeJSONForComparison(data []byte) (any, bool) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if errDecode := decoder.Decode(&value); errDecode != nil {
		return nil, false
	}
	var trailing any
	if errTrailing := decoder.Decode(&trailing); !errors.Is(errTrailing, io.EOF) {
		return nil, false
	}
	return value, true
}

func deepEqualJSON(a, b any) bool {
	switch valA := a.(type) {
	case map[string]any:
		valB, ok := b.(map[string]any)
		if !ok || len(valA) != len(valB) {
			return false
		}
		for key, subA := range valA {
			subB, ok1 := valB[key]
			if !ok1 || !deepEqualJSON(subA, subB) {
				return false
			}
		}
		return true
	case []any:
		sliceB, ok := b.([]any)
		if !ok || len(valA) != len(sliceB) {
			return false
		}
		for i := range valA {
			if !deepEqualJSON(valA[i], sliceB[i]) {
				return false
			}
		}
		return true
	case json.Number:
		valB, ok := b.(json.Number)
		if !ok {
			return false
		}
		return jsonsemantic.NumbersEqual(valA.String(), valB.String())
	case string:
		valB, ok := b.(string)
		if !ok {
			return false
		}
		return valA == valB
	case bool:
		valB, ok := b.(bool)
		if !ok {
			return false
		}
		return valA == valB
	case nil:
		return b == nil
	default:
		return false
	}
}
