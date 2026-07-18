package authfileguard

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	rootLockFileName    = ".auth-root-lock"
	targetLockPrefix    = ".auth-lock-"
	targetLockHexLength = 32
	lockRetryInterval   = 10 * time.Millisecond
)

// ErrAtomicExchangeUnsupported reports that the current platform or
// filesystem cannot atomically swap two auth file generations.
var ErrAtomicExchangeUnsupported = errors.New("auth file guard: atomic file exchange is unsupported")

// ErrStagedFileCleanupRequired marks a successful no-replace installation
// whose original staged path still needs removal.
var ErrStagedFileCleanupRequired = errors.New("auth file guard: staged file cleanup is required")

type persistentProcessLock struct {
	mu             sync.Mutex
	notify         chan struct{}
	readers        int
	writer         bool
	waitingWriters int
	refs           int
}

var persistentProcessLocks = struct {
	sync.Mutex
	entries map[string]*persistentProcessLock
}{entries: make(map[string]*persistentProcessLock)}

// LockRootMutation acquires the process-shared lock used by auth directory
// mutations. Multiple mutations may proceed while a rebuild is not active.
func LockRootMutation(root *os.Root) (unlock func() error, err error) {
	return LockRootMutationContext(context.Background(), root)
}

// LockRootMutationContext is LockRootMutation with cancellable lock waiting.
func LockRootMutationContext(ctx context.Context, root *os.Root) (unlock func() error, err error) {
	return lockRoot(ctx, root, false)
}

// LockRootRebuild acquires the process-shared exclusive lock used while
// rebuilding an entire auth directory.
func LockRootRebuild(root *os.Root) (unlock func() error, err error) {
	return LockRootRebuildContext(context.Background(), root)
}

// LockRootRebuildContext is LockRootRebuild with cancellable lock waiting.
func LockRootRebuildContext(ctx context.Context, root *os.Root) (unlock func() error, err error) {
	return lockRoot(ctx, root, true)
}

// IsPersistentLockFileName reports whether name is reserved for an auth file
// guard lock. Callers must also verify that the entry is a regular file.
func IsPersistentLockFileName(name string) bool {
	return isPersistentLockFileName(name, runtime.GOOS == "darwin" || runtime.GOOS == "windows")
}

func isPersistentLockFileName(name string, caseInsensitive bool) bool {
	if caseInsensitive {
		name = normalizeTargetLockKey(name, true)
	}
	if name == rootLockFileName {
		return true
	}
	if len(name) != len(targetLockPrefix)+targetLockHexLength || !strings.HasPrefix(name, targetLockPrefix) {
		return false
	}
	for _, char := range name[len(targetLockPrefix):] {
		if char < '0' || char > '9' {
			if char < 'a' || char > 'f' {
				return false
			}
		}
	}
	return true
}

// IsPersistentLockPath reports whether any path component is reserved for an
// auth file guard lock.
func IsPersistentLockPath(path string) bool {
	cleanPath := filepath.Clean(filepath.FromSlash(strings.TrimSpace(path)))
	if cleanPath == "." {
		return false
	}
	for {
		if IsPersistentLockFileName(filepath.Base(cleanPath)) {
			return true
		}
		parent := filepath.Dir(cleanPath)
		if parent == cleanPath || parent == "." {
			return false
		}
		cleanPath = parent
	}
}

// InstallStagedFileNoReplace installs stagedPath at targetPath without
// replacing an existing target. It falls back to an atomic no-replace rename
// when the filesystem does not support hard links.
func InstallStagedFileNoReplace(root *os.Root, stagedPath, targetPath string) (cleanupWarning error, err error) {
	return installStagedFileNoReplace(root, stagedPath, targetPath, func(root *os.Root, oldPath, newPath string) error {
		return root.Link(oldPath, newPath)
	})
}

// ExchangeStagedFile atomically installs stagedPath at targetPath while
// keeping the previous target generation at the returned root-relative path.
func ExchangeStagedFile(root *os.Root, stagedPath, targetPath string) (displacedPath string, err error) {
	if root == nil {
		return "", errors.New("auth file guard: root is nil")
	}
	stagedPath = filepath.Clean(stagedPath)
	targetPath = filepath.Clean(targetPath)
	stagedDir := filepath.Dir(stagedPath)
	if stagedDir != filepath.Dir(targetPath) {
		return "", errors.New("auth file guard: staged and target files must share a directory")
	}
	parent := root
	closeParent := false
	if stagedDir != "." {
		var errOpen error
		parent, errOpen = root.OpenRoot(stagedDir)
		if errOpen != nil {
			return "", fmt.Errorf("auth file guard: open exchange parent: %w", errOpen)
		}
		closeParent = true
	}
	displacedName, errExchange := exchangeFile(parent, filepath.Base(stagedPath), filepath.Base(targetPath))
	if closeParent {
		errExchange = errors.Join(errExchange, parent.Close())
	}
	displacedPath = displacedName
	if displacedName != "" && stagedDir != "." {
		displacedPath = filepath.Join(stagedDir, displacedName)
	}
	return displacedPath, errExchange
}

func installStagedFileNoReplace(root *os.Root, stagedPath, targetPath string, link func(*os.Root, string, string) error) (cleanupWarning error, err error) {
	if root == nil {
		return nil, errors.New("auth file guard: root is nil")
	}
	if link == nil {
		return nil, errors.New("auth file guard: staged file linker is nil")
	}
	errLink := link(root, stagedPath, targetPath)
	if errLink == nil {
		if errRemove := root.Remove(stagedPath); errRemove != nil && !errors.Is(errRemove, fs.ErrNotExist) {
			return errors.Join(
				ErrStagedFileCleanupRequired,
				fmt.Errorf("remove linked staged auth file: %w", errRemove),
			), nil
		}
		return nil, nil
	} else if errors.Is(errLink, fs.ErrExist) {
		return nil, fmt.Errorf("%w: auth target appeared during create", ErrPersistGenerationStale)
	}

	cleanupWarning, errRename := renameStagedFileNoReplace(root, stagedPath, targetPath)
	if errRename != nil {
		if errors.Is(errRename, fs.ErrExist) {
			return nil, fmt.Errorf("%w: auth target appeared during create", ErrPersistGenerationStale)
		}
		return nil, fmt.Errorf("auth file guard: install staged auth file without replacement: %w", errors.Join(errRename, errLink))
	}
	return cleanupWarning, nil
}

func renameStagedFileNoReplace(root *os.Root, stagedPath, targetPath string) (cleanupWarning error, err error) {
	stagedPath = filepath.Clean(stagedPath)
	targetPath = filepath.Clean(targetPath)
	stagedDir := filepath.Dir(stagedPath)
	targetDir := filepath.Dir(targetPath)
	if stagedDir != targetDir {
		return nil, errors.New("auth file guard: staged and target files must share a directory")
	}
	parent := root
	closeParent := false
	if stagedDir != "." {
		var errOpen error
		parent, errOpen = root.OpenRoot(stagedDir)
		if errOpen != nil {
			return nil, fmt.Errorf("auth file guard: open staged file parent: %w", errOpen)
		}
		closeParent = true
	}
	cleanupWarning, err = renameFileNoReplace(parent, filepath.Base(stagedPath), filepath.Base(targetPath))
	if closeParent {
		errClose := parent.Close()
		if err != nil {
			err = errors.Join(err, errClose)
		} else {
			cleanupWarning = errors.Join(cleanupWarning, errClose)
		}
	}
	return cleanupWarning, err
}

// LockRootTarget acquires the process-shared lock used for one root-relative
// auth target. Callers using the same root and relative path serialize across
// processes as well as packages.
func LockRootTarget(root *os.Root, relativePath string) (unlock func() error, err error) {
	return LockRootTargetContext(context.Background(), root, relativePath)
}

// LockRootTargetContext is LockRootTarget with cancellable lock waiting.
func LockRootTargetContext(ctx context.Context, root *os.Root, relativePath string) (unlock func() error, err error) {
	if root == nil {
		return nil, errors.New("auth file guard: root is nil")
	}
	lockPath, errLockPath := rootTargetLockPath(relativePath)
	if errLockPath != nil {
		return nil, errLockPath
	}
	return lockPersistentFile(ctx, root, lockPath, true)
}

// RootTargetLockIdentity returns the normalized identity used by LockRootTarget.
func RootTargetLockIdentity(root *os.Root, relativePath string) (string, error) {
	if root == nil {
		return "", errors.New("auth file guard: root is nil")
	}
	lockPath, errLockPath := rootTargetLockPath(relativePath)
	if errLockPath != nil {
		return "", errLockPath
	}
	return persistentProcessLockIdentity(root, lockPath)
}

func rootTargetLockPath(relativePath string) (string, error) {
	cleanPath := filepath.Clean(relativePath)
	if cleanPath == "." || filepath.IsAbs(cleanPath) || cleanPath == ".." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) {
		return "", errors.New("auth file guard: invalid target path")
	}
	lockKey := filepath.ToSlash(cleanPath)
	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		lockKey = normalizeTargetLockKey(lockKey, true)
	}
	digest := sha256.Sum256([]byte(lockKey))
	return fmt.Sprintf("%s%x", targetLockPrefix, digest[:16]), nil
}

func normalizeTargetLockKey(lockKey string, caseInsensitive bool) string {
	if !caseInsensitive {
		return lockKey
	}
	return norm.NFC.String(cases.Fold().String(norm.NFC.String(lockKey)))
}

func lockRoot(ctx context.Context, root *os.Root, exclusive bool) (unlock func() error, err error) {
	if root == nil {
		return nil, errors.New("auth file guard: root is nil")
	}
	if serializeRootMutationLocks {
		exclusive = true
	}
	return lockPersistentFile(ctx, root, rootLockFileName, exclusive)
}

func lockPersistentFile(ctx context.Context, root *os.Root, lockPath string, exclusive bool) (unlock func() error, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return nil, errContext
	}
	unlockProcess, errProcess := lockPersistentProcess(ctx, root, lockPath, exclusive)
	if errProcess != nil {
		return nil, errProcess
	}
	if errContext := ctx.Err(); errContext != nil {
		unlockProcess()
		return nil, errContext
	}
	file, errOpen := openPersistentLockFile(root, lockPath)
	if errOpen != nil {
		unlockProcess()
		return nil, errOpen
	}
	closeBeforeProcessUnlock := func(errs ...error) error {
		errClose := file.Close()
		unlockProcess()
		errs = append(errs, errClose)
		return errors.Join(errs...)
	}
	var unlockFile func() error
	for {
		if errContext := ctx.Err(); errContext != nil {
			return nil, closeBeforeProcessUnlock(errContext)
		}
		var acquired bool
		unlockFile, acquired, err = tryAcquirePersistentFileLock(file, exclusive)
		if err != nil {
			return nil, closeBeforeProcessUnlock(err)
		}
		if acquired {
			if errContext := ctx.Err(); errContext != nil {
				errUnlock := unlockFile()
				return nil, closeBeforeProcessUnlock(errContext, errUnlock)
			}
			if _, errIdentity := validatePersistentLockFileIdentity(root, lockPath, file); errIdentity != nil {
				errUnlock := unlockFile()
				return nil, closeBeforeProcessUnlock(errIdentity, errUnlock)
			}
			break
		}
		timer := time.NewTimer(lockRetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, closeBeforeProcessUnlock(ctx.Err())
		case <-timer.C:
		}
	}
	var once sync.Once
	var unlockErr error
	return func() error {
		once.Do(func() {
			unlockErr = errors.Join(unlockFile(), file.Close())
			unlockProcess()
		})
		return unlockErr
	}, nil
}

func lockPersistentProcess(ctx context.Context, root *os.Root, lockPath string, exclusive bool) (func(), error) {
	key, errIdentity := persistentProcessLockIdentity(root, lockPath)
	if errIdentity != nil {
		return nil, errIdentity
	}
	persistentProcessLocks.Lock()
	entry := persistentProcessLocks.entries[key]
	if entry == nil {
		entry = &persistentProcessLock{notify: make(chan struct{})}
		persistentProcessLocks.entries[key] = entry
	}
	entry.refs++
	persistentProcessLocks.Unlock()

	releaseReference := func() {
		persistentProcessLocks.Lock()
		entry.refs--
		if entry.refs == 0 && persistentProcessLocks.entries[key] == entry {
			delete(persistentProcessLocks.entries, key)
		}
		persistentProcessLocks.Unlock()
	}
	waitingWriter := false
	for {
		entry.mu.Lock()
		if errContext := ctx.Err(); errContext != nil {
			if waitingWriter {
				entry.waitingWriters--
				entry.signalLocked()
			}
			entry.mu.Unlock()
			releaseReference()
			return nil, errContext
		}
		if exclusive {
			if !entry.writer && entry.readers == 0 {
				if waitingWriter {
					entry.waitingWriters--
				}
				entry.writer = true
				entry.mu.Unlock()
				unlock := func() {
					entry.mu.Lock()
					entry.writer = false
					entry.signalLocked()
					entry.mu.Unlock()
					releaseReference()
				}
				if errContext := ctx.Err(); errContext != nil {
					unlock()
					return nil, errContext
				}
				return unlock, nil
			}
			if !waitingWriter {
				entry.waitingWriters++
				waitingWriter = true
			}
		} else if !entry.writer && entry.waitingWriters == 0 {
			entry.readers++
			entry.mu.Unlock()
			unlock := func() {
				entry.mu.Lock()
				entry.readers--
				if entry.readers == 0 {
					entry.signalLocked()
				}
				entry.mu.Unlock()
				releaseReference()
			}
			if errContext := ctx.Err(); errContext != nil {
				unlock()
				return nil, errContext
			}
			return unlock, nil
		}
		notify := entry.notify
		entry.mu.Unlock()
		select {
		case <-ctx.Done():
			if waitingWriter {
				entry.mu.Lock()
				entry.waitingWriters--
				entry.signalLocked()
				entry.mu.Unlock()
			}
			releaseReference()
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

func (l *persistentProcessLock) signalLocked() {
	close(l.notify)
	l.notify = make(chan struct{})
}

func openPersistentLockFile(root *os.Root, lockPath string) (*os.File, error) {
	before, errBefore := root.Lstat(lockPath)
	if errBefore != nil && !errors.Is(errBefore, fs.ErrNotExist) {
		return nil, fmt.Errorf("auth file guard: inspect persistent lock: %w", errBefore)
	}
	if errBefore == nil && (before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular()) {
		return nil, errors.New("auth file guard: persistent lock is not a regular file")
	}
	var file *os.File
	var errOpen error
	for attempt := 0; attempt < 8; attempt++ {
		file, errOpen = root.OpenFile(lockPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if errOpen == nil {
			break
		}
		if !errors.Is(errOpen, fs.ErrExist) {
			break
		}
		file, errOpen = root.OpenFile(lockPath, os.O_RDWR, 0)
		if errOpen == nil || !errors.Is(errOpen, fs.ErrNotExist) {
			break
		}
	}
	if errOpen != nil {
		return nil, fmt.Errorf("auth file guard: open persistent lock: %w", errOpen)
	}
	opened, errIdentity := validatePersistentLockFileIdentity(root, lockPath, file)
	if errIdentity != nil || errBefore == nil && !os.SameFile(before, opened) {
		return nil, errors.Join(errIdentity, file.Close(), errors.New("auth file guard: persistent lock changed while opening"))
	}
	return file, nil
}

func validatePersistentLockFileIdentity(root *os.Root, lockPath string, file *os.File) (fs.FileInfo, error) {
	if root == nil || file == nil {
		return nil, errors.New("auth file guard: persistent lock is unavailable")
	}
	opened, errOpened := file.Stat()
	current, errCurrent := root.Lstat(lockPath)
	if errOpened != nil || errCurrent != nil {
		return nil, errors.Join(errOpened, errCurrent, errors.New("auth file guard: inspect persistent lock identity"))
	}
	if current.Mode()&os.ModeSymlink != 0 || !current.Mode().IsRegular() || !opened.Mode().IsRegular() || !os.SameFile(opened, current) {
		return nil, errors.New("auth file guard: persistent lock path changed")
	}
	return opened, nil
}
