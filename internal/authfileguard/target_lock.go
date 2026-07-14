package authfileguard

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// LockRootTarget acquires the process-shared lock used for one root-relative
// auth target. Callers using the same root and relative path serialize across
// processes as well as packages.
func LockRootTarget(root *os.Root, relativePath string) (unlock func() error, err error) {
	if root == nil {
		return nil, errors.New("auth file guard: root is nil")
	}
	cleanPath := filepath.Clean(relativePath)
	if cleanPath == "." || filepath.IsAbs(cleanPath) || cleanPath == ".." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) {
		return nil, errors.New("auth file guard: invalid target path")
	}
	digest := sha256.Sum256([]byte(cleanPath))
	lockPath := filepath.Join(filepath.Dir(cleanPath), fmt.Sprintf(".auth-lock-%x", digest[:16]))
	before, errBefore := root.Lstat(lockPath)
	if errBefore != nil && !errors.Is(errBefore, fs.ErrNotExist) {
		return nil, fmt.Errorf("auth file guard: inspect target lock: %w", errBefore)
	}
	if errBefore == nil && (before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular()) {
		return nil, errors.New("auth file guard: target lock is not a regular file")
	}
	file, errOpen := root.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if errOpen != nil {
		return nil, fmt.Errorf("auth file guard: open target lock: %w", errOpen)
	}
	opened, errOpened := file.Stat()
	after, errAfter := root.Lstat(lockPath)
	if errOpened != nil || errAfter != nil || after.Mode()&os.ModeSymlink != 0 || !after.Mode().IsRegular() || !os.SameFile(opened, after) || (errBefore == nil && !os.SameFile(before, opened)) {
		return nil, errors.Join(errOpened, errAfter, file.Close(), errors.New("auth file guard: target lock changed while opening"))
	}
	unlockFile, errLock := acquireTargetFileLock(file)
	if errLock != nil {
		return nil, errors.Join(errLock, file.Close())
	}
	return func() error {
		return errors.Join(unlockFile(), file.Close())
	}, nil
}
