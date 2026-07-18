//go:build aix

package authfileguard

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func persistentProcessLockIdentity(root *os.Root, lockPath string) (string, error) {
	cleanPath := filepath.Clean(lockPath)
	parent := root
	closeParent := false
	if parentPath := filepath.Dir(cleanPath); parentPath != "." {
		var errOpen error
		parent, errOpen = root.OpenRoot(parentPath)
		if errOpen != nil {
			return "", fmt.Errorf("auth file guard: open persistent lock parent: %w", errOpen)
		}
		closeParent = true
	}
	directory, errOpen := parent.Open(".")
	if errOpen != nil {
		if closeParent {
			return "", errors.Join(errOpen, parent.Close())
		}
		return "", errOpen
	}
	var stat unix.Stat_t
	errStat := unix.Fstat(int(directory.Fd()), &stat)
	errClose := directory.Close()
	if closeParent {
		errClose = errors.Join(errClose, parent.Close())
	}
	if errStat != nil || errClose != nil {
		return "", errors.Join(errStat, errClose)
	}
	return fmt.Sprintf("aix:%d:%d:%s", stat.Dev, stat.Ino, filepath.Base(cleanPath)), nil
}
