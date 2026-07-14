//go:build windows

package authfileguard

import (
	"os"

	"golang.org/x/sys/windows"
)

func acquireTargetFileLock(file *os.File) (func() error, error) {
	overlapped := &windows.Overlapped{}
	handle := windows.Handle(file.Fd())
	if errLock := windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, overlapped); errLock != nil {
		return nil, errLock
	}
	return func() error { return windows.UnlockFileEx(handle, 0, 1, 0, overlapped) }, nil
}
