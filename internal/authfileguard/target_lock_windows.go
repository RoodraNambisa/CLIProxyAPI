//go:build windows

package authfileguard

import (
	"os"

	"golang.org/x/sys/windows"
)

const serializeRootMutationLocks = false

func tryAcquirePersistentFileLock(file *os.File, exclusive bool) (func() error, bool, error) {
	overlapped := &windows.Overlapped{}
	handle := windows.Handle(file.Fd())
	flags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	if errLock := windows.LockFileEx(handle, flags, 0, 1, 0, overlapped); errLock != nil {
		if errLock == windows.ERROR_LOCK_VIOLATION {
			return nil, false, nil
		}
		return nil, false, errLock
	}
	return func() error { return windows.UnlockFileEx(handle, 0, 1, 0, overlapped) }, true, nil
}
