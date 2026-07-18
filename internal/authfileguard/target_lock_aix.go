//go:build aix

package authfileguard

import (
	"os"

	"golang.org/x/sys/unix"
)

// AIX record locks are process-scoped, so shared root mutations must be serialized.
const serializeRootMutationLocks = true

func tryAcquirePersistentFileLock(file *os.File, exclusive bool) (func() error, bool, error) {
	lockType := int16(unix.F_RDLCK)
	if exclusive {
		lockType = int16(unix.F_WRLCK)
	}
	lock := unix.Flock_t{Type: lockType}
	if errLock := unix.FcntlFlock(file.Fd(), unix.F_SETLK, &lock); errLock != nil {
		if errLock == unix.EACCES || errLock == unix.EAGAIN {
			return nil, false, nil
		}
		return nil, false, errLock
	}
	return func() error {
		lock.Type = int16(unix.F_UNLCK)
		return unix.FcntlFlock(file.Fd(), unix.F_SETLK, &lock)
	}, true, nil
}
