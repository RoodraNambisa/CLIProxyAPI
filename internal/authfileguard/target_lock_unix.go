//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package authfileguard

import (
	"os"

	"golang.org/x/sys/unix"
)

const serializeRootMutationLocks = false

func tryAcquirePersistentFileLock(file *os.File, exclusive bool) (func() error, bool, error) {
	mode := unix.LOCK_SH
	if exclusive {
		mode = unix.LOCK_EX
	}
	if errLock := unix.Flock(int(file.Fd()), mode|unix.LOCK_NB); errLock != nil {
		if errLock == unix.EWOULDBLOCK || errLock == unix.EAGAIN {
			return nil, false, nil
		}
		return nil, false, errLock
	}
	return func() error { return unix.Flock(int(file.Fd()), unix.LOCK_UN) }, true, nil
}
