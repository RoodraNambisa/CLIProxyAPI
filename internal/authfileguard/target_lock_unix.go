//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package authfileguard

import (
	"os"

	"golang.org/x/sys/unix"
)

func acquireTargetFileLock(file *os.File) (func() error, error) {
	if errLock := unix.Flock(int(file.Fd()), unix.LOCK_EX); errLock != nil {
		return nil, errLock
	}
	return func() error { return unix.Flock(int(file.Fd()), unix.LOCK_UN) }, nil
}
