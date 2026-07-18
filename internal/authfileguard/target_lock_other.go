//go:build !aix && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package authfileguard

import (
	"errors"
	"os"
)

const serializeRootMutationLocks = false

func tryAcquirePersistentFileLock(*os.File, bool) (func() error, bool, error) {
	return nil, false, errors.New("auth file guard: process-shared file locks are unsupported on this platform")
}
