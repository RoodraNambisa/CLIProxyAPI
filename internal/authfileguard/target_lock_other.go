//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package authfileguard

import "os"

func acquireTargetFileLock(*os.File) (func() error, error) {
	return func() error { return nil }, nil
}
