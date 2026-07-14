//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package store

import "os"

func acquireStoreFileLock(*os.File) (func() error, error) {
	return func() error { return nil }, nil
}
