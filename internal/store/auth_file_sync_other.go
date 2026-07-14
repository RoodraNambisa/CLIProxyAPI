//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package store

import "os"

func syncAuthSnapshotDirectory(*os.Root) error {
	return nil
}
