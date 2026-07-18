//go:build !aix && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package store

import "os"

func openAuthSnapshotFile(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY, 0)
}
