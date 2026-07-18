//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package auth

import (
	"os"

	"golang.org/x/sys/unix"
)

func openFileTokenSnapshotFile(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
}
