//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package store

import (
	"errors"
	"os"
)

func syncAuthSnapshotDirectory(root *os.Root) (err error) {
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		return errOpen
	}
	defer func() { err = errors.Join(err, directory.Close()) }()
	return directory.Sync()
}
