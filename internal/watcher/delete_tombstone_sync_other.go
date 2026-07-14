//go:build !windows

package watcher

import (
	"errors"
	"os"
)

func syncRootDirectory(root *os.Root, path string) error {
	directory, errOpen := root.Open(path)
	if errOpen != nil {
		return errOpen
	}
	errSync := directory.Sync()
	errClose := directory.Close()
	return errors.Join(errSync, errClose)
}
