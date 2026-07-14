//go:build !windows

package management

import (
	"errors"
	"os"
)

func syncManagedAuthDirectory(root *os.Root, relativePath string) (err error) {
	directory, errOpen := root.Open(relativePath)
	if errOpen != nil {
		return errOpen
	}
	defer func() { err = errors.Join(err, directory.Close()) }()
	return directory.Sync()
}
