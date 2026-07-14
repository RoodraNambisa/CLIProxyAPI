//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package auth

import (
	"errors"
	"os"
)

func syncAuthRootDirectory(root *os.Root, relativePath string) (err error) {
	directory, errOpen := root.Open(relativePath)
	if errOpen != nil {
		return errOpen
	}
	defer func() { err = errors.Join(err, directory.Close()) }()
	return directory.Sync()
}
