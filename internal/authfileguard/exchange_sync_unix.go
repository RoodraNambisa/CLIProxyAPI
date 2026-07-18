//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package authfileguard

import (
	"errors"
	"os"
)

func syncExchangeDirectory(root *os.Root) (err error) {
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		return errOpen
	}
	defer func() { err = errors.Join(err, directory.Close()) }()
	return directory.Sync()
}
