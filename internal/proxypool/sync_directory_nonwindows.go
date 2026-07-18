//go:build !windows

package proxypool

import (
	"errors"
	"os"
)

func syncProxyBindingDirectory(directory string) (err error) {
	handle, errOpen := os.Open(directory)
	if errOpen != nil {
		return errOpen
	}
	defer func() { err = errors.Join(err, handle.Close()) }()
	return handle.Sync()
}
