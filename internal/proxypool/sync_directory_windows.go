//go:build windows

package proxypool

import (
	"errors"
	"os"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
)

func syncProxyBindingDirectory(directory string) (err error) {
	root, errOpen := os.OpenRoot(directory)
	if errOpen != nil {
		return errOpen
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	return authfileguard.SyncRootDirectory(root, ".")
}
