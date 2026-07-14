//go:build windows

package auth

import (
	"os"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
)

func syncAuthRootDirectory(root *os.Root, relativePath string) error {
	return authfileguard.SyncRootDirectory(root, relativePath)
}
