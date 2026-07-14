//go:build windows

package management

import (
	"os"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
)

func syncManagedAuthDirectory(root *os.Root, relativePath string) error {
	return authfileguard.SyncRootDirectory(root, relativePath)
}
