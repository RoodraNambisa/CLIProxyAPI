//go:build windows

package watcher

import (
	"os"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
)

func syncRootDirectory(root *os.Root, path string) error {
	return authfileguard.SyncRootDirectory(root, path)
}
