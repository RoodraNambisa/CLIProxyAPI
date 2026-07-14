//go:build windows

package store

import (
	"os"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
)

func syncAuthSnapshotDirectory(root *os.Root) error {
	return authfileguard.SyncRootDirectory(root, ".")
}
