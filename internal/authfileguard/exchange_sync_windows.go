//go:build windows

package authfileguard

import "os"

func syncExchangeDirectory(root *os.Root) error {
	return SyncRootDirectory(root, ".")
}
