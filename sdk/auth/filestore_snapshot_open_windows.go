//go:build windows

package auth

import "os"

func openFileTokenSnapshotFile(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY, 0)
}
