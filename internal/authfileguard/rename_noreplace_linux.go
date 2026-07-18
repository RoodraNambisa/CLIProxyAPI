//go:build linux

package authfileguard

import (
	"errors"
	"io/fs"
	"os"

	"golang.org/x/sys/unix"
)

func renameFileNoReplace(root *os.Root, stagedName, targetName string) (cleanupWarning error, err error) {
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		return nil, errOpen
	}
	errRename := unix.Renameat2(int(directory.Fd()), stagedName, int(directory.Fd()), targetName, unix.RENAME_NOREPLACE)
	errClose := directory.Close()
	if errRename != nil {
		if errors.Is(errRename, unix.EEXIST) {
			errRename = fs.ErrExist
		}
		return nil, errors.Join(errRename, errClose)
	}
	return errClose, nil
}
