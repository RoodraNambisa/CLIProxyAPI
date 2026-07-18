//go:build darwin

package authfileguard

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func exchangeFile(root *os.Root, stagedName, targetName string) (string, error) {
	directory, errOpen := root.Open(".")
	if errOpen != nil {
		return "", errOpen
	}
	errExchange := unix.RenameatxNp(int(directory.Fd()), stagedName, int(directory.Fd()), targetName, unix.RENAME_SWAP)
	errClose := directory.Close()
	if errExchange == nil {
		return stagedName, errClose
	}
	if !errors.Is(errExchange, unix.ENOSYS) && !errors.Is(errExchange, unix.ENOTSUP) && !errors.Is(errExchange, unix.EINVAL) {
		return "", errors.Join(errExchange, errClose)
	}
	displaced, errFallback := exchangeFileByRename(root, stagedName, targetName)
	if errFallback != nil {
		return displaced, errors.Join(
			ErrAtomicExchangeUnsupported,
			errExchange,
			errClose,
			errFallback,
		)
	}
	return displaced, errClose
}
