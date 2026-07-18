//go:build linux

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
	errExchange := unix.Renameat2(int(directory.Fd()), stagedName, int(directory.Fd()), targetName, unix.RENAME_EXCHANGE)
	errClose := directory.Close()
	if errExchange == nil {
		return stagedName, errClose
	}
	if !errors.Is(errExchange, unix.ENOSYS) && !errors.Is(errExchange, unix.EINVAL) && !errors.Is(errExchange, unix.EOPNOTSUPP) {
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
