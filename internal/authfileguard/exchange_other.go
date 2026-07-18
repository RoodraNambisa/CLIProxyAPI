//go:build !darwin && !linux && !windows

package authfileguard

import (
	"errors"
	"os"
)

func exchangeFile(root *os.Root, stagedName, targetName string) (string, error) {
	displaced, errFallback := exchangeFileByRename(root, stagedName, targetName)
	if errFallback != nil {
		return displaced, errors.Join(
			ErrAtomicExchangeUnsupported,
			errors.New("auth file guard: platform has no atomic file exchange primitive"),
			errFallback,
		)
	}
	return displaced, nil
}
