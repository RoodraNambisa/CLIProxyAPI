//go:build !darwin && !linux && !windows

package authfileguard

import (
	"errors"
	"os"
)

func renameFileNoReplace(*os.Root, string, string) (cleanupWarning error, err error) {
	return nil, errors.New("atomic no-replace rename is unsupported on this platform")
}
