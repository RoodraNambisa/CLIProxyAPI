//go:build !aix && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package authfileguard

import "os"

func syncExchangeDirectory(*os.Root) error {
	return ErrAtomicExchangeUnsupported
}
