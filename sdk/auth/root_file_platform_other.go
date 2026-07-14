//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package auth

import "os"

func syncAuthRootDirectory(*os.Root, string) error {
	return nil
}
