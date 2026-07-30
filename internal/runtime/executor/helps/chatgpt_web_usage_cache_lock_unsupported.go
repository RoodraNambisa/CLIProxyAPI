//go:build aix || (!darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !windows)

package helps

import (
	"os"
)

func chatGPTWebUsageFileLockSupported() bool {
	return false
}

func openChatGPTWebUsageLockFile(path string, create bool) (*os.File, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	return os.OpenFile(path, flags, 0o600)
}

func lockChatGPTWebUsageFile(_ *os.File, _ bool) (bool, error) {
	return true, nil
}

func unlockChatGPTWebUsageFile(_ *os.File) error {
	return nil
}
