//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package helps

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func chatGPTWebUsageFileLockSupported() bool {
	return true
}

func openChatGPTWebUsageLockFile(path string, create bool) (*os.File, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	return os.OpenFile(path, flags, 0o600)
}

func lockChatGPTWebUsageFile(file *os.File, nonblocking bool) (bool, error) {
	if file == nil {
		return false, errors.New("usage cache lock file is nil")
	}
	operation := unix.LOCK_EX
	if nonblocking {
		operation |= unix.LOCK_NB
	}
	if err := unix.Flock(int(file.Fd()), operation); err != nil {
		if nonblocking && (errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func unlockChatGPTWebUsageFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
