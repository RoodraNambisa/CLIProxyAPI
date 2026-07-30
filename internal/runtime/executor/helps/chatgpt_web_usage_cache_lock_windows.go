//go:build windows

package helps

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func chatGPTWebUsageFileLockSupported() bool {
	return true
}

func openChatGPTWebUsageLockFile(path string, create bool) (*os.File, error) {
	pathUTF16, errPath := windows.UTF16PtrFromString(path)
	if errPath != nil {
		return nil, errPath
	}
	disposition := uint32(windows.OPEN_EXISTING)
	if create {
		disposition = windows.OPEN_ALWAYS
	}
	handle, errOpen := windows.CreateFile(
		pathUTF16,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		disposition,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if errOpen != nil {
		return nil, errOpen
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, errors.New("usage cache lock file could not be opened")
	}
	return file, nil
}

func lockChatGPTWebUsageFile(file *os.File, nonblocking bool) (bool, error) {
	if file == nil {
		return false, errors.New("usage cache lock file is nil")
	}
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK)
	if nonblocking {
		flags |= windows.LOCKFILE_FAIL_IMMEDIATELY
	}
	var overlapped windows.Overlapped
	err := windows.LockFileEx(windows.Handle(file.Fd()), flags, 0, 1, 0, &overlapped)
	if err != nil {
		if nonblocking && errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
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
	var overlapped windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &overlapped)
}
