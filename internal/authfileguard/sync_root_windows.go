//go:build windows

package authfileguard

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// SyncRootDirectory flushes a directory selected through root without trusting
// a second path lookup to identify the directory being flushed.
func SyncRootDirectory(root *os.Root, relativePath string) (err error) {
	guarded, errOpen := root.Open(relativePath)
	if errOpen != nil {
		return errOpen
	}
	defer func() { err = errors.Join(err, guarded.Close()) }()

	directPath, errFinalPath := finalPathByHandle(windows.Handle(guarded.Fd()))
	if errFinalPath != nil {
		return errFinalPath
	}
	directPathUTF16, errPath := windows.UTF16PtrFromString(directPath)
	if errPath != nil {
		return errPath
	}
	handle, errCreate := windows.CreateFile(
		directPathUTF16,
		windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if errCreate != nil {
		return errCreate
	}
	direct := os.NewFile(uintptr(handle), directPath)
	if direct == nil {
		_ = windows.CloseHandle(handle)
		return fmt.Errorf("wrap directory handle")
	}
	defer func() { err = errors.Join(err, direct.Close()) }()

	guardedInfo, errGuardedStat := guarded.Stat()
	directInfo, errDirectStat := direct.Stat()
	if errGuardedStat != nil || errDirectStat != nil {
		return errors.Join(errGuardedStat, errDirectStat)
	}
	if !guardedInfo.IsDir() || !directInfo.IsDir() || !os.SameFile(guardedInfo, directInfo) {
		return fmt.Errorf("directory changed while opening for sync")
	}
	return windows.FlushFileBuffers(handle)
}

func finalPathByHandle(handle windows.Handle) (string, error) {
	const fileNameOpened = 0x8
	path, errPath := finalPathByHandleWithFlags(handle, 0)
	if errors.Is(errPath, windows.ERROR_ACCESS_DENIED) {
		return finalPathByHandleWithFlags(handle, fileNameOpened)
	}
	return path, errPath
}

func finalPathByHandleWithFlags(handle windows.Handle, flags uint32) (string, error) {
	buffer := make([]uint16, 256)
	for {
		length, errPath := windows.GetFinalPathNameByHandle(handle, &buffer[0], uint32(len(buffer)), flags)
		if errPath != nil {
			return "", errPath
		}
		if length < uint32(len(buffer)) {
			return windows.UTF16ToString(buffer[:length]), nil
		}
		buffer = make([]uint16, length)
	}
}
