//go:build windows

package systemmetrics

import "golang.org/x/sys/windows"

func platformFilesystemCapacity(path string) (filesystemCapacity, error) {
	pathPointer, errPath := windows.UTF16PtrFromString(path)
	if errPath != nil {
		return filesystemCapacity{}, errPath
	}
	var availableBytes uint64
	var totalBytes uint64
	var freeBytes uint64
	if errSpace := windows.GetDiskFreeSpaceEx(
		pathPointer,
		&availableBytes,
		&totalBytes,
		&freeBytes,
	); errSpace != nil {
		return filesystemCapacity{}, errSpace
	}
	return filesystemCapacity{
		totalBytes:     totalBytes,
		freeBytes:      freeBytes,
		availableBytes: availableBytes,
	}, nil
}
