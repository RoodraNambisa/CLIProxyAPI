//go:build windows

package systemmetrics

import (
	"fmt"
	"strings"

	"golang.org/x/sys/windows"
)

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
		filesystemID:   windowsFilesystemIdentity(pathPointer),
	}, nil
}

func windowsFilesystemIdentity(pathPointer *uint16) string {
	volumePath := make([]uint16, 32768)
	if errPath := windows.GetVolumePathName(pathPointer, &volumePath[0], uint32(len(volumePath))); errPath != nil {
		return ""
	}
	volumeName := make([]uint16, 32768)
	if errName := windows.GetVolumeNameForVolumeMountPoint(
		&volumePath[0],
		&volumeName[0],
		uint32(len(volumeName)),
	); errName == nil {
		if identity := strings.TrimSpace(windows.UTF16ToString(volumeName)); identity != "" {
			return strings.ToLower(identity)
		}
	}
	var serial uint32
	if errInfo := windows.GetVolumeInformation(
		&volumePath[0],
		nil,
		0,
		&serial,
		nil,
		nil,
		nil,
		0,
	); errInfo == nil {
		return fmt.Sprintf("volume-serial:%08x", serial)
	}
	return ""
}
