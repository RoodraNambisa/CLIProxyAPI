//go:build dragonfly

package systemmetrics

import "golang.org/x/sys/unix"

func platformFilesystemCapacity(path string) (filesystemCapacity, error) {
	var stat unix.Statfs_t
	if errStat := unix.Statfs(path, &stat); errStat != nil {
		return filesystemCapacity{}, errStat
	}
	blockSize := nonNegativeInt64(stat.Bsize)
	if blockSize == 0 {
		return filesystemCapacity{}, errFilesystemUnsupported
	}
	return filesystemCapacity{
		totalBytes:     saturatingMultiply(nonNegativeInt64(stat.Blocks), blockSize),
		freeBytes:      saturatingMultiply(nonNegativeInt64(stat.Bfree), blockSize),
		availableBytes: saturatingMultiply(nonNegativeInt64(stat.Bavail), blockSize),
	}, nil
}
