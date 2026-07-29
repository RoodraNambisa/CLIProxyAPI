//go:build netbsd

package systemmetrics

import "golang.org/x/sys/unix"

func platformFilesystemCapacity(path string) (filesystemCapacity, error) {
	var stat unix.Statvfs_t
	if errStat := unix.Statvfs(path, &stat); errStat != nil {
		return filesystemCapacity{}, errStat
	}
	blockSize := stat.Frsize
	if blockSize == 0 {
		blockSize = stat.Bsize
	}
	if blockSize == 0 {
		return filesystemCapacity{}, errFilesystemUnsupported
	}
	return filesystemCapacity{
		totalBytes:     saturatingMultiply(stat.Blocks, blockSize),
		freeBytes:      saturatingMultiply(stat.Bfree, blockSize),
		availableBytes: saturatingMultiply(stat.Bavail, blockSize),
	}, nil
}
