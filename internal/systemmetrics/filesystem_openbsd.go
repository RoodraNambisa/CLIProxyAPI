//go:build openbsd

package systemmetrics

import "golang.org/x/sys/unix"

func platformFilesystemCapacity(path string) (filesystemCapacity, error) {
	var stat unix.Statfs_t
	if errStat := unix.Statfs(path, &stat); errStat != nil {
		return filesystemCapacity{}, errStat
	}
	if stat.F_bsize == 0 {
		return filesystemCapacity{}, errFilesystemUnsupported
	}
	blockSize := uint64(stat.F_bsize)
	return filesystemCapacity{
		totalBytes:     saturatingMultiply(stat.F_blocks, blockSize),
		freeBytes:      saturatingMultiply(stat.F_bfree, blockSize),
		availableBytes: saturatingMultiply(nonNegativeInt64(stat.F_bavail), blockSize),
	}, nil
}
