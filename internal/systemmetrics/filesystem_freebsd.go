//go:build freebsd

package systemmetrics

import "golang.org/x/sys/unix"

func platformFilesystemCapacity(path string) (filesystemCapacity, error) {
	var stat unix.Statfs_t
	if errStat := unix.Statfs(path, &stat); errStat != nil {
		return filesystemCapacity{}, errStat
	}
	if stat.Bsize == 0 {
		return filesystemCapacity{}, errFilesystemUnsupported
	}
	return filesystemCapacity{
		totalBytes:     saturatingMultiply(stat.Blocks, stat.Bsize),
		freeBytes:      saturatingMultiply(stat.Bfree, stat.Bsize),
		availableBytes: saturatingMultiply(nonNegativeInt64(stat.Bavail), stat.Bsize),
	}, nil
}
