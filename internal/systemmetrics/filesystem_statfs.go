//go:build android || darwin || linux

package systemmetrics

import "golang.org/x/sys/unix"

func platformFilesystemCapacity(path string) (filesystemCapacity, error) {
	var stat unix.Statfs_t
	if errStat := unix.Statfs(path, &stat); errStat != nil {
		return filesystemCapacity{}, errStat
	}
	if stat.Bsize <= 0 {
		return filesystemCapacity{}, errFilesystemUnsupported
	}
	blockSize := uint64(stat.Bsize)
	return filesystemCapacity{
		totalBytes:     saturatingMultiply(stat.Blocks, blockSize),
		freeBytes:      saturatingMultiply(stat.Bfree, blockSize),
		availableBytes: saturatingMultiply(stat.Bavail, blockSize),
		filesystemID:   filesystemIdentity(path),
	}, nil
}
