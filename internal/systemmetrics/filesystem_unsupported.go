//go:build !android && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package systemmetrics

func platformFilesystemCapacity(string) (filesystemCapacity, error) {
	return filesystemCapacity{}, errFilesystemUnsupported
}
