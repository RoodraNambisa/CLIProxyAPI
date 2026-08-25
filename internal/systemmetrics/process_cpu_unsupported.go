//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris && !zos

package systemmetrics

func platformProcessCPUTime() (uint64, bool) {
	return 0, false
}
