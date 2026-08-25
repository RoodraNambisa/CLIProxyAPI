//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris || zos

package systemmetrics

import "golang.org/x/sys/unix"

func platformProcessCPUTime() (uint64, bool) {
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	userNanos := unix.TimevalToNsec(usage.Utime)
	systemNanos := unix.TimevalToNsec(usage.Stime)
	if userNanos < 0 || systemNanos < 0 {
		return 0, false
	}
	return saturatingAdd(uint64(userNanos), uint64(systemNanos)), true
}
