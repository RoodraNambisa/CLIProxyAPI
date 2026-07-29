//go:build android || darwin || dragonfly || freebsd || linux || netbsd || openbsd

package systemmetrics

import (
	"strconv"

	"golang.org/x/sys/unix"
)

func filesystemIdentity(path string) string {
	var stat unix.Stat_t
	if errStat := unix.Stat(path, &stat); errStat != nil {
		return ""
	}
	return "dev:" + strconv.FormatUint(uint64(stat.Dev), 10)
}
