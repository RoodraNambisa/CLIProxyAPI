//go:build linux

package systemmetrics

import (
	"os"
	"strconv"
	"strings"
)

func platformProcessResidentSet() (uint64, bool) {
	payload, errRead := os.ReadFile("/proc/self/statm")
	if errRead != nil {
		return 0, false
	}
	fields := strings.Fields(string(payload))
	if len(fields) < 2 {
		return 0, false
	}
	residentPages, errParse := strconv.ParseUint(fields[1], 10, 64)
	if errParse != nil {
		return 0, false
	}
	return saturatingMultiply(residentPages, uint64(os.Getpagesize())), true
}
