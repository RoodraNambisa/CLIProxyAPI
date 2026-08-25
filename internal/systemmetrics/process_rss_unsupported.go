//go:build !linux

package systemmetrics

func platformProcessResidentSet() (uint64, bool) {
	return 0, false
}
