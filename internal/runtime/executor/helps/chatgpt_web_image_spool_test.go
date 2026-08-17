package helps

import (
	"errors"
	"testing"
)

func TestChatGPTWebImageSpoolTrackerCountsLifecycleExactlyOnce(t *testing.T) {
	tracker := &chatGPTWebImageSpoolTracker{}
	first := tracker.begin()
	second := tracker.begin()
	first.SetBytes(12)
	first.SetBytes(20)
	second.SetBytes(7)

	if snapshot := tracker.snapshot(); snapshot.CurrentFiles != 2 || snapshot.CurrentBytes != 27 || snapshot.PeakBytes != 27 || snapshot.CreatedFiles != 2 {
		t.Fatalf("active snapshot = %#v", snapshot)
	}
	first.FinishCleanup(nil)
	first.FinishCleanup(nil)
	first.SetBytes(100)
	second.FinishCleanup(errors.New("remove failed"))
	second.FinishCleanup(nil)

	if snapshot := tracker.snapshot(); snapshot.CurrentFiles != 1 || snapshot.CurrentBytes != 7 || snapshot.PeakBytes != 27 || snapshot.CleanedFiles != 1 || snapshot.CleanupFailures != 1 {
		t.Fatalf("terminal snapshot = %#v", snapshot)
	}
}
