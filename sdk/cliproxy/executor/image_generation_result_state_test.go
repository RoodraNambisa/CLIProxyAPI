package executor

import (
	"sync"
	"testing"
)

func TestImageGenerationResultStateCountsConcurrentSuccesses(t *testing.T) {
	state := &ImageGenerationResultState{}
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			state.AddSucceeded(2)
		}()
	}
	wait.Wait()
	state.MarkSucceeded()

	if !state.Succeeded() {
		t.Fatal("Succeeded() = false")
	}
	if got := state.SucceededCount(); got != 17 {
		t.Fatalf("SucceededCount() = %d, want 17", got)
	}
}
