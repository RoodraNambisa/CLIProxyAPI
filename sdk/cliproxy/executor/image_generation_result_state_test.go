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

func TestImageGenerationResultStateConsumesProducedImagesOnce(t *testing.T) {
	state := &ImageGenerationResultState{}
	state.AddProduced(2)
	state.AddProduced(1)
	if got := state.ProducedCount(); got != 3 {
		t.Fatalf("ProducedCount() = %d, want 3", got)
	}
	if got := state.TakeProducedCount(); got != 3 {
		t.Fatalf("TakeProducedCount() = %d, want 3", got)
	}
	if got := state.TakeProducedCount(); got != 0 {
		t.Fatalf("second TakeProducedCount() = %d, want 0", got)
	}
}
