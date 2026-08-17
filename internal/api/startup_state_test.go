package api

import (
	"testing"
	"time"
)

func TestStartupStateTracksSafeIdempotentStages(t *testing.T) {
	state := NewStartupState()
	state.SetPhase(StartupPhaseAuthLoading)
	finish := state.BeginStage("auth_store_load")
	finish(42, "")
	finish(99, "must_not_replace")

	snapshot := state.Snapshot()
	if snapshot.Ready || snapshot.Phase != StartupPhaseAuthLoading {
		t.Fatalf("initial snapshot = %+v", snapshot)
	}
	if len(snapshot.Stages) != 1 {
		t.Fatalf("stages = %d, want 1", len(snapshot.Stages))
	}
	stage := snapshot.Stages[0]
	if stage.Name != "auth_store_load" || stage.Status != "completed" || stage.Processed != 42 || stage.SafeErrorCode != "" {
		t.Fatalf("stage = %+v", stage)
	}
	if stage.CompletedAt == nil || stage.CompletedAt.Before(stage.StartedAt) || stage.DurationMillis < 0 {
		t.Fatalf("stage timing = %+v", stage)
	}

	snapshot.Stages[0].Name = "mutated"
	if got := state.Snapshot().Stages[0].Name; got != "auth_store_load" {
		t.Fatalf("snapshot mutated state: %q", got)
	}

	state.MarkReady()
	ready := state.Snapshot()
	if !ready.Ready || ready.Phase != StartupPhaseReady || ready.UpdatedAt.Before(snapshot.UpdatedAt.Add(-time.Nanosecond)) {
		t.Fatalf("ready snapshot = %+v", ready)
	}
}
