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

func TestStartupStateDistinguishesDegradedAndFailed(t *testing.T) {
	state := NewStartupState()
	finish := state.BeginReportedStage("auth_store_load")
	finish(8, 2, "")
	state.AddIssue("auth_store_load", "auth_records_skipped", "warning")
	state.MarkReady()

	degraded := state.Snapshot()
	if degraded.Status != StartupStatusDegraded || !degraded.Ready || !degraded.Degraded {
		t.Fatalf("degraded snapshot = %+v", degraded)
	}
	if len(degraded.Issues) != 1 || degraded.Issues[0].Code != "auth_records_skipped" {
		t.Fatalf("degraded issues = %+v", degraded.Issues)
	}
	if len(degraded.Stages) != 1 || degraded.Stages[0].Processed != 8 || degraded.Stages[0].Skipped != 2 {
		t.Fatalf("reported stage = %+v", degraded.Stages)
	}

	state.AddIssue("watcher_initial_sync", "watcher_initial_sync_failed", "error")
	state.MarkFailed()
	failed := state.Snapshot()
	if failed.Status != StartupStatusFailed || failed.Ready {
		t.Fatalf("failed snapshot = %+v", failed)
	}
}
