package management

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCodexAgentIdentityTaskProgressIsMonotonicAndCancelable(t *testing.T) {
	manager := newCodexAgentIdentityTaskManager()
	task, taskCtx, errCreate := manager.create([]codexAgentIdentityTaskResult{
		{SourceName: "first", Stage: "queued", Status: agentIdentityItemQueued},
		{SourceName: "second", Stage: "queued", Status: agentIdentityItemQueued},
	})
	if errCreate != nil {
		t.Fatalf("create() error = %v", errCreate)
	}
	if !manager.start(task.ID) || !manager.markRunning(task.ID, 0) {
		t.Fatal("task did not start")
	}
	if !manager.updateStage(task.ID, 0, "registering_task", 70) || !manager.updateStage(task.ID, 0, "validating", 10) {
		t.Fatal("task stage update failed")
	}
	snapshot, ok := manager.get(task.ID)
	if !ok || snapshot.Results[0].ProgressPercent != 70 || snapshot.ProgressPercent != 35 {
		t.Fatalf("progress regressed: %#v", snapshot)
	}

	canceled, ok := manager.cancel(task.ID)
	if !ok || canceled.Status != agentIdentityTaskCanceling {
		t.Fatalf("cancel() = %#v, %v", canceled, ok)
	}
	select {
	case <-taskCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("task context was not canceled")
	}
	manager.setResult(task.ID, 0, codexAgentIdentityTaskResult{SourceName: "first", Stage: "canceled", Status: agentIdentityItemCanceled})
	manager.finish(task.ID, true)
	snapshot, ok = manager.get(task.ID)
	if !ok || snapshot.Status != agentIdentityTaskCanceled || snapshot.Processed != 2 || snapshot.Canceled != 2 || snapshot.ProgressPercent != 100 {
		t.Fatalf("canceled task = %#v", snapshot)
	}
	if snapshot.Results[0].Stage != "canceled" {
		t.Fatalf("running item cancellation stage = %q, want canceled", snapshot.Results[0].Stage)
	}
}

func TestCodexAgentIdentityTaskCapacityAndShutdown(t *testing.T) {
	manager := newCodexAgentIdentityTaskManager()
	taskIDs := make([]string, 0, agentIdentityTaskMaxActive)
	contexts := make([]context.Context, 0, agentIdentityTaskMaxActive)
	for index := 0; index < agentIdentityTaskMaxActive; index++ {
		task, taskCtx, errCreate := manager.create([]codexAgentIdentityTaskResult{{SourceName: "source", Stage: "queued", Status: agentIdentityItemQueued}})
		if errCreate != nil {
			t.Fatalf("create(%d) error = %v", index, errCreate)
		}
		taskIDs = append(taskIDs, task.ID)
		contexts = append(contexts, taskCtx)
	}
	if _, _, errCreate := manager.create([]codexAgentIdentityTaskResult{{SourceName: "overflow"}}); !errors.Is(errCreate, errAgentIdentityTaskCapacity) {
		t.Fatalf("overflow create error = %v, want capacity", errCreate)
	}

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- manager.shutdown(ctx)
	}()
	for index, taskCtx := range contexts {
		select {
		case <-taskCtx.Done():
		case <-time.After(time.Second):
			t.Fatalf("task %d was not canceled by shutdown", index)
		}
		manager.finish(taskIDs[index], true)
	}
	if errShutdown := <-shutdownDone; errShutdown != nil {
		t.Fatalf("shutdown() error = %v", errShutdown)
	}
	if _, _, errCreate := manager.create([]codexAgentIdentityTaskResult{{SourceName: "after-shutdown"}}); !errors.Is(errCreate, errAgentIdentityTaskClosed) {
		t.Fatalf("create after shutdown error = %v, want closed", errCreate)
	}
}

func TestCodexAgentIdentityTaskHasNoFixedItemLimit(t *testing.T) {
	manager := newCodexAgentIdentityTaskManager()
	results := make([]codexAgentIdentityTaskResult, 250)
	for index := range results {
		results[index] = codexAgentIdentityTaskResult{SourceName: "source", Stage: "queued", Status: agentIdentityItemQueued}
	}
	task, _, errCreate := manager.create(results)
	if errCreate != nil {
		t.Fatalf("create() error = %v", errCreate)
	}
	if task.Total != len(results) {
		t.Fatalf("task total = %d, want %d", task.Total, len(results))
	}
	manager.finish(task.ID, true)
}
