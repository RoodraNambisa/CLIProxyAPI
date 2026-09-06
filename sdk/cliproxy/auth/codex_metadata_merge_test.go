package auth

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestCodexRefreshPreservesConcurrentRuntimeMetadata(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	registered, err := manager.Register(t.Context(), &Auth{ID: "codex-metadata-refresh", Provider: "codex", Metadata: map[string]any{"access_token": "old", "extension": map[string]any{"mode": "old", "preserved": true}, "remove_me": true}})
	if err != nil {
		t.Fatal(err)
	}
	exec := &retiredBlockingRefreshExecutor{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"}, started: make(chan struct{}), release: make(chan struct{})}
	manager.RegisterExecutor(exec)
	done := make(chan struct{})
	go func() { defer close(done); manager.refreshAuth(t.Context(), registered.ID) }()
	select {
	case <-exec.started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	_, updated, err := manager.MutateRuntimeMetadataIfCurrent(context.Background(), registered, func(current *Auth) {
		current.Metadata["extension"] = map[string]any{"mode": "edited", "preserved": true}
		current.Metadata["new_runtime_field"] = "keep"
		delete(current.Metadata, "remove_me")
	})
	close(exec.release)
	<-done
	if err != nil || !updated {
		t.Fatal("runtime metadata edit failed")
	}
	current, _ := manager.GetByID(registered.ID)
	extension := current.Metadata["extension"].(map[string]any)
	if extension["mode"] != "edited" || current.Metadata["new_runtime_field"] != "keep" {
		t.Fatal("refresh overwrote a concurrent Codex metadata edit")
	}
	if _, exists := current.Metadata["remove_me"]; exists {
		t.Fatal("refresh resurrected removed Codex metadata")
	}
}

func TestCodexMetadataMergeKeepsIndependentEditsDeletesAndNumericSemantics(t *testing.T) {
	if merged := mergeCodexRefreshMetadata(map[string]any{}, map[string]any{}, map[string]any{}); merged == nil {
		t.Fatal("empty metadata object became absent and could skip persistence")
	}
	base := map[string]any{"nested": map[string]any{"user": "before", "token": "before", "delete": true}, "number": 20, "array": []any{"before"}, "upstream_delete": true}
	current := map[string]any{"nested": map[string]any{"user": "user edit", "token": "before"}, "number": float64(20), "array": []any{"user edit"}, "upstream_delete": true, "new_user": true}
	refreshed := map[string]any{"nested": map[string]any{"user": "before", "token": "new token", "delete": true}, "number": 19, "array": []any{"upstream edit"}, "new_upstream": true}
	merged := mergeCodexRefreshMetadata(base, current, refreshed)
	want := map[string]any{"nested": map[string]any{"user": "user edit", "token": "new token"}, "number": 19, "array": []any{"user edit"}, "new_user": true, "new_upstream": true}
	if !reflect.DeepEqual(merged, want) {
		t.Fatal("merge lost independent updates, removals or current conflict priority")
	}
	merged["nested"].(map[string]any)["user"] = "mutated result"
	merged["array"].([]any)[0] = "mutated result"
	if current["nested"].(map[string]any)["user"] != "user edit" || current["array"].([]any)[0] != "user edit" {
		t.Fatal("merge result aliases a current metadata container")
	}
	if value, present := mergeCodexMetadataValue(true, true, nil, false, false, true); present || value != nil {
		t.Fatal("concurrent deletion was resurrected")
	}
	if value, present := mergeCodexMetadataValue(nil, false, nil, true, true, true); !present || value != nil {
		t.Fatal("explicit null was confused with deletion")
	}
}

func TestCodexPreparationKeepsConcurrentRuntimeMetadataAndPreparedTask(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	registered, err := manager.Register(t.Context(), &Auth{ID: "prepared-codex-metadata", Provider: "codex", Metadata: map[string]any{"access_token": "token", "task_id": "before", "custom_runtime": "before"}})
	if err != nil {
		t.Fatal(err)
	}
	prepared := registered.Clone()
	prepared.Metadata["task_id"] = "prepared-task"
	_, changed, err := manager.MutateRuntimeMetadataIfCurrent(t.Context(), registered, func(current *Auth) {
		current.Metadata["custom_runtime"] = "current edit"
		current.Metadata["new_runtime"] = true
	})
	if err != nil || !changed {
		t.Fatal("metadata edit failed")
	}
	installed, err := manager.installPreparedRequestAuth(t.Context(), registered, prepared, false)
	if err != nil || installed == nil {
		t.Fatal("prepared Codex update failed")
	}
	if installed.Metadata["custom_runtime"] != "current edit" || installed.Metadata["new_runtime"] != true || installed.Metadata["task_id"] != "prepared-task" {
		t.Fatal("preparation overwrote current metadata or dropped its new task")
	}
}
