package auth

import (
	"context"
	"testing"
	"time"
)

func managedLifecycleAuth() *Auth {
	return &Auth{ID: "channel", Provider: "codex", Status: StatusActive, Label: "First", Attributes: map[string]string{"source": "config:codex[channel]", "api_key": "old", "config_id": "channel", "config_usage_index": "0123456789abcdef", "config_revision": "v1"}}
}

func TestManagedAPIRenamePreservesExecutionAndRotationDrains(t *testing.T) {
	m := NewManager(nil, nil, nil)
	first, err := m.Register(WithSkipPersist(context.Background()), managedLifecycleAuth())
	if err != nil {
		t.Fatal(err)
	}
	execCtx, release, ok := first.BeginRuntimeExecution(context.Background())
	if !ok {
		t.Fatal("execution not admitted")
	}
	renamed := first.Clone()
	renamed.Label = "Second"
	renamed.Attributes["config_revision"] = "v2"
	renamed.Attributes["priority"] = "5"
	updated, err := m.Update(WithSkipPersist(context.Background()), renamed)
	if err != nil {
		t.Fatal(err)
	}
	if updated.RuntimeInstanceID() != first.RuntimeInstanceID() || execCtx.Err() != nil {
		t.Fatal("rename interrupted execution")
	}
	rotated := updated.Clone()
	rotated.Attributes["api_key"] = "new"
	next, err := m.Update(WithSkipPersist(context.Background()), rotated)
	if err != nil {
		t.Fatal(err)
	}
	if next.RuntimeInstanceID() == first.RuntimeInstanceID() || execCtx.Err() != nil {
		t.Fatal("rotation did not preserve the old execution while installing new credentials")
	}
	if _, staleRelease, allowed := first.BeginRuntimeExecution(context.Background()); allowed {
		staleRelease()
		t.Fatal("stale generation admitted new work")
	}
	newCtx, newRelease, allowed := next.BeginRuntimeExecution(context.Background())
	if !allowed {
		t.Fatal("new generation blocked")
	}
	if release() {
		t.Fatal("old execution was retired before finishing")
	}
	select {
	case <-first.RuntimeInstanceCleanupDone():
	case <-time.After(time.Second):
		t.Fatal("old instance was not cleaned up")
	}
	if newCtx.Err() != nil {
		t.Fatal("old cleanup canceled new work")
	}
	newRelease()
}
