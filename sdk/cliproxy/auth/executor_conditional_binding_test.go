package auth

import "testing"

type reentrantConditionalExecutor struct {
	*replaceAwareExecutor
	manager *Manager
}

func (e *reentrantConditionalExecutor) CloseExecutionSession(id string) {
	e.replaceAwareExecutor.CloseExecutionSession(id)
	e.manager.RegisterExecutor(&replaceAwareExecutor{id: "other"})
}

func TestConditionalExecutorBindingKeepsExistingOwnership(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	first := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "codex"}}
	unused := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "codex"}}
	if !manager.RegisterExecutorIfTypeChanged(first) || manager.RegisterExecutorIfTypeChanged(unused) {
		t.Fatal("conditional installation did not preserve an existing type")
	}
	if current, _ := manager.Executor("codex"); current != first {
		t.Fatal("unused wrapper replaced the installed executor")
	}
	if manager.RegisterExecutorIfTypeChanged(first) {
		t.Fatal("the already owned instance was treated as a replacement")
	}
	if first.CloseCalls() != 0 || unused.CloseCalls() != 0 || len(first.ClosedSessionIDs()) != 0 || len(unused.ClosedSessionIDs()) != 0 {
		t.Fatal("rejected binding closed installed or caller-owned state")
	}
	if err := manager.CloseExecutors(); err != nil {
		t.Fatal(err)
	}
	if first.CloseCalls() != 1 || unused.CloseCalls() != 0 {
		t.Fatal("shutdown changed rejected executor ownership")
	}
	if manager.RegisterExecutorIfTypeChanged(unused) || unused.CloseCalls() != 0 {
		t.Fatal("conditional registration took ownership after shutdown")
	}
	if manager.RegisterExecutorIfTypeChanged(first) || first.CloseCalls() != 1 {
		t.Fatal("an already closed owned instance was closed twice")
	}
	if manager.RegisterExecutorIfTypeChanged(nil) || manager.RegisterExecutorIfTypeChanged(&replaceAwareExecutor{}) {
		t.Fatal("invalid executor was installed")
	}
}

func TestConditionalExecutorTypeReplacementSupportsNonComparableValues(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	base := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(nonComparableReplaceAwareExecutor{replaceAwareExecutor: base, marker: []byte("fixture")})
	next := &replaceAwareExecutor{id: "codex"}
	if !manager.RegisterExecutorIfTypeChanged(next) || len(base.ClosedSessionIDs()) != 1 {
		t.Fatal("different executor type was not replaced and closed")
	}
	forced := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(forced)
	if current, _ := manager.Executor("codex"); current != forced || len(next.ClosedSessionIDs()) != 1 {
		t.Fatal("unconditional replacement behavior changed")
	}
}

func TestConditionalExecutorCleanupCanReenterManager(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	previous := &reentrantConditionalExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "codex"}, manager: manager}
	manager.RegisterExecutor(previous)
	if !manager.RegisterExecutorIfTypeChanged(&replaceAwareExecutor{id: "codex"}) {
		t.Fatal("type replacement was not installed")
	}
	if _, ok := manager.Executor("other"); !ok {
		t.Fatal("cleanup callback did not run outside registration locks")
	}
}
