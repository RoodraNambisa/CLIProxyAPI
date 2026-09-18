package executor

import (
	"context"
	"sync"
)

type codexStateSnapshotKey struct{}
type CodexStateChoice struct {
	Value, Policy string
	Eligible      bool
	Version       uint64
}

func CodexStateChoiceForRequest(ctx context.Context, key string) (CodexStateChoice, bool) {
	if ctx == nil {
		return CodexStateChoice{}, false
	}
	snapshot, _ := ctx.Value(codexStateSnapshotKey{}).(*codexStateChoices)
	if snapshot == nil {
		return CodexStateChoice{}, false
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	choice, ok := snapshot.choices[key]
	return choice, ok
}

type codexStateChoices struct {
	mu      sync.Mutex
	choices map[string]CodexStateChoice
}

func HasCodexStateChoice(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	snapshot, _ := ctx.Value(codexStateSnapshotKey{}).(*codexStateChoices)
	if snapshot == nil {
		return false
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	return len(snapshot.choices) > 0
}

// WithCodexStateSnapshot freezes managed state per account/model across retries.
func WithCodexStateSnapshot(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Value(codexStateSnapshotKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, codexStateSnapshotKey{}, &codexStateChoices{})
}
func CodexStateForRequest(ctx context.Context, key string, choose func() CodexStateChoice) CodexStateChoice {
	if ctx == nil {
		return choose()
	}
	snapshot, _ := ctx.Value(codexStateSnapshotKey{}).(*codexStateChoices)
	if snapshot == nil {
		return choose()
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if choice, ok := snapshot.choices[key]; ok {
		return choice
	}
	choice := choose()
	if snapshot.choices == nil {
		snapshot.choices = make(map[string]CodexStateChoice)
	}
	snapshot.choices[key] = choice
	return choice
}
