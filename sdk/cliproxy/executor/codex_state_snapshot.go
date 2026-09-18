package executor

import (
	"context"
	"sync"
)

type codexStateSnapshotKey struct{}
type CodexStateChoice struct {
	Value, Policy string
	Eligible      bool
}
type codexStateChoices struct {
	mu      sync.Mutex
	choices map[string]CodexStateChoice
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
