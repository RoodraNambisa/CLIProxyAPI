package auth

import "context"

type skipPersistContextKey struct{}
type skipStateCarryForwardContextKey struct{}

// WithSkipPersist returns a derived context that disables persistence for Manager Update/Register calls.
// It is intended for code paths that are reacting to file watcher events, where the file on disk is
// already the source of truth and persisting again would create a write-back loop.
func WithSkipPersist(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, skipPersistContextKey{}, true)
}

// WithSkipStateCarryForward returns a derived context that disables same-source
// runtime state carry-forward for Manager Register/Update calls.
func WithSkipStateCarryForward(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, skipStateCarryForwardContextKey{}, true)
}

func shouldSkipPersist(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v := ctx.Value(skipPersistContextKey{})
	enabled, ok := v.(bool)
	return ok && enabled
}

func shouldSkipStateCarryForward(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v := ctx.Value(skipStateCarryForwardContextKey{})
	enabled, ok := v.(bool)
	return ok && enabled
}
