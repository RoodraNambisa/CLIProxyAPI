package auth

import "context"

type skipPersistContextKey struct{}
type skipStateCarryForwardContextKey struct{}
type forceRuntimeReplacementContextKey struct{}
type chatGPTWebImportContextKey struct{}

// ChatGPTWebImportPolicy controls optional work started by the auth hook after
// a locally validated ChatGPT Web import is installed.
type ChatGPTWebImportPolicy struct {
	ValidateModels bool
}

// WithChatGPTWebImportPolicy marks a Manager mutation as an import and carries
// only the hook behavior that must be decided after installation.
func WithChatGPTWebImportPolicy(ctx context.Context, policy ChatGPTWebImportPolicy) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, chatGPTWebImportContextKey{}, policy)
}

// ChatGPTWebImportPolicyFromContext reports an import policy attached by the
// management import path.
func ChatGPTWebImportPolicyFromContext(ctx context.Context) (ChatGPTWebImportPolicy, bool) {
	if ctx == nil {
		return ChatGPTWebImportPolicy{}, false
	}
	policy, ok := ctx.Value(chatGPTWebImportContextKey{}).(ChatGPTWebImportPolicy)
	return policy, ok
}

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

// WithForceRuntimeReplacement returns a derived context that prevents an
// existing runtime instance from being reused during an auth update, register,
// or conditional update.
func WithForceRuntimeReplacement(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, forceRuntimeReplacementContextKey{}, true)
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

func shouldForceRuntimeReplacement(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v := ctx.Value(forceRuntimeReplacementContextKey{})
	enabled, ok := v.(bool)
	return ok && enabled
}
