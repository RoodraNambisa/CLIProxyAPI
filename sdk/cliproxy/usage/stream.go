package usage

import "context"

type streamContextKey struct{}

// WithStream pins the logical response mode for usage accounting.
func WithStream(ctx context.Context, stream bool) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, streamContextKey{}, stream)
}

// WithStreamDefault supplies an execution-mode fallback without overriding a
// caller's pinned mode when a non-streaming request uses streaming upstream.
func WithStreamDefault(ctx context.Context, stream bool) context.Context {
	if ctx != nil {
		if _, present := ctx.Value(streamContextKey{}).(bool); present {
			return ctx
		}
	}
	return WithStream(ctx, stream)
}

// StreamFromContext returns false for legacy callers without an explicit mode.
func StreamFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	stream, _ := ctx.Value(streamContextKey{}).(bool)
	return stream
}
