package executor

import "context"

type downstreamWebsocketContextKey struct{}
type requiredUpstreamWebsocketContextKey struct{}
type requestExecutionDiagnosticsContextKey struct{}

// WithRequestExecutionDiagnostics makes ownership evidence available to all provider reporters.
func WithRequestExecutionDiagnostics(ctx context.Context, diagnostics *RequestExecutionDiagnostics) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestExecutionDiagnosticsContextKey{}, diagnostics)
}

func RequestExecutionDiagnosticsFromContext(ctx context.Context) *RequestExecutionDiagnostics {
	if ctx == nil {
		return nil
	}
	diagnostics, _ := ctx.Value(requestExecutionDiagnosticsContextKey{}).(*RequestExecutionDiagnostics)
	return diagnostics
}

// WithDownstreamWebsocket marks the current request as coming from a downstream websocket connection.
func WithDownstreamWebsocket(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, downstreamWebsocketContextKey{}, true)
}

// DownstreamWebsocket reports whether the current request originates from a downstream websocket connection.
func DownstreamWebsocket(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	raw := ctx.Value(downstreamWebsocketContextKey{})
	enabled, ok := raw.(bool)
	return ok && enabled
}

// WithRequiredUpstreamWebsocket marks incremental input tied to an existing
// upstream connection. Reconnecting or switching transport would lose context.
func WithRequiredUpstreamWebsocket(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requiredUpstreamWebsocketContextKey{}, true)
}

func RequiredUpstreamWebsocket(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	required, _ := ctx.Value(requiredUpstreamWebsocketContextKey{}).(bool)
	return required
}
