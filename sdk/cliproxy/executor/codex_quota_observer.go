package executor

import (
	"context"
	"net/http"
)

// CodexQuotaObserver receives synchronous, borrowed response headers. A sink
// must filter and copy any data it retains; no request body is passed here.
type CodexQuotaObserver func(authID, instanceID, source string, headers http.Header)

type codexQuotaObserverKey struct{}

// WithCodexQuotaObserver replaces an inherited observer, including explicit
// disabling with nil. The default disabled path keeps the existing context.
func WithCodexQuotaObserver(ctx context.Context, observer CodexQuotaObserver) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observer == nil && CodexQuotaObserverFromContext(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, codexQuotaObserverKey{}, observer)
}

func CodexQuotaObserverFromContext(ctx context.Context) CodexQuotaObserver {
	if ctx == nil {
		return nil
	}
	observer, _ := ctx.Value(codexQuotaObserverKey{}).(CodexQuotaObserver)
	return observer
}
