package auth

import (
	"context"
	"net/http"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type upstreamErrorHistoryKey struct{}
type upstreamErrorHistory struct {
	mu   sync.Mutex
	last error
}

func withUpstreamErrorHistory(ctx context.Context) (context.Context, *upstreamErrorHistory) {
	if ctx == nil {
		ctx = context.Background()
	}
	history := &upstreamErrorHistory{}
	return context.WithValue(ctx, upstreamErrorHistoryKey{}, history), history
}

func (h *upstreamErrorHistory) preferred(err error) error {
	if h == nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return executor.PreferUpstreamError(h.last, err)
}

func recordExecutionAttemptError(ctx context.Context, auth *Auth, provider string, err error, headers ...http.Header) error {
	err = executor.ErrorFromUpstreamAttempt(ctx, err)
	if ctx != nil && executor.IsUpstreamAttemptError(err) {
		if history, ok := ctx.Value(upstreamErrorHistoryKey{}).(*upstreamErrorHistory); ok {
			recorded := err
			if len(headers) > 0 {
				recorded = newStreamBootstrapError(err, headers[0])
			}
			history.mu.Lock()
			history.last = withAuthErrorResponseSource(recorded, auth, provider)
			history.mu.Unlock()
		}
	}
	return err
}
