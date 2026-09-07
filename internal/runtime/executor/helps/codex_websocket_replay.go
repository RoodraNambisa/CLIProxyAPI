package helps

import (
	"context"
	"errors"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// CodexWebsocketReplayError preserves cancellation and explicit status errors;
// ordinary write failures require a full replay and retain their original cause.
func CodexWebsocketReplayError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var status interface{ StatusCode() int }
	if errors.As(err, &status) && status.StatusCode() > 0 {
		return err
	}
	return executor.NewUpstreamWebsocketReplayRequiredError(err)
}
