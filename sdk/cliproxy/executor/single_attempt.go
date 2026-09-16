package executor

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
)

type singleAttemptContextKey struct{}

// WithSingleAttempt disables replay for a management diagnostic request.
func WithSingleAttempt(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, singleAttemptContextKey{}, true)
}

// SingleAttempt identifies authenticated, explicitly targeted diagnostic requests.
// They must return the first outcome without replaying against any credential.
func SingleAttempt(ctx context.Context) bool {
	if ctx != nil && ctx.Value(singleAttemptContextKey{}) == true {
		return true
	}
	return access.CredentialTargetAuthID(ctx) != ""
}
