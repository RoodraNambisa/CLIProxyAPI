package executor

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
)

// SingleAttempt identifies authenticated, explicitly targeted diagnostic requests.
// They must return the first outcome without replaying against any credential.
func SingleAttempt(ctx context.Context) bool {
	return access.CredentialTargetAuthID(ctx) != ""
}
