package auth_test

import (
	"context"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

var _ = cliproxyauth.Result{"auth-id", "codex", "model", true, nil, nil}

type externalTransactionalBinder struct{}

func (externalTransactionalBinder) BindSessionWithRollback(context.Context, string, string, cliproxyexecutor.Options, string) func() {
	return func() {}
}

var _ cliproxyauth.SessionAffinityTransactionalBinder = externalTransactionalBinder{}
