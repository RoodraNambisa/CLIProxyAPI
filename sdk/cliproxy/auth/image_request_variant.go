package auth

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func executeProviderRequest(ctx context.Context, provider ProviderExecutor, auth *Auth, req executor.Request, opts executor.Options) (executor.Response, error) {
	req, opts = executor.PrepareImageRequestForProvider(auth.Provider, req, opts)
	executor.ObserveImageResponseProvider(auth.Provider, opts)
	return provider.Execute(ctx, auth, req, opts)
}

func executeProviderStream(ctx context.Context, provider ProviderExecutor, auth *Auth, req executor.Request, opts executor.Options) (*executor.StreamResult, error) {
	req, opts = executor.PrepareImageRequestForProvider(auth.Provider, req, opts)
	executor.ObserveImageResponseProvider(auth.Provider, opts)
	return provider.ExecuteStream(ctx, auth, req, opts)
}
