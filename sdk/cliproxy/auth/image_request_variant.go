package auth

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func executeProviderRequest(ctx context.Context, provider ProviderExecutor, auth *Auth, req executor.Request, opts executor.Options) (executor.Response, error) {
	req, opts = executor.PrepareImageRequestForProvider(auth.Provider, req, opts)
	executor.ObserveImageResponseProvider(auth.Provider, opts)
	if err := executor.ImageRequestBudgetFromOptions(opts).Select(auth.Provider); err != nil {
		return executor.Response{}, err
	}
	response, err := provider.Execute(ctx, auth, req, opts)
	return response, executor.ImageRequestContextError(ctx, err)
}

func executeProviderStream(ctx context.Context, provider ProviderExecutor, auth *Auth, req executor.Request, opts executor.Options) (*executor.StreamResult, error) {
	req, opts = executor.PrepareImageRequestForProvider(auth.Provider, req, opts)
	executor.ObserveImageResponseProvider(auth.Provider, opts)
	if err := executor.ImageRequestBudgetFromOptions(opts).Select(auth.Provider); err != nil {
		return nil, err
	}
	result, err := provider.ExecuteStream(ctx, auth, req, opts)
	return result, executor.ImageRequestContextError(ctx, err)
}
