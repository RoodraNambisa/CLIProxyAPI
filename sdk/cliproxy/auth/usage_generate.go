package auth

import (
	"context"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func contextWithGenerateMetadata(ctx context.Context, opts core.Options) context.Context {
	if generate, ok := opts.Metadata[core.GenerateMetadataKey].(bool); ok {
		return usage.WithGenerate(ctx, generate)
	}
	return ctx
}
