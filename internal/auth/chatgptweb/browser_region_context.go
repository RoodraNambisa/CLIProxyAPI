package chatgptweb

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
)

type regionPolicyKey struct{}
type regionPolicy struct {
	Config config.ChatGPTWebConfig
	Route  proxyregion.Route
}

// WithBrowserRegionPolicy freezes the consumer policy for one login or refresh.
// The actual client proxy replaces Route.ProxyURL after login routing resolves.
func WithBrowserRegionPolicy(ctx context.Context, cfg config.ChatGPTWebConfig, route proxyregion.Route) context.Context {
	return context.WithValue(ctx, regionPolicyKey{}, regionPolicy{cfg, route})
}
