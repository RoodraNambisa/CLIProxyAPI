package auth

import (
	"context"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func (m *Manager) requestOAuthErrorRules(ctx context.Context, provider string) *config.CompiledRequestScopedErrors {
	if policy := m.selectionPolicy(ctx); policy != nil {
		return policy.oauthErrorRules[strings.ToLower(strings.TrimSpace(provider))]
	}
	return nil
}
