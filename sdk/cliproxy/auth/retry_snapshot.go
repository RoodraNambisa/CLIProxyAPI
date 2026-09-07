package auth

import (
	"context"
	"time"
)

type retrySettingsSnapshot struct {
	retries     int
	credentials int
	wait        time.Duration
}

type retrySettingsContextKey struct{}
type requestRetrySettings struct {
	manager  *Manager
	settings *retrySettingsSnapshot
}

func (m *Manager) withRetrySettingsSnapshot(ctx context.Context) context.Context {
	if prior, _ := ctx.Value(retrySettingsContextKey{}).(*requestRetrySettings); prior != nil && prior.manager == m {
		return ctx
	}
	return context.WithValue(ctx, retrySettingsContextKey{}, &requestRetrySettings{manager: m, settings: m.retryConfig.Load()})
}

func (m *Manager) retrySettings(contexts ...context.Context) (int, int, time.Duration) {
	if m == nil {
		return 0, 0, 0
	}
	settings := m.retryConfig.Load()
	if len(contexts) > 0 && contexts[0] != nil {
		if captured, _ := contexts[0].Value(retrySettingsContextKey{}).(*requestRetrySettings); captured != nil && captured.manager == m {
			settings = captured.settings
		}
	}
	if settings == nil {
		return 0, 0, 0
	}
	return settings.retries, settings.credentials, settings.wait
}
