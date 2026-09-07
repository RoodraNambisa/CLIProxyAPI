package auth

import (
	"context"
	"slices"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

type retrySettingsSnapshot struct {
	retries     int
	credentials int
	wait        time.Duration
}

type retrySettingsContextKey struct{}
type cooldownRulesSnapshot struct {
	noCooldownStatusCodes []int
	fixedErrorCooldowns   []internalconfig.FixedErrorCooldownRule
}

type requestRetrySettings struct {
	codexOptimizeMultiAgentV2 bool
	manager                   *Manager
	settings                  *retrySettingsSnapshot
	errorRules                []internalconfig.NonRetryableErrorRule
	cooldownRules             cooldownRulesSnapshot
}

func (m *Manager) withRetrySettingsSnapshot(ctx context.Context) context.Context {
	if prior, _ := ctx.Value(retrySettingsContextKey{}).(*requestRetrySettings); prior != nil && prior.manager == m {
		return ctx
	}
	cfg := m.currentConfig()
	cooldownRules := cooldownRulesForConfig(cfg)
	cooldownRules.noCooldownStatusCodes = slices.Clone(cooldownRules.noCooldownStatusCodes)
	cooldownRules.fixedErrorCooldowns = slices.Clone(cooldownRules.fixedErrorCooldowns)
	return context.WithValue(ctx, retrySettingsContextKey{}, &requestRetrySettings{
		codexOptimizeMultiAgentV2: cfg != nil && cfg.Codex.OptimizeMultiAgentV2,
		manager:                   m, settings: m.retryConfig.Load(),
		errorRules: slices.Clone(nonRetryableErrorRulesForConfig(cfg)), cooldownRules: cooldownRules,
	})
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

func (m *Manager) requestNonRetryableErrorRules(contexts ...context.Context) []internalconfig.NonRetryableErrorRule {
	if len(contexts) > 0 && contexts[0] != nil {
		if captured, _ := contexts[0].Value(retrySettingsContextKey{}).(*requestRetrySettings); captured != nil && captured.manager == m {
			return captured.errorRules
		}
	}
	return nonRetryableErrorRulesForConfig(m.currentConfig())
}

func cooldownRulesForConfig(cfg *internalconfig.Config) cooldownRulesSnapshot {
	if cfg == nil {
		return cooldownRulesSnapshot{}
	}
	return cooldownRulesSnapshot{noCooldownStatusCodes: cfg.NoCooldownStatusCodes, fixedErrorCooldowns: cfg.FixedErrorCooldowns}
}

func (m *Manager) requestCooldownRules(contexts ...context.Context) cooldownRulesSnapshot {
	if len(contexts) > 0 && contexts[0] != nil {
		if captured, _ := contexts[0].Value(retrySettingsContextKey{}).(*requestRetrySettings); captured != nil && captured.manager == m {
			return captured.cooldownRules
		}
	}
	return cooldownRulesForConfig(m.currentConfig())
}
