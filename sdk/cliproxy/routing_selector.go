package cliproxy

import (
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// Only these effective settings construct the selector. Request limits,
// priority overrides and permissions are published separately by Manager.SetConfig.
type routingSelectorSettings struct {
	strategy                   string
	fillRange                  int
	affinity, failover, across bool
	subagents, history         bool
	useHistory                 bool
	ttl                        time.Duration
}

func routingSelectorSettingsForConfig(cfg *config.Config) routingSelectorSettings {
	settings := routingSelectorSettings{strategy: normalizeRuntimeRoutingStrategy(cfg.Routing.Strategy), fillRange: 1}
	if settings.strategy == "fill-first" {
		settings.fillRange = normalizedRoutingFillFirstRange(cfg)
	}
	settings.affinity = cfg.Routing.SessionAffinity || cfg.Routing.ClaudeCodeSessionAffinity
	if !settings.affinity {
		return settings
	}
	settings.failover = routingSessionAffinityFailoverEnabled(cfg)
	settings.across = cfg.Routing.SessionAffinityAcrossPriorities
	settings.subagents = cfg.Routing.SessionAffinity && cfg.Routing.SessionAffinitySubagents
	settings.useHistory = cfg.Routing.SessionAffinityHistoryEnabled()
	settings.history = cfg.Routing.SessionAffinity && cfg.Routing.SessionAffinityLCP
	settings.ttl = time.Hour
	if parsed, err := time.ParseDuration(strings.TrimSpace(cfg.Routing.SessionAffinityTTL)); err == nil && parsed > 0 {
		settings.ttl = parsed
	}
	return settings
}
