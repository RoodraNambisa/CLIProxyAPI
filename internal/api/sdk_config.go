package api

import "github.com/router-for-me/CLIProxyAPI/v6/internal/config"

// sdkHandlerConfig copies derived provider policies without modifying the
// saved configuration or sharing a mutable policy with an in-flight request.
func sdkHandlerConfig(cfg *config.Config) *config.SDKConfig {
	if cfg == nil {
		return nil
	}
	snapshot := cfg.SDKConfig
	snapshot.CodexOptimizeMultiAgentV2 = cfg.Codex.OptimizeMultiAgentV2
	snapshot.CodexOrphanDelegationCompatibility = cfg.Codex.OrphanDelegationCompatibility
	return &snapshot
}
