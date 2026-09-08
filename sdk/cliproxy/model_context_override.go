package cliproxy

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
)

func applyConfiguredModelCatalogMetadata(info *ModelInfo, model modelEntry) {
	if info == nil {
		return
	}
	// Inherit the actual upstream model's catalog, never another alias/client.
	if upstream := registry.LookupStaticModelInfo(thinking.ParseSuffix(info.UpstreamID).ModelName); upstream != nil {
		info.ContextLength = upstream.ContextLength
		if info.ContextLength <= 0 {
			info.ContextLength = upstream.InputTokenLimit
		}
		info.InputTokenLimit = upstream.InputTokenLimit
		if info.InputTokenLimit <= 0 {
			info.InputTokenLimit = upstream.ContextLength
		}
		// MaxCompletionTokens also controls executor defaults; only inherit the
		// catalog output limit here so request behavior remains unchanged.
		info.OutputTokenLimit = upstream.OutputTokenLimit
		if info.OutputTokenLimit <= 0 {
			info.OutputTokenLimit = upstream.MaxCompletionTokens
		}
		info.SupportedInputModalities = upstream.SupportedInputModalities
		info.SupportedOutputModalities = upstream.SupportedOutputModalities
		if info.Thinking == nil {
			info.Thinking = upstream.Thinking
		}
	}
	if limit := model.GetMaxContextLength(); limit > 0 {
		info.ContextLength = limit
		info.MaxContextLength = limit
	}
	if support := model.GetThinking(); support != nil {
		info.Thinking = config.NormalizeModelThinkingSupport(support)
		info.UserDefined = false
	}
}
