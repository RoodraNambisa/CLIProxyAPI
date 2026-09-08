package cliproxy

import "github.com/router-for-me/CLIProxyAPI/v6/internal/registry"

func applyConfiguredModelContextLength(info *ModelInfo, model modelEntry) {
	if info == nil {
		return
	}
	// Inherit the actual upstream model's catalog, never another alias/client.
	if upstream := registry.LookupStaticModelInfo(info.UpstreamID); upstream != nil {
		info.ContextLength = upstream.ContextLength
		if info.ContextLength <= 0 {
			info.ContextLength = upstream.InputTokenLimit
		}
	}
	if limit := model.GetMaxContextLength(); limit > 0 {
		info.ContextLength = limit
		info.MaxContextLength = limit
	}
}
