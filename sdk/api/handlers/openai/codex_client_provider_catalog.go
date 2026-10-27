package openai

import (
	"slices"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
)

func codexClientModelsForRequest(c *gin.Context, base *handlers.BaseAPIHandler, version string, optimizeMultiAgentV2 bool, tierConfigs ...*config.SDKConfig) map[string]any {
	catalog, restricted := base.RestrictedModelCatalog(c, "openai")
	if !restricted {
		catalog = registry.GetGlobalRegistry().GetOpenAIModelCatalog()
	}
	response := codexClientModelsResponseForClient(catalog.Models, version,
		func(id string) []string { return catalog.Providers[id] }, optimizeMultiAgentV2,
		func(id string) *registry.ModelInfo { return catalog.Metadata[id] })
	var tierConfig *config.SDKConfig
	if len(tierConfigs) > 0 {
		tierConfig = tierConfigs[0]
	}
	for _, entry := range response["models"].([]map[string]any) {
		providers := catalog.Providers[stringModelValue(entry, "slug")]
		if slices.Contains(providers, "codex") {
			if len(providers) == 1 {
				applyCodexClientServiceTiers(entry, tierConfig)
			} else {
				// A shared slug cannot promise a tier on every possible provider route.
				clearCodexClientServiceTiers(entry)
			}
		}
	}
	return response
}
