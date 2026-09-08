package openai

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
)

func codexClientModelsForRequest(c *gin.Context, base *handlers.BaseAPIHandler, version string, optimizeMultiAgentV2 bool) map[string]any {
	catalog, restricted := base.RestrictedModelCatalog(c, "openai")
	if !restricted {
		catalog = registry.GetGlobalRegistry().GetOpenAIModelCatalog()
	}
	return codexClientModelsResponseForClient(catalog.Models, version,
		func(id string) []string { return catalog.Providers[id] }, optimizeMultiAgentV2,
		func(id string) *registry.ModelInfo { return catalog.Metadata[id] })
}
