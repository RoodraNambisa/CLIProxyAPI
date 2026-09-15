package openai

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/client/grokbuild"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

// grokBuildModels preserves the request API key's provider visibility before
// adding the capability fields expected by the official Grok Build client.
func (h *OpenAIAPIHandler) grokBuildModels(c *gin.Context) {
	catalog, restricted := h.RestrictedModelCatalog(c, "openai")
	models := catalog.Models
	if !restricted {
		models = h.Models()
	}
	entries := make([]grokbuild.ModelInfo, 0, len(models))
	for _, model := range models {
		id, _ := model["id"].(string)
		info := catalog.Metadata[id]
		if !restricted {
			info = registry.GetGlobalRegistry().GetModelInfo(id, "")
		}
		entry := grokbuild.ModelInfo{ID: id}
		if info != nil {
			entry.DisplayName, entry.ContextLength = info.DisplayName, info.ContextLength
			if info.MaxContextLength > 0 {
				entry.ContextLength = info.MaxContextLength
			}
			if entry.ContextLength <= 0 {
				entry.ContextLength = info.InputTokenLimit
			}
			if info.Thinking != nil {
				entry.ReasoningLevels = info.Thinking.Levels
			}
		}
		entries = append(entries, entry)
	}
	c.JSON(http.StatusOK, grokbuild.BuildResponse(entries))
}
