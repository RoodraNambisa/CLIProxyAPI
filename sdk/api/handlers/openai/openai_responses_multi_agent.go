package openai

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

// prepareCodexMultiAgentV2 pins one request's real client policy and prepares
// shared tool descriptions before selecting a provider or releasing the body.
func (h *OpenAIResponsesAPIHandler) prepareCodexMultiAgentV2(c *gin.Context, payload []byte) []byte {
	if c == nil || c.Request == nil {
		return payload
	}
	cfg := h.ConfigSnapshot()
	policy := helps.NewCodexMultiAgentPolicy(c.Request.Header, cfg != nil && cfg.CodexOptimizeMultiAgentV2)
	policy.ToolsPrepared = policy.Enabled
	c.Set(helps.CodexMultiAgentPolicyGinKey, policy)
	if !policy.Enabled {
		return payload
	}
	var modelList string
	if helps.CodexCollaborationNeedsModelList(payload) {
		models := h.ModelsForProviderAccess(c, "openai")
		response := codexClientModelsResponseForClient(models, policy.ClientVersion, registry.GetGlobalRegistry().GetModelProviders, policy.Enabled)
		modelList = helps.FormatCodexCollaborationModels(response["models"].([]map[string]any))
	}
	return helps.PrepareCodexCollaborationTools(payload, modelList)
}
