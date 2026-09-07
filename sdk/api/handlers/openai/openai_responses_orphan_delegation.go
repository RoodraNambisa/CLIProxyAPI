package openai

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

// prepareOrphanDelegation fixes the source Responses protocol before tool-cache
// repair or translation to any selected provider. It never alters routing state.
func (h *OpenAIResponsesAPIHandler) prepareOrphanDelegation(c *gin.Context, payload []byte, cachedCall ...func(string) bool) []byte {
	cfg := h.ConfigSnapshot()
	if cfg == nil || c == nil || c.Request == nil {
		return payload
	}
	enabled := helps.CodexOrphanDelegationEnabled(nil, c.Request.Header, cfg.CodexOrphanDelegationCompatibility)
	// Executors must not normalize a second time after the boundary consulted
	// a caller-scoped cache that is intentionally unavailable to the executor.
	c.Set(helps.CodexOrphanDelegationPreparedContextKey, true)
	return helps.RewriteCodexOrphanDelegationInput(payload, enabled, cachedCall...)
}
