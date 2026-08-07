package management

import "github.com/gin-gonic/gin"

// Quota exceeded toggles
func (h *Handler) GetSwitchProject(c *gin.Context) {
	cfg := h.currentConfig()
	c.JSON(200, gin.H{"switch-project": cfg != nil && cfg.QuotaExceeded.SwitchProject})
}
func (h *Handler) PutSwitchProject(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.QuotaExceeded.SwitchProject = v })
}

func (h *Handler) GetSwitchPreviewModel(c *gin.Context) {
	cfg := h.currentConfig()
	c.JSON(200, gin.H{"switch-preview-model": cfg != nil && cfg.QuotaExceeded.SwitchPreviewModel})
}
func (h *Handler) PutSwitchPreviewModel(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.QuotaExceeded.SwitchPreviewModel = v })
}
