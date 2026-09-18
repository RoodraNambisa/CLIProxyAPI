package management

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func (h *Handler) GetAuthFileResponseModelRewrite(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	name := strings.TrimSpace(c.Query("name"))
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "credential name is required"})
		return
	}
	manager := h.coreAuthRuntimeManager()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "credential manager unavailable"})
		return
	}
	auth := h.findManagedAuthWithManager(name, manager)
	if auth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "credential not found"})
		return
	}
	c.JSON(http.StatusOK, manager.AuthResponseModelRewriteSummary(auth, true))
}
