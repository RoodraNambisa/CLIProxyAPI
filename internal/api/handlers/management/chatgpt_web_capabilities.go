package management

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetChatGPTWebCapabilities reports versioned credential features supported
// by the import and re-login pipeline.
func (h *Handler) GetChatGPTWebCapabilities(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{
		"credential_schema_versions": []int{1, 2},
		"features":                   []string{"webauthn_v1"},
	})
}
