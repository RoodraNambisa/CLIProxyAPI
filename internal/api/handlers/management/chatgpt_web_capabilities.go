package management

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetChatGPTWebCapabilities reports supported credential and transport features.
func (h *Handler) GetChatGPTWebCapabilities(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{
		"credential_schema_versions": []int{1, 2, 3},
		"features":                   []string{"webauthn_v1", "api798_login_v1", "advanced_account_security_v1", "tls_trust_anchors_v1"},
	})
}
