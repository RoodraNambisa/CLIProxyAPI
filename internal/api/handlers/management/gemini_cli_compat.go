package management

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// RequestGeminiCLIToken keeps the retired login route explicit for older clients.
func (h *Handler) RequestGeminiCLIToken(c *gin.Context) {
	c.JSON(http.StatusGone, gin.H{"error": "Gemini CLI authentication is no longer supported"})
}
