package logging

import (
	"strings"

	"github.com/gin-gonic/gin"
)

// ResolveClientIP returns the canonical client IP used by both HTTP logs and
// usage statistics so they stay aligned in proxied deployments.
func ResolveClientIP(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return ""
	}
	return strings.TrimSpace(c.ClientIP())
}
