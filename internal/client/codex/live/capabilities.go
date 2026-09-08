package live

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Unsupported advertises no upstream connection or capability. These routes
// remain explicit even when new Live sessions are disabled.
func (*Handler) Unsupported(capability string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if _, authenticated := requestCallOwner(c); !authenticated {
			writeRealtimeError(c, http.StatusUnauthorized, "Realtime requires an authenticated API caller", "authentication_error", "realtime_auth_required")
			return
		}
		writeRealtimeError(c, http.StatusNotImplemented, capability+" are not supported by the Codex OAuth upstream", "not_supported_error", "realtime_capability_not_supported")
	}
}
