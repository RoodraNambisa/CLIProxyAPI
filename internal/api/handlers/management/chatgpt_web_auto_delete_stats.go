package management

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type chatGPTWebAutoDeleteDeadStatsResponse struct {
	DeletedCount uint64 `json:"deleted_count"`
}

// GetChatGPTWebAutoDeleteDeadStats returns the successful automatic deletion count for this process.
func (h *Handler) GetChatGPTWebAutoDeleteDeadStats(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	var count uint64
	if h != nil && h.deadAuthDeleteCount != nil {
		count = h.deadAuthDeleteCount()
	}
	c.JSON(http.StatusOK, chatGPTWebAutoDeleteDeadStatsResponse{DeletedCount: count})
}
