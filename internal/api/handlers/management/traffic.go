package management

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (h *Handler) GetTraffic(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth manager unavailable"})
		return
	}
	minutes := 30
	if raw := c.Query("window_minutes"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 || v > 1440 {
			c.JSON(400, gin.H{"error": "window_minutes must be 1–1440"})
			return
		}
		minutes = v
	}
	mode, basis := c.DefaultQuery("mode", "rolling"), c.DefaultQuery("basis", "admitted")
	if mode != "rolling" && mode != "current_minute" || basis != "admitted" && basis != "completed" {
		c.JSON(400, gin.H{"error": "invalid traffic query"})
		return
	}
	c.JSON(http.StatusOK, h.authManager.TrafficController().Overview(time.Now().UTC(), minutes*60, mode == "current_minute", basis == "completed"))
}

func (h *Handler) GetCredentialTraffic(c *gin.Context) { h.batchTraffic(c, false) }
func (h *Handler) GetKeyTraffic(c *gin.Context)        { h.batchTraffic(c, true) }
func (h *Handler) batchTraffic(c *gin.Context, keys bool) {
	var query struct {
		IDs []string `json:"ids"`
	}
	if c.ShouldBindJSON(&query) != nil || len(query.IDs) > 200 {
		c.JSON(400, gin.H{"error": "provide at most 200 ids"})
		return
	}
	for _, id := range query.IDs {
		if len(id) > 256 {
			c.JSON(400, gin.H{"error": "invalid traffic id"})
			return
		}
	}
	if h.authManager == nil {
		c.JSON(503, gin.H{"error": "auth manager unavailable"})
		return
	}
	now := time.Now().UTC()
	if keys {
		c.JSON(200, gin.H{"as_of": now, "items": h.authManager.TrafficController().KeySnapshots(query.IDs, now)})
		return
	}
	c.JSON(200, gin.H{"as_of": now, "items": h.authManager.CredentialTraffic(query.IDs, now)})
}

func (h *Handler) GetAdmissionOptions(c *gin.Context) {
	c.JSON(200, gin.H{"features": gin.H{"admission": true, "traffic": true, "credential_concurrency": true, "key_rpm": true, "queue": true}})
}
