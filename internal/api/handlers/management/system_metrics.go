package management

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/systemmetrics"
)

type systemMetricsFilesystems struct {
	WorkingDirectory systemmetrics.FilesystemSnapshot `json:"working_directory"`
	AuthDirectory    systemmetrics.FilesystemSnapshot `json:"auth_directory"`
	UsageCache       systemmetrics.FilesystemSnapshot `json:"usage_cache"`
}

type systemMetricsResponse struct {
	CollectedAt time.Time                     `json:"collected_at"`
	Runtime     systemmetrics.RuntimeSnapshot `json:"runtime"`
	Filesystems systemMetricsFilesystems      `json:"filesystems"`
}

// GetSystemMetrics returns low-overhead process and filesystem metrics.
func (h *Handler) GetSystemMetrics(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	authDirectory := strings.TrimSpace(cfg.AuthDir)
	usageCachePath := chatGPTWebUsageCacheFilesystemPath(
		cfg.ChatGPTWeb.UsageCache.Resolved().Path,
	)

	workingDirectory, errWorkingDirectory := os.Getwd()
	if errWorkingDirectory != nil {
		workingDirectory = ""
	}
	c.JSON(http.StatusOK, systemMetricsResponse{
		CollectedAt: time.Now().UTC(),
		Runtime:     systemmetrics.CollectRuntime(),
		Filesystems: systemMetricsFilesystems{
			WorkingDirectory: systemmetrics.CollectFilesystem(workingDirectory),
			AuthDirectory:    systemmetrics.CollectFilesystem(authDirectory),
			UsageCache:       systemmetrics.CollectFilesystem(usageCachePath),
		},
	})
}

func chatGPTWebUsageCacheFilesystemPath(configuredPath string) string {
	if path := strings.TrimSpace(configuredPath); path != "" {
		return path
	}
	return os.TempDir()
}
