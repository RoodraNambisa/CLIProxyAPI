package management

import (
	"math"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type historyCleanupRequest struct {
	OlderThanDays       *int   `json:"older_than_days"`
	MaxStorageMegabytes *int64 `json:"max_storage_megabytes"`
}

type usageStorageConfigRequest struct {
	PersistenceEnabled  *bool `json:"usage-statistics-persistence-enabled"`
	PersistInterval     *int  `json:"usage-statistics-persist-interval-seconds"`
	DetailRetentionDays *int  `json:"usage-statistics-detail-retention-days"`
	MaxStorageMegabytes *int  `json:"usage-statistics-max-storage-megabytes"`
}

// GetUsageStorageConfig returns Usage persistence independently from collection.
func (h *Handler) GetUsageStorageConfig(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"usage-statistics-persistence-enabled":      cfg.UsageStatisticsPersistence(),
		"usage-statistics-persist-interval-seconds": cfg.UsageStatisticsPersistIntervalSeconds,
		"usage-statistics-detail-retention-days":    cfg.UsageStatisticsDetailRetentionDays,
		"usage-statistics-max-storage-megabytes":    cfg.UsageStatisticsMaxStorageMB,
	})
}

// PutUsageStorageConfig updates any supplied Usage persistence fields.
func (h *Handler) PutUsageStorageConfig(c *gin.Context) {
	var request usageStorageConfigRequest
	if errBind := c.ShouldBindJSON(&request); errBind != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if request.PersistenceEnabled == nil && request.PersistInterval == nil && request.DetailRetentionDays == nil && request.MaxStorageMegabytes == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no usage storage fields supplied"})
		return
	}
	for _, value := range []*int{request.PersistInterval, request.DetailRetentionDays, request.MaxStorageMegabytes} {
		if value != nil && *value < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "usage storage values must not be negative"})
			return
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	if request.PersistenceEnabled != nil {
		value := *request.PersistenceEnabled
		h.cfg.UsageStatisticsPersistenceEnabled = &value
	}
	if request.PersistInterval != nil {
		h.cfg.UsageStatisticsPersistIntervalSeconds = *request.PersistInterval
	}
	if request.DetailRetentionDays != nil {
		h.cfg.UsageStatisticsDetailRetentionDays = *request.DetailRetentionDays
	}
	if request.MaxStorageMegabytes != nil {
		h.cfg.UsageStatisticsMaxStorageMB = *request.MaxStorageMegabytes
	}
	h.persistLocked(c)
}

// GetStorageHistory returns safe aggregate storage and restore information.
func (h *Handler) GetStorageHistory(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	stats := h.usageStatisticsSnapshot()
	var usageMeta usage.MetaSnapshot
	detailCount := 0
	if stats != nil {
		usageMeta = stats.Meta()
		detailCount = stats.DetailCount()
	}
	usageStorage, errUsage := usage.InspectStatisticsStorage(h.usageStatisticsFilePath())
	if errUsage != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to inspect usage storage"})
		return
	}
	logDir := h.logDirectory()
	protectedPath := ""
	if cfg.LoggingToFile && strings.TrimSpace(logDir) != "" {
		protectedPath = filepath.Join(logDir, defaultLogFileName)
	}
	logStorage, errLogs := logging.InspectLogDirectory(logDir, protectedPath)
	if errLogs != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to inspect log storage"})
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{
		"usage": gin.H{
			"collection_enabled":       cfg.UsageStatisticsEnabled,
			"persistence_enabled":      cfg.UsageStatisticsPersistence(),
			"persist_interval_seconds": cfg.UsageStatisticsPersistIntervalSeconds,
			"detail_retention_days":    cfg.UsageStatisticsDetailRetentionDays,
			"max_storage_megabytes":    cfg.UsageStatisticsMaxStorageMB,
			"detail_count":             detailCount,
			"meta":                     usageMeta,
			"storage":                  usageStorage,
			"restore":                  h.usageRestoreStatusSnapshot(),
		},
		"logs": gin.H{
			"file_logging_enabled": cfg.LoggingToFile,
			"retention_days":       cfg.LogsRetentionDays,
			"max_total_size_mb":    cfg.LogsMaxTotalSizeMB,
			"storage":              logStorage,
		},
	})
}

// PruneUsageHistory applies an explicit time and size retention pass.
func (h *Handler) PruneUsageHistory(c *gin.Context) {
	cfg := h.currentConfig()
	stats := h.usageStatisticsSnapshot()
	if cfg == nil || stats == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "usage statistics unavailable"})
		return
	}
	restore := h.usageRestoreStatusSnapshot()
	if restore.Active {
		c.JSON(http.StatusConflict, gin.H{"error": "usage statistics restore is still running", "code": "usage_restore_in_progress"})
		return
	}
	request, ok := bindHistoryCleanupRequest(c, cfg.UsageStatisticsDetailRetentionDays, int64(cfg.UsageStatisticsMaxStorageMB))
	if !ok {
		return
	}
	if errBarrier := coreusage.DefaultManager().Barrier(c.Request.Context()); errBarrier != nil {
		status, message := usageBarrierErrorResponse(errBarrier)
		c.JSON(status, gin.H{"error": message})
		return
	}
	before := stats.Meta()
	beforeDetails := stats.DetailCount()
	result, errPrune := usage.PruneAndPersistRequestStatistics(h.usageStatisticsFilePath(), stats, usage.PersistencePolicy{
		DetailRetentionDays: *request.OlderThanDays,
		MaxBytes:            historyMegabytesToBytes(*request.MaxStorageMegabytes),
	})
	if errPrune != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to prune usage history"})
		return
	}
	after := stats.Meta()
	c.JSON(http.StatusOK, gin.H{
		"pruned":                result.Pruned,
		"saved":                 result.Saved,
		"size_bytes":            result.SizeBytes,
		"detail_count_before":   beforeDetails,
		"detail_count_after":    stats.DetailCount(),
		"total_requests_before": before.TotalRequests,
		"total_requests_after":  after.TotalRequests,
	})
}

// PruneLogHistory applies an explicit time and size retention pass.
func (h *Handler) PruneLogHistory(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	request, ok := bindHistoryCleanupRequest(c, cfg.LogsRetentionDays, int64(cfg.LogsMaxTotalSizeMB))
	if !ok {
		return
	}
	logDir := h.logDirectory()
	protectedPath := ""
	if cfg.LoggingToFile && strings.TrimSpace(logDir) != "" {
		protectedPath = filepath.Join(logDir, defaultLogFileName)
	}
	result, errCleanup := logging.CleanupLogDirectory(logDir, *request.OlderThanDays, historyMegabytesToBytes(*request.MaxStorageMegabytes), protectedPath, time.Now().UTC())
	if errCleanup != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to prune log history"})
		return
	}
	c.JSON(http.StatusOK, result)
}

func bindHistoryCleanupRequest(c *gin.Context, defaultDays int, defaultMegabytes int64) (historyCleanupRequest, bool) {
	var request historyCleanupRequest
	if c.Request.ContentLength != 0 {
		if errBind := c.ShouldBindJSON(&request); errBind != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
			return request, false
		}
	}
	if request.OlderThanDays == nil {
		value := defaultDays
		request.OlderThanDays = &value
	}
	if request.MaxStorageMegabytes == nil {
		value := defaultMegabytes
		request.MaxStorageMegabytes = &value
	}
	if *request.OlderThanDays < 0 || *request.MaxStorageMegabytes < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "retention values must not be negative"})
		return request, false
	}
	if *request.OlderThanDays == 0 && *request.MaxStorageMegabytes == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one retention policy is required"})
		return request, false
	}
	return request, true
}

func historyMegabytesToBytes(megabytes int64) int64 {
	if megabytes <= 0 {
		return 0
	}
	if megabytes > math.MaxInt64/(1024*1024) {
		return math.MaxInt64
	}
	return megabytes * 1024 * 1024
}
