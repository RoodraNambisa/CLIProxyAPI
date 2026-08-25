package management

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/api/middleware"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/systemmetrics"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type systemMetricsFilesystems struct {
	WorkingDirectory systemmetrics.FilesystemSnapshot `json:"working_directory"`
	AuthDirectory    systemmetrics.FilesystemSnapshot `json:"auth_directory"`
	UsageCache       systemmetrics.FilesystemSnapshot `json:"usage_cache"`
}

type systemMetricsResponse struct {
	CollectedAt                time.Time                                              `json:"collected_at"`
	Runtime                    systemmetrics.RuntimeSnapshot                          `json:"runtime"`
	Filesystems                systemMetricsFilesystems                               `json:"filesystems"`
	RequestBodyAudit           middleware.RequestBodyAuditRuntimeSnapshot             `json:"request_body_audit"`
	UsageBatch                 usage.UsageBatchRuntimeSnapshot                        `json:"usage_batch"`
	ImageProcessing            helps.ChatGPTWebImageMemoryRuntimeSnapshot             `json:"image_post_processing"`
	ImageRequestMemory         helps.ChatGPTWebImageMemoryRuntimeSnapshot             `json:"image_request_memory"`
	ChatGPTWebImageInFlight    coreexecutor.ImageExecutionAdmissionSnapshot           `json:"chatgpt_web_image_in_flight"`
	ChatGPTWebFinalizers       coreexecutor.ImageExecutionAdmissionSnapshot           `json:"chatgpt_web_image_finalizers"`
	ChatGPTWebMemoryFinalizers coreexecutor.ImageExecutionAdmissionSnapshot           `json:"chatgpt_web_image_memory_finalizers"`
	ChatGPTWebPollSlots        runtimeexecutor.ChatGPTWebImagePollRuntimeSnapshot     `json:"chatgpt_web_image_poll_slots"`
	ChatGPTWebImageProtocol    runtimeexecutor.ChatGPTWebImageProtocolRuntimeSnapshot `json:"chatgpt_web_image_protocol"`
	ImageSpool                 helps.ChatGPTWebImageSpoolRuntimeSnapshot              `json:"image_spool"`
	ImageRequestPhases         systemMetricsImageRequestPhases                        `json:"image_request_phases"`
}

type systemMetricsImageRequestPhases struct {
	HandlerScope                string                                                  `json:"handler_scope"`
	WebScope                    string                                                  `json:"chatgpt_web_scope"`
	ResponseWriteCountSemantics string                                                  `json:"response_write_count_semantics"`
	Metrics                     map[string]coreexecutor.ImageRequestPhaseMetricSnapshot `json:"metrics"`
	Rolling                     coreexecutor.ImageRequestPhaseRollingSnapshot           `json:"rolling"`
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
	imageMemory := helps.ChatGPTWebImageMemorySnapshot()
	c.JSON(http.StatusOK, systemMetricsResponse{
		CollectedAt:                time.Now().UTC(),
		Runtime:                    systemmetrics.CollectRuntime(),
		RequestBodyAudit:           middleware.RequestBodyAuditSnapshot(),
		UsageBatch:                 usage.BatchRuntimeSnapshot(),
		ImageProcessing:            imageMemory,
		ImageRequestMemory:         imageMemory,
		ChatGPTWebImageInFlight:    coreexecutor.ChatGPTWebImageExecutionAdmissionSnapshot(),
		ChatGPTWebFinalizers:       coreexecutor.ChatGPTWebImageFinalizerAdmissionSnapshot(),
		ChatGPTWebMemoryFinalizers: coreexecutor.ChatGPTWebImageMemoryFinalizerAdmissionSnapshot(),
		ChatGPTWebPollSlots:        runtimeexecutor.ChatGPTWebImagePollSnapshot(),
		ChatGPTWebImageProtocol:    runtimeexecutor.ChatGPTWebImageProtocolSnapshot(),
		ImageSpool:                 helps.ChatGPTWebImageSpoolSnapshot(),
		ImageRequestPhases: systemMetricsImageRequestPhases{
			HandlerScope:                "all_image_routes",
			WebScope:                    "chatgpt_web_only_after_executor_selection",
			ResponseWriteCountSemantics: "write_operations",
			Metrics:                     coreexecutor.ImageRequestPhaseSnapshot(),
			Rolling:                     coreexecutor.ImageRequestPhaseRollingWindowSnapshot(),
		},
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
