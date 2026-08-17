package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/systemmetrics"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestGetSystemMetricsReturnsRuntimeAndConfiguredFilesystems(t *testing.T) {
	helps.ConfigureChatGPTWebImageMemoryCapacity(96 << 20)
	coreexecutor.ConfigureChatGPTWebImageAdmissions(7, 4, 2)
	coreexecutor.ConfigureChatGPTWebImageRuntimeAdmissions(7, 5, 3)
	t.Cleanup(func() {
		helps.ConfigureChatGPTWebImageMemoryCapacity(int64(config.DefaultChatGPTWebImageMemoryCapacityMB) << 20)
		coreexecutor.ConfigureChatGPTWebImageAdmissions(
			config.DefaultChatGPTWebImageMaxInFlight,
			config.DefaultChatGPTWebImageAdmissionQueueSize,
			config.DefaultChatGPTWebImageMaxFinalizers,
		)
		coreexecutor.ConfigureChatGPTWebImageRuntimeAdmissions(
			config.DefaultChatGPTWebImageMaxInFlight,
			config.DefaultChatGPTWebImagePollConcurrency,
			config.DefaultChatGPTWebImageMemoryFinalizerConcurrency,
		)
	})
	root := t.TempDir()
	authDirectory := filepath.Join(root, "auth")
	cacheDirectory := filepath.Join(root, "usage-cache")

	handler := NewHandlerWithoutConfigFilePath(&config.Config{
		AuthDir: authDirectory,
		ChatGPTWeb: config.ChatGPTWebConfig{
			UsageCache: config.ChatGPTWebUsageCacheConfig{Path: cacheDirectory},
		},
	}, nil)
	t.Cleanup(func() {
		if errShutdown := handler.Shutdown(context.Background()); errShutdown != nil {
			t.Fatalf("Shutdown() error = %v", errShutdown)
		}
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/system/metrics", nil)
	handler.GetSystemMetrics(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	var response systemMetricsResponse
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("json.Unmarshal() error = %v", errDecode)
	}
	if response.CollectedAt.IsZero() || time.Since(response.CollectedAt) > time.Minute {
		t.Fatalf("CollectedAt = %v", response.CollectedAt)
	}
	if response.Runtime.GoVersion == "" || response.Runtime.GOMAXPROCS < 1 ||
		response.Runtime.Goroutines < 1 {
		t.Fatalf("Runtime = %#v", response.Runtime)
	}
	if response.Filesystems.WorkingDirectory.Status != systemmetrics.FilesystemStatusOK ||
		response.Filesystems.AuthDirectory.Status != systemmetrics.FilesystemStatusOK ||
		response.Filesystems.UsageCache.Status != systemmetrics.FilesystemStatusOK {
		t.Fatalf("Filesystems = %#v", response.Filesystems)
	}
	if response.Filesystems.AuthDirectory.Path != root ||
		response.Filesystems.UsageCache.Path != root {
		t.Fatalf("nearest existing filesystems = %#v", response.Filesystems)
	}
	if response.UsageBatch.QueueCapacity < 1 {
		t.Fatalf("UsageBatch = %#v, want bounded production queue", response.UsageBatch)
	}
	if response.ImageProcessing.CapacityBytes != 96<<20 || response.ImageRequestMemory.CapacityBytes != 96<<20 {
		t.Fatalf("image memory aliases = %#v / %#v, want configured capacity", response.ImageProcessing, response.ImageRequestMemory)
	}
	if response.ChatGPTWebImageInFlight.Limit != 7 || response.ChatGPTWebImageInFlight.QueueLimit != 4 ||
		response.ChatGPTWebFinalizers.Limit != 2 || response.ChatGPTWebFinalizers.QueueLimit != 7 {
		t.Fatalf("image lifecycle admissions = %#v / %#v", response.ChatGPTWebImageInFlight, response.ChatGPTWebFinalizers)
	}
	if response.ChatGPTWebPollSlots.Limit != 5 || response.ChatGPTWebPollSlots.QueueLimit != 14 || response.ChatGPTWebPollSlots.Queued != 0 || response.ChatGPTWebPollSlots.Shrinking {
		t.Fatalf("ChatGPTWebPollSlots = %#v", response.ChatGPTWebPollSlots)
	}
	if response.ChatGPTWebMemoryFinalizers.Limit != 3 || response.ChatGPTWebMemoryFinalizers.QueueLimit != 7 || response.ChatGPTWebMemoryFinalizers.Active != 0 {
		t.Fatalf("ChatGPTWebMemoryFinalizers = %#v", response.ChatGPTWebMemoryFinalizers)
	}
	if response.ImageRequestPhases.HandlerScope != "all_image_routes" ||
		response.ImageRequestPhases.WebScope != "chatgpt_web_only_after_executor_selection" ||
		response.ImageRequestPhases.ResponseWriteCountSemantics != "write_operations" ||
		response.ImageRequestPhases.Metrics == nil {
		t.Fatalf("ImageRequestPhases = %#v", response.ImageRequestPhases)
	}
}
