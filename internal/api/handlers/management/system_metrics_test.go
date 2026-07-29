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
	"github.com/router-for-me/CLIProxyAPI/v6/internal/systemmetrics"
)

func TestGetSystemMetricsReturnsRuntimeAndConfiguredFilesystems(t *testing.T) {
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
}
