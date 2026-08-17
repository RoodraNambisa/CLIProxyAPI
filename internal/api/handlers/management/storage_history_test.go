package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestStorageHistoryReportsSafeAggregates(t *testing.T) {
	root := t.TempDir()
	t.Setenv("WRITABLE_PATH", root)
	logDir := filepath.Join(root, "logs")
	if errMkdir := os.MkdirAll(logDir, 0o755); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errWrite := os.WriteFile(filepath.Join(logDir, "main.log"), []byte("safe"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	persistenceEnabled := true
	cfg := &config.Config{
		AuthDir:                               root,
		LoggingToFile:                         true,
		LogsRetentionDays:                     7,
		LogsMaxTotalSizeMB:                    10,
		UsageStatisticsEnabled:                true,
		UsageStatisticsPersistenceEnabled:     &persistenceEnabled,
		UsageStatisticsPersistIntervalSeconds: 30,
		UsageStatisticsDetailRetentionDays:    14,
		UsageStatisticsMaxStorageMB:           20,
	}
	handler := NewHandlerWithoutConfigFilePath(cfg, nil)
	stats := usage.NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{APIKey: "key", Model: "model", RequestedAt: time.Now().UTC()})
	handler.SetUsageStatistics(stats)
	handler.SetUsageRestoreStatusProvider(func() usage.RestoreRuntimeSnapshot {
		return usage.RestoreRuntimeSnapshot{Enabled: true, Status: "running", Active: true}
	})
	if _, errPersist := usage.PersistRequestStatistics(handler.usageStatisticsFilePath(), stats); errPersist != nil {
		t.Fatal(errPersist)
	}

	ctx, recorder := newStorageHistoryContext(http.MethodGet, "/v0/management/storage/history", nil)
	handler.GetStorageHistory(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var body struct {
		Usage struct {
			DetailCount int                             `json:"detail_count"`
			Restore     usage.RestoreRuntimeSnapshot    `json:"restore"`
			Storage     usage.StatisticsStorageSnapshot `json:"storage"`
		} `json:"usage"`
		Logs struct {
			Storage struct {
				FileCount  int   `json:"file_count"`
				TotalBytes int64 `json:"total_bytes"`
			} `json:"storage"`
		} `json:"logs"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &body); errDecode != nil {
		t.Fatal(errDecode)
	}
	if body.Usage.DetailCount != 1 || !body.Usage.Restore.Active || !body.Usage.Storage.Main.Exists {
		t.Fatalf("usage history = %+v", body.Usage)
	}
	if body.Logs.Storage.FileCount != 1 || body.Logs.Storage.TotalBytes != 4 {
		t.Fatalf("log history = %+v", body.Logs)
	}
}

func TestPruneUsageHistoryRejectsRestoreAndAppliesRetention(t *testing.T) {
	root := t.TempDir()
	t.Setenv("WRITABLE_PATH", root)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: root}, nil)
	stats := usage.NewRequestStatistics()
	now := time.Now().UTC()
	stats.Record(context.Background(), coreusage.Record{APIKey: "old", Model: "model", RequestedAt: now.Add(-72 * time.Hour)})
	stats.Record(context.Background(), coreusage.Record{APIKey: "new", Model: "model", RequestedAt: now})
	handler.SetUsageStatistics(stats)
	handler.SetUsageRestoreStatusProvider(func() usage.RestoreRuntimeSnapshot {
		return usage.RestoreRuntimeSnapshot{Status: "running", Active: true}
	})
	requestBody := []byte(`{"older_than_days":2,"max_storage_megabytes":0}`)
	ctx, recorder := newStorageHistoryContext(http.MethodPost, "/v0/management/usage/prune", requestBody)
	handler.PruneUsageHistory(ctx)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("active restore status = %d: %s", recorder.Code, recorder.Body.String())
	}

	handler.SetUsageRestoreStatusProvider(func() usage.RestoreRuntimeSnapshot {
		return usage.RestoreRuntimeSnapshot{Status: "completed", Applied: true}
	})
	ctx, recorder = newStorageHistoryContext(http.MethodPost, "/v0/management/usage/prune", requestBody)
	handler.PruneUsageHistory(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	if stats.DetailCount() != 1 {
		t.Fatalf("detail count = %d, want 1", stats.DetailCount())
	}
	loaded, errLoad := usage.LoadSnapshotFile(handler.usageStatisticsFilePath())
	if errLoad != nil {
		t.Fatal(errLoad)
	}
	if loaded.TotalRequests != 1 {
		t.Fatalf("persisted total = %d, want 1", loaded.TotalRequests)
	}
}

func TestPruneLogHistoryProtectsActiveLog(t *testing.T) {
	root := t.TempDir()
	t.Setenv("WRITABLE_PATH", root)
	logDir := filepath.Join(root, "logs")
	if errMkdir := os.MkdirAll(logDir, 0o755); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	now := time.Now().UTC()
	for _, name := range []string{"main.log", "old.log"} {
		path := filepath.Join(logDir, name)
		if errWrite := os.WriteFile(path, []byte("history"), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		if errTimes := os.Chtimes(path, now.Add(-72*time.Hour), now.Add(-72*time.Hour)); errTimes != nil {
			t.Fatal(errTimes)
		}
	}
	handler := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: root, LoggingToFile: true}, nil)
	ctx, recorder := newStorageHistoryContext(http.MethodPost, "/v0/management/logs/prune", []byte(`{"older_than_days":2}`))
	handler.PruneLogHistory(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	if _, errStat := os.Stat(filepath.Join(logDir, "main.log")); errStat != nil {
		t.Fatalf("active log removed: %v", errStat)
	}
	if _, errStat := os.Stat(filepath.Join(logDir, "old.log")); !os.IsNotExist(errStat) {
		t.Fatalf("old log remains: %v", errStat)
	}
}

func TestPutUsageStorageConfigPersistsIndependentSettings(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("usage-statistics-enabled: true\n"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	cfg, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatal(errLoad)
	}
	handler := NewHandler(cfg, configPath, nil)
	t.Cleanup(func() { _ = handler.Shutdown(t.Context()) })

	body := []byte(`{
		"usage-statistics-persistence-enabled": false,
		"usage-statistics-persist-interval-seconds": 0,
		"usage-statistics-detail-retention-days": 14,
		"usage-statistics-max-storage-megabytes": 256
	}`)
	ctx, recorder := newStorageHistoryContext(http.MethodPut, "/v0/management/usage/storage-config", body)
	handler.PutUsageStorageConfig(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	persisted, errReload := config.LoadConfig(configPath)
	if errReload != nil {
		t.Fatal(errReload)
	}
	if persisted.UsageStatisticsPersistence() || persisted.UsageStatisticsPersistIntervalSeconds != 0 ||
		persisted.UsageStatisticsDetailRetentionDays != 14 || persisted.UsageStatisticsMaxStorageMB != 256 {
		t.Fatalf("persisted usage storage settings = %+v", persisted)
	}
}

func TestPutUsageStorageConfigRejectsNegativeValueWithoutMutation(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("usage-statistics-detail-retention-days: 7\n"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	cfg, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatal(errLoad)
	}
	handler := NewHandler(cfg, configPath, nil)
	t.Cleanup(func() { _ = handler.Shutdown(t.Context()) })

	ctx, recorder := newStorageHistoryContext(http.MethodPut, "/v0/management/usage/storage-config", []byte(`{"usage-statistics-detail-retention-days":-1}`))
	handler.PutUsageStorageConfig(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	if cfg.UsageStatisticsDetailRetentionDays != 7 {
		t.Fatalf("runtime retention mutated to %d", cfg.UsageStatisticsDetailRetentionDays)
	}
}

func newStorageHistoryContext(method, target string, body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(method, target, bytes.NewReader(body))
	if len(body) > 0 {
		ctx.Request.Header.Set("Content-Type", "application/json")
	}
	return ctx, recorder
}
