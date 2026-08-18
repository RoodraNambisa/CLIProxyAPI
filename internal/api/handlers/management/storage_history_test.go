package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var submitted usagePruneTaskSnapshot
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &submitted); errDecode != nil {
		t.Fatalf("decode submitted task: %v", errDecode)
	}
	completed := waitForUsagePruneTask(t, handler, submitted.TaskID)
	if completed.Status != "completed" || completed.Pruned != 1 || completed.DetailCountBefore != 2 || completed.DetailCountAfter != 1 {
		t.Fatalf("completed task = %+v", completed)
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

func TestUsagePruneTaskRejectsConcurrentSubmissionAndRemainsQueryable(t *testing.T) {
	root := t.TempDir()
	t.Setenv("WRITABLE_PATH", root)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: root}, nil)
	t.Cleanup(func() { _ = handler.Shutdown(t.Context()) })
	handler.SetUsageStatistics(usage.NewRequestStatistics())
	started := make(chan struct{})
	release := make(chan struct{})
	handler.usagePruneRunner = func(ctx context.Context, _ usagePrunePolicy) usagePruneOutcome {
		close(started)
		select {
		case <-release:
			return usagePruneOutcome{Processed: 12, Pruned: 7, DetailCountBefore: 12, DetailCountAfter: 5}
		case <-ctx.Done():
			return usagePruneOutcome{SafeErrorCode: "usage_prune_canceled"}
		}
	}

	body := []byte(`{"older_than_days":2}`)
	ctx, recorder := newStorageHistoryContext(http.MethodPost, "/v0/management/usage/prune", body)
	handler.PruneUsageHistory(ctx)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("first status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var submitted usagePruneTaskSnapshot
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &submitted); errDecode != nil {
		t.Fatal(errDecode)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("usage prune runner did not start")
	}

	ctx, recorder = newStorageHistoryContext(http.MethodPost, "/v0/management/usage/prune", body)
	handler.PruneUsageHistory(ctx)
	if recorder.Code != http.StatusConflict || !bytes.Contains(recorder.Body.Bytes(), []byte(submitted.TaskID)) {
		t.Fatalf("second status = %d: %s", recorder.Code, recorder.Body.String())
	}

	ctx, recorder = newStorageHistoryContext(http.MethodGet, "/v0/management/usage/prune/"+submitted.TaskID, nil)
	ctx.Params = gin.Params{{Key: "task_id", Value: submitted.TaskID}}
	handler.GetUsagePruneTask(ctx)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte(`"status":"running"`)) {
		t.Fatalf("running task = %d: %s", recorder.Code, recorder.Body.String())
	}

	ctx, recorder = newStorageHistoryContext(http.MethodGet, "/v0/management/storage/history", nil)
	handler.GetStorageHistory(ctx)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte(submitted.TaskID)) {
		t.Fatalf("history with active task = %d: %s", recorder.Code, recorder.Body.String())
	}

	close(release)
	completed := waitForUsagePruneTask(t, handler, submitted.TaskID)
	if completed.Status != "completed" || completed.Processed != 12 || completed.Pruned != 7 {
		t.Fatalf("completed task = %+v", completed)
	}
	ctx, recorder = newStorageHistoryContext(http.MethodGet, "/v0/management/storage/history", nil)
	handler.GetStorageHistory(ctx)
	if recorder.Code != http.StatusOK || !bytes.Contains(recorder.Body.Bytes(), []byte(submitted.TaskID)) || !bytes.Contains(recorder.Body.Bytes(), []byte(`"recent"`)) {
		t.Fatalf("history with recent task = %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestUsagePruneTaskShutdownCancelsManagerOwnedWork(t *testing.T) {
	manager := newUsagePruneTaskManager()
	started := make(chan struct{})
	task, active, errSubmit := manager.submit(usagePrunePolicy{OlderThanDays: 1}, func(ctx context.Context, _ usagePrunePolicy) usagePruneOutcome {
		close(started)
		<-ctx.Done()
		return usagePruneOutcome{SafeErrorCode: "usage_prune_canceled"}
	})
	if errSubmit != nil || active {
		t.Fatalf("submit active=%t error=%v", active, errSubmit)
	}
	<-started
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if errShutdown := manager.shutdown(shutdownCtx); errShutdown != nil {
		t.Fatalf("shutdown() error = %v", errShutdown)
	}
	completed, ok := manager.get(task.TaskID)
	if !ok || completed.Status != "failed" || completed.SafeErrorCode != "usage_prune_canceled" {
		t.Fatalf("canceled task = %+v, found=%t", completed, ok)
	}
	if _, _, errSubmit = manager.submit(usagePrunePolicy{OlderThanDays: 1}, func(context.Context, usagePrunePolicy) usagePruneOutcome { return usagePruneOutcome{} }); !errors.Is(errSubmit, errUsagePruneTasksClosed) {
		t.Fatalf("submit after shutdown error = %v", errSubmit)
	}
}

func TestUsagePruneTaskPanicFailsSafelyAndReleasesSlot(t *testing.T) {
	manager := newUsagePruneTaskManager()
	t.Cleanup(func() { _ = manager.shutdown(context.Background()) })
	task, active, errSubmit := manager.submit(usagePrunePolicy{OlderThanDays: 1}, func(context.Context, usagePrunePolicy) usagePruneOutcome {
		panic("sensitive runner detail")
	})
	if errSubmit != nil || active {
		t.Fatalf("submit active=%t error=%v", active, errSubmit)
	}
	var completed usagePruneTaskSnapshot
	var ok bool
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		completed, ok = manager.get(task.TaskID)
		if ok && completed.Status == "failed" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !ok || completed.Status != "failed" || completed.SafeErrorCode != "usage_prune_failed" {
		t.Fatalf("panicked task = %+v, found=%t", completed, ok)
	}
	second, active, errSubmit := manager.submit(usagePrunePolicy{OlderThanDays: 2}, func(context.Context, usagePrunePolicy) usagePruneOutcome {
		return usagePruneOutcome{}
	})
	if errSubmit != nil || active || second.TaskID == "" || second.TaskID == task.TaskID {
		t.Fatalf("second submit = %+v active=%t error=%v", second, active, errSubmit)
	}
}

func waitForUsagePruneTask(t *testing.T, handler *Handler, taskID string) usagePruneTaskSnapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if snapshot, ok := handler.usagePruneTaskManagerSnapshot().get(taskID); ok && (snapshot.Status == "completed" || snapshot.Status == "failed") {
			return snapshot
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("usage prune task %s did not complete", taskID)
	return usagePruneTaskSnapshot{}
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
