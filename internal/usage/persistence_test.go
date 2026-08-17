package usage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func usageTestRecord(stats *RequestStatistics, key string, timestamp time.Time, tokens int64) {
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: timestamp,
		Source:      key,
		Detail:      coreusage.Detail{TotalTokens: tokens},
	})
}

func TestLoadSnapshotFileContextHonorsCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	if errSave := SaveSnapshotFile(path, StatisticsSnapshot{}); errSave != nil {
		t.Fatalf("SaveSnapshotFile() error = %v", errSave)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, errLoad := LoadSnapshotFileContext(ctx, path); !errors.Is(errLoad, context.Canceled) {
		t.Fatalf("LoadSnapshotFileContext() error = %v, want context canceled", errLoad)
	}
}

func TestLoadSnapshotFileContextSupportsLegacySnapshotAndRejectsTrailingData(t *testing.T) {
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, legacyStatisticsFileName)
	legacy := StatisticsSnapshot{
		TotalRequests: 3,
		SuccessCount:  2,
		FailureCount:  1,
		TotalTokens:   17,
		APIs:          map[string]APISnapshot{},
	}
	data, errMarshal := json.Marshal(legacy)
	if errMarshal != nil {
		t.Fatalf("Marshal() error = %v", errMarshal)
	}
	if errWrite := os.WriteFile(legacyPath, data, 0o600); errWrite != nil {
		t.Fatalf("WriteFile() error = %v", errWrite)
	}
	loaded, errLoad := LoadSnapshotFileContext(context.Background(), legacyPath)
	if errLoad != nil {
		t.Fatalf("LoadSnapshotFileContext() error = %v", errLoad)
	}
	if loaded.TotalRequests != 3 || loaded.TotalTokens != 17 || loaded.FailureCount != 1 {
		t.Fatalf("legacy snapshot = %+v", loaded)
	}

	if errWrite := os.WriteFile(legacyPath, append(data, []byte("\n{}")...), 0o600); errWrite != nil {
		t.Fatalf("WriteFile(trailing) error = %v", errWrite)
	}
	if _, errLoad = LoadSnapshotFileContext(context.Background(), legacyPath); errLoad == nil || !strings.Contains(errLoad.Error(), "trailing") {
		t.Fatalf("trailing data error = %v", errLoad)
	}
}

func TestPreparedRestorePreservesLiveWindowAndRejectsLateResurrection(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	historical := NewRequestStatistics()
	usageTestRecord(historical, "historical", time.Date(2026, time.August, 1, 1, 0, 0, 0, time.UTC), 10)
	if errSave := SaveSnapshotFile(path, historical.Snapshot()); errSave != nil {
		t.Fatalf("SaveSnapshotFile() error = %v", errSave)
	}

	live := NewRequestStatistics()
	expectedGeneration := live.HistoryGeneration()
	loaded, prepared, _, errPrepare := PrepareRequestStatistics(context.Background(), path)
	if errPrepare != nil || !loaded {
		t.Fatalf("PrepareRequestStatistics() loaded=%t error=%v", loaded, errPrepare)
	}
	usageTestRecord(live, "live", time.Date(2026, time.August, 17, 1, 0, 0, 0, time.UTC), 20)
	if _, applied := live.ApplyPreparedRestore(prepared, expectedGeneration); !applied {
		t.Fatal("prepared restore was not applied")
	}
	meta := live.Meta()
	if meta.TotalRequests != 2 || meta.TotalTokens != 30 {
		t.Fatalf("merged live meta = %+v", meta)
	}

	staleTarget := NewRequestStatistics()
	staleGeneration := staleTarget.HistoryGeneration()
	staleTarget.Clear()
	if _, applied := staleTarget.ApplyPreparedRestore(prepared, staleGeneration); applied {
		t.Fatal("restore resurrected history after Clear")
	}
	if got := staleTarget.Meta().TotalRequests; got != 0 {
		t.Fatalf("requests after rejected restore = %d, want 0", got)
	}

	importTarget := NewRequestStatistics()
	importGeneration := importTarget.HistoryGeneration()
	imported := NewRequestStatistics()
	usageTestRecord(imported, "manual-import", time.Date(2026, time.August, 16, 1, 0, 0, 0, time.UTC), 7)
	if merge := importTarget.MergeSnapshot(imported.Snapshot()); merge.Added != 1 {
		t.Fatalf("manual merge = %+v", merge)
	}
	if _, applied := importTarget.ApplyPreparedRestore(prepared, importGeneration); applied {
		t.Fatal("restore replaced a newer manual import")
	}
	if got := importTarget.Meta().TotalTokens; got != 7 {
		t.Fatalf("manual import tokens after rejected restore = %d, want 7", got)
	}
}

func TestUsageRetentionPrunesTimeAndExactSnapshotSize(t *testing.T) {
	stats := NewRequestStatistics()
	now := time.Now().UTC()
	usageTestRecord(stats, "old", now.Add(-72*time.Hour), 1)
	usageTestRecord(stats, "new", now.Add(-time.Hour), 2)
	if removed := stats.PruneBefore(now.Add(-24 * time.Hour)); removed != 1 {
		t.Fatalf("PruneBefore() = %d, want 1", removed)
	}
	if meta := stats.Meta(); meta.TotalRequests != 1 || meta.TotalTokens != 2 {
		t.Fatalf("retained meta = %+v", meta)
	}

	for index := 0; index < 80; index++ {
		usageTestRecord(stats, strings.Repeat("x", 512), now.Add(time.Duration(index)*time.Second), int64(index+1))
	}
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	const maxBytes = int64(8 * 1024)
	result, errPersist := PersistRequestStatisticsWithPolicy(path, stats, PersistencePolicy{MaxBytes: maxBytes})
	if errPersist != nil {
		t.Fatalf("PersistRequestStatisticsWithPolicy() error = %v", errPersist)
	}
	if !result.Saved || result.Pruned == 0 || result.SizeBytes > maxBytes {
		t.Fatalf("persistence result = %+v", result)
	}
	info, errStat := os.Stat(path)
	if errStat != nil {
		t.Fatalf("Stat() error = %v", errStat)
	}
	if info.Size() > maxBytes {
		t.Fatalf("snapshot size=%d, want <=%d", info.Size(), maxBytes)
	}
	loaded, errLoad := LoadSnapshotFile(path)
	if errLoad != nil {
		t.Fatalf("LoadSnapshotFile() error = %v", errLoad)
	}
	if loaded.TotalRequests != stats.Meta().TotalRequests {
		t.Fatalf("disk requests=%d live requests=%d", loaded.TotalRequests, stats.Meta().TotalRequests)
	}
}

func TestPendingUsageSidecarSurvivesInterruptedRestore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, StatisticsFileName)
	mainStats := NewRequestStatistics()
	usageTestRecord(mainStats, "main", time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC), 1)
	if errSave := SaveSnapshotFile(path, mainStats.Snapshot()); errSave != nil {
		t.Fatalf("SaveSnapshotFile() error = %v", errSave)
	}
	live := NewRequestStatistics()
	usageTestRecord(live, "pending", time.Date(2026, time.August, 17, 0, 0, 0, 0, time.UTC), 2)
	if errPending := PersistPendingRequestStatistics(path, live); errPending != nil {
		t.Fatalf("PersistPendingRequestStatistics() error = %v", errPending)
	}

	loaded, prepared, _, errPrepare := PrepareRequestStatistics(context.Background(), path)
	if errPrepare != nil || !loaded {
		t.Fatalf("PrepareRequestStatistics() loaded=%t error=%v", loaded, errPrepare)
	}
	target := NewRequestStatistics()
	if _, applied := target.ApplyPreparedRestore(prepared, target.HistoryGeneration()); !applied {
		t.Fatal("combined restore was not applied")
	}
	if meta := target.Meta(); meta.TotalRequests != 2 || meta.TotalTokens != 3 {
		t.Fatalf("combined meta = %+v", meta)
	}
	if _, errPersist := PersistRequestStatisticsWithPolicy(path, target, PersistencePolicy{}); errPersist != nil {
		t.Fatalf("PersistRequestStatisticsWithPolicy() error = %v", errPersist)
	}
	if _, errStat := os.Stat(PendingStatisticsFilePath(path)); !os.IsNotExist(errStat) {
		t.Fatalf("pending sidecar remains after durable merge, stat error=%v", errStat)
	}
}
