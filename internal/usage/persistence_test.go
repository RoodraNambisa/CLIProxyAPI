package usage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

func TestVersion2SnapshotUsesRecordStreamAndRequiresFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	stats := NewRequestStatistics()
	now := time.Now().UTC()
	usageTestRecord(stats, "first", now.Add(-time.Minute), 1)
	usageTestRecord(stats, "second", now, 2)
	if _, errPersist := RewriteRequestStatisticsWithPolicy(path, stats, PersistencePolicy{}); errPersist != nil {
		t.Fatalf("RewriteRequestStatisticsWithPolicy() error = %v", errPersist)
	}

	file, errOpen := os.Open(path)
	if errOpen != nil {
		t.Fatalf("Open() error = %v", errOpen)
	}
	scanner := bufio.NewScanner(file)
	kinds := make([]string, 0, 4)
	for scanner.Scan() {
		var envelope statisticsStreamEnvelope
		if errDecode := json.Unmarshal(scanner.Bytes(), &envelope); errDecode != nil {
			t.Fatalf("decode stream line: %v", errDecode)
		}
		kinds = append(kinds, envelope.Kind)
		if len(kinds) == 1 && envelope.Version != StatisticsFileVersion {
			t.Fatalf("header version = %d, want %d", envelope.Version, StatisticsFileVersion)
		}
	}
	_ = file.Close()
	if errScan := scanner.Err(); errScan != nil {
		t.Fatalf("scan stream: %v", errScan)
	}
	if got := strings.Join(kinds, ","); got != "header,request,request,footer" {
		t.Fatalf("record kinds = %q", got)
	}

	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("ReadFile() error = %v", errRead)
	}
	footerAt := strings.LastIndex(string(data), `{"kind":"footer"`)
	if footerAt < 0 {
		t.Fatal("footer was not found")
	}
	if errWrite := os.WriteFile(path, data[:footerAt], 0o600); errWrite != nil {
		t.Fatalf("truncate stream: %v", errWrite)
	}
	if _, _, _, errPrepare := PrepareRequestStatistics(context.Background(), path); errPrepare == nil || !strings.Contains(errPrepare.Error(), "truncated") {
		t.Fatalf("truncated stream error = %v", errPrepare)
	}
	unsupportedHeader, errMarshal := marshalStatisticsStreamLine(statisticsStreamEnvelope{Kind: "header", Version: StatisticsFileVersion + 1})
	if errMarshal != nil {
		t.Fatalf("marshal unsupported header: %v", errMarshal)
	}
	if errWrite := os.WriteFile(path, append(unsupportedHeader, []byte("{\"kind\":\"footer\"}\n")...), 0o600); errWrite != nil {
		t.Fatalf("write unsupported stream: %v", errWrite)
	}
	if _, _, _, errPrepare := PrepareRequestStatistics(context.Background(), path); errPrepare == nil || !strings.Contains(errPrepare.Error(), "unsupported snapshot version") {
		t.Fatalf("unsupported stream error = %v", errPrepare)
	}
}

func TestVersion2RestoreFiltersExpiredRecordsBeforeInsertion(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	source := NewRequestStatistics()
	now := time.Now().UTC()
	usageTestRecord(source, "expired", now.Add(-72*time.Hour), 1)
	usageTestRecord(source, "retained", now.Add(-time.Hour), 2)
	if _, errPersist := RewriteRequestStatisticsWithPolicy(path, source, PersistencePolicy{}); errPersist != nil {
		t.Fatalf("persist source: %v", errPersist)
	}

	loaded, preparation, result, errPrepare := PrepareRequestStatisticsWithPolicy(context.Background(), path, PersistencePolicy{DetailRetentionDays: 1})
	if errPrepare != nil || !loaded {
		t.Fatalf("PrepareRequestStatisticsWithPolicy() loaded=%t error=%v", loaded, errPrepare)
	}
	if result.Added != 1 || result.Skipped != 1 {
		t.Fatalf("restore result = %+v", result)
	}
	if meta := preparation.Statistics.Meta(); meta.TotalRequests != 1 || meta.TotalTokens != 2 {
		t.Fatalf("retained meta = %+v", meta)
	}
}

func TestVersion2SpoolKeepsCapturedHighWaterAcrossIndexReorder(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	stats := NewRequestStatistics()
	base := time.Now().UTC().Add(-time.Hour)
	for index := 0; index < statisticsStreamBatchSize+37; index++ {
		usageTestRecord(stats, fmt.Sprintf("captured-%04d", index), base.Add(time.Duration(index)*time.Second), int64(index+1))
	}
	spool, errSpool := spoolRequestStatisticsWithCapture(path, stats, func() {
		usageTestRecord(stats, "after-high-water", base.Add(-time.Hour), 999)
		stats.mu.Lock()
		stats.sortDetailIndexLocked()
		stats.mu.Unlock()
	})
	if errSpool != nil {
		t.Fatalf("spoolRequestStatisticsWithCapture() error = %v", errSpool)
	}
	defer spool.closeAndRemove()
	if spool.records != statisticsStreamBatchSize+37 {
		t.Fatalf("spooled records = %d, want %d", spool.records, statisticsStreamBatchSize+37)
	}
	if spool.version >= stats.changeCount {
		t.Fatalf("captured version = %d, current = %d", spool.version, stats.changeCount)
	}
	if _, errSeek := spool.file.Seek(0, io.SeekStart); errSeek != nil {
		t.Fatalf("Seek() error = %v", errSeek)
	}
	seen := make(map[string]int)
	decoder := json.NewDecoder(spool.file)
	for {
		var envelope statisticsStreamEnvelope
		errDecode := decoder.Decode(&envelope)
		if errDecode == io.EOF {
			break
		}
		if errDecode != nil {
			t.Fatalf("decode spooled record: %v", errDecode)
		}
		seen[envelope.Detail.Source]++
	}
	if seen["after-high-water"] != 0 {
		t.Fatal("record created after the captured high-water entered the snapshot")
	}
	for index := 0; index < statisticsStreamBatchSize+37; index++ {
		key := fmt.Sprintf("captured-%04d", index)
		if seen[key] != 1 {
			t.Fatalf("spooled occurrences for %s = %d, want 1", key, seen[key])
		}
	}
	trim, errTrim := calculateStatisticsStreamTrim(spool, 0)
	if errTrim != nil {
		t.Fatalf("calculateStatisticsStreamTrim() error = %v", errTrim)
	}
	if errInstall := installStatisticsStream(path, spool, trim); errInstall != nil {
		t.Fatalf("installStatisticsStream() error = %v", errInstall)
	}
	stats.MarkPersisted(spool.version)
	if !stats.HasPendingPersistence() {
		t.Fatal("record created after the captured high-water was marked persisted")
	}
	firstSnapshot, errLoad := LoadSnapshotFile(path)
	if errLoad != nil {
		t.Fatalf("LoadSnapshotFile(first) error = %v", errLoad)
	}
	if firstSnapshot.TotalRequests != int64(statisticsStreamBatchSize+37) {
		t.Fatalf("first persisted requests = %d, want %d", firstSnapshot.TotalRequests, statisticsStreamBatchSize+37)
	}
	if _, errPersist := PersistRequestStatisticsWithPolicy(path, stats, PersistencePolicy{}); errPersist != nil {
		t.Fatalf("PersistRequestStatisticsWithPolicy() error = %v", errPersist)
	}
	secondSnapshot, errLoad := LoadSnapshotFile(path)
	if errLoad != nil {
		t.Fatalf("LoadSnapshotFile(second) error = %v", errLoad)
	}
	if secondSnapshot.TotalRequests != int64(statisticsStreamBatchSize+38) {
		t.Fatalf("second persisted requests = %d, want %d", secondSnapshot.TotalRequests, statisticsStreamBatchSize+38)
	}
}

func TestLegacySnapshotMigratesOnlyAfterPreparedRestoreApplies(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	legacyStats := NewRequestStatistics()
	usageTestRecord(legacyStats, "legacy", time.Now().UTC().Add(-time.Hour), 9)
	legacyData, errMarshal := json.Marshal(StatisticsFilePayload{
		Version:    legacyStatisticsVersion,
		ExportedAt: time.Now().UTC(),
		Usage:      legacyStats.Snapshot(),
	})
	if errMarshal != nil {
		t.Fatalf("marshal legacy snapshot: %v", errMarshal)
	}
	if errWrite := os.WriteFile(path, legacyData, 0o600); errWrite != nil {
		t.Fatalf("write legacy snapshot: %v", errWrite)
	}

	loaded, preparation, _, errPrepare := PrepareRequestStatisticsWithPolicy(context.Background(), path, PersistencePolicy{})
	if errPrepare != nil || !loaded || !preparation.NeedsMigration {
		t.Fatalf("legacy preparation loaded=%t migration=%t error=%v", loaded, preparation.NeedsMigration, errPrepare)
	}
	target := NewRequestStatistics()
	if _, applied := target.ApplyPreparedRestore(preparation.Statistics, target.HistoryGeneration()); !applied {
		t.Fatal("legacy preparation was not applied")
	}
	if _, errRewrite := RewriteRequestStatisticsWithPolicy(path, target, PersistencePolicy{}); errRewrite != nil {
		t.Fatalf("rewrite migrated snapshot: %v", errRewrite)
	}
	file, errOpen := os.Open(path)
	if errOpen != nil {
		t.Fatalf("open migrated snapshot: %v", errOpen)
	}
	defer file.Close()
	var header statisticsStreamEnvelope
	if errDecode := json.NewDecoder(file).Decode(&header); errDecode != nil {
		t.Fatalf("decode migrated header: %v", errDecode)
	}
	if header.Kind != "header" || header.Version != StatisticsFileVersion {
		t.Fatalf("migrated header = %+v", header)
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

func TestClearAndPersistRequestStatisticsMetaKeepsOnlyAggregateResponse(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	stats := NewRequestStatistics()
	now := time.Now().UTC()
	for index := 0; index < 100; index++ {
		usageTestRecord(stats, fmt.Sprintf("clear-%03d", index), now.Add(time.Duration(index)*time.Second), 1)
	}
	if _, errPersist := PersistRequestStatisticsWithPolicy(path, stats, PersistencePolicy{}); errPersist != nil {
		t.Fatalf("persist before clear: %v", errPersist)
	}
	previous, errClear := ClearAndPersistRequestStatisticsMeta(path, stats)
	if errClear != nil {
		t.Fatalf("ClearAndPersistRequestStatisticsMeta() error = %v", errClear)
	}
	if previous.TotalRequests != 100 || previous.TotalTokens != 100 || stats.DetailCount() != 0 {
		t.Fatalf("clear meta = %+v, remaining=%d", previous, stats.DetailCount())
	}
	loaded, errLoad := LoadSnapshotFile(path)
	if errLoad != nil {
		t.Fatalf("load cleared snapshot: %v", errLoad)
	}
	if loaded.TotalRequests != 0 || snapshotDetailCount(loaded) != 0 {
		t.Fatalf("cleared disk snapshot = %+v", loaded)
	}
}

func TestPendingSnapshotRemovalFailureFallsBackToEmptyAtomicSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), StatisticsFileName)
	pendingPath := PendingStatisticsFilePath(path)
	pending := NewRequestStatistics()
	usageTestRecord(pending, "pending-old", time.Now().UTC(), 9)
	if errSave := SaveSnapshotFile(pendingPath, pending.Snapshot()); errSave != nil {
		t.Fatalf("save pending snapshot: %v", errSave)
	}
	forcedRemoveError := errors.New("forced unlink failure")
	errClear := removeOrClearPendingStatisticsFileWith(path, func(string) error {
		return forcedRemoveError
	}, SaveSnapshotFile)
	if errClear != nil {
		t.Fatalf("removeOrClearPendingStatisticsFileWith() error = %v", errClear)
	}
	cleared, errLoad := LoadSnapshotFile(pendingPath)
	if errLoad != nil {
		t.Fatalf("load fallback pending snapshot: %v", errLoad)
	}
	if cleared.TotalRequests != 0 || snapshotDetailCount(cleared) != 0 {
		t.Fatalf("fallback pending snapshot = %+v", cleared)
	}
}
