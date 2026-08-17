package usage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
)

const (
	StatisticsFileVersion    = 1
	StatisticsFileName       = "usage-statistics.snapshot"
	legacyStatisticsFileName = "usage-statistics.json"
	pendingStatisticsSuffix  = ".pending"
)

var statisticsPersistenceMu sync.Mutex

// StatisticsFilePayload is the on-disk representation used for automatic
// persistence.
type StatisticsFilePayload struct {
	Version    int                `json:"version"`
	ExportedAt time.Time          `json:"exported_at"`
	Usage      StatisticsSnapshot `json:"usage"`
}

// PersistencePolicy controls destructive retention applied immediately before
// an automatic snapshot write.
type PersistencePolicy struct {
	DetailRetentionDays int
	MaxBytes            int64
}

// PersistenceResult describes one automatic snapshot write.
type PersistenceResult struct {
	Saved        bool   `json:"saved"`
	Pruned       int    `json:"pruned"`
	SizeBytes    int64  `json:"size_bytes"`
	DetailCount  int    `json:"detail_count"`
	SnapshotPath string `json:"-"`
}

// StatisticsStorageFileSnapshot describes one known Usage snapshot file.
type StatisticsStorageFileSnapshot struct {
	Exists     bool       `json:"exists"`
	SizeBytes  int64      `json:"size_bytes"`
	ModifiedAt *time.Time `json:"modified_at,omitempty"`
}

// StatisticsStorageSnapshot is a safe aggregate view of Usage history files.
type StatisticsStorageSnapshot struct {
	Available  bool                          `json:"available"`
	TotalBytes int64                         `json:"total_bytes"`
	Main       StatisticsStorageFileSnapshot `json:"main"`
	Pending    StatisticsStorageFileSnapshot `json:"pending"`
	Legacy     StatisticsStorageFileSnapshot `json:"legacy"`
}

// StatisticsFilePath returns the default on-disk path used for automatic usage
// statistics persistence.
func StatisticsFilePath(cfg *config.Config) string {
	logDir := strings.TrimSpace(logging.ResolveLogDirectory(cfg))
	if logDir == "" {
		return StatisticsFileName
	}
	return filepath.Join(filepath.Clean(logDir), StatisticsFileName)
}

// PendingStatisticsFilePath returns the sidecar used when shutdown happens
// before the main background restore can be applied safely.
func PendingStatisticsFilePath(path string) string {
	target := strings.TrimSpace(path)
	if target == "" {
		return ""
	}
	return filepath.Clean(target) + pendingStatisticsSuffix
}

// LegacyStatisticsFilePath returns the legacy snapshot path, when applicable.
func LegacyStatisticsFilePath(path string) string {
	return legacyStatisticsFilePath(path)
}

func legacyStatisticsFilePath(path string) string {
	target := strings.TrimSpace(path)
	if target == "" {
		return ""
	}
	target = filepath.Clean(target)
	if !strings.EqualFold(filepath.Base(target), StatisticsFileName) {
		return ""
	}
	return filepath.Join(filepath.Dir(target), legacyStatisticsFileName)
}

// InspectStatisticsStorage returns file counts and sizes without exposing paths.
func InspectStatisticsStorage(path string) (StatisticsStorageSnapshot, error) {
	var snapshot StatisticsStorageSnapshot
	paths := []struct {
		path   string
		target *StatisticsStorageFileSnapshot
	}{
		{path: path, target: &snapshot.Main},
		{path: PendingStatisticsFilePath(path), target: &snapshot.Pending},
		{path: legacyStatisticsFilePath(path), target: &snapshot.Legacy},
	}
	for _, item := range paths {
		if strings.TrimSpace(item.path) == "" {
			continue
		}
		info, errStat := os.Stat(item.path)
		if errStat != nil {
			if os.IsNotExist(errStat) {
				continue
			}
			return StatisticsStorageSnapshot{}, errStat
		}
		snapshot.Available = true
		if !info.Mode().IsRegular() {
			continue
		}
		modifiedAt := info.ModTime().UTC()
		item.target.Exists = true
		item.target.SizeBytes = info.Size()
		item.target.ModifiedAt = &modifiedAt
		snapshot.TotalBytes += info.Size()
	}
	return snapshot, nil
}

// SaveSnapshotFile writes a complete statistics snapshot to disk atomically.
func SaveSnapshotFile(path string, snapshot StatisticsSnapshot) error {
	data, errMarshal := marshalSnapshotFile(snapshot)
	if errMarshal != nil {
		return errMarshal
	}
	return writeFileAtomic(path, data)
}

// LoadSnapshotFile reads a persisted snapshot from disk.
func LoadSnapshotFile(path string) (StatisticsSnapshot, error) {
	return LoadSnapshotFileContext(context.Background(), path)
}

// LoadSnapshotFileContext streams one snapshot from disk in a single JSON
// decoder pass and observes cancellation between reads.
func LoadSnapshotFileContext(ctx context.Context, path string) (StatisticsSnapshot, error) {
	var snapshot StatisticsSnapshot
	if ctx == nil {
		ctx = context.Background()
	}
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return snapshot, errOpen
	}
	defer func() { _ = file.Close() }()

	decoded := statisticsFileDecode{}
	decoder := json.NewDecoder(&contextReader{ctx: ctx, reader: file})
	if errDecode := decoder.Decode(&decoded); errDecode != nil {
		if errDecode == io.EOF {
			return snapshot, fmt.Errorf("usage: statistics file is empty")
		}
		return snapshot, fmt.Errorf("usage: decode snapshot: %w", errDecode)
	}
	if errContext := ctx.Err(); errContext != nil {
		return snapshot, errContext
	}
	var trailing json.RawMessage
	if errTrailing := decoder.Decode(&trailing); errTrailing != io.EOF {
		if errTrailing == nil {
			return snapshot, fmt.Errorf("usage: snapshot contains trailing JSON data")
		}
		return snapshot, fmt.Errorf("usage: decode trailing snapshot data: %w", errTrailing)
	}
	if decoded.Usage != nil {
		if decoded.Version != 0 && decoded.Version != StatisticsFileVersion {
			return snapshot, fmt.Errorf("usage: unsupported snapshot version %d", decoded.Version)
		}
		return *decoded.Usage, nil
	}
	return decoded.legacySnapshot(), nil
}

type statisticsFileDecode struct {
	Version        int                    `json:"version"`
	Usage          *StatisticsSnapshot    `json:"usage"`
	TotalRequests  int64                  `json:"total_requests"`
	SuccessCount   int64                  `json:"success_count"`
	FailureCount   int64                  `json:"failure_count"`
	TotalTokens    int64                  `json:"total_tokens"`
	APIs           map[string]APISnapshot `json:"apis"`
	RequestsByDay  map[string]int64       `json:"requests_by_day"`
	RequestsByHour map[string]int64       `json:"requests_by_hour"`
	TokensByDay    map[string]int64       `json:"tokens_by_day"`
	TokensByHour   map[string]int64       `json:"tokens_by_hour"`
}

func (decoded statisticsFileDecode) legacySnapshot() StatisticsSnapshot {
	return StatisticsSnapshot{
		TotalRequests:  decoded.TotalRequests,
		SuccessCount:   decoded.SuccessCount,
		FailureCount:   decoded.FailureCount,
		TotalTokens:    decoded.TotalTokens,
		APIs:           decoded.APIs,
		RequestsByDay:  decoded.RequestsByDay,
		RequestsByHour: decoded.RequestsByHour,
		TokensByDay:    decoded.TokensByDay,
		TokensByHour:   decoded.TokensByHour,
	}
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(buffer []byte) (int, error) {
	select {
	case <-reader.ctx.Done():
		return 0, reader.ctx.Err()
	default:
		return reader.reader.Read(buffer)
	}
}

// RestoreRequestStatistics merges the persisted snapshot at path into stats.
func RestoreRequestStatistics(path string, stats *RequestStatistics) (loaded bool, result MergeResult, err error) {
	if stats == nil {
		return false, result, nil
	}
	statisticsPersistenceMu.Lock()
	defer statisticsPersistenceMu.Unlock()

	snapshot, errLoad := LoadSnapshotFile(path)
	if errLoad != nil {
		if os.IsNotExist(errLoad) {
			if legacyPath := legacyStatisticsFilePath(path); legacyPath != "" {
				snapshot, errLoad = LoadSnapshotFile(legacyPath)
			}
			if os.IsNotExist(errLoad) {
				return false, result, nil
			}
		}
		if errLoad != nil {
			return false, result, errLoad
		}
	}
	result = stats.mergePersistedSnapshot(snapshot)
	return true, result, nil
}

// PrepareRequestStatistics builds persisted indexes in an isolated store so
// the live collector remains responsive. A pending shutdown sidecar is merged
// as unpersisted data and is retained until the combined main snapshot is
// durably written.
func PrepareRequestStatistics(ctx context.Context, path string) (loaded bool, prepared *RequestStatistics, result MergeResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	prepared = NewRequestStatistics()
	snapshot, errLoad := LoadSnapshotFileContext(ctx, path)
	if errLoad != nil {
		if os.IsNotExist(errLoad) {
			if legacyPath := legacyStatisticsFilePath(path); legacyPath != "" {
				snapshot, errLoad = LoadSnapshotFileContext(ctx, legacyPath)
				loaded = errLoad == nil
			}
		}
		if errLoad != nil && !os.IsNotExist(errLoad) {
			return false, nil, result, errLoad
		}
	} else {
		loaded = true
	}
	if errLoad == nil {
		var errMerge error
		result, errMerge = prepared.mergeSnapshotContext(ctx, snapshot, true, false)
		if errMerge != nil {
			return false, nil, result, errMerge
		}
	}

	pendingPath := PendingStatisticsFilePath(path)
	if pendingPath == "" {
		return loaded, prepared, result, nil
	}
	pending, errPending := LoadSnapshotFileContext(ctx, pendingPath)
	if errPending != nil {
		if os.IsNotExist(errPending) {
			return loaded, prepared, result, nil
		}
		return false, nil, result, errPending
	}
	pendingResult, errMergePending := prepared.mergeSnapshotContext(ctx, pending, false, false)
	result.Added += pendingResult.Added
	result.Skipped += pendingResult.Skipped
	if errMergePending != nil {
		return false, nil, result, errMergePending
	}
	return true, prepared, result, nil
}

// PersistRequestStatistics writes the current statistics snapshot to disk when
// there are unpersisted changes.
func PersistRequestStatistics(path string, stats *RequestStatistics) (bool, error) {
	result, errPersist := PersistRequestStatisticsWithPolicy(path, stats, PersistencePolicy{})
	return result.Saved, errPersist
}

// PersistRequestStatisticsWithPolicy applies retention and writes the current
// snapshot atomically when there are unpersisted changes.
func PersistRequestStatisticsWithPolicy(path string, stats *RequestStatistics, policy PersistencePolicy) (PersistenceResult, error) {
	if stats == nil {
		return PersistenceResult{SnapshotPath: path}, nil
	}
	statisticsPersistenceMu.Lock()
	defer statisticsPersistenceMu.Unlock()
	return persistRequestStatisticsWithPolicyLocked(path, stats, policy, false)
}

// PruneAndPersistRequestStatistics applies retention even when the current
// snapshot was previously persisted, then durably writes any changed result.
func PruneAndPersistRequestStatistics(path string, stats *RequestStatistics, policy PersistencePolicy) (PersistenceResult, error) {
	if stats == nil {
		return PersistenceResult{SnapshotPath: path}, nil
	}
	statisticsPersistenceMu.Lock()
	defer statisticsPersistenceMu.Unlock()
	return persistRequestStatisticsWithPolicyLocked(path, stats, policy, true)
}

func persistRequestStatisticsWithPolicyLocked(path string, stats *RequestStatistics, policy PersistencePolicy, inspectPersisted bool) (PersistenceResult, error) {
	result := PersistenceResult{SnapshotPath: path}

	if policy.DetailRetentionDays > 0 {
		cutoff := time.Now().UTC().AddDate(0, 0, -policy.DetailRetentionDays)
		result.Pruned += stats.PruneBefore(cutoff)
	}

	for {
		snapshot, version, persistedVersion := stats.SnapshotWithState()
		if version == persistedVersion && !inspectPersisted {
			return result, nil
		}
		data, errMarshal := marshalSnapshotFile(snapshot)
		if errMarshal != nil {
			return result, errMarshal
		}
		if policy.MaxBytes <= 0 || int64(len(data)) <= policy.MaxBytes {
			result.SizeBytes = int64(len(data))
			result.DetailCount = stats.DetailCount()
			if version == persistedVersion {
				return result, nil
			}
			if errWrite := writeFileAtomic(path, data); errWrite != nil {
				return result, errWrite
			}
			result.Saved = true
			if pendingPath := PendingStatisticsFilePath(path); pendingPath != "" {
				if errRemove := os.Remove(pendingPath); errRemove != nil && !os.IsNotExist(errRemove) {
					return result, fmt.Errorf("usage: remove pending snapshot: %w", errRemove)
				}
			}
			// Keep the state dirty until the pending sidecar is removed. If the
			// removal fails, the next persistence pass rewrites the combined main
			// snapshot and retries instead of leaving duplicate data for restart.
			stats.MarkPersisted(version)
			return result, nil
		}

		detailCount := stats.DetailCount()
		if detailCount == 0 {
			return result, fmt.Errorf("usage: snapshot metadata exceeds maximum size %d", policy.MaxBytes)
		}
		keepRatio := float64(policy.MaxBytes) / float64(len(data))
		keepCount := int(float64(detailCount) * keepRatio * 0.95)
		if keepCount < 0 {
			keepCount = 0
		}
		removeCount := detailCount - keepCount
		if removeCount < 1 {
			removeCount = 1
		}
		removed := stats.PruneOldest(removeCount)
		if removed == 0 {
			return result, fmt.Errorf("usage: unable to prune snapshot below maximum size %d", policy.MaxBytes)
		}
		result.Pruned += removed
	}
}

func persistRequestStatisticsWithSave(path string, stats *RequestStatistics, save func(string, StatisticsSnapshot) error) (bool, error) {
	if stats == nil {
		return false, nil
	}
	statisticsPersistenceMu.Lock()
	defer statisticsPersistenceMu.Unlock()

	snapshot, version, persistedVersion := stats.SnapshotWithState()
	if version == persistedVersion {
		return false, nil
	}
	if err := save(path, snapshot); err != nil {
		return false, err
	}
	stats.MarkPersisted(version)
	return true, nil
}

// ClearAndPersistRequestStatistics clears stats and synchronously writes the
// empty snapshot without allowing an older background snapshot to overwrite it.
func ClearAndPersistRequestStatistics(path string, stats *RequestStatistics) (StatisticsSnapshot, error) {
	return clearAndPersistRequestStatisticsWithSave(path, stats, SaveSnapshotFile)
}

func clearAndPersistRequestStatisticsWithSave(path string, stats *RequestStatistics, save func(string, StatisticsSnapshot) error) (StatisticsSnapshot, error) {
	return clearAndPersistRequestStatisticsWithHooks(path, stats, save, nil, nil)
}

func clearAndPersistRequestStatisticsWithHooks(
	path string,
	stats *RequestStatistics,
	save func(string, StatisticsSnapshot) error,
	beforeLock func(),
	afterLock func(),
) (StatisticsSnapshot, error) {
	if stats == nil {
		return StatisticsSnapshot{}, nil
	}
	if beforeLock != nil {
		beforeLock()
	}
	statisticsPersistenceMu.Lock()
	defer statisticsPersistenceMu.Unlock()
	if afterLock != nil {
		afterLock()
	}

	previous, empty, version := stats.clearWithState()
	if err := save(path, empty); err != nil {
		return previous, err
	}
	if pendingPath := PendingStatisticsFilePath(path); pendingPath != "" {
		if errRemove := os.Remove(pendingPath); errRemove != nil && !os.IsNotExist(errRemove) {
			return previous, fmt.Errorf("usage: remove pending snapshot: %w", errRemove)
		}
	}
	stats.MarkPersisted(version)
	return previous, nil
}

// PersistPendingRequestStatistics merges the current live window into a
// shutdown sidecar without replacing a main snapshot that has not finished
// restoring.
func PersistPendingRequestStatistics(path string, stats *RequestStatistics) error {
	if stats == nil {
		return nil
	}
	pendingPath := PendingStatisticsFilePath(path)
	if pendingPath == "" {
		return nil
	}
	statisticsPersistenceMu.Lock()
	defer statisticsPersistenceMu.Unlock()

	combined := NewRequestStatistics()
	if existing, errLoad := LoadSnapshotFile(pendingPath); errLoad == nil {
		combined.mergePersistedSnapshot(existing)
	} else if !os.IsNotExist(errLoad) {
		return errLoad
	}
	combined.MergeSnapshot(stats.Snapshot())
	return SaveSnapshotFile(pendingPath, combined.Snapshot())
}

func marshalSnapshotFile(snapshot StatisticsSnapshot) ([]byte, error) {
	payload := StatisticsFilePayload{
		Version:    StatisticsFileVersion,
		ExportedAt: time.Now().UTC(),
		Usage:      snapshot,
	}
	data, errMarshal := json.MarshalIndent(payload, "", "  ")
	if errMarshal != nil {
		return nil, fmt.Errorf("usage: marshal snapshot file: %w", errMarshal)
	}
	return append(data, '\n'), nil
}

func writeFileAtomic(path string, data []byte) error {
	target := strings.TrimSpace(path)
	if target == "" {
		return fmt.Errorf("usage: empty snapshot path")
	}
	target = filepath.Clean(target)

	dir := filepath.Dir(target)
	if errMkdir := os.MkdirAll(dir, 0o755); errMkdir != nil {
		return fmt.Errorf("usage: create snapshot directory: %w", errMkdir)
	}

	tmpFile, errCreate := os.CreateTemp(dir, "usage-statistics-*.tmp")
	if errCreate != nil {
		return fmt.Errorf("usage: create temp snapshot file: %w", errCreate)
	}

	tmpName := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
	}()

	if _, errWrite := tmpFile.Write(data); errWrite != nil {
		return fmt.Errorf("usage: write temp snapshot file: %w", errWrite)
	}
	if errSync := tmpFile.Sync(); errSync != nil {
		return fmt.Errorf("usage: sync temp snapshot file: %w", errSync)
	}
	if errClose := tmpFile.Close(); errClose != nil {
		return fmt.Errorf("usage: close temp snapshot file: %w", errClose)
	}
	if errRename := os.Rename(tmpName, target); errRename != nil {
		return fmt.Errorf("usage: rename snapshot file: %w", errRename)
	}

	if dirHandle, errOpenDir := os.Open(dir); errOpenDir == nil {
		_ = dirHandle.Sync()
		_ = dirHandle.Close()
	}

	return nil
}
