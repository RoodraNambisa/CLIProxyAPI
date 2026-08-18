package usage

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
)

const (
	StatisticsFileVersion    = 2
	legacyStatisticsVersion  = 1
	StatisticsFileName       = "usage-statistics.snapshot"
	legacyStatisticsFileName = "usage-statistics.json"
	pendingStatisticsSuffix  = ".pending"
)

var statisticsPersistenceMu sync.Mutex

// StatisticsFilePayload is the legacy version 1 representation retained for
// compatibility imports and one-time migration.
type StatisticsFilePayload struct {
	Version    int                `json:"version"`
	ExportedAt time.Time          `json:"exported_at"`
	Usage      StatisticsSnapshot `json:"usage"`
}

const statisticsStreamBatchSize = 512

type statisticsStreamEnvelope struct {
	Kind          string         `json:"kind"`
	Version       int            `json:"version,omitempty"`
	ExportedAt    time.Time      `json:"exported_at,omitempty"`
	TotalRequests int64          `json:"total_requests,omitempty"`
	SuccessCount  int64          `json:"success_count,omitempty"`
	FailureCount  int64          `json:"failure_count,omitempty"`
	TotalTokens   int64          `json:"total_tokens,omitempty"`
	API           string         `json:"api,omitempty"`
	Model         string         `json:"model,omitempty"`
	Detail        *RequestDetail `json:"detail,omitempty"`
	Records       int64          `json:"records,omitempty"`
}

type persistedUsageRecord struct {
	API    string
	Model  string
	Detail RequestDetail
}

// PreparedStatisticsRestore contains an isolated restored store and migration
// metadata. Migration paths are internal filesystem details and must never be
// exposed through management responses.
type PreparedStatisticsRestore struct {
	Statistics      *RequestStatistics
	NeedsMigration  bool
	MigrationSource string
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
	if snapshotDetailCount(snapshot) == 0 && (snapshot.TotalRequests != 0 || snapshot.SuccessCount != 0 || snapshot.FailureCount != 0 || snapshot.TotalTokens != 0) {
		header, errHeader := marshalStatisticsStreamLine(statisticsStreamEnvelope{
			Kind:          "header",
			Version:       StatisticsFileVersion,
			ExportedAt:    time.Now().UTC(),
			TotalRequests: snapshot.TotalRequests,
			SuccessCount:  snapshot.SuccessCount,
			FailureCount:  snapshot.FailureCount,
			TotalTokens:   snapshot.TotalTokens,
		})
		if errHeader != nil {
			return errHeader
		}
		footer, errFooter := marshalStatisticsStreamLine(statisticsStreamEnvelope{Kind: "footer"})
		if errFooter != nil {
			return errFooter
		}
		return writeFileAtomic(path, append(header, footer...))
	}
	stats := NewRequestStatistics()
	if _, errMerge := stats.mergeSnapshotContext(context.Background(), snapshot, false, false); errMerge != nil {
		return errMerge
	}
	_, errWrite := rewriteRequestStatisticsWithPolicyLocked(path, stats, PersistencePolicy{})
	return errWrite
}

// LoadSnapshotFile reads a persisted snapshot from disk.
func LoadSnapshotFile(path string) (StatisticsSnapshot, error) {
	return LoadSnapshotFileContext(context.Background(), path)
}

// LoadSnapshotFileContext loads one snapshot for compatibility callers and
// observes cancellation between reads. Automatic version 2 restore uses the
// record-level path below instead of constructing this exported snapshot.
func LoadSnapshotFileContext(ctx context.Context, path string) (StatisticsSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return StatisticsSnapshot{}, errOpen
	}
	decoder := json.NewDecoder(&contextReader{ctx: ctx, reader: file})
	var first json.RawMessage
	errFirst := decoder.Decode(&first)
	_ = file.Close()
	if errFirst != nil {
		if errFirst == io.EOF {
			return StatisticsSnapshot{}, fmt.Errorf("usage: statistics file is empty")
		}
		return StatisticsSnapshot{}, fmt.Errorf("usage: decode snapshot: %w", errFirst)
	}
	var probe struct {
		Kind    string `json:"kind"`
		Version int    `json:"version"`
	}
	if errProbe := json.Unmarshal(first, &probe); errProbe != nil {
		return StatisticsSnapshot{}, fmt.Errorf("usage: decode snapshot header: %w", errProbe)
	}
	if probe.Kind == "header" && probe.Version != StatisticsFileVersion {
		return StatisticsSnapshot{}, fmt.Errorf("usage: unsupported snapshot version %d", probe.Version)
	}
	if probe.Version != StatisticsFileVersion || probe.Kind != "header" {
		decoded := statisticsFileDecode{}
		if errDecode := json.Unmarshal(first, &decoded); errDecode != nil {
			return StatisticsSnapshot{}, fmt.Errorf("usage: decode legacy snapshot: %w", errDecode)
		}
		if decoded.Version != 0 && decoded.Version != legacyStatisticsVersion {
			return StatisticsSnapshot{}, fmt.Errorf("usage: unsupported snapshot version %d", decoded.Version)
		}
		fileTrailing, errReopen := os.Open(path)
		if errReopen != nil {
			return StatisticsSnapshot{}, errReopen
		}
		defer func() { _ = fileTrailing.Close() }()
		trailingDecoder := json.NewDecoder(&contextReader{ctx: ctx, reader: fileTrailing})
		var ignored json.RawMessage
		if errDecode := trailingDecoder.Decode(&ignored); errDecode != nil {
			return StatisticsSnapshot{}, fmt.Errorf("usage: decode legacy snapshot: %w", errDecode)
		}
		var trailing json.RawMessage
		if errTrailing := trailingDecoder.Decode(&trailing); errTrailing != io.EOF {
			if errTrailing == nil {
				return StatisticsSnapshot{}, fmt.Errorf("usage: snapshot contains trailing JSON data")
			}
			return StatisticsSnapshot{}, fmt.Errorf("usage: decode trailing snapshot data: %w", errTrailing)
		}
		if decoded.Usage != nil {
			return *decoded.Usage, nil
		}
		return decoded.legacySnapshot(), nil
	}

	stats := NewRequestStatistics()
	_, _, errLoad := loadStatisticsFileInto(ctx, path, stats, time.Time{}, false)
	if errLoad != nil {
		return StatisticsSnapshot{}, errLoad
	}
	snapshot := stats.Snapshot()
	if snapshot.TotalRequests == 0 && snapshot.TotalTokens == 0 {
		header := statisticsStreamEnvelope{}
		if errHeader := json.Unmarshal(first, &header); errHeader == nil {
			snapshot.TotalRequests = header.TotalRequests
			snapshot.SuccessCount = header.SuccessCount
			snapshot.FailureCount = header.FailureCount
			snapshot.TotalTokens = header.TotalTokens
		}
	}
	return snapshot, nil
}

func snapshotDetailCount(snapshot StatisticsSnapshot) int {
	count := 0
	for _, apiSnapshot := range snapshot.APIs {
		for _, modelSnapshot := range apiSnapshot.Models {
			count += len(modelSnapshot.Details)
		}
	}
	return count
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

func loadStatisticsFileInto(ctx context.Context, path string, stats *RequestStatistics, cutoff time.Time, markPersisted bool) (int, MergeResult, error) {
	result := MergeResult{}
	if ctx == nil {
		ctx = context.Background()
	}
	if stats == nil {
		return 0, result, fmt.Errorf("usage: nil statistics restore target")
	}
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return 0, result, errOpen
	}
	defer func() { _ = file.Close() }()

	decoder := json.NewDecoder(&contextReader{ctx: ctx, reader: file})
	var first json.RawMessage
	if errDecode := decoder.Decode(&first); errDecode != nil {
		if errDecode == io.EOF {
			return 0, result, fmt.Errorf("usage: statistics file is empty")
		}
		return 0, result, fmt.Errorf("usage: decode snapshot: %w", errDecode)
	}
	var probe struct {
		Kind    string `json:"kind"`
		Version int    `json:"version"`
	}
	if errProbe := json.Unmarshal(first, &probe); errProbe != nil {
		return 0, result, fmt.Errorf("usage: decode snapshot header: %w", errProbe)
	}
	if probe.Kind == "header" && probe.Version != StatisticsFileVersion {
		return probe.Version, result, fmt.Errorf("usage: unsupported snapshot version %d", probe.Version)
	}
	if probe.Version == StatisticsFileVersion && probe.Kind == "header" {
		streamResult, errStream := decodeStatisticsStream(ctx, decoder, stats, cutoff, markPersisted)
		return StatisticsFileVersion, streamResult, errStream
	}

	decoded := statisticsFileDecode{}
	if errDecode := json.Unmarshal(first, &decoded); errDecode != nil {
		return 0, result, fmt.Errorf("usage: decode legacy snapshot: %w", errDecode)
	}
	if decoded.Version != 0 && decoded.Version != legacyStatisticsVersion {
		return decoded.Version, result, fmt.Errorf("usage: unsupported snapshot version %d", decoded.Version)
	}
	var trailing json.RawMessage
	if errTrailing := decoder.Decode(&trailing); errTrailing != io.EOF {
		if errTrailing == nil {
			return legacyStatisticsVersion, result, fmt.Errorf("usage: snapshot contains trailing JSON data")
		}
		return legacyStatisticsVersion, result, fmt.Errorf("usage: decode trailing snapshot data: %w", errTrailing)
	}
	snapshot := decoded.legacySnapshot()
	if decoded.Usage != nil {
		snapshot = *decoded.Usage
	}
	legacyResult, errMerge := mergePersistedSnapshotRecords(ctx, stats, snapshot, cutoff)
	if errMerge == nil && markPersisted {
		stats.MarkAllPersisted()
	}
	return legacyStatisticsVersion, legacyResult, errMerge
}

func decodeStatisticsStream(ctx context.Context, decoder *json.Decoder, stats *RequestStatistics, cutoff time.Time, markPersisted bool) (MergeResult, error) {
	result := MergeResult{}
	batch := make([]persistedUsageRecord, 0, statisticsStreamBatchSize)
	var seen int64
	footerSeen := false
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		added, errInsert := insertPersistedUsageBatch(ctx, stats, batch)
		result.Added += added
		batch = batch[:0]
		return errInsert
	}
	for {
		var envelope statisticsStreamEnvelope
		errDecode := decoder.Decode(&envelope)
		if errDecode == io.EOF {
			if !footerSeen {
				return result, fmt.Errorf("usage: truncated version 2 snapshot")
			}
			if errFlush := flush(); errFlush != nil {
				return result, errFlush
			}
			if markPersisted {
				stats.MarkAllPersisted()
			}
			return result, nil
		}
		if errDecode != nil {
			return result, fmt.Errorf("usage: decode version 2 snapshot: %w", errDecode)
		}
		if footerSeen {
			return result, fmt.Errorf("usage: version 2 snapshot contains data after footer")
		}
		switch envelope.Kind {
		case "request":
			if envelope.Detail == nil {
				return result, fmt.Errorf("usage: version 2 request record is missing detail")
			}
			seen++
			if !cutoff.IsZero() && !envelope.Detail.Timestamp.IsZero() && envelope.Detail.Timestamp.Before(cutoff) {
				result.Skipped++
				continue
			}
			batch = append(batch, persistedUsageRecord{API: envelope.API, Model: envelope.Model, Detail: *envelope.Detail})
			if len(batch) >= statisticsStreamBatchSize {
				if errFlush := flush(); errFlush != nil {
					return result, errFlush
				}
			}
		case "footer":
			if envelope.Records != seen {
				return result, fmt.Errorf("usage: version 2 snapshot record count mismatch")
			}
			footerSeen = true
		default:
			return result, fmt.Errorf("usage: unsupported version 2 record kind %q", envelope.Kind)
		}
	}
}

func mergePersistedSnapshotRecords(ctx context.Context, stats *RequestStatistics, snapshot StatisticsSnapshot, cutoff time.Time) (MergeResult, error) {
	result := MergeResult{}
	batch := make([]persistedUsageRecord, 0, statisticsStreamBatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		added, errInsert := insertPersistedUsageBatch(ctx, stats, batch)
		result.Added += added
		batch = batch[:0]
		return errInsert
	}
	for apiName, apiSnapshot := range snapshot.APIs {
		for modelName, modelSnapshot := range apiSnapshot.Models {
			for _, detail := range modelSnapshot.Details {
				if !cutoff.IsZero() && !detail.Timestamp.IsZero() && detail.Timestamp.Before(cutoff) {
					result.Skipped++
					continue
				}
				batch = append(batch, persistedUsageRecord{API: apiName, Model: modelName, Detail: detail})
				if len(batch) >= statisticsStreamBatchSize {
					if errFlush := flush(); errFlush != nil {
						return result, errFlush
					}
				}
			}
		}
	}
	if errFlush := flush(); errFlush != nil {
		return result, errFlush
	}
	return result, nil
}

func insertPersistedUsageBatch(ctx context.Context, stats *RequestStatistics, records []persistedUsageRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	now := time.Now().UTC()
	stats.mu.Lock()
	defer stats.mu.Unlock()
	var added int64
	for index := range records {
		if index%128 == 0 {
			select {
			case <-ctx.Done():
				return added, ctx.Err()
			default:
			}
		}
		record := records[index]
		apiName := strings.TrimSpace(record.API)
		if apiName == "" {
			apiName = "unknown"
		}
		modelName := strings.TrimSpace(record.Model)
		if modelName == "" {
			modelName = "unknown"
		}
		detail := record.Detail
		detail.Tokens = normaliseTokenStats(detail.Tokens)
		if detail.LatencyMs < 0 {
			detail.LatencyMs = 0
		}
		if detail.Timestamp.IsZero() {
			detail.Timestamp = now
		}
		apiValue := stats.apis[apiName]
		if apiValue == nil {
			apiValue = &apiStats{Models: make(map[string]*modelStats)}
			stats.apis[apiName] = apiValue
		}
		stats.recordImported(apiName, modelName, apiValue, detail, now)
		added++
	}
	return added, nil
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
	loaded, preparation, result, err := PrepareRequestStatisticsWithPolicy(ctx, path, PersistencePolicy{})
	return loaded, preparation.Statistics, result, err
}

// PrepareRequestStatisticsWithPolicy restores automatic snapshots directly
// into an isolated store. Version 2 records are decoded and inserted in fixed
// batches; legacy version 1 remains a one-time full-object compatibility path.
func PrepareRequestStatisticsWithPolicy(ctx context.Context, path string, policy PersistencePolicy) (loaded bool, preparation PreparedStatisticsRestore, result MergeResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	prepared := NewRequestStatistics()
	preparation.Statistics = prepared
	cutoff := time.Time{}
	if policy.DetailRetentionDays > 0 {
		cutoff = time.Now().UTC().AddDate(0, 0, -policy.DetailRetentionDays)
	}
	version, mainResult, errLoad := loadStatisticsFileInto(ctx, path, prepared, cutoff, true)
	if errLoad != nil {
		if os.IsNotExist(errLoad) {
			if legacyPath := legacyStatisticsFilePath(path); legacyPath != "" {
				version, mainResult, errLoad = loadStatisticsFileInto(ctx, legacyPath, prepared, cutoff, true)
				loaded = errLoad == nil
				if loaded {
					preparation.MigrationSource = legacyPath
				}
			}
		}
		if errLoad != nil && !os.IsNotExist(errLoad) {
			return false, PreparedStatisticsRestore{}, result, errLoad
		}
	} else {
		loaded = true
	}
	if errLoad == nil {
		result = mainResult
		preparation.NeedsMigration = version < StatisticsFileVersion
	}

	pendingPath := PendingStatisticsFilePath(path)
	if pendingPath == "" {
		return loaded, preparation, result, nil
	}
	pendingVersion, pendingResult, errPending := loadStatisticsFileInto(ctx, pendingPath, prepared, cutoff, false)
	if errPending != nil {
		if os.IsNotExist(errPending) {
			return loaded, preparation, result, nil
		}
		return false, PreparedStatisticsRestore{}, result, errPending
	}
	result.Added += pendingResult.Added
	result.Skipped += pendingResult.Skipped
	if pendingVersion < StatisticsFileVersion {
		preparation.NeedsMigration = true
	}
	return true, preparation, result, nil
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

// RewriteRequestStatisticsWithPolicy forces a version 2 rewrite. It is used
// after a generation-fenced legacy restore has been applied successfully.
func RewriteRequestStatisticsWithPolicy(path string, stats *RequestStatistics, policy PersistencePolicy) (PersistenceResult, error) {
	if stats == nil {
		return PersistenceResult{SnapshotPath: path}, nil
	}
	statisticsPersistenceMu.Lock()
	defer statisticsPersistenceMu.Unlock()
	return rewriteRequestStatisticsWithPolicyLocked(path, stats, policy)
}

func persistRequestStatisticsWithPolicyLocked(path string, stats *RequestStatistics, policy PersistencePolicy, inspectPersisted bool) (PersistenceResult, error) {
	return writeRequestStatisticsWithPolicyLocked(path, stats, policy, inspectPersisted, false)
}

func rewriteRequestStatisticsWithPolicyLocked(path string, stats *RequestStatistics, policy PersistencePolicy) (PersistenceResult, error) {
	return writeRequestStatisticsWithPolicyLocked(path, stats, policy, true, true)
}

func writeRequestStatisticsWithPolicyLocked(path string, stats *RequestStatistics, policy PersistencePolicy, inspectPersisted, force bool) (PersistenceResult, error) {
	result := PersistenceResult{SnapshotPath: path}

	if policy.DetailRetentionDays > 0 {
		cutoff := time.Now().UTC().AddDate(0, 0, -policy.DetailRetentionDays)
		result.Pruned += stats.PruneBefore(cutoff)
	}

	for {
		spool, errSpool := spoolRequestStatistics(path, stats)
		if errSpool != nil {
			return result, errSpool
		}
		version := spool.version
		persistedVersion := spool.persistedVersion
		if version == persistedVersion && !inspectPersisted {
			spool.closeAndRemove()
			return result, nil
		}
		trim, errTrim := calculateStatisticsStreamTrim(spool, policy.MaxBytes)
		if errTrim != nil {
			spool.closeAndRemove()
			return result, errTrim
		}
		if trim.removeRecords == 0 {
			result.SizeBytes = trim.finalBytes
			result.DetailCount = trim.keepRecords
			if version == persistedVersion && !force {
				spool.closeAndRemove()
				return result, nil
			}
			if errWrite := installStatisticsStream(path, spool, trim); errWrite != nil {
				spool.closeAndRemove()
				return result, errWrite
			}
			spool.closeAndRemove()
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

		spool.closeAndRemove()
		removed := stats.PruneOldest(trim.removeRecords)
		if removed == 0 {
			return result, fmt.Errorf("usage: unable to prune snapshot below maximum size %d", policy.MaxBytes)
		}
		result.Pruned += removed
	}
}

type statisticsRecordSpool struct {
	file             *os.File
	path             string
	header           []byte
	recordBytes      int64
	records          int
	version          uint64
	persistedVersion uint64
}

type statisticsPersistenceCursor struct {
	timestamp time.Time
	id        uint64
	valid     bool
}

func (spool *statisticsRecordSpool) closeAndRemove() {
	if spool == nil {
		return
	}
	if spool.file != nil {
		_ = spool.file.Close()
	}
	if spool.path != "" {
		_ = os.Remove(spool.path)
	}
}

type statisticsStreamTrim struct {
	offset        int64
	remaining     int64
	removeRecords int
	keepRecords   int
	footer        []byte
	finalBytes    int64
}

func spoolRequestStatistics(path string, stats *RequestStatistics) (*statisticsRecordSpool, error) {
	return spoolRequestStatisticsWithCapture(path, stats, nil)
}

func spoolRequestStatisticsWithCapture(path string, stats *RequestStatistics, afterCapture func()) (*statisticsRecordSpool, error) {
	target := strings.TrimSpace(path)
	if target == "" {
		return nil, fmt.Errorf("usage: empty snapshot path")
	}
	target = filepath.Clean(target)
	dir := filepath.Dir(target)
	if errMkdir := os.MkdirAll(dir, 0o755); errMkdir != nil {
		return nil, fmt.Errorf("usage: create snapshot directory: %w", errMkdir)
	}
	raw, errCreate := os.CreateTemp(dir, "usage-records-*.tmp")
	if errCreate != nil {
		return nil, fmt.Errorf("usage: create record spool: %w", errCreate)
	}
	spool := &statisticsRecordSpool{file: raw, path: raw.Name()}
	fail := func(err error) (*statisticsRecordSpool, error) {
		spool.closeAndRemove()
		return nil, err
	}

	header, errHeader := marshalStatisticsStreamLine(statisticsStreamEnvelope{
		Kind:       "header",
		Version:    StatisticsFileVersion,
		ExportedAt: time.Now().UTC(),
	})
	if errHeader != nil {
		return fail(errHeader)
	}
	spool.header = header

	stats.flushPending()
	stats.mu.Lock()
	stats.sortDetailIndexLocked()
	highWaterID := stats.nextDetailID
	spool.version = stats.changeCount
	spool.persistedVersion = stats.persistedCount
	stats.mu.Unlock()
	if afterCapture != nil {
		afterCapture()
	}

	encoder := json.NewEncoder(raw)
	cursor := statisticsPersistenceCursor{}
	for {
		records, nextCursor, done := persistenceRecordBatchAfter(stats, cursor, highWaterID, statisticsStreamBatchSize)
		for index := range records {
			detail := records[index].Detail
			if errEncode := encoder.Encode(statisticsStreamEnvelope{
				Kind:   "request",
				API:    records[index].API,
				Model:  records[index].Model,
				Detail: &detail,
			}); errEncode != nil {
				return fail(fmt.Errorf("usage: encode version 2 request record: %w", errEncode))
			}
			spool.records++
		}
		cursor = nextCursor
		if done {
			break
		}
	}
	position, errPosition := raw.Seek(0, io.SeekCurrent)
	if errPosition != nil {
		return fail(fmt.Errorf("usage: inspect record spool: %w", errPosition))
	}
	spool.recordBytes = position
	return spool, nil
}

func persistenceRecordBatchAfter(stats *RequestStatistics, cursor statisticsPersistenceCursor, highWaterID uint64, batchSize int) ([]persistedUsageRecord, statisticsPersistenceCursor, bool) {
	if stats == nil || batchSize <= 0 {
		return nil, cursor, true
	}
	stats.mu.Lock()
	defer stats.mu.Unlock()
	stats.sortDetailIndexLocked()
	start := 0
	if cursor.valid {
		start = sort.Search(len(stats.detailIndex), func(index int) bool {
			ref := stats.detailIndex[index]
			if !ref.Timestamp.Equal(cursor.timestamp) {
				return ref.Timestamp.After(cursor.timestamp)
			}
			return ref.ID > cursor.id
		})
	}
	records := make([]persistedUsageRecord, 0, batchSize)
	nextCursor := cursor
	index := start
	for ; index < len(stats.detailIndex); index++ {
		ref := stats.detailIndex[index]
		nextCursor = statisticsPersistenceCursor{timestamp: ref.Timestamp, id: ref.ID, valid: true}
		if ref.ID > highWaterID {
			continue
		}
		location, ok := stats.detailLocations[ref.ID]
		if !ok {
			continue
		}
		apiValue := stats.apis[location.API]
		if apiValue == nil {
			continue
		}
		modelValue := apiValue.Models[location.Model]
		if modelValue == nil || location.Offset < 0 || location.Offset >= len(modelValue.Details) {
			continue
		}
		detail := modelValue.Details[location.Offset]
		if detail.internalID != ref.ID {
			continue
		}
		records = append(records, persistedUsageRecord{
			API:    location.API,
			Model:  location.Model,
			Detail: publicRequestDetail(detail),
		})
		if len(records) >= batchSize {
			index++
			break
		}
	}
	return records, nextCursor, index >= len(stats.detailIndex)
}

func calculateStatisticsStreamTrim(spool *statisticsRecordSpool, maxBytes int64) (statisticsStreamTrim, error) {
	trim := statisticsStreamTrim{remaining: spool.recordBytes, keepRecords: spool.records}
	footer, errFooter := marshalStatisticsStreamLine(statisticsStreamEnvelope{Kind: "footer", Records: int64(trim.keepRecords)})
	if errFooter != nil {
		return trim, errFooter
	}
	trim.footer = footer
	trim.finalBytes = int64(len(spool.header)) + trim.remaining + int64(len(trim.footer))
	if maxBytes <= 0 || trim.finalBytes <= maxBytes {
		return trim, nil
	}
	emptyFooter, errEmptyFooter := marshalStatisticsStreamLine(statisticsStreamEnvelope{Kind: "footer"})
	if errEmptyFooter != nil {
		return trim, errEmptyFooter
	}
	if int64(len(spool.header)+len(emptyFooter)) > maxBytes {
		return trim, fmt.Errorf("usage: snapshot metadata exceeds maximum size %d", maxBytes)
	}
	if _, errSeek := spool.file.Seek(0, io.SeekStart); errSeek != nil {
		return trim, fmt.Errorf("usage: rewind record spool: %w", errSeek)
	}
	reader := bufio.NewReader(spool.file)
	for trim.keepRecords > 0 && trim.finalBytes > maxBytes {
		line, errRead := reader.ReadBytes('\n')
		if errRead != nil {
			return trim, fmt.Errorf("usage: scan record spool: %w", errRead)
		}
		lineBytes := int64(len(line))
		trim.offset += lineBytes
		trim.remaining -= lineBytes
		trim.removeRecords++
		trim.keepRecords--
		trim.footer, errFooter = marshalStatisticsStreamLine(statisticsStreamEnvelope{Kind: "footer", Records: int64(trim.keepRecords)})
		if errFooter != nil {
			return trim, errFooter
		}
		trim.finalBytes = int64(len(spool.header)) + trim.remaining + int64(len(trim.footer))
	}
	return trim, nil
}

func installStatisticsStream(path string, spool *statisticsRecordSpool, trim statisticsStreamTrim) error {
	target := filepath.Clean(strings.TrimSpace(path))
	dir := filepath.Dir(target)
	temporary, errCreate := os.CreateTemp(dir, "usage-statistics-*.tmp")
	if errCreate != nil {
		return fmt.Errorf("usage: create temp snapshot file: %w", errCreate)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if _, errWrite := temporary.Write(spool.header); errWrite != nil {
		return fmt.Errorf("usage: write version 2 header: %w", errWrite)
	}
	if _, errSeek := spool.file.Seek(trim.offset, io.SeekStart); errSeek != nil {
		return fmt.Errorf("usage: seek retained record window: %w", errSeek)
	}
	if _, errCopy := io.CopyN(temporary, spool.file, trim.remaining); errCopy != nil {
		return fmt.Errorf("usage: copy retained record window: %w", errCopy)
	}
	if _, errWrite := temporary.Write(trim.footer); errWrite != nil {
		return fmt.Errorf("usage: write version 2 footer: %w", errWrite)
	}
	if errSync := temporary.Sync(); errSync != nil {
		return fmt.Errorf("usage: sync temp snapshot file: %w", errSync)
	}
	if errClose := temporary.Close(); errClose != nil {
		return fmt.Errorf("usage: close temp snapshot file: %w", errClose)
	}
	if errRename := os.Rename(temporaryPath, target); errRename != nil {
		return fmt.Errorf("usage: rename snapshot file: %w", errRename)
	}
	committed = true
	if dirHandle, errOpenDir := os.Open(dir); errOpenDir == nil {
		_ = dirHandle.Sync()
		_ = dirHandle.Close()
	}
	return nil
}

func marshalStatisticsStreamLine(envelope statisticsStreamEnvelope) ([]byte, error) {
	data, errMarshal := json.Marshal(envelope)
	if errMarshal != nil {
		return nil, fmt.Errorf("usage: marshal version 2 snapshot record: %w", errMarshal)
	}
	return append(data, '\n'), nil
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
	if _, _, errLoad := loadStatisticsFileInto(context.Background(), pendingPath, combined, time.Time{}, false); errLoad == nil {
	} else if !os.IsNotExist(errLoad) {
		return errLoad
	}
	if errCopy := copyRequestStatisticsRecords(context.Background(), stats, combined); errCopy != nil {
		return errCopy
	}
	_, errWrite := rewriteRequestStatisticsWithPolicyLocked(pendingPath, combined, PersistencePolicy{})
	return errWrite
}

func copyRequestStatisticsRecords(ctx context.Context, source, target *RequestStatistics) error {
	if source == nil || target == nil {
		return nil
	}
	source.flushPending()
	source.mu.Lock()
	source.sortDetailIndexLocked()
	highWaterID := source.nextDetailID
	source.mu.Unlock()
	seen := make(map[string]struct{})
	target.mu.RLock()
	for apiName, apiValue := range target.apis {
		if apiValue == nil {
			continue
		}
		for modelName, modelValue := range apiValue.Models {
			if modelValue == nil {
				continue
			}
			for _, detail := range modelValue.Details {
				seen[dedupKey(apiName, modelName, detail)] = struct{}{}
			}
		}
	}
	target.mu.RUnlock()
	cursor := statisticsPersistenceCursor{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		records, nextCursor, done := persistenceRecordBatchAfter(source, cursor, highWaterID, statisticsStreamBatchSize)
		kept := records[:0]
		for _, record := range records {
			key := dedupKey(record.API, record.Model, record.Detail)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			kept = append(kept, record)
		}
		if _, errInsert := insertPersistedUsageBatch(ctx, target, kept); errInsert != nil {
			return errInsert
		}
		cursor = nextCursor
		if done {
			break
		}
	}
	return nil
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
