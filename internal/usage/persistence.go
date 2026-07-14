package usage

import (
	"bytes"
	"encoding/json"
	"fmt"
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
)

var statisticsPersistenceMu sync.Mutex

// StatisticsFilePayload is the on-disk representation used for automatic
// persistence.
type StatisticsFilePayload struct {
	Version    int                `json:"version"`
	ExportedAt time.Time          `json:"exported_at"`
	Usage      StatisticsSnapshot `json:"usage"`
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

// SaveSnapshotFile writes a complete statistics snapshot to disk atomically.
func SaveSnapshotFile(path string, snapshot StatisticsSnapshot) error {
	payload := StatisticsFilePayload{
		Version:    StatisticsFileVersion,
		ExportedAt: time.Now().UTC(),
		Usage:      snapshot,
	}
	data, errMarshal := json.MarshalIndent(payload, "", "  ")
	if errMarshal != nil {
		return fmt.Errorf("usage: marshal snapshot file: %w", errMarshal)
	}
	data = append(data, '\n')
	return writeFileAtomic(path, data)
}

// LoadSnapshotFile reads a persisted snapshot from disk.
func LoadSnapshotFile(path string) (StatisticsSnapshot, error) {
	var snapshot StatisticsSnapshot

	data, errRead := os.ReadFile(path)
	if errRead != nil {
		return snapshot, errRead
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return snapshot, fmt.Errorf("usage: statistics file is empty")
	}

	var envelope map[string]json.RawMessage
	if errEnvelope := json.Unmarshal(trimmed, &envelope); errEnvelope == nil {
		if _, ok := envelope["usage"]; ok {
			var payload StatisticsFilePayload
			if errPayload := json.Unmarshal(trimmed, &payload); errPayload != nil {
				return snapshot, fmt.Errorf("usage: decode snapshot payload: %w", errPayload)
			}
			if payload.Version != 0 && payload.Version != StatisticsFileVersion {
				return snapshot, fmt.Errorf("usage: unsupported snapshot version %d", payload.Version)
			}
			return payload.Usage, nil
		}
	}

	if errSnapshot := json.Unmarshal(trimmed, &snapshot); errSnapshot != nil {
		return snapshot, fmt.Errorf("usage: decode snapshot: %w", errSnapshot)
	}
	return snapshot, nil
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

// PersistRequestStatistics writes the current statistics snapshot to disk when
// there are unpersisted changes.
func PersistRequestStatistics(path string, stats *RequestStatistics) (bool, error) {
	return persistRequestStatisticsWithSave(path, stats, SaveSnapshotFile)
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
	stats.MarkPersisted(version)
	return previous, nil
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
