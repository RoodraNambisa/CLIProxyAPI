package logging

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const logDirCleanerInterval = time.Minute

var logDirCleanerCancel context.CancelFunc

// LogDirectorySnapshot is a safe aggregate view of retained log files.
type LogDirectorySnapshot struct {
	Available  bool       `json:"available"`
	FileCount  int        `json:"file_count"`
	TotalBytes int64      `json:"total_bytes"`
	OldestAt   *time.Time `json:"oldest_at,omitempty"`
	NewestAt   *time.Time `json:"newest_at,omitempty"`
}

// LogCleanupResult describes one age and size cleanup pass.
type LogCleanupResult struct {
	Before       LogDirectorySnapshot `json:"before"`
	After        LogDirectorySnapshot `json:"after"`
	RemovedFiles int                  `json:"removed_files"`
	RemovedBytes int64                `json:"removed_bytes"`
	FailedFiles  int                  `json:"failed_files"`
}

type logHistoryFile struct {
	path       string
	size       int64
	modifiedAt time.Time
	protected  bool
}

func configureLogDirCleanerLocked(logDir string, maxTotalSizeMB, retentionDays int, protectedPath string) {
	stopLogDirCleanerLocked()
	if maxTotalSizeMB <= 0 && retentionDays <= 0 {
		return
	}
	dir := strings.TrimSpace(logDir)
	if dir == "" {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	logDirCleanerCancel = cancel
	go runLogDirCleaner(ctx, filepath.Clean(dir), megabytesToBytesSaturated(maxTotalSizeMB), max(retentionDays, 0), strings.TrimSpace(protectedPath))
}

func megabytesToBytesSaturated(megabytes int) int64 {
	if megabytes <= 0 {
		return 0
	}
	if int64(megabytes) > math.MaxInt64/(1024*1024) {
		return math.MaxInt64
	}
	return int64(megabytes) * 1024 * 1024
}

func stopLogDirCleanerLocked() {
	if logDirCleanerCancel == nil {
		return
	}
	logDirCleanerCancel()
	logDirCleanerCancel = nil
}

func runLogDirCleaner(ctx context.Context, logDir string, maxBytes int64, retentionDays int, protectedPath string) {
	ticker := time.NewTicker(logDirCleanerInterval)
	defer ticker.Stop()
	cleanOnce := func() {
		result, errClean := CleanupLogDirectory(logDir, retentionDays, maxBytes, protectedPath, time.Now().UTC())
		if errClean != nil {
			log.WithError(errClean).Warn("logging: failed to clean log history")
			return
		}
		if result.RemovedFiles > 0 {
			log.WithFields(log.Fields{"removed_files": result.RemovedFiles, "removed_bytes": result.RemovedBytes}).Debug("logging: cleaned old log history")
		}
	}
	cleanOnce()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanOnce()
		}
	}
}

// InspectLogDirectory returns aggregate storage information without exposing paths.
func InspectLogDirectory(logDir, protectedPath string) (LogDirectorySnapshot, error) {
	files, available, errFiles := collectLogHistoryFiles(logDir, protectedPath)
	if errFiles != nil {
		return LogDirectorySnapshot{}, errFiles
	}
	return snapshotLogHistoryFiles(files, available), nil
}

// CleanupLogDirectory removes expired log files first, then the oldest files
// until maxBytes is satisfied. The protected file is never removed.
func CleanupLogDirectory(logDir string, retentionDays int, maxBytes int64, protectedPath string, now time.Time) (LogCleanupResult, error) {
	files, available, errFiles := collectLogHistoryFiles(logDir, protectedPath)
	if errFiles != nil {
		return LogCleanupResult{}, errFiles
	}
	result := LogCleanupResult{Before: snapshotLogHistoryFiles(files, available)}
	if !available || len(files) == 0 || (retentionDays <= 0 && maxBytes <= 0) {
		result.After = result.Before
		return result, nil
	}
	sort.SliceStable(files, func(i, j int) bool { return files[i].modifiedAt.Before(files[j].modifiedAt) })
	cutoff := time.Time{}
	if retentionDays > 0 {
		cutoff = now.UTC().AddDate(0, 0, -retentionDays)
	}
	remaining := make([]logHistoryFile, 0, len(files))
	for _, file := range files {
		if file.protected || cutoff.IsZero() || !file.modifiedAt.Before(cutoff) {
			remaining = append(remaining, file)
			continue
		}
		if removeLogHistoryFile(file.path) {
			result.RemovedFiles++
			result.RemovedBytes += file.size
			continue
		}
		result.FailedFiles++
		remaining = append(remaining, file)
	}
	if maxBytes > 0 {
		totalBytes := totalLogHistoryBytes(remaining)
		kept := make([]logHistoryFile, 0, len(remaining))
		for _, file := range remaining {
			if totalBytes <= maxBytes || file.protected {
				kept = append(kept, file)
				continue
			}
			if removeLogHistoryFile(file.path) {
				result.RemovedFiles++
				result.RemovedBytes += file.size
				totalBytes -= file.size
				continue
			}
			result.FailedFiles++
			kept = append(kept, file)
		}
		remaining = kept
	}
	result.After = snapshotLogHistoryFiles(remaining, true)
	return result, nil
}

func collectLogHistoryFiles(logDir, protectedPath string) ([]logHistoryFile, bool, error) {
	dir := strings.TrimSpace(logDir)
	if dir == "" {
		return nil, false, nil
	}
	dir = filepath.Clean(dir)
	entries, errRead := os.ReadDir(dir)
	if errRead != nil {
		if os.IsNotExist(errRead) {
			return nil, false, nil
		}
		return nil, false, errRead
	}
	protected := strings.TrimSpace(protectedPath)
	if protected != "" {
		protected = filepath.Clean(protected)
	}
	files := make([]logHistoryFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !isLogFileName(entry.Name()) {
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil || !info.Mode().IsRegular() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		files = append(files, logHistoryFile{path: path, size: info.Size(), modifiedAt: info.ModTime().UTC(), protected: protected != "" && filepath.Clean(path) == protected})
	}
	return files, true, nil
}

func snapshotLogHistoryFiles(files []logHistoryFile, available bool) LogDirectorySnapshot {
	snapshot := LogDirectorySnapshot{Available: available, FileCount: len(files)}
	for _, file := range files {
		snapshot.TotalBytes += file.size
		if snapshot.OldestAt == nil || file.modifiedAt.Before(*snapshot.OldestAt) {
			value := file.modifiedAt
			snapshot.OldestAt = &value
		}
		if snapshot.NewestAt == nil || file.modifiedAt.After(*snapshot.NewestAt) {
			value := file.modifiedAt
			snapshot.NewestAt = &value
		}
	}
	return snapshot
}

func totalLogHistoryBytes(files []logHistoryFile) int64 {
	var total int64
	for _, file := range files {
		total += file.size
	}
	return total
}

func removeLogHistoryFile(path string) bool {
	if errRemove := os.Remove(path); errRemove != nil {
		if !os.IsNotExist(errRemove) {
			log.WithError(errRemove).Warn("logging: failed to remove retained log file")
			return false
		}
	}
	return true
}

func enforceLogDirSizeLimit(logDir string, maxBytes int64, protectedPath string) (int, error) {
	result, errCleanup := CleanupLogDirectory(logDir, 0, maxBytes, protectedPath, time.Now().UTC())
	return result.RemovedFiles, errCleanup
}

func isLogFileName(name string) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return false
	}
	lower := strings.ToLower(trimmed)
	return strings.HasSuffix(lower, ".log") || strings.HasSuffix(lower, ".log.gz")
}
