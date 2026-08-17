package logging

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnforceLogDirSizeLimitDeletesOldest(t *testing.T) {
	dir := t.TempDir()

	writeLogFile(t, filepath.Join(dir, "old.log"), 60, time.Unix(1, 0))
	writeLogFile(t, filepath.Join(dir, "mid.log"), 60, time.Unix(2, 0))
	protected := filepath.Join(dir, "main.log")
	writeLogFile(t, protected, 60, time.Unix(3, 0))

	deleted, err := enforceLogDirSizeLimit(dir, 120, protected)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted file, got %d", deleted)
	}

	if _, err := os.Stat(filepath.Join(dir, "old.log")); !os.IsNotExist(err) {
		t.Fatalf("expected old.log to be removed, stat error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "mid.log")); err != nil {
		t.Fatalf("expected mid.log to remain, stat error: %v", err)
	}
	if _, err := os.Stat(protected); err != nil {
		t.Fatalf("expected protected main.log to remain, stat error: %v", err)
	}
}

func TestEnforceLogDirSizeLimitSkipsProtected(t *testing.T) {
	dir := t.TempDir()

	protected := filepath.Join(dir, "main.log")
	writeLogFile(t, protected, 200, time.Unix(1, 0))
	writeLogFile(t, filepath.Join(dir, "other.log"), 50, time.Unix(2, 0))

	deleted, err := enforceLogDirSizeLimit(dir, 100, protected)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted file, got %d", deleted)
	}

	if _, err := os.Stat(protected); err != nil {
		t.Fatalf("expected protected main.log to remain, stat error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "other.log")); !os.IsNotExist(err) {
		t.Fatalf("expected other.log to be removed, stat error: %v", err)
	}
}

func TestCleanupLogDirectoryAppliesRetentionBeforeSize(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, time.August, 17, 12, 0, 0, 0, time.UTC)
	old := filepath.Join(dir, "old.log.gz")
	recent := filepath.Join(dir, "recent.log")
	protected := filepath.Join(dir, "main.log")
	ignored := filepath.Join(dir, "usage-statistics.snapshot")
	writeLogFile(t, old, 80, now.Add(-72*time.Hour))
	writeLogFile(t, recent, 60, now.Add(-time.Hour))
	writeLogFile(t, protected, 70, now.Add(-96*time.Hour))
	writeLogFile(t, ignored, 200, now.Add(-96*time.Hour))

	result, errCleanup := CleanupLogDirectory(dir, 2, 100, protected, now)
	if errCleanup != nil {
		t.Fatalf("CleanupLogDirectory() error = %v", errCleanup)
	}
	if result.Before.FileCount != 3 || result.Before.TotalBytes != 210 {
		t.Fatalf("before = %+v", result.Before)
	}
	if result.RemovedFiles != 2 || result.RemovedBytes != 140 || result.FailedFiles != 0 {
		t.Fatalf("result = %+v", result)
	}
	if result.After.FileCount != 1 || result.After.TotalBytes != 70 {
		t.Fatalf("after = %+v", result.After)
	}
	if _, errStat := os.Stat(protected); errStat != nil {
		t.Fatalf("protected file removed: %v", errStat)
	}
	if _, errStat := os.Stat(ignored); errStat != nil {
		t.Fatalf("non-log file removed: %v", errStat)
	}
}

func TestInspectLogDirectoryDoesNotExposePaths(t *testing.T) {
	dir := t.TempDir()
	modified := time.Date(2026, time.August, 17, 1, 2, 3, 0, time.UTC)
	writeLogFile(t, filepath.Join(dir, "main.log"), 42, modified)

	snapshot, errInspect := InspectLogDirectory(dir, filepath.Join(dir, "main.log"))
	if errInspect != nil {
		t.Fatalf("InspectLogDirectory() error = %v", errInspect)
	}
	if !snapshot.Available || snapshot.FileCount != 1 || snapshot.TotalBytes != 42 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if snapshot.OldestAt == nil || !snapshot.OldestAt.Equal(modified) || snapshot.NewestAt == nil || !snapshot.NewestAt.Equal(modified) {
		t.Fatalf("snapshot timestamps = %+v", snapshot)
	}
}

func writeLogFile(t *testing.T, path string, size int, modTime time.Time) {
	t.Helper()

	data := make([]byte, size)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("set times: %v", err)
	}
}
