package systemmetrics

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestCollectFilesystemUsesNearestExistingAncestor(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "future", "cache")
	var capturedPath string
	readCapacity := func(path string) (filesystemCapacity, error) {
		capturedPath = path
		return filesystemCapacity{
			totalBytes:     1_000,
			freeBytes:      400,
			availableBytes: 350,
		}, nil
	}

	snapshot := collectFilesystem(target, readCapacity)
	if snapshot.Status != FilesystemStatusOK {
		t.Fatalf("Status = %q, want %q", snapshot.Status, FilesystemStatusOK)
	}
	if capturedPath != root || snapshot.Path != root {
		t.Fatalf("probe path = %q, snapshot path = %q, want %q", capturedPath, snapshot.Path, root)
	}
	if snapshot.TotalBytes != 1_000 || snapshot.UsedBytes != 600 ||
		snapshot.FreeBytes != 400 || snapshot.AvailableBytes != 350 ||
		math.Abs(snapshot.UsedPercent-60) > 0.001 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if _, errStat := os.Stat(target); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("CollectFilesystem created target: stat error = %v", errStat)
	}
}

func TestCollectFilesystemReportsUnavailableAndUnsupported(t *testing.T) {
	unavailable := collectFilesystem(t.TempDir(), func(string) (filesystemCapacity, error) {
		return filesystemCapacity{}, errors.New("read failed")
	})
	if unavailable.Status != FilesystemStatusUnavailable || unavailable.Path == "" {
		t.Fatalf("unavailable snapshot = %#v", unavailable)
	}

	unsupported := collectFilesystem(t.TempDir(), func(string) (filesystemCapacity, error) {
		return filesystemCapacity{}, errFilesystemUnsupported
	})
	if unsupported.Status != FilesystemStatusUnsupported || unsupported.Path == "" {
		t.Fatalf("unsupported snapshot = %#v", unsupported)
	}
}

func TestCollectFilesystemRejectsEmptyPath(t *testing.T) {
	snapshot := CollectFilesystem(" ")
	if snapshot.Status != FilesystemStatusUnavailable || snapshot.Path != "" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestCollectRuntimeReturnsProcessMetrics(t *testing.T) {
	snapshot := CollectRuntime()
	if snapshot.GoVersion == "" || snapshot.GOOS == "" || snapshot.GOARCH == "" {
		t.Fatalf("runtime identity = %#v", snapshot)
	}
	if snapshot.LogicalCPUs < 1 || snapshot.GOMAXPROCS < 1 || snapshot.Goroutines < 1 {
		t.Fatalf("runtime scheduling = %#v", snapshot)
	}
	if snapshot.RuntimeSysBytes < snapshot.HeapAllocBytes ||
		snapshot.HeapInuseBytes < snapshot.HeapAllocBytes {
		t.Fatalf("runtime memory = %#v", snapshot)
	}
}
