package systemmetrics

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
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
			filesystemID:   "test-filesystem",
		}, nil
	}

	snapshot := collectFilesystem(target, readCapacity)
	if snapshot.Status != FilesystemStatusOK {
		t.Fatalf("Status = %q, want %q", snapshot.Status, FilesystemStatusOK)
	}
	if capturedPath != root || snapshot.Path != root {
		t.Fatalf("probe path = %q, snapshot path = %q, want %q", capturedPath, snapshot.Path, root)
	}
	if snapshot.FilesystemID != "test-filesystem" {
		t.Fatalf("filesystem ID = %q, want test-filesystem", snapshot.FilesystemID)
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

func TestRuntimeRateSamplerCalculatesProcessAndGCRates(t *testing.T) {
	sampler := &runtimeRateSampler{}
	started := time.Unix(100, 0)
	if first := sampler.observe(runtimeRatePoint{
		at:             started,
		processCPUNano: uint64(time.Second),
		cpuAvailable:   true,
		totalAlloc:     100,
		gcCycles:       2,
		gcPauseNanos:   uint64(10 * time.Millisecond),
	}, 4); first.available {
		t.Fatalf("first sample = %#v, want unavailable", first)
	}
	rate := sampler.observe(runtimeRatePoint{
		at:             started.Add(2 * time.Second),
		processCPUNano: uint64(3 * time.Second),
		cpuAvailable:   true,
		totalAlloc:     1_100,
		gcCycles:       6,
		gcPauseNanos:   uint64(30 * time.Millisecond),
	}, 4)
	if !rate.available || !rate.processCPUAvailable || rate.sampleSeconds != 2 ||
		math.Abs(rate.processCPUPercent-100) > 0.001 ||
		math.Abs(rate.processCPUNormalizedPercent-25) > 0.001 ||
		math.Abs(rate.allocationBytesPerSecond-500) > 0.001 ||
		math.Abs(rate.gcCyclesPerSecond-2) > 0.001 ||
		math.Abs(rate.gcPausePercent-1) > 0.001 {
		t.Fatalf("rate = %#v", rate)
	}
}

func TestRuntimeRateSamplerResetsAfterCounterRegression(t *testing.T) {
	sampler := &runtimeRateSampler{}
	started := time.Unix(100, 0)
	_ = sampler.observe(runtimeRatePoint{at: started, totalAlloc: 100, gcCycles: 2, gcPauseNanos: 10}, 1)
	regressed := sampler.observe(runtimeRatePoint{at: started.Add(time.Second), totalAlloc: 99, gcCycles: 2, gcPauseNanos: 10}, 1)
	if regressed.available {
		t.Fatalf("regressed sample = %#v, want unavailable", regressed)
	}
	recovered := sampler.observe(runtimeRatePoint{at: started.Add(2 * time.Second), totalAlloc: 199, gcCycles: 3, gcPauseNanos: 20}, 1)
	if !recovered.available || recovered.allocationBytesPerSecond != 100 {
		t.Fatalf("recovered sample = %#v", recovered)
	}
}

func TestRuntimeRateSamplerRejectsStaleInterval(t *testing.T) {
	sampler := &runtimeRateSampler{}
	started := time.Unix(100, 0)
	_ = sampler.observe(runtimeRatePoint{at: started, totalAlloc: 100}, 1)
	stale := sampler.observe(runtimeRatePoint{at: started.Add(runtimeRateMaxSampleInterval + time.Second), totalAlloc: 200}, 1)
	if stale.available {
		t.Fatalf("stale sample = %#v, want unavailable", stale)
	}
	recovered := sampler.observe(runtimeRatePoint{at: started.Add(runtimeRateMaxSampleInterval + 2*time.Second), totalAlloc: 300}, 1)
	if !recovered.available || recovered.allocationBytesPerSecond != 100 {
		t.Fatalf("recovered sample = %#v", recovered)
	}
}
