// Package systemmetrics provides low-overhead, read-only process and filesystem metrics.
package systemmetrics

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	runtimemetrics "runtime/metrics"
	"strings"
	"time"
)

const (
	FilesystemStatusOK          = "ok"
	FilesystemStatusUnavailable = "unavailable"
	FilesystemStatusUnsupported = "unsupported"
)

var errFilesystemUnsupported = errors.New("filesystem metrics are unsupported")

type filesystemCapacity struct {
	totalBytes     uint64
	freeBytes      uint64
	availableBytes uint64
}

type filesystemCapacityReader func(string) (filesystemCapacity, error)

// FilesystemSnapshot describes the filesystem containing Path.
type FilesystemSnapshot struct {
	Status         string  `json:"status"`
	Path           string  `json:"path"`
	TotalBytes     uint64  `json:"total_bytes"`
	UsedBytes      uint64  `json:"used_bytes"`
	FreeBytes      uint64  `json:"free_bytes"`
	AvailableBytes uint64  `json:"available_bytes"`
	UsedPercent    float64 `json:"used_percent"`
}

// RuntimeSnapshot describes the current Go process without starting a sampler.
type RuntimeSnapshot struct {
	GoVersion       string     `json:"go_version"`
	GOOS            string     `json:"goos"`
	GOARCH          string     `json:"goarch"`
	LogicalCPUs     int        `json:"logical_cpus"`
	GOMAXPROCS      uint64     `json:"gomaxprocs"`
	Goroutines      uint64     `json:"goroutines"`
	HeapAllocBytes  uint64     `json:"heap_alloc_bytes"`
	HeapInuseBytes  uint64     `json:"heap_inuse_bytes"`
	StackInuseBytes uint64     `json:"stack_inuse_bytes"`
	RuntimeSysBytes uint64     `json:"runtime_sys_bytes"`
	TotalAllocBytes uint64     `json:"total_alloc_bytes"`
	GCCycles        uint64     `json:"gc_cycles"`
	LastGCAt        *time.Time `json:"last_gc_at"`
}

// CollectFilesystem reads capacity for the nearest existing ancestor of path.
func CollectFilesystem(path string) FilesystemSnapshot {
	return collectFilesystem(path, platformFilesystemCapacity)
}

func collectFilesystem(path string, readCapacity filesystemCapacityReader) FilesystemSnapshot {
	probePath, errResolve := resolveFilesystemPath(path)
	if errResolve != nil {
		return FilesystemSnapshot{Status: FilesystemStatusUnavailable}
	}
	capacity, errRead := readCapacity(probePath)
	if errRead != nil {
		status := FilesystemStatusUnavailable
		if errors.Is(errRead, errFilesystemUnsupported) {
			status = FilesystemStatusUnsupported
		}
		return FilesystemSnapshot{Status: status, Path: probePath}
	}
	usedBytes := uint64(0)
	if capacity.totalBytes > capacity.freeBytes {
		usedBytes = capacity.totalBytes - capacity.freeBytes
	}
	usedPercent := float64(0)
	if capacity.totalBytes > 0 {
		usedPercent = float64(usedBytes) / float64(capacity.totalBytes) * 100
	}
	return FilesystemSnapshot{
		Status:         FilesystemStatusOK,
		Path:           probePath,
		TotalBytes:     capacity.totalBytes,
		UsedBytes:      usedBytes,
		FreeBytes:      capacity.freeBytes,
		AvailableBytes: capacity.availableBytes,
		UsedPercent:    usedPercent,
	}
}

// CollectRuntime reads selected process metrics on demand.
func CollectRuntime() RuntimeSnapshot {
	samples := []runtimemetrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/memory/classes/heap/stacks:bytes"},
		{Name: "/memory/classes/total:bytes"},
		{Name: "/gc/heap/allocs:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/sched/gomaxprocs:threads"},
		{Name: "/sched/goroutines:goroutines"},
	}
	runtimemetrics.Read(samples)
	values := make(map[string]uint64, len(samples))
	for _, sample := range samples {
		if sample.Value.Kind() == runtimemetrics.KindUint64 {
			values[sample.Name] = sample.Value.Uint64()
		}
	}

	var lastGCAt *time.Time
	var gcStats debug.GCStats
	debug.ReadGCStats(&gcStats)
	if gcStats.NumGC > 0 && !gcStats.LastGC.IsZero() {
		lastGC := gcStats.LastGC.UTC()
		lastGCAt = &lastGC
	}

	heapAlloc := values["/memory/classes/heap/objects:bytes"]
	heapInuse := saturatingAdd(heapAlloc, values["/memory/classes/heap/unused:bytes"])
	return RuntimeSnapshot{
		GoVersion:       runtime.Version(),
		GOOS:            runtime.GOOS,
		GOARCH:          runtime.GOARCH,
		LogicalCPUs:     runtime.NumCPU(),
		GOMAXPROCS:      values["/sched/gomaxprocs:threads"],
		Goroutines:      values["/sched/goroutines:goroutines"],
		HeapAllocBytes:  heapAlloc,
		HeapInuseBytes:  heapInuse,
		StackInuseBytes: values["/memory/classes/heap/stacks:bytes"],
		RuntimeSysBytes: values["/memory/classes/total:bytes"],
		TotalAllocBytes: values["/gc/heap/allocs:bytes"],
		GCCycles:        values["/gc/cycles/total:gc-cycles"],
		LastGCAt:        lastGCAt,
	}
}

func resolveFilesystemPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", os.ErrInvalid
	}
	absolutePath, errAbsolute := filepath.Abs(filepath.Clean(path))
	if errAbsolute != nil {
		return "", errAbsolute
	}
	probePath := absolutePath
	for {
		_, errStat := os.Stat(probePath)
		if errStat == nil {
			return probePath, nil
		}
		if !errors.Is(errStat, os.ErrNotExist) {
			return "", errStat
		}
		parent := filepath.Dir(probePath)
		if parent == probePath {
			return "", errStat
		}
		probePath = parent
	}
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

func saturatingMultiply(left, right uint64) uint64 {
	if left == 0 || right == 0 {
		return 0
	}
	if left > math.MaxUint64/right {
		return math.MaxUint64
	}
	return left * right
}

func nonNegativeInt64(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}
