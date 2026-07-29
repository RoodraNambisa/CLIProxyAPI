package helps

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/systemmetrics"
)

func TestChatGPTWebUsageProjectionPrecomputesSmallInput(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	projection, err := cache.NewProjection("gpt-5.4", chatGPTWebUsageTestRequest("describe this"), ChatGPTWebUsageCacheOptions{
		Enabled:            true,
		DiskThresholdBytes: 1 << 20,
		MaxDiskBytes:       8 << 20,
		AutoOutputQuality:  "medium",
	})
	if err != nil {
		t.Fatalf("NewProjection() error = %v", err)
	}
	projection.AddImage(ChatGPTWebUsageImage{Model: "gpt-5.4", Detail: "high", Width: 1024, Height: 1024})
	if estimateErrors := projection.PrecomputeInput(); len(estimateErrors) != 0 {
		t.Fatalf("PrecomputeInput() errors = %v", estimateErrors)
	}
	if !projection.inputPrecomputed || len(projection.records) != 0 || len(projection.images) != 0 {
		t.Fatalf("projection retained input after precompute: %#v", projection)
	}
	if snapshot := cache.Snapshot(); snapshot.ActiveMemoryBytes != 0 || snapshot.ActiveMemoryEntries != 1 {
		t.Fatalf("snapshot after precompute = %#v", snapshot)
	}
	usage, estimateErrors := projection.Estimate("finished", nil)
	if len(estimateErrors) != 0 {
		t.Fatalf("Estimate() errors = %v", estimateErrors)
	}
	if usage["input_tokens"].(int64) <= 0 || usage["output_tokens"].(int64) <= 0 {
		t.Fatalf("usage = %#v", usage)
	}
	projection.Complete()
	if snapshot := cache.Snapshot(); snapshot.SuccessfulCalculations != 1 || snapshot.ActiveMemoryEntries != 0 {
		t.Fatalf("completed snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebUsageProjectionSpillsLargeInputUntilSuccess(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	directory := t.TempDir()
	projection, err := cache.NewProjection("gpt-5.4", chatGPTWebUsageTestRequest("large input"), ChatGPTWebUsageCacheOptions{
		Enabled:            true,
		DiskThresholdBytes: 1,
		MaxDiskBytes:       8 << 20,
		Path:               directory,
	})
	if err != nil {
		t.Fatalf("NewProjection() error = %v", err)
	}
	if projection.filePath == "" || projection.inputPrecomputed {
		t.Fatalf("projection = %#v, want lazy disk projection", projection)
	}
	if estimateErrors := projection.PrecomputeInput(); len(estimateErrors) != 0 || projection.inputPrecomputed {
		t.Fatalf("disk PrecomputeInput() errors = %v, projection = %#v", estimateErrors, projection)
	}
	info, errStat := os.Stat(projection.filePath)
	if errStat != nil {
		t.Fatalf("stat projection file: %v", errStat)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("projection mode = %o, want 600", info.Mode().Perm())
	}
	path := projection.filePath
	usage, estimateErrors := projection.Estimate("finished", nil)
	if len(estimateErrors) != 0 || usage["input_tokens"].(int64) <= 0 {
		t.Fatalf("Estimate() usage = %#v, errors = %v", usage, estimateErrors)
	}
	projection.Complete()
	if _, errStat = os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("projection file still exists: %v", errStat)
	}
	if matches, errGlob := filepath.Glob(filepath.Join(directory, "usage-*.bin")); errGlob != nil || len(matches) != 0 {
		t.Fatalf("remaining projection files = %v, error = %v", matches, errGlob)
	}
}

func TestChatGPTWebUsageProjectionCountsLargeTextAcrossDiskChunksExactly(t *testing.T) {
	tests := map[string]string{
		"continuous word": strings.Repeat("hello", chatGPTWebUsageRecordChunkBytes/len("hello")+100),
		"whitespace":      strings.Repeat(" ", chatGPTWebUsageRecordChunkBytes*3+17),
		"unicode":         strings.Repeat("世界🙂abc", chatGPTWebUsageRecordChunkBytes/len("世界🙂abc")+100),
	}
	for name, text := range tests {
		t.Run(name, func(t *testing.T) {
			cache := NewChatGPTWebUsageCache()
			t.Cleanup(cache.Close)
			request := chatGPTWebUsageTestRequest(text)
			encoder, errEncoder := TokenizerForModel("gpt-5.4")
			if errEncoder != nil {
				t.Fatalf("TokenizerForModel() error = %v", errEncoder)
			}
			segments := chatGPTWebTextTokenSegments(request)
			var want int64
			for index, segment := range segments {
				if index > 0 {
					count, errCount := encoder.Count("\n")
					if errCount != nil {
						t.Fatalf("Count(newline) error = %v", errCount)
					}
					want += int64(count)
				}
				count, errCount := encoder.Count(segment)
				if errCount != nil {
					t.Fatalf("Count(segment) error = %v", errCount)
				}
				want += int64(count)
			}

			projection, errProjection := cache.NewProjection("gpt-5.4", request, ChatGPTWebUsageCacheOptions{
				Enabled:            true,
				DiskThresholdBytes: 1,
				MaxDiskBytes:       8 << 20,
				Path:               t.TempDir(),
			})
			if errProjection != nil {
				t.Fatalf("NewProjection() error = %v", errProjection)
			}
			if projection.filePath == "" {
				t.Fatal("projection did not spill to disk")
			}
			usage, estimateErrors := projection.Estimate("", nil)
			if len(estimateErrors) != 0 {
				t.Fatalf("Estimate() errors = %v", estimateErrors)
			}
			if got := usage["input_tokens"].(int64); got != want {
				t.Fatalf("input tokens = %d, want exact whole-segment count %d", got, want)
			}
			projection.Complete()
		})
	}
}

func TestChatGPTWebUsageProjectionDisabledCacheStaysLazyInMemory(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	projection, err := cache.NewProjection("gpt-5.4", chatGPTWebUsageTestRequest("lazy input"), ChatGPTWebUsageCacheOptions{
		Enabled:            false,
		DiskThresholdBytes: 1,
		MaxDiskBytes:       1,
	})
	if err != nil {
		t.Fatalf("NewProjection() error = %v", err)
	}
	if estimateErrors := projection.PrecomputeInput(); len(estimateErrors) != 0 || projection.inputPrecomputed {
		t.Fatalf("PrecomputeInput() errors = %v, projection = %#v", estimateErrors, projection)
	}
	projection.Discard()
	if snapshot := cache.Snapshot(); snapshot.FailedDiscards != 1 || snapshot.ActiveMemoryEntries != 0 {
		t.Fatalf("discard snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebUsageProjectionRejectsDiskCapacityBeforeUpstream(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	_, err := cache.NewProjection("gpt-5.4", chatGPTWebUsageTestRequest("too large"), ChatGPTWebUsageCacheOptions{
		Enabled:            true,
		DiskThresholdBytes: 1,
		MaxDiskBytes:       1,
		Path:               t.TempDir(),
	})
	var cacheErr *ChatGPTWebUsageCacheError
	if !errors.As(err, &cacheErr) || cacheErr.Code != "chatgpt_web_usage_cache_full" {
		t.Fatalf("NewProjection() error = %v", err)
	}
}

func TestChatGPTWebUsageProjectionRejectsPredictedDiskPressureBeforeUpstream(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			TotalBytes:     1000,
			UsedBytes:      900,
			AvailableBytes: 100,
		}
	}
	_, err := cache.NewProjection("gpt-5.4", chatGPTWebUsageTestRequest("too large"), ChatGPTWebUsageCacheOptions{
		Enabled:                  true,
		DiskThresholdBytes:       1,
		MaxDiskBytes:             8 << 20,
		ResourceGuardEnabled:     true,
		MinAvailableDiskBytes:    80,
		MaxFilesystemUsedPercent: 95,
		Path:                     t.TempDir(),
	})
	var cacheErr *ChatGPTWebUsageCacheError
	if !errors.As(err, &cacheErr) || cacheErr.Code != "chatgpt_web_usage_cache_disk_pressure" {
		t.Fatalf("NewProjection() error = %v", err)
	}
	if snapshot := cache.Snapshot(); snapshot.ResourceRejections != 1 || snapshot.ActiveDiskBytes != 0 {
		t.Fatalf("snapshot after resource rejection = %#v", snapshot)
	}
	cache.resourceMu.Lock()
	defer cache.resourceMu.Unlock()
	if len(cache.resourcePathKeys) != 0 || len(cache.resourceStates) != 0 {
		t.Fatalf("resource mappings retained after rejection: paths=%v states=%v", cache.resourcePathKeys, cache.resourceStates)
	}
}

func TestChatGPTWebUsageProjectionResourceGuardAllowsReservedWrite(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			TotalBytes:     100 << 20,
			UsedBytes:      20 << 20,
			AvailableBytes: 80 << 20,
		}
	}
	projection, err := cache.NewProjection("gpt-5.4", chatGPTWebUsageTestRequest("allowed"), ChatGPTWebUsageCacheOptions{
		Enabled:                  true,
		DiskThresholdBytes:       1,
		MaxDiskBytes:             8 << 20,
		ResourceGuardEnabled:     true,
		MinAvailableDiskBytes:    1 << 20,
		MaxFilesystemUsedPercent: 95,
		Path:                     t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewProjection() error = %v", err)
	}
	projection.Discard()
	if snapshot := cache.Snapshot(); snapshot.ResourceRejections != 0 || snapshot.ActiveDiskBytes != 0 {
		t.Fatalf("snapshot after allowed projection = %#v", snapshot)
	}
}

func TestChatGPTWebUsageCacheResourceGuardRejectsUnavailableFilesystem(t *testing.T) {
	err := chatGPTWebUsageCacheResourceError(
		systemmetrics.FilesystemSnapshot{Status: systemmetrics.FilesystemStatusUnavailable},
		0,
		1,
		0,
		100,
	)
	var cacheErr *ChatGPTWebUsageCacheError
	if !errors.As(err, &cacheErr) || cacheErr.Code != "chatgpt_web_usage_cache_storage_unavailable" {
		t.Fatalf("resource error = %v", err)
	}
}

func TestChatGPTWebUsageCacheResourceGuardRejectsPredictedFilesystemUsage(t *testing.T) {
	err := chatGPTWebUsageCacheResourceError(
		systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			TotalBytes:     1000,
			UsedBytes:      899,
			AvailableBytes: 101,
		},
		0,
		1,
		0,
		90,
	)
	var cacheErr *ChatGPTWebUsageCacheError
	if !errors.As(err, &cacheErr) || cacheErr.Code != "chatgpt_web_usage_cache_disk_pressure" {
		t.Fatalf("resource error = %v", err)
	}
}

func TestChatGPTWebUsageCacheResourceProbeDoesNotBlockSnapshots(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	started := make(chan struct{})
	release := make(chan struct{})
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		close(started)
		<-release
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			TotalBytes:     1000,
			UsedBytes:      100,
			AvailableBytes: 900,
		}
	}
	reserved := make(chan error, 1)
	go func() {
		reserved <- cache.reserveDiskProjection(ChatGPTWebUsageCacheOptions{
			MaxDiskBytes:             1000,
			ResourceGuardEnabled:     true,
			MaxFilesystemUsedPercent: 95,
		}, 10)
	}()
	<-started

	snapshotDone := make(chan struct{})
	go func() {
		_ = cache.Snapshot()
		close(snapshotDone)
	}()
	select {
	case <-snapshotDone:
	case <-time.After(time.Second):
		t.Fatal("Snapshot() blocked behind filesystem probe")
	}
	close(release)
	if err := <-reserved; err != nil {
		t.Fatalf("reserveDiskProjection() error = %v", err)
	}
	cache.mu.Lock()
	cache.stats.ActiveDiskBytes -= 10
	cache.mu.Unlock()
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(""), 10)
	cache.createWG.Done()
}

func TestChatGPTWebUsageCacheSlowResourceProbeDoesNotBlockOtherPath(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	fastPath := t.TempDir()
	slowPath := t.TempDir()
	options := ChatGPTWebUsageCacheOptions{
		MaxDiskBytes:             10_000,
		ResourceGuardEnabled:     true,
		MaxFilesystemUsedPercent: 95,
		Path:                     fastPath,
	}
	cache.collectFilesystem = func(path string) systemmetrics.FilesystemSnapshot {
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			FilesystemID:   "fast-filesystem",
			TotalBytes:     10_000,
			UsedBytes:      100,
			AvailableBytes: 9_900,
		}
	}
	if err := cache.reserveDiskProjection(options, 100); err != nil {
		t.Fatalf("initial fast-path reservation: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	cache.collectFilesystem = func(path string) systemmetrics.FilesystemSnapshot {
		if path == chatGPTWebUsageCacheResourcePath(slowPath) {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			return systemmetrics.FilesystemSnapshot{
				Status:         systemmetrics.FilesystemStatusOK,
				FilesystemID:   "slow-filesystem",
				TotalBytes:     10_000,
				UsedBytes:      100,
				AvailableBytes: 9_900,
			}
		}
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			FilesystemID:   "fast-filesystem",
			TotalBytes:     10_000,
			UsedBytes:      100,
			AvailableBytes: 9_900,
		}
	}
	slowResult := make(chan error, 1)
	slowOptions := options
	slowOptions.Path = slowPath
	go func() {
		slowResult <- cache.reserveDiskProjection(slowOptions, 100)
	}()
	<-started

	fastResult := make(chan error, 1)
	fastOptions := options
	fastOptions.Path = fastPath
	go func() {
		fastResult <- cache.reserveDiskProjection(fastOptions, 100)
	}()
	select {
	case err := <-fastResult:
		if err != nil {
			t.Fatalf("fast path blocked reservation failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow filesystem probe blocked another path reservation")
	}

	completed := make(chan struct{})
	go func() {
		cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(fastPath), 100)
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("slow filesystem probe blocked another path reservation cleanup")
	}

	close(release)
	if err := <-slowResult; err != nil {
		t.Fatalf("slow-path reservation: %v", err)
	}
	cache.mu.Lock()
	cache.stats.ActiveDiskBytes -= 300
	cache.mu.Unlock()
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(fastPath), 100)
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(slowPath), 100)
	cache.createWG.Done()
	cache.createWG.Done()
	cache.createWG.Done()
}

func TestChatGPTWebUsageCacheResourceProbeCoalescesSamePath(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	path := t.TempDir()
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	probeCalls := make(chan struct{}, 2)
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		probeCalls <- struct{}{}
		select {
		case <-probeStarted:
		default:
			close(probeStarted)
		}
		<-releaseProbe
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			FilesystemID:   "coalesced-filesystem",
			TotalBytes:     10_000,
			UsedBytes:      100,
			AvailableBytes: 9_900,
		}
	}
	options := ChatGPTWebUsageCacheOptions{
		MaxDiskBytes:             10_000,
		ResourceGuardEnabled:     true,
		MaxFilesystemUsedPercent: 95,
		Path:                     path,
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			results <- cache.reserveDiskProjection(options, 100)
		}()
	}
	close(start)
	<-probeStarted
	time.Sleep(20 * time.Millisecond)
	close(releaseProbe)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("reserveDiskProjection() error = %v", err)
		}
	}
	if got := len(probeCalls); got != 1 {
		t.Fatalf("filesystem probes = %d, want 1 for concurrent same-path reservations", got)
	}

	cache.mu.Lock()
	cache.stats.ActiveDiskBytes -= 200
	cache.mu.Unlock()
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(path), 100)
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(path), 100)
	cache.createWG.Done()
	cache.createWG.Done()
}

func TestChatGPTWebUsageCacheResourceProbeRetriesAcrossCompletedGeneration(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	firstPath := t.TempDir()
	secondPath := t.TempDir()
	options := ChatGPTWebUsageCacheOptions{
		MaxDiskBytes:             10_000,
		ResourceGuardEnabled:     true,
		MaxFilesystemUsedPercent: 100,
		Path:                     firstPath,
	}
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			FilesystemID:   "shared-filesystem",
			TotalBytes:     1000,
			UsedBytes:      0,
			AvailableBytes: 1000,
		}
	}
	if err := cache.reserveDiskProjection(options, 400); err != nil {
		t.Fatalf("initial reservation: %v", err)
	}

	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	probeCalls := make(chan struct{}, 2)
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		probeCalls <- struct{}{}
		if len(probeCalls) == 1 {
			close(probeStarted)
			<-releaseProbe
			return systemmetrics.FilesystemSnapshot{
				Status:         systemmetrics.FilesystemStatusOK,
				FilesystemID:   "shared-filesystem",
				TotalBytes:     1000,
				UsedBytes:      0,
				AvailableBytes: 1000,
			}
		}
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			FilesystemID:   "shared-filesystem",
			TotalBytes:     1000,
			UsedBytes:      500,
			AvailableBytes: 500,
		}
	}
	options.Path = secondPath
	result := make(chan error, 1)
	go func() {
		result <- cache.reserveDiskProjection(options, 600)
	}()
	<-probeStarted
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(firstPath), 400)
	close(releaseProbe)

	var cacheErr *ChatGPTWebUsageCacheError
	if err := <-result; !errors.As(err, &cacheErr) || cacheErr.Code != "chatgpt_web_usage_cache_disk_pressure" {
		t.Fatalf("reservation after generation change error = %v", err)
	}
	if got := len(probeCalls); got != 2 {
		t.Fatalf("filesystem probes = %d, want stale snapshot retry", got)
	}
	cache.mu.Lock()
	cache.stats.ActiveDiskBytes -= 400
	cache.mu.Unlock()
	cache.createWG.Done()
}

func TestChatGPTWebUsageCacheResourceProbeSharesLeaderGeneration(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	firstPath := t.TempDir()
	secondPath := t.TempDir()
	options := ChatGPTWebUsageCacheOptions{
		MaxDiskBytes:             10_000,
		ResourceGuardEnabled:     true,
		MaxFilesystemUsedPercent: 100,
		Path:                     firstPath,
	}
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			FilesystemID:   "shared-filesystem",
			TotalBytes:     1000,
			UsedBytes:      0,
			AvailableBytes: 1000,
		}
	}
	if err := cache.reserveDiskProjection(options, 400); err != nil {
		t.Fatalf("initial reservation: %v", err)
	}
	successes := 0
	initialCompleted := false
	defer func() {
		if !initialCompleted {
			cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(firstPath), 400)
		}
		for range successes {
			cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(secondPath), 300)
		}
		cache.mu.Lock()
		cache.stats.ActiveDiskBytes -= 400 + int64(successes*300)
		cache.mu.Unlock()
		for range 1 + successes {
			cache.createWG.Done()
		}
	}()

	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	probeCalls := make(chan struct{}, 3)
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		probeCalls <- struct{}{}
		if len(probeCalls) == 1 {
			close(probeStarted)
			<-releaseProbe
			return systemmetrics.FilesystemSnapshot{
				Status:         systemmetrics.FilesystemStatusOK,
				FilesystemID:   "shared-filesystem",
				TotalBytes:     1000,
				UsedBytes:      0,
				AvailableBytes: 1000,
			}
		}
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			FilesystemID:   "shared-filesystem",
			TotalBytes:     1000,
			UsedBytes:      500,
			AvailableBytes: 500,
		}
	}
	options.Path = secondPath
	results := make(chan error, 2)
	go func() {
		results <- cache.reserveDiskProjection(options, 300)
	}()
	<-probeStarted
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(firstPath), 400)
	initialCompleted = true
	followerStarted := make(chan struct{})
	go func() {
		close(followerStarted)
		results <- cache.reserveDiskProjection(options, 300)
	}()
	<-followerStarted
	time.Sleep(20 * time.Millisecond)
	close(releaseProbe)

	failures := 0
	for range 2 {
		err := <-results
		if err == nil {
			successes++
			continue
		}
		var cacheErr *ChatGPTWebUsageCacheError
		if !errors.As(err, &cacheErr) || cacheErr.Code != "chatgpt_web_usage_cache_disk_pressure" {
			t.Fatalf("unexpected reservation error = %v", err)
		}
		failures++
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("results = %d successes, %d failures; want one of each", successes, failures)
	}
	if got := len(probeCalls); got < 2 || got > 3 {
		t.Fatalf("filesystem probes = %d, want one stale shared probe and at most two refreshes", got)
	}
}

func TestChatGPTWebUsageCacheResourceReservationsShareSnapshotGeneration(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	collectCalls := 0
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		collectCalls++
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			TotalBytes:     1000,
			UsedBytes:      100,
			AvailableBytes: 900,
		}
	}
	options := ChatGPTWebUsageCacheOptions{
		MaxDiskBytes:             1000,
		ResourceGuardEnabled:     true,
		MaxFilesystemUsedPercent: 95,
	}
	for range 2 {
		if err := cache.reserveDiskProjection(options, 100); err != nil {
			t.Fatalf("reserveDiskProjection() error = %v", err)
		}
	}
	if collectCalls != 1 {
		t.Fatalf("filesystem probes = %d, want 1 for one reservation generation", collectCalls)
	}
	cache.mu.Lock()
	cache.stats.ActiveDiskBytes -= 200
	cache.mu.Unlock()
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(""), 100)
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(""), 100)
	cache.createWG.Done()
	cache.createWG.Done()

	if err := cache.reserveDiskProjection(options, 100); err != nil {
		t.Fatalf("next reserveDiskProjection() error = %v", err)
	}
	if collectCalls != 2 {
		t.Fatalf("filesystem probes = %d, want fresh probe after pending writes complete", collectCalls)
	}
	cache.mu.Lock()
	cache.stats.ActiveDiskBytes -= 100
	cache.mu.Unlock()
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(""), 100)
	cache.createWG.Done()
}

func TestChatGPTWebUsageCacheResourceReservationsStayIsolatedAcrossPaths(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			TotalBytes:     1000,
			UsedBytes:      0,
			AvailableBytes: 1000,
		}
	}
	options := ChatGPTWebUsageCacheOptions{
		MaxDiskBytes:             10_000,
		ResourceGuardEnabled:     true,
		MaxFilesystemUsedPercent: 100,
	}
	firstPath := filepath.Join(t.TempDir(), "first")
	secondPath := filepath.Join(t.TempDir(), "second")
	options.Path = firstPath
	if err := cache.reserveDiskProjection(options, 400); err != nil {
		t.Fatalf("reserve first path: %v", err)
	}
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(firstPath), 400)
	cache.createWG.Done()

	options.Path = secondPath
	if err := cache.reserveDiskProjection(options, 400); err != nil {
		t.Fatalf("reserve second path: %v", err)
	}

	cache.mu.Lock()
	cache.stats.ActiveDiskBytes -= 400
	cache.mu.Unlock()
	if err := cache.reserveDiskProjection(options, 700); err == nil {
		t.Fatal("second path reservation ignored its existing generation")
	}

	cache.mu.Lock()
	cache.stats.ActiveDiskBytes -= 400
	cache.mu.Unlock()
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(secondPath), 400)
	cache.createWG.Done()
}

func TestChatGPTWebUsageCacheResourceReservationsShareFilesystemAcrossPaths(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	cache.collectFilesystem = func(string) systemmetrics.FilesystemSnapshot {
		return systemmetrics.FilesystemSnapshot{
			Status:         systemmetrics.FilesystemStatusOK,
			FilesystemID:   "shared-filesystem",
			TotalBytes:     1000,
			UsedBytes:      0,
			AvailableBytes: 1000,
		}
	}
	options := ChatGPTWebUsageCacheOptions{
		MaxDiskBytes:             10_000,
		ResourceGuardEnabled:     true,
		MaxFilesystemUsedPercent: 100,
	}
	firstPath := filepath.Join(t.TempDir(), "first")
	secondPath := filepath.Join(t.TempDir(), "second")
	options.Path = firstPath
	if err := cache.reserveDiskProjection(options, 400); err != nil {
		t.Fatalf("reserve first path: %v", err)
	}
	options.Path = secondPath
	if err := cache.reserveDiskProjection(options, 400); err != nil {
		t.Fatalf("reserve second path: %v", err)
	}
	if err := cache.reserveDiskProjection(options, 300); err == nil {
		t.Fatal("same-filesystem paths accepted reservations beyond available capacity")
	}

	cache.mu.Lock()
	cache.stats.ActiveDiskBytes -= 800
	cache.mu.Unlock()
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(firstPath), 400)
	cache.completeDiskReservation(chatGPTWebUsageCacheResourcePath(secondPath), 400)
	cache.createWG.Done()
	cache.createWG.Done()
}

func TestChatGPTWebUsageProjectionDoesNotRetainImagePayload(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	withImage := chatGPTWebUsageTestRequest("describe this")
	withImage.Messages[0].Parts[0].ImageURL = "data:image/png;base64,SECRET_IMAGE_BYTES"
	withoutImage := chatGPTWebUsageTestRequest("describe this")

	first, errFirst := cache.NewProjection("gpt-5.4", withImage, ChatGPTWebUsageCacheOptions{})
	second, errSecond := cache.NewProjection("gpt-5.4", withoutImage, ChatGPTWebUsageCacheOptions{})
	if errFirst != nil || errSecond != nil {
		t.Fatalf("NewProjection() errors = %v, %v", errFirst, errSecond)
	}
	firstUsage, firstErrors := first.Estimate("", nil)
	secondUsage, secondErrors := second.Estimate("", nil)
	if len(firstErrors) != 0 || len(secondErrors) != 0 {
		t.Fatalf("Estimate() errors = %v, %v", firstErrors, secondErrors)
	}
	if firstUsage["input_tokens"] != secondUsage["input_tokens"] {
		t.Fatalf("image payload changed text usage: with=%#v without=%#v", firstUsage, secondUsage)
	}
	first.Discard()
	second.Discard()
}

func TestChatGPTWebImageOutputTokenCount(t *testing.T) {
	for _, test := range []struct {
		quality string
		want    int64
	}{
		{quality: "low", want: 196},
		{quality: "medium", want: 1756},
		{quality: "high", want: 7024},
	} {
		got, err := ChatGPTWebImageOutputTokenCount("gpt-image-2", test.quality, "medium", 1024, 1024)
		if err != nil || got != test.want {
			t.Fatalf("quality %s: got %d, error %v, want %d", test.quality, got, err, test.want)
		}
	}
}

func TestChatGPTWebImageProjectionSeparatesResponseAndToolUsage(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	request := chatGPTWebUsageTestRequest("draw a tiger")
	request.Image = &ChatGPTWebImageRequest{Model: "gpt-image-2", Prompt: "draw a tiger"}
	projection, err := cache.NewProjection("gpt-5.4", request, ChatGPTWebUsageCacheOptions{AutoOutputQuality: "high"})
	if err != nil {
		t.Fatalf("NewProjection() error = %v", err)
	}
	usage, estimateErrors := projection.Estimate("", []ChatGPTWebUsageImage{{
		Model: "gpt-image-2", Quality: "auto", Width: 1024, Height: 1024,
	}})
	if len(estimateErrors) != 0 {
		t.Fatalf("Estimate() errors = %v", estimateErrors)
	}
	if usage["output_tokens"].(int64) != 0 {
		t.Fatalf("response usage contains image output tokens: %#v", usage)
	}
	toolUsage := usage["tool_usage"].(map[string]any)["image_gen"].(map[string]any)
	if toolUsage["output_tokens"].(int64) != 7024 ||
		toolUsage["output_tokens_details"].(map[string]any)["image_tokens"].(int64) != 7024 {
		t.Fatalf("tool usage = %#v", toolUsage)
	}
	projection.Complete()
}

func chatGPTWebUsageTestRequest(text string) ChatGPTWebRequest {
	return ChatGPTWebRequest{Messages: []ChatGPTWebMessage{{
		Role:  "user",
		Parts: []ChatGPTWebContentPart{{Text: text}},
	}}}
}
