package helps

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const chatGPTWebUsageCacheCrashHelperEnv = "CLI_PROXY_USAGE_CACHE_CRASH_HELPER"

func TestChatGPTWebUsageCachePrepareCleansSubprocessCrash(t *testing.T) {
	base := t.TempDir()
	marker := filepath.Join(t.TempDir(), "path")
	command := exec.Command(os.Args[0], "-test.run=^TestChatGPTWebUsageCacheCrashHelper$")
	command.Env = append(os.Environ(),
		chatGPTWebUsageCacheCrashHelperEnv+"=1",
		"CLI_PROXY_USAGE_CACHE_CRASH_BASE="+base,
		"CLI_PROXY_USAGE_CACHE_CRASH_MARKER="+marker,
	)
	if output, errRun := command.CombinedOutput(); errRun != nil {
		t.Fatalf("crash helper failed: %v\n%s", errRun, output)
	}
	data, errRead := os.ReadFile(marker)
	if errRead != nil {
		t.Fatal(errRead)
	}
	orphanPath := string(data)
	if _, errStat := os.Stat(orphanPath); errStat != nil {
		t.Fatalf("crash helper did not leave an instance directory: %v", errStat)
	}

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(orphanPath); !os.IsNotExist(errStat) {
		t.Fatalf("crashed instance directory still exists: %v", errStat)
	}
}

func TestChatGPTWebUsageCacheCrashHelper(t *testing.T) {
	if os.Getenv(chatGPTWebUsageCacheCrashHelperEnv) != "1" {
		t.Skip("helper process")
	}
	cache := NewChatGPTWebUsageCache()
	projection, errProjection := cache.NewProjection(
		"gpt-5.4",
		chatGPTWebUsageTestRequest("crashed disk projection"),
		ChatGPTWebUsageCacheOptions{
			Enabled:            true,
			DiskThresholdBytes: 1,
			MaxDiskBytes:       8 << 20,
			Path:               os.Getenv("CLI_PROXY_USAGE_CACHE_CRASH_BASE"),
		},
	)
	if errProjection != nil {
		t.Fatal(errProjection)
	}
	if errWrite := os.WriteFile(
		os.Getenv("CLI_PROXY_USAGE_CACHE_CRASH_MARKER"),
		[]byte(projection.ownedDirectory.path),
		0o600,
	); errWrite != nil {
		t.Fatal(errWrite)
	}
}

func TestChatGPTWebUsageCachePrepareRemovesInactiveOwnedDirectory(t *testing.T) {
	base := t.TempDir()
	owned, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, time.Now())
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	path := owned.path
	if errUnlock := unlockChatGPTWebUsageFile(owned.lockFile); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if errClose := owned.lockFile.Close(); errClose != nil {
		t.Fatal(errClose)
	}

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(path); !os.IsNotExist(errStat) {
		t.Fatalf("orphan directory still exists: %v", errStat)
	}
	snapshot := cache.Snapshot()
	if snapshot.CleanupCount != 1 || snapshot.CleanupErrors != 0 {
		t.Fatalf("cleanup stats = %#v", snapshot)
	}
}

func TestChatGPTWebUsageCachePrepareSkipsActiveOtherInstance(t *testing.T) {
	base := t.TempDir()
	owned, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, time.Now())
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	t.Cleanup(func() {
		_ = removeChatGPTWebUsageCacheOwnedDirectory(owned)
	})

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(owned.path); errStat != nil {
		t.Fatalf("active instance directory was removed: %v", errStat)
	}
	snapshot := cache.Snapshot()
	if snapshot.CleanupCount != 0 || snapshot.OrphanDirectoryCount != 0 {
		t.Fatalf("active instance counted as orphan: %#v", snapshot)
	}
}

func TestChatGPTWebUsageCachePrepareRetainsYoungOrphan(t *testing.T) {
	base := t.TempDir()
	owned, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, time.Now())
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	path := owned.path
	if errUnlock := unlockChatGPTWebUsageFile(owned.lockFile); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if errClose := owned.lockFile.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	t.Cleanup(func() { _ = removeInactiveChatGPTWebUsageCacheDirectory(path, owned.instanceID) })

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, time.Hour); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(path); errStat != nil {
		t.Fatalf("young orphan was removed: %v", errStat)
	}
	snapshot := cache.Snapshot()
	if snapshot.OrphanDirectoryCount != 1 || snapshot.CleanupCount != 0 {
		t.Fatalf("retained orphan stats = %#v", snapshot)
	}
}

func TestChatGPTWebUsageCacheRetentionStartsWhenOrphanIsFirstObserved(t *testing.T) {
	base := t.TempDir()
	owned, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, time.Now().Add(-24*time.Hour))
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	path := owned.path
	if errUnlock := unlockChatGPTWebUsageFile(owned.lockFile); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if errClose := owned.lockFile.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	t.Cleanup(func() { _ = removeInactiveChatGPTWebUsageCacheDirectory(path, owned.instanceID) })

	firstObservedAt := time.Now().UTC().Add(24 * time.Hour)
	first, errFirst := cleanupChatGPTWebUsageCacheBase(base, time.Hour, firstObservedAt)
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	if first.orphanDirectories != 1 || first.cleanupCount != 0 {
		t.Fatalf("first cleanup result = %#v", first)
	}
	if _, errStat := os.Stat(path); errStat != nil {
		t.Fatalf("old process cache was not retained from first orphan observation: %v", errStat)
	}

	second, errSecond := cleanupChatGPTWebUsageCacheBase(base, time.Hour, firstObservedAt.Add(2*time.Hour))
	if errSecond != nil {
		t.Fatal(errSecond)
	}
	if second.cleanupCount != 1 {
		t.Fatalf("second cleanup result = %#v", second)
	}
	if _, errStat := os.Stat(path); !os.IsNotExist(errStat) {
		t.Fatalf("expired orphan still exists: %v", errStat)
	}
}

func TestChatGPTWebUsageCachePrepareCleansInterruptedQuarantine(t *testing.T) {
	base := t.TempDir()
	owned, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, time.Now())
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	quarantinePath, errQuarantine := quarantineChatGPTWebUsageCacheDirectory(
		owned.path,
		owned.info,
		owned.instanceID,
	)
	if errQuarantine != nil {
		t.Fatal(errQuarantine)
	}
	if errUnlock := unlockChatGPTWebUsageFile(owned.lockFile); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if errClose := owned.lockFile.Close(); errClose != nil {
		t.Fatal(errClose)
	}

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(quarantinePath); !os.IsNotExist(errStat) {
		t.Fatalf("interrupted quarantine still exists: %v", errStat)
	}
	if snapshot := cache.Snapshot(); snapshot.CleanupCount != 1 {
		t.Fatalf("cleanup stats = %#v", snapshot)
	}
}

func TestChatGPTWebUsageCachePrepareResumesPartialQuarantineCleanup(t *testing.T) {
	base := t.TempDir()
	owned, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, time.Now())
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	if errWrite := os.WriteFile(filepath.Join(owned.path, "usage-1.bin"), []byte("retained"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	quarantinePath, errQuarantine := quarantineChatGPTWebUsageCacheDirectory(
		owned.path,
		owned.info,
		owned.instanceID,
	)
	if errQuarantine != nil {
		t.Fatal(errQuarantine)
	}
	if errUnlock := unlockChatGPTWebUsageFile(owned.lockFile); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if errClose := owned.lockFile.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	for _, name := range []string{
		chatGPTWebUsageCacheOwnerManifestName,
		chatGPTWebUsageCacheOwnerLockName,
	} {
		if errRemove := os.Remove(filepath.Join(quarantinePath, name)); errRemove != nil {
			t.Fatal(errRemove)
		}
	}

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(quarantinePath); !os.IsNotExist(errStat) {
		t.Fatalf("partial quarantine still exists: %v", errStat)
	}
	if snapshot := cache.Snapshot(); snapshot.CleanupCount != 1 || snapshot.CleanupErrors != 0 {
		t.Fatalf("cleanup stats = %#v", snapshot)
	}
}

func TestChatGPTWebUsageCachePrepareCleansQuarantineBeforeMarkerWrite(t *testing.T) {
	base := t.TempDir()
	owned, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, time.Now())
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	quarantinePath, errQuarantine := quarantineChatGPTWebUsageCacheDirectory(
		owned.path,
		owned.info,
		owned.instanceID,
	)
	if errQuarantine != nil {
		t.Fatal(errQuarantine)
	}
	if errRemove := os.Remove(filepath.Join(quarantinePath, chatGPTWebUsageCacheCleanupMarkerName)); errRemove != nil {
		t.Fatal(errRemove)
	}
	if errUnlock := unlockChatGPTWebUsageFile(owned.lockFile); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if errClose := owned.lockFile.Close(); errClose != nil {
		t.Fatal(errClose)
	}

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(quarantinePath); !os.IsNotExist(errStat) {
		t.Fatalf("pre-marker quarantine still exists: %v", errStat)
	}
}

func TestChatGPTWebUsageCachePrepareUsesManifestWhenCleanupMarkerIsPartial(t *testing.T) {
	base := t.TempDir()
	owned, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, time.Now())
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	quarantinePath, errQuarantine := quarantineChatGPTWebUsageCacheDirectory(
		owned.path,
		owned.info,
		owned.instanceID,
	)
	if errQuarantine != nil {
		t.Fatal(errQuarantine)
	}
	if errWrite := os.WriteFile(
		filepath.Join(quarantinePath, chatGPTWebUsageCacheCleanupMarkerName),
		nil,
		0o600,
	); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errUnlock := unlockChatGPTWebUsageFile(owned.lockFile); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if errClose := owned.lockFile.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	if errRootClose := owned.root.Close(); errRootClose != nil {
		t.Fatal(errRootClose)
	}

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(quarantinePath); !os.IsNotExist(errStat) {
		t.Fatalf("quarantine with partial marker still exists: %v", errStat)
	}
}

func TestChatGPTWebUsageCachePrepareRunsOnlyOncePerProcess(t *testing.T) {
	firstBase := t.TempDir()
	secondBase := t.TempDir()
	first, errFirst := createChatGPTWebUsageCacheOwnedDirectory(firstBase, time.Now())
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	if errUnlock := unlockChatGPTWebUsageFile(first.lockFile); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if errClose := first.lockFile.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	second, errSecond := createChatGPTWebUsageCacheOwnedDirectory(secondBase, time.Now())
	if errSecond != nil {
		t.Fatal(errSecond)
	}
	if errUnlock := unlockChatGPTWebUsageFile(second.lockFile); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if errClose := second.lockFile.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	t.Cleanup(func() {
		_ = removeInactiveChatGPTWebUsageCacheDirectory(second.path, second.instanceID)
	})

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(firstBase, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if errPrepare := cache.Prepare(secondBase, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(first.path); !os.IsNotExist(errStat) {
		t.Fatalf("startup orphan still exists: %v", errStat)
	}
	if _, errStat := os.Stat(second.path); errStat != nil {
		t.Fatalf("runtime path change unexpectedly cleaned orphan: %v", errStat)
	}
}

func TestChatGPTWebUsageCacheInventoriesNewPathBeforeCapacityReservation(t *testing.T) {
	startupBase := t.TempDir()
	secondBase := t.TempDir()
	orphan, errCreate := createChatGPTWebUsageCacheOwnedDirectory(secondBase, time.Now())
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	const orphanBytes = int64(6)
	if errWrite := orphan.root.WriteFile("usage-1.bin", []byte("orphan"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errRootClose := orphan.root.Close(); errRootClose != nil {
		t.Fatal(errRootClose)
	}
	orphan.root = nil
	if errUnlock := unlockChatGPTWebUsageFile(orphan.lockFile); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if errClose := orphan.lockFile.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	orphan.lockFile = nil
	t.Cleanup(func() {
		_ = removeInactiveChatGPTWebUsageCacheDirectory(orphan.path, orphan.instanceID)
	})

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(startupBase, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	request := chatGPTWebUsageTestRequest("new path")
	_, diskBytes := chatGPTWebUsageRecordSizes(chatGPTWebUsageTextRecords(request))
	_, errProjection := cache.NewProjection("gpt-5.4", request, ChatGPTWebUsageCacheOptions{
		Enabled:            true,
		DiskThresholdBytes: 1,
		MaxDiskBytes:       diskBytes + orphanBytes - 1,
		Path:               secondBase,
	})
	var cacheErr *ChatGPTWebUsageCacheError
	if !errors.As(errProjection, &cacheErr) || cacheErr.Code != "chatgpt_web_usage_cache_full" {
		t.Fatalf("NewProjection() error = %v, want capacity rejection", errProjection)
	}
	resolvedSecondBase, errResolved := filepath.EvalSymlinks(secondBase)
	if errResolved != nil {
		t.Fatal(errResolved)
	}
	if retained := cache.retainedOrphanBytes[filepath.Clean(resolvedSecondBase)]; retained != orphanBytes {
		t.Fatalf("retained orphan bytes = %d, want %d", retained, orphanBytes)
	}
}

func TestChatGPTWebUsageCacheUnsupportedLockInventoryCountsV2Files(t *testing.T) {
	base := t.TempDir()
	owned, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, time.Now())
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	t.Cleanup(func() { _ = removeChatGPTWebUsageCacheOwnedDirectory(owned) })
	if errWrite := owned.root.WriteFile("usage-1.bin", []byte("active"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	result, errInventory := inventoryChatGPTWebUsageCacheBaseWithFileLock(base, false)
	if errInventory != nil {
		t.Fatal(errInventory)
	}
	if result.orphanDirectories != 1 || result.orphanFiles != 1 || result.orphanBytes != 6 {
		t.Fatalf("unsupported-lock inventory = %#v", result)
	}
}

func TestChatGPTWebUsageCacheUnsupportedLockStartupCountsV2Files(t *testing.T) {
	base := t.TempDir()
	owned, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, time.Now().Add(-time.Hour))
	if errCreate != nil {
		t.Fatal(errCreate)
	}
	if errWrite := owned.root.WriteFile("usage-1.bin", []byte("orphan"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	t.Cleanup(func() {
		if owned.root != nil {
			_ = owned.root.Close()
		}
		if owned.lockFile != nil {
			_ = unlockChatGPTWebUsageFile(owned.lockFile)
			_ = owned.lockFile.Close()
		}
		_ = removeInactiveChatGPTWebUsageCacheDirectory(owned.path, owned.instanceID)
	})

	result, errCleanup := cleanupChatGPTWebUsageCacheBaseWithFileLock(
		base,
		0,
		time.Now(),
		false,
	)
	if errCleanup != nil {
		t.Fatal(errCleanup)
	}
	if result.orphanDirectories != 1 || result.orphanFiles != 1 || result.orphanBytes != 6 {
		t.Fatalf("unsupported startup cleanup stats = %#v", result)
	}
	if _, errStat := os.Stat(owned.path); errStat != nil {
		t.Fatalf("unsupported startup cleanup removed v2 directory: %v", errStat)
	}
}

func TestChatGPTWebUsageCacheInventoryAggregatesBasesWithoutReplacingCleanupTime(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	cleanupAt := time.Now().UTC().Truncate(time.Second)
	firstBase := filepath.Join(t.TempDir(), "first")
	secondBase := filepath.Join(t.TempDir(), "second")
	cache.applyUsageCacheCleanupResult(firstBase, chatGPTWebUsageCacheCleanupResult{
		orphanDirectories: 1,
		orphanFiles:       2,
		orphanBytes:       3,
		cleanedAt:         cleanupAt,
	})
	cache.applyUsageCacheCleanupResult(secondBase, chatGPTWebUsageCacheCleanupResult{
		orphanDirectories: 4,
		orphanFiles:       5,
		orphanBytes:       6,
		legacyDirectories: 7,
		legacyFiles:       8,
		legacyBytes:       9,
	})

	snapshot := cache.Snapshot()
	if snapshot.OrphanDirectoryCount != 5 ||
		snapshot.OrphanFileCount != 7 ||
		snapshot.OrphanBytes != 9 ||
		snapshot.LegacyDirectoryCount != 7 ||
		snapshot.LegacyFileCount != 8 ||
		snapshot.LegacyBytes != 9 ||
		snapshot.RetainedOrphanBytes != 9 {
		t.Fatalf("aggregated inventory snapshot = %#v", snapshot)
	}
	if snapshot.LastCleanupAt == nil || !snapshot.LastCleanupAt.Equal(cleanupAt) {
		t.Fatalf("last cleanup at = %v, want %v", snapshot.LastCleanupAt, cleanupAt)
	}
}

func TestChatGPTWebUsageCachePrepareCountsLegacyWithoutDeleting(t *testing.T) {
	base := t.TempDir()
	legacy, errMkdir := os.MkdirTemp(base, chatGPTWebUsageCacheLegacyDirectoryPrefix)
	if errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errWrite := os.WriteFile(filepath.Join(legacy, "usage-1.bin"), []byte("legacy"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(legacy); errStat != nil {
		t.Fatalf("legacy directory was removed: %v", errStat)
	}
	snapshot := cache.Snapshot()
	if snapshot.LegacyDirectoryCount != 1 || snapshot.LegacyFileCount != 1 || snapshot.LegacyBytes != 6 {
		t.Fatalf("legacy stats = %#v", snapshot)
	}
}

func TestChatGPTWebUsageCachePrepareReportsIncompleteV2Directory(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, chatGPTWebUsageCacheDirectoryPrefix+"0123456789abcdef0123456789abcdef")
	if errMkdir := os.Mkdir(path, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errWrite := os.WriteFile(filepath.Join(path, chatGPTWebUsageCacheOwnerLockName), nil, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errWrite := os.WriteFile(filepath.Join(path, "usage-1.bin"), []byte("orphan"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(path); errStat != nil {
		t.Fatalf("incomplete directory was unexpectedly removed: %v", errStat)
	}
	snapshot := cache.Snapshot()
	if snapshot.CleanupErrors != 1 ||
		snapshot.OrphanDirectoryCount != 1 ||
		snapshot.OrphanFileCount != 1 ||
		snapshot.OrphanBytes != 6 {
		t.Fatalf("incomplete directory stats = %#v", snapshot)
	}
}

func TestChatGPTWebUsageCachePrepareDoesNotCountTopLevelSymlink(t *testing.T) {
	base := t.TempDir()
	victim := t.TempDir()
	victimFile := filepath.Join(victim, "usage-1.bin")
	if errWrite := os.WriteFile(victimFile, []byte("victim"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	linkPath := filepath.Join(base, chatGPTWebUsageCacheDirectoryPrefix+"0123456789abcdef0123456789abcdef")
	if errSymlink := os.Symlink(victim, linkPath); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}

	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	if errPrepare := cache.Prepare(base, 0); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errStat := os.Stat(victimFile); errStat != nil {
		t.Fatalf("victim file was removed: %v", errStat)
	}
	snapshot := cache.Snapshot()
	if snapshot.OrphanBytes != 0 || snapshot.OrphanFileCount != 0 {
		t.Fatalf("top-level symlink was counted as retained cache: %#v", snapshot)
	}
}

func TestChatGPTWebUsageCacheCloseRemovesOwnedDirectory(t *testing.T) {
	base := t.TempDir()
	cache := NewChatGPTWebUsageCache()
	directory, errDirectory := cache.ensureOwnedUsageCacheDirectory(base, 0)
	if errDirectory != nil {
		t.Fatal(errDirectory)
	}
	cache.Close()
	if _, errStat := os.Stat(directory); !os.IsNotExist(errStat) {
		t.Fatalf("owned directory still exists after close: %v", errStat)
	}
}

func TestChatGPTWebUsageCacheCloseIsConcurrentAndIdempotent(t *testing.T) {
	base := t.TempDir()
	cache := NewChatGPTWebUsageCache()
	directory, errDirectory := cache.ensureOwnedUsageCacheDirectory(base, 0)
	if errDirectory != nil {
		t.Fatal(errDirectory)
	}

	var callers sync.WaitGroup
	for range 16 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			cache.Close()
		}()
	}
	callers.Wait()

	if _, errStat := os.Stat(directory); !os.IsNotExist(errStat) {
		t.Fatalf("owned directory still exists after concurrent close: %v", errStat)
	}
	if snapshot := cache.Snapshot(); snapshot.CleanupErrors != 0 {
		t.Fatalf("concurrent close recorded cleanup errors: %#v", snapshot)
	}
}

func TestChatGPTWebUsageCacheCloseWaitsForRetiredDirectoryRemoval(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	firstOptions := ChatGPTWebUsageCacheOptions{
		Enabled:            true,
		DiskThresholdBytes: 1,
		MaxDiskBytes:       8 << 20,
		Path:               t.TempDir(),
	}
	first, errFirst := cache.NewProjection(
		"gpt-5.4",
		chatGPTWebUsageTestRequest("first"),
		firstOptions,
	)
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	firstOwned := first.ownedDirectory
	secondOptions := firstOptions
	secondOptions.Path = t.TempDir()
	second, errSecond := cache.NewProjection(
		"gpt-5.4",
		chatGPTWebUsageTestRequest("second"),
		secondOptions,
	)
	if errSecond != nil {
		t.Fatal(errSecond)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	cache.removeOwnedDirectory = func(owned *chatGPTWebUsageCacheOwnedDirectory) error {
		if owned == firstOwned {
			close(entered)
			<-release
		}
		return removeChatGPTWebUsageCacheOwnedDirectory(owned)
	}
	discardDone := make(chan struct{})
	go func() {
		first.Discard()
		close(discardDone)
	}()
	<-entered

	closeDone := make(chan struct{})
	go func() {
		cache.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("Close() returned before retired directory removal completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-discardDone
	<-closeDone
	second.Discard()
}

func TestChatGPTWebUsageCacheCloseDoesNotFollowReplacedDirectory(t *testing.T) {
	base := t.TempDir()
	cache := NewChatGPTWebUsageCache()
	directory, errDirectory := cache.ensureOwnedUsageCacheDirectory(base, 0)
	if errDirectory != nil {
		t.Fatal(errDirectory)
	}
	moved := directory + ".moved"
	if errRename := os.Rename(directory, moved); errRename != nil {
		t.Fatal(errRename)
	}
	victim := t.TempDir()
	victimFile := filepath.Join(victim, "keep.txt")
	if errWrite := os.WriteFile(victimFile, []byte("keep"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errSymlink := os.Symlink(victim, directory); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	cache.Close()
	if _, errStat := os.Stat(victimFile); errStat != nil {
		t.Fatalf("close followed the replaced directory: %v", errStat)
	}
	lockFile, errOpen := openChatGPTWebUsageLockFile(filepath.Join(moved, chatGPTWebUsageCacheOwnerLockName), false)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	locked, errLock := lockChatGPTWebUsageFile(lockFile, true)
	if errLock != nil || !locked {
		_ = lockFile.Close()
		t.Fatalf("owner lock was not released after cleanup failure: locked=%v err=%v", locked, errLock)
	}
	_ = unlockChatGPTWebUsageFile(lockFile)
	_ = lockFile.Close()
	if errRemove := os.Remove(directory); errRemove != nil {
		t.Fatal(errRemove)
	}
	if errRemove := removeInactiveChatGPTWebUsageCacheDirectory(moved, filepath.Base(directory)[len(chatGPTWebUsageCacheDirectoryPrefix):]); errRemove != nil {
		t.Fatal(errRemove)
	}
}

func TestChatGPTWebUsageCacheFailedWriteCleanupCountsRetainedFile(t *testing.T) {
	base := t.TempDir()
	cache := NewChatGPTWebUsageCache()
	owned, errOwned := cache.acquireOwnedUsageCacheDirectory(base, 0)
	if errOwned != nil {
		t.Fatal(errOwned)
	}
	const fileName = "usage-0123456789abcdef0123456789abcdef.bin"
	const contents = "partial"
	if errWrite := owned.root.WriteFile(fileName, []byte(contents), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	cache.removeProjectionFile = func(*os.Root, string) error {
		return errors.New("injected remove failure")
	}
	cache.cleanupFailedProjectionFile(owned, fileName, 100)
	snapshot := cache.Snapshot()
	if snapshot.RetainedOrphanBytes != int64(len(contents)) ||
		snapshot.OrphanFileCount != 1 ||
		snapshot.CleanupErrors != 1 {
		t.Fatalf("failed cleanup stats = %#v", snapshot)
	}
	cache.removeProjectionFile = func(root *os.Root, name string) error {
		return root.Remove(name)
	}
	cache.releaseOwnedUsageCacheDirectory(owned)
	cache.Close()
}

func TestChatGPTWebUsageProjectionUsesOwnedRootAfterPathReplacement(t *testing.T) {
	base := t.TempDir()
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	options := ChatGPTWebUsageCacheOptions{
		Enabled:            true,
		DiskThresholdBytes: 1,
		MaxDiskBytes:       8 << 20,
		Path:               base,
	}
	first, errFirst := cache.NewProjection(
		"gpt-5.4",
		chatGPTWebUsageTestRequest("first"),
		options,
	)
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	owned := first.ownedDirectory
	moved := owned.path + ".moved"
	if errRename := os.Rename(owned.path, moved); errRename != nil {
		t.Fatal(errRename)
	}
	victim := t.TempDir()
	if errSymlink := os.Symlink(victim, owned.path); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}

	_, errSecond := cache.NewProjection(
		"gpt-5.4",
		chatGPTWebUsageTestRequest("second"),
		options,
	)
	var cacheErr *ChatGPTWebUsageCacheError
	if !errors.As(errSecond, &cacheErr) ||
		cacheErr.Code != "chatgpt_web_usage_cache_storage_unavailable" {
		t.Fatalf("NewProjection() error = %v, want storage unavailable", errSecond)
	}
	victimEntries, errReadVictim := os.ReadDir(victim)
	if errReadVictim != nil {
		t.Fatal(errReadVictim)
	}
	if len(victimEntries) != 0 {
		t.Fatalf("projection leaked into replacement directory: %v", victimEntries)
	}
	first.Discard()
	cache.Close()
	if errRemove := os.Remove(owned.path); errRemove != nil {
		t.Fatal(errRemove)
	}
	if errRemove := removeInactiveChatGPTWebUsageCacheDirectory(moved, owned.instanceID); errRemove != nil {
		t.Fatal(errRemove)
	}
}

func TestChatGPTWebUsageCacheRetiresInactivePreviousPath(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	options := ChatGPTWebUsageCacheOptions{
		Enabled:            true,
		DiskThresholdBytes: 1,
		MaxDiskBytes:       8 << 20,
		Path:               t.TempDir(),
	}
	first, errFirst := cache.NewProjection(
		"gpt-5.4",
		chatGPTWebUsageTestRequest("first"),
		options,
	)
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	firstDirectory := first.ownedDirectory.path

	options.Path = t.TempDir()
	second, errSecond := cache.NewProjection(
		"gpt-5.4",
		chatGPTWebUsageTestRequest("second"),
		options,
	)
	if errSecond != nil {
		t.Fatal(errSecond)
	}
	if _, errStat := os.Stat(firstDirectory); errStat != nil {
		t.Fatalf("active previous directory was removed: %v", errStat)
	}

	first.Discard()
	if _, errStat := os.Stat(firstDirectory); !os.IsNotExist(errStat) {
		t.Fatalf("inactive previous directory was not retired: %v", errStat)
	}
	cache.mu.Lock()
	ownedCount := len(cache.ownedDirectories)
	cache.mu.Unlock()
	if ownedCount != 1 {
		t.Fatalf("owned directory count = %d, want 1", ownedCount)
	}
	second.Discard()
}

func TestChatGPTWebUsageCacheCustomBasePermissionsRemainUnchanged(t *testing.T) {
	base := filepath.Join(t.TempDir(), "shared")
	if errMkdir := os.Mkdir(base, 0o755); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	directory, errDirectory := cache.ensureOwnedUsageCacheDirectory(base, 0)
	if errDirectory != nil {
		t.Fatal(errDirectory)
	}
	resolvedBase, errResolved := filepath.EvalSymlinks(base)
	if errResolved != nil {
		t.Fatal(errResolved)
	}
	if filepath.Dir(directory) != resolvedBase {
		t.Fatalf("owned directory = %q, want child of %q", directory, resolvedBase)
	}
	info, errInfo := os.Stat(base)
	if errInfo != nil {
		t.Fatal(errInfo)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("custom base permissions = %o, want 755", info.Mode().Perm())
	}
}

func TestChatGPTWebUsageCacheCanonicalizesBaseAliases(t *testing.T) {
	base := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	if errSymlink := os.Symlink(base, alias); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	cache := NewChatGPTWebUsageCache()
	t.Cleanup(cache.Close)
	first, errFirst := cache.ensureOwnedUsageCacheDirectory(base, 0)
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	second, errSecond := cache.ensureOwnedUsageCacheDirectory(alias, 0)
	if errSecond != nil {
		t.Fatal(errSecond)
	}
	if first != second {
		t.Fatalf("base aliases created different instance directories: %q != %q", first, second)
	}
	cache.mu.Lock()
	ownedCount := len(cache.ownedDirectories)
	inventoryCount := len(cache.inventoryByBase)
	cache.mu.Unlock()
	if ownedCount != 1 || inventoryCount != 1 {
		t.Fatalf("alias inventory counts = owned:%d inventory:%d", ownedCount, inventoryCount)
	}
}

func TestChatGPTWebUsageCacheMaximumIncludesRetainedOrphans(t *testing.T) {
	stats := ChatGPTWebUsageCacheSnapshot{
		ActiveDiskBytes:     20,
		RetainedOrphanBytes: 30,
	}
	if !chatGPTWebUsageCacheExceedsMaximum(stats, 60, 11) {
		t.Fatal("retained orphan bytes were omitted from maximum capacity")
	}
	if chatGPTWebUsageCacheExceedsMaximum(stats, 60, 10) {
		t.Fatal("exact maximum capacity was rejected")
	}
}

func TestChatGPTWebUsageCacheMaximumUsesCurrentRootOrphans(t *testing.T) {
	cache := NewChatGPTWebUsageCache()
	first := filepath.Clean(filepath.Join(t.TempDir(), "first"))
	second := filepath.Clean(filepath.Join(t.TempDir(), "second"))
	cache.stats.ActiveDiskBytes = 20
	cache.retainedOrphanBytes[first] = 30
	cache.stats.RetainedOrphanBytes = 30

	if !cache.exceedsMaximumLocked(first, 60, 11) {
		t.Fatal("current root orphan bytes were omitted")
	}
	if cache.exceedsMaximumLocked(second, 60, 11) {
		t.Fatal("orphan bytes from another root affected the current root")
	}
}
