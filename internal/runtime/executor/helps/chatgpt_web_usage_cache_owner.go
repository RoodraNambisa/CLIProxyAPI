package helps

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	chatGPTWebUsageCacheDirectoryPrefix       = "cli-proxy-api-chatgpt-web-usage-v2-"
	chatGPTWebUsageCacheLegacyDirectoryPrefix = "cli-proxy-api-chatgpt-web-usage-"
	chatGPTWebUsageCacheOwnerManifestName     = ".owner.json"
	chatGPTWebUsageCacheOwnerLockName         = ".owner.lock"
	chatGPTWebUsageCacheOrphanMarkerName      = ".orphaned"
	chatGPTWebUsageCacheCleanupMarkerName     = ".cleanup"
	chatGPTWebUsageCacheCleanupPrefix         = ".cli-proxy-api-chatgpt-web-usage-delete-"
	chatGPTWebUsageCacheOwnerVersion          = 2
)

var (
	chatGPTWebUsageCacheDirectoryPattern = regexp.MustCompile(`^cli-proxy-api-chatgpt-web-usage-v2-([0-9a-f]{32})$`)
	chatGPTWebUsageCacheCleanupPattern   = regexp.MustCompile(`^\.cli-proxy-api-chatgpt-web-usage-delete-([0-9a-f]{32})-([0-9a-f]{32})$`)
	chatGPTWebUsageCacheLegacyPattern    = regexp.MustCompile(`^cli-proxy-api-chatgpt-web-usage-[0-9]+$`)
	chatGPTWebUsageCacheFilePattern      = regexp.MustCompile(`^usage-(?:[0-9]+|[0-9a-f]{32})\.bin$`)
)

type chatGPTWebUsageCacheOwnerManifest struct {
	Version          int       `json:"version"`
	InstanceID       string    `json:"instance_id"`
	PID              int       `json:"pid"`
	ProcessStartedAt time.Time `json:"process_started_at"`
	CreatedAt        time.Time `json:"created_at"`
}

type chatGPTWebUsageCacheOwnedDirectory struct {
	instanceID string
	base       string
	path       string
	baseInfo   fs.FileInfo
	info       fs.FileInfo
	lockFile   *os.File
	root       *os.Root
	references int
	invalid    bool
}

type chatGPTWebUsageCacheCleanupResult struct {
	orphanDirectories int
	orphanFiles       int
	orphanBytes       int64
	legacyDirectories int
	legacyFiles       int
	legacyBytes       int64
	cleanupCount      uint64
	cleanupErrors     uint64
	cleanedAt         time.Time
}

func (cache *ChatGPTWebUsageCache) Prepare(configuredPath string, retention time.Duration) error {
	if cache == nil {
		return errors.New("chatgpt web usage cache is unavailable")
	}
	if retention < 0 {
		retention = 0
	}
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return errors.New("chatgpt web usage cache is closed")
	}
	if cache.cleanupPrepared {
		cache.mu.Unlock()
		return nil
	}
	cache.mu.Unlock()

	_, errPrepare, _ := cache.prepareGroup.Do("startup", func() (any, error) {
		cache.mu.Lock()
		if cache.closed {
			cache.mu.Unlock()
			return nil, errors.New("chatgpt web usage cache is closed")
		}
		if cache.cleanupPrepared {
			cache.mu.Unlock()
			return nil, nil
		}
		cache.mu.Unlock()
		base, errBase := prepareChatGPTWebUsageCacheBase(configuredPath)
		if errBase != nil {
			cache.recordUsageCacheCleanupError()
			return nil, errBase
		}
		result, errCleanup := cleanupChatGPTWebUsageCacheBase(base, retention, time.Now())
		cache.applyUsageCacheCleanupResult(base, result)
		if errCleanup != nil {
			return nil, errCleanup
		}
		cache.mu.Lock()
		if !cache.closed {
			cache.cleanupPrepared = true
		}
		cache.mu.Unlock()
		return nil, nil
	})
	return errPrepare
}

func (cache *ChatGPTWebUsageCache) prepareUsageCacheBase(
	configuredPath string,
	retention time.Duration,
) (string, error) {
	base, errBase := prepareChatGPTWebUsageCacheBase(configuredPath)
	if errBase != nil {
		return "", errBase
	}
	if errPrepare := cache.Prepare(configuredPath, retention); errPrepare != nil {
		return "", errPrepare
	}
	if errInventory := cache.ensureUsageCacheBaseInventory(base); errInventory != nil {
		return "", errInventory
	}
	return base, nil
}

func (cache *ChatGPTWebUsageCache) ensureUsageCacheBaseInventory(base string) error {
	if cache == nil {
		return errors.New("chatgpt web usage cache is unavailable")
	}
	base = filepath.Clean(base)
	cache.mu.Lock()
	if _, inventoried := cache.inventoriedBases[base]; inventoried &&
		chatGPTWebUsageCachePathMatches(base, cache.inventoryBaseInfo[base]) {
		cache.mu.Unlock()
		return nil
	}
	delete(cache.inventoriedBases, base)
	delete(cache.inventoryBaseInfo, base)
	delete(cache.inventoryByBase, base)
	delete(cache.retainedOrphanBytes, base)
	cache.recomputeUsageCacheInventoryStatsLocked()
	if cache.closed {
		cache.mu.Unlock()
		return errors.New("chatgpt web usage cache is closed")
	}
	cache.mu.Unlock()

	_, errInventory, _ := cache.prepareGroup.Do("inventory:"+base, func() (any, error) {
		cache.mu.Lock()
		if _, inventoried := cache.inventoriedBases[base]; inventoried &&
			chatGPTWebUsageCachePathMatches(base, cache.inventoryBaseInfo[base]) {
			cache.mu.Unlock()
			return nil, nil
		}
		if cache.closed {
			cache.mu.Unlock()
			return nil, errors.New("chatgpt web usage cache is closed")
		}
		cache.mu.Unlock()
		infoBefore, errInfoBefore := os.Lstat(base)
		if errInfoBefore != nil || !infoBefore.IsDir() || infoBefore.Mode()&os.ModeSymlink != 0 {
			if errInfoBefore != nil {
				return nil, errInfoBefore
			}
			return nil, errors.New("usage cache base directory changed before inventory")
		}
		result, errScan := inventoryChatGPTWebUsageCacheBase(base)
		if errScan != nil {
			return nil, errScan
		}
		if !chatGPTWebUsageCachePathMatches(base, infoBefore) {
			return nil, errors.New("usage cache base directory changed during inventory")
		}
		cache.applyUsageCacheCleanupResult(base, result)
		cache.mu.Lock()
		if !cache.closed {
			if cache.inventoriedBases == nil {
				cache.inventoriedBases = make(map[string]struct{})
			}
			cache.inventoriedBases[base] = struct{}{}
			if cache.inventoryBaseInfo == nil {
				cache.inventoryBaseInfo = make(map[string]fs.FileInfo)
			}
			cache.inventoryBaseInfo[base] = infoBefore
		}
		cache.mu.Unlock()
		return nil, nil
	})
	return errInventory
}

func (cache *ChatGPTWebUsageCache) ensureUsageCacheBaseInventoryForOwned(
	owned *chatGPTWebUsageCacheOwnedDirectory,
) error {
	if cache == nil || owned == nil || owned.baseInfo == nil {
		return errors.New("usage cache directory is unavailable")
	}
	base := filepath.Clean(owned.base)
	cache.mu.Lock()
	inventoryInfo := cache.inventoryBaseInfo[base]
	if _, inventoried := cache.inventoriedBases[base]; inventoried &&
		inventoryInfo != nil && os.SameFile(inventoryInfo, owned.baseInfo) {
		cache.mu.Unlock()
		return nil
	}
	delete(cache.inventoriedBases, base)
	delete(cache.inventoryBaseInfo, base)
	delete(cache.inventoryByBase, base)
	delete(cache.retainedOrphanBytes, base)
	cache.recomputeUsageCacheInventoryStatsLocked()
	cache.mu.Unlock()
	if errInventory := cache.ensureUsageCacheBaseInventory(base); errInventory != nil {
		return errInventory
	}
	cache.mu.Lock()
	inventoryInfo = cache.inventoryBaseInfo[base]
	cache.mu.Unlock()
	if inventoryInfo == nil || !os.SameFile(inventoryInfo, owned.baseInfo) {
		return errors.New("usage cache base directory changed between inventory and ownership")
	}
	return nil
}

func (cache *ChatGPTWebUsageCache) ensureOwnedUsageCacheDirectory(
	configuredPath string,
	retention time.Duration,
) (string, error) {
	if !cache.beginUsageCacheOwnershipOperation() {
		return "", errors.New("chatgpt web usage cache is closed")
	}
	defer cache.ownershipWG.Done()
	owned, errOwned := cache.getOwnedUsageCacheDirectory(configuredPath, retention, false)
	if errOwned != nil {
		return "", errOwned
	}
	return owned.path, nil
}

func (cache *ChatGPTWebUsageCache) acquireOwnedUsageCacheDirectory(
	configuredPath string,
	retention time.Duration,
) (*chatGPTWebUsageCacheOwnedDirectory, error) {
	return cache.getOwnedUsageCacheDirectory(configuredPath, retention, true)
}

func (cache *ChatGPTWebUsageCache) getOwnedUsageCacheDirectory(
	configuredPath string,
	retention time.Duration,
	acquire bool,
) (*chatGPTWebUsageCacheOwnedDirectory, error) {
	base, errBase := cache.prepareUsageCacheBase(configuredPath, retention)
	if errBase != nil {
		return nil, errBase
	}
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return nil, errors.New("chatgpt web usage cache is closed")
	}
	if owned := cache.ownedDirectories[base]; owned != nil {
		if owned.invalid {
			cache.mu.Unlock()
			return nil, errors.New("chatgpt web usage cache directory is being replaced")
		}
		retired := cache.activateOwnedUsageCacheDirectoryLocked(base, owned, acquire)
		cache.scheduleRetiredOwnedUsageCacheDirectoriesLocked(retired)
		cache.mu.Unlock()
		cache.removeRetiredOwnedUsageCacheDirectories(retired)
		return owned, nil
	}
	cache.mu.Unlock()

	candidate, errCreate := createChatGPTWebUsageCacheOwnedDirectory(base, cache.startedAt)
	if errCreate != nil {
		return nil, errCreate
	}
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		_ = cache.removeOwnedUsageCacheDirectory(candidate)
		return nil, errors.New("chatgpt web usage cache is closed")
	}
	owned := cache.ownedDirectories[base]
	if owned == nil {
		owned = candidate
		cache.ownedDirectories[base] = owned
		candidate = nil
	}
	retired := cache.activateOwnedUsageCacheDirectoryLocked(base, owned, acquire)
	if candidate != nil {
		retired = append(retired, candidate)
	}
	cache.scheduleRetiredOwnedUsageCacheDirectoriesLocked(retired)
	cache.mu.Unlock()
	cache.removeRetiredOwnedUsageCacheDirectories(retired)
	return owned, nil
}

func (cache *ChatGPTWebUsageCache) activateOwnedUsageCacheDirectoryLocked(
	base string,
	owned *chatGPTWebUsageCacheOwnedDirectory,
	acquire bool,
) []*chatGPTWebUsageCacheOwnedDirectory {
	if acquire {
		owned.references++
	}
	cache.activeOwnedBase = base
	cache.stats.InstanceDirectory = owned.path
	cache.stats.OwnershipStatus = "owned"
	retired := make([]*chatGPTWebUsageCacheOwnedDirectory, 0)
	for candidateBase, candidate := range cache.ownedDirectories {
		if candidateBase == base || candidate == nil || candidate.references > 0 || cache.closed {
			continue
		}
		delete(cache.ownedDirectories, candidateBase)
		retired = append(retired, candidate)
	}
	return retired
}

func (cache *ChatGPTWebUsageCache) releaseOwnedUsageCacheDirectory(
	owned *chatGPTWebUsageCacheOwnedDirectory,
) {
	if cache == nil || owned == nil {
		return
	}
	var retired *chatGPTWebUsageCacheOwnedDirectory
	cache.mu.Lock()
	if owned.references > 0 {
		owned.references--
	}
	if !cache.closed && owned.references == 0 && (owned.invalid || owned.base != cache.activeOwnedBase) &&
		cache.ownedDirectories[owned.base] == owned {
		delete(cache.ownedDirectories, owned.base)
		retired = owned
		cache.scheduleRetiredOwnedUsageCacheDirectoriesLocked(
			[]*chatGPTWebUsageCacheOwnedDirectory{retired},
		)
	}
	cache.mu.Unlock()
	cache.removeRetiredOwnedUsageCacheDirectories([]*chatGPTWebUsageCacheOwnedDirectory{retired})
}

func (cache *ChatGPTWebUsageCache) invalidateOwnedUsageCacheDirectory(
	owned *chatGPTWebUsageCacheOwnedDirectory,
) {
	if cache == nil || owned == nil {
		return
	}
	var retired *chatGPTWebUsageCacheOwnedDirectory
	cache.mu.Lock()
	owned.invalid = true
	if cache.activeOwnedBase == owned.base {
		cache.activeOwnedBase = ""
		cache.stats.InstanceDirectory = ""
		cache.stats.OwnershipStatus = "unavailable"
	}
	if owned.references == 0 && cache.ownedDirectories[owned.base] == owned {
		delete(cache.ownedDirectories, owned.base)
		retired = owned
		cache.scheduleRetiredOwnedUsageCacheDirectoriesLocked(
			[]*chatGPTWebUsageCacheOwnedDirectory{retired},
		)
	}
	cache.mu.Unlock()
	cache.removeRetiredOwnedUsageCacheDirectories([]*chatGPTWebUsageCacheOwnedDirectory{retired})
}

func (cache *ChatGPTWebUsageCache) scheduleRetiredOwnedUsageCacheDirectoriesLocked(
	ownedDirectories []*chatGPTWebUsageCacheOwnedDirectory,
) {
	for _, owned := range ownedDirectories {
		if owned != nil {
			cache.retireWG.Add(1)
		}
	}
}

func (cache *ChatGPTWebUsageCache) removeRetiredOwnedUsageCacheDirectories(
	ownedDirectories []*chatGPTWebUsageCacheOwnedDirectory,
) {
	for _, owned := range ownedDirectories {
		if owned == nil {
			continue
		}
		func() {
			defer cache.retireWG.Done()
			if errRemove := cache.removeOwnedUsageCacheDirectory(owned); errRemove != nil {
				cache.recordUsageCacheCleanupError()
			}
		}()
	}
}

func (cache *ChatGPTWebUsageCache) removeOwnedUsageCacheDirectory(
	owned *chatGPTWebUsageCacheOwnedDirectory,
) error {
	remove := cache.removeOwnedDirectory
	if remove == nil {
		remove = removeChatGPTWebUsageCacheOwnedDirectory
	}
	return remove(owned)
}

func prepareChatGPTWebUsageCacheBase(configuredPath string) (string, error) {
	base := chatGPTWebUsageCacheResourcePath(configuredPath)
	if strings.TrimSpace(configuredPath) != "" {
		if errMkdir := os.MkdirAll(base, 0o700); errMkdir != nil {
			return "", errMkdir
		}
	}
	info, errInfo := os.Stat(base)
	if errInfo != nil {
		return "", errInfo
	}
	if !info.IsDir() {
		return "", fmt.Errorf("usage cache path is not a directory")
	}
	resolved, errResolved := filepath.EvalSymlinks(base)
	if errResolved != nil {
		return "", errResolved
	}
	return filepath.Clean(resolved), nil
}

func createChatGPTWebUsageCacheOwnedDirectory(
	base string,
	processStartedAt time.Time,
) (*chatGPTWebUsageCacheOwnedDirectory, error) {
	instanceID, errInstanceID := newChatGPTWebUsageCacheRandomID()
	if errInstanceID != nil {
		return nil, errInstanceID
	}
	directoryName := chatGPTWebUsageCacheDirectoryPrefix + instanceID
	path := filepath.Join(base, directoryName)
	baseRoot, errBaseRoot := os.OpenRoot(base)
	if errBaseRoot != nil {
		return nil, errBaseRoot
	}
	closeBaseRoot := true
	defer func() {
		if closeBaseRoot {
			_ = baseRoot.Close()
		}
	}()
	baseInfo, errBaseInfo := baseRoot.Lstat(".")
	if errBaseInfo != nil || !baseInfo.IsDir() || baseInfo.Mode()&os.ModeSymlink != 0 {
		if errBaseInfo != nil {
			return nil, errBaseInfo
		}
		return nil, errors.New("usage cache base directory changed during creation")
	}
	if errMkdir := baseRoot.Mkdir(directoryName, 0o700); errMkdir != nil {
		return nil, errMkdir
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = baseRoot.RemoveAll(directoryName)
		}
	}()

	root, errRoot := baseRoot.OpenRoot(directoryName)
	if errRoot != nil {
		return nil, errRoot
	}
	closeRoot := true
	defer func() {
		if closeRoot {
			_ = root.Close()
		}
	}()
	info, errInfo := root.Lstat(".")
	if errInfo != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		if errInfo != nil {
			return nil, errInfo
		}
		return nil, errors.New("usage cache instance directory changed during creation")
	}

	lockFile, errOpen := root.OpenFile(
		chatGPTWebUsageCacheOwnerLockName,
		os.O_RDWR|os.O_CREATE,
		0o600,
	)
	if errOpen != nil {
		return nil, errOpen
	}
	locked, errLock := lockChatGPTWebUsageFile(lockFile, false)
	if errLock != nil || !locked {
		_ = lockFile.Close()
		if errLock != nil {
			return nil, errLock
		}
		return nil, errors.New("usage cache owner lock was not acquired")
	}
	closeLock := true
	defer func() {
		if closeLock {
			_ = unlockChatGPTWebUsageFile(lockFile)
			_ = lockFile.Close()
		}
	}()

	now := time.Now().UTC()
	manifest := chatGPTWebUsageCacheOwnerManifest{
		Version:          chatGPTWebUsageCacheOwnerVersion,
		InstanceID:       instanceID,
		PID:              os.Getpid(),
		ProcessStartedAt: processStartedAt.UTC(),
		CreatedAt:        now,
	}
	data, errMarshal := json.Marshal(manifest)
	if errMarshal != nil {
		return nil, errMarshal
	}
	if errWrite := root.WriteFile(chatGPTWebUsageCacheOwnerManifestName, data, 0o600); errWrite != nil {
		return nil, errWrite
	}
	rootInfo, errRootInfo := root.Lstat(".")
	if errRootInfo != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(info, rootInfo) {
		if errRootInfo != nil {
			return nil, errRootInfo
		}
		return nil, errors.New("usage cache instance directory changed while opening")
	}
	currentBaseInfo, errCurrentBaseInfo := os.Lstat(base)
	if errCurrentBaseInfo != nil || !currentBaseInfo.IsDir() ||
		currentBaseInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(baseInfo, currentBaseInfo) {
		if errCurrentBaseInfo != nil {
			return nil, errCurrentBaseInfo
		}
		return nil, errors.New("usage cache base directory changed while opening")
	}
	_ = baseRoot.Close()
	closeBaseRoot = false
	cleanup = false
	closeLock = false
	closeRoot = false
	return &chatGPTWebUsageCacheOwnedDirectory{
		instanceID: instanceID,
		base:       base,
		path:       path,
		baseInfo:   baseInfo,
		info:       info,
		lockFile:   lockFile,
		root:       root,
	}, nil
}

func (owned *chatGPTWebUsageCacheOwnedDirectory) pathIdentityCurrent() bool {
	if owned == nil || owned.root == nil || owned.baseInfo == nil || owned.info == nil {
		return false
	}
	rootInfo, errRootInfo := owned.root.Lstat(".")
	if errRootInfo != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(owned.info, rootInfo) {
		return false
	}
	pathInfo, errPathInfo := os.Lstat(owned.path)
	if errPathInfo != nil || !pathInfo.IsDir() || pathInfo.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(owned.info, pathInfo) {
		return false
	}
	baseInfo, errBaseInfo := os.Lstat(owned.base)
	return errBaseInfo == nil && baseInfo.IsDir() && baseInfo.Mode()&os.ModeSymlink == 0 &&
		os.SameFile(owned.baseInfo, baseInfo)
}

func cleanupChatGPTWebUsageCacheBase(
	base string,
	retention time.Duration,
	now time.Time,
) (chatGPTWebUsageCacheCleanupResult, error) {
	return cleanupChatGPTWebUsageCacheBaseWithFileLock(
		base,
		retention,
		now,
		chatGPTWebUsageFileLockSupported(),
	)
}

func cleanupChatGPTWebUsageCacheBaseWithFileLock(
	base string,
	retention time.Duration,
	now time.Time,
	fileLockSupported bool,
) (chatGPTWebUsageCacheCleanupResult, error) {
	result := chatGPTWebUsageCacheCleanupResult{cleanedAt: now.UTC()}
	entries, errRead := os.ReadDir(base)
	if errRead != nil {
		result.cleanupErrors++
		return result, errRead
	}
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(base, name)
		if match := chatGPTWebUsageCacheCleanupPattern.FindStringSubmatch(name); len(match) == 3 {
			if !fileLockSupported {
				files, bytes := countRetainedChatGPTWebUsageCacheFiles(path)
				result.orphanDirectories++
				result.orphanFiles += files
				result.orphanBytes += bytes
				continue
			}
			files, bytes, active, valid, errInspect := inspectChatGPTWebUsageCleanupDirectory(path, match[1])
			if errInspect != nil || !valid {
				result.cleanupErrors++
				files, bytes = countRetainedChatGPTWebUsageCacheFiles(path)
				result.orphanDirectories++
				result.orphanFiles += files
				result.orphanBytes += bytes
				continue
			}
			if active {
				continue
			}
			if errRemove := removeInactiveChatGPTWebUsageCleanupDirectory(path, match[1]); errRemove != nil {
				result.cleanupErrors++
				result.orphanDirectories++
				result.orphanFiles += files
				result.orphanBytes += bytes
				continue
			}
			result.cleanupCount++
			continue
		}
		if match := chatGPTWebUsageCacheDirectoryPattern.FindStringSubmatch(name); len(match) == 2 {
			if !fileLockSupported {
				files, bytes := countRetainedChatGPTWebUsageCacheFiles(path)
				result.orphanDirectories++
				result.orphanFiles += files
				result.orphanBytes += bytes
				continue
			}
			files, bytes, _, active, valid, errInspect := inspectChatGPTWebUsageCacheDirectory(path, match[1])
			if errInspect != nil {
				result.cleanupErrors++
				files, bytes = countRetainedChatGPTWebUsageCacheFiles(path)
				result.orphanDirectories++
				result.orphanFiles += files
				result.orphanBytes += bytes
				continue
			}
			if !valid {
				result.cleanupErrors++
				files, bytes = countRetainedChatGPTWebUsageCacheFiles(path)
				result.orphanDirectories++
				result.orphanFiles += files
				result.orphanBytes += bytes
				continue
			}
			if active {
				continue
			}
			if retention > 0 {
				orphanedAt, errOrphanedAt := ensureChatGPTWebUsageCacheOrphanMarker(path, match[1], now)
				if errOrphanedAt != nil {
					result.cleanupErrors++
					result.orphanDirectories++
					result.orphanFiles += files
					result.orphanBytes += bytes
					continue
				}
				if now.Sub(orphanedAt) < retention {
					result.orphanDirectories++
					result.orphanFiles += files
					result.orphanBytes += bytes
					continue
				}
			}
			if errRemove := removeInactiveChatGPTWebUsageCacheDirectory(path, match[1]); errRemove != nil {
				result.cleanupErrors++
				result.orphanDirectories++
				result.orphanFiles += files
				result.orphanBytes += bytes
				continue
			}
			result.cleanupCount++
			continue
		}
		if chatGPTWebUsageCacheLegacyPattern.MatchString(name) && entry.IsDir() {
			files, bytes := countLegacyChatGPTWebUsageCacheFiles(path)
			result.legacyDirectories++
			result.legacyFiles += files
			result.legacyBytes += bytes
		}
	}
	legacyFiles, legacyBytes := countLegacyChatGPTWebUsageCacheFiles(base)
	result.legacyFiles += legacyFiles
	result.legacyBytes += legacyBytes
	return result, nil
}

func inventoryChatGPTWebUsageCacheBase(base string) (chatGPTWebUsageCacheCleanupResult, error) {
	return inventoryChatGPTWebUsageCacheBaseWithFileLock(base, chatGPTWebUsageFileLockSupported())
}

func inventoryChatGPTWebUsageCacheBaseWithFileLock(
	base string,
	fileLockSupported bool,
) (chatGPTWebUsageCacheCleanupResult, error) {
	var result chatGPTWebUsageCacheCleanupResult
	entries, errRead := os.ReadDir(base)
	if errRead != nil {
		result.cleanupErrors++
		return result, errRead
	}
	recordOrphan := func(files int, bytes int64) {
		result.orphanDirectories++
		result.orphanFiles += files
		result.orphanBytes += bytes
	}
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(base, name)
		if match := chatGPTWebUsageCacheCleanupPattern.FindStringSubmatch(name); len(match) == 3 {
			files, bytes, active, valid, errInspect := inventoryChatGPTWebUsageCleanupDirectory(
				path,
				match[1],
				fileLockSupported,
			)
			if errInspect != nil || !valid {
				result.cleanupErrors++
				files, bytes = countRetainedChatGPTWebUsageCacheFiles(path)
				recordOrphan(files, bytes)
				continue
			}
			if active {
				continue
			}
			recordOrphan(files, bytes)
			continue
		}
		if match := chatGPTWebUsageCacheDirectoryPattern.FindStringSubmatch(name); len(match) == 2 {
			files, bytes, active, valid, errInspect := inventoryChatGPTWebUsageCacheDirectory(
				path,
				match[1],
				fileLockSupported,
			)
			if errInspect != nil || !valid {
				result.cleanupErrors++
				files, bytes = countRetainedChatGPTWebUsageCacheFiles(path)
				recordOrphan(files, bytes)
				continue
			}
			if active {
				continue
			}
			recordOrphan(files, bytes)
			continue
		}
		if chatGPTWebUsageCacheLegacyPattern.MatchString(name) && entry.IsDir() {
			files, bytes := countLegacyChatGPTWebUsageCacheFiles(path)
			result.legacyDirectories++
			result.legacyFiles += files
			result.legacyBytes += bytes
		}
	}
	legacyFiles, legacyBytes := countLegacyChatGPTWebUsageCacheFiles(base)
	result.legacyFiles += legacyFiles
	result.legacyBytes += legacyBytes
	return result, nil
}

func inventoryChatGPTWebUsageCacheDirectory(
	path string,
	expectedID string,
	fileLockSupported bool,
) (int, int64, bool, bool, error) {
	if fileLockSupported {
		files, bytes, _, active, valid, errInspect := inspectChatGPTWebUsageCacheDirectory(path, expectedID)
		return files, bytes, active, valid, errInspect
	}
	files, bytes, _, valid, errValidate := validateChatGPTWebUsageCacheDirectory(path, expectedID)
	return files, bytes, false, valid, errValidate
}

func inventoryChatGPTWebUsageCleanupDirectory(
	path string,
	expectedID string,
	fileLockSupported bool,
) (int, int64, bool, bool, error) {
	if fileLockSupported {
		return inspectChatGPTWebUsageCleanupDirectory(path, expectedID)
	}
	root, entries, valid, errOpen := openChatGPTWebUsageCleanupRoot(path, nil, expectedID)
	if root != nil {
		_ = root.Close()
	}
	if errOpen != nil || !valid {
		return 0, 0, false, valid, errOpen
	}
	files, bytes := countChatGPTWebUsageFiles(entries)
	return files, bytes, false, true, nil
}

func inspectChatGPTWebUsageCacheDirectory(
	path string,
	expectedID string,
) (int, int64, chatGPTWebUsageCacheOwnerManifest, bool, bool, error) {
	root, directoryInfo, files, bytes, manifest, valid, errOpen := openValidatedChatGPTWebUsageCacheRoot(
		path,
		expectedID,
	)
	if errOpen != nil || !valid {
		return files, bytes, manifest, false, valid, errOpen
	}
	defer func() { _ = root.Close() }()
	lockFile, errOpen := root.OpenFile(chatGPTWebUsageCacheOwnerLockName, os.O_RDWR, 0)
	if errOpen != nil {
		return 0, 0, manifest, false, false, errOpen
	}
	locked, errLock := lockChatGPTWebUsageFile(lockFile, true)
	if errLock != nil {
		_ = lockFile.Close()
		return 0, 0, manifest, false, false, errLock
	}
	if !locked {
		_ = lockFile.Close()
		return files, bytes, manifest, true, true, nil
	}
	_, currentFiles, currentBytes, currentManifest, currentValid, errValidate :=
		validateChatGPTWebUsageCacheRoot(root, expectedID)
	if errValidate != nil || !currentValid || !chatGPTWebUsageCachePathMatches(path, directoryInfo) {
		_ = unlockChatGPTWebUsageFile(lockFile)
		_ = lockFile.Close()
		if errValidate != nil {
			return currentFiles, currentBytes, currentManifest, false, false, errValidate
		}
		return currentFiles, currentBytes, currentManifest, false, false,
			errors.New("usage cache directory changed while checking ownership")
	}
	_ = unlockChatGPTWebUsageFile(lockFile)
	_ = lockFile.Close()
	return currentFiles, currentBytes, currentManifest, false, true, nil
}

func inspectChatGPTWebUsageCleanupDirectory(path, expectedID string) (int, int64, bool, bool, error) {
	root, entries, valid, errOpen := openChatGPTWebUsageCleanupRoot(path, nil, expectedID)
	if errOpen != nil || !valid {
		return 0, 0, false, valid, errOpen
	}
	defer func() { _ = root.Close() }()
	files, bytes := countChatGPTWebUsageFiles(entries)
	lockPresent := false
	for _, entry := range entries {
		if entry.Name() == chatGPTWebUsageCacheOwnerLockName {
			lockPresent = true
			break
		}
	}
	if !lockPresent {
		return files, bytes, false, true, nil
	}
	lockFile, errLockFile := root.OpenFile(chatGPTWebUsageCacheOwnerLockName, os.O_RDWR, 0)
	if errLockFile != nil {
		return 0, 0, false, false, errLockFile
	}
	locked, errLock := lockChatGPTWebUsageFile(lockFile, true)
	if errLock != nil {
		_ = lockFile.Close()
		return 0, 0, false, false, errLock
	}
	if !locked {
		_ = lockFile.Close()
		return files, bytes, true, true, nil
	}
	_ = unlockChatGPTWebUsageFile(lockFile)
	_ = lockFile.Close()
	return files, bytes, false, true, nil
}

func validateChatGPTWebUsageCacheDirectory(
	path string,
	expectedID string,
) (int, int64, chatGPTWebUsageCacheOwnerManifest, bool, error) {
	root, _, files, bytes, manifest, valid, errOpen := openValidatedChatGPTWebUsageCacheRoot(
		path,
		expectedID,
	)
	if root != nil {
		_ = root.Close()
	}
	return files, bytes, manifest, valid, errOpen
}

func openValidatedChatGPTWebUsageCacheRoot(
	path string,
	expectedID string,
) (*os.Root, fs.FileInfo, int, int64, chatGPTWebUsageCacheOwnerManifest, bool, error) {
	var manifest chatGPTWebUsageCacheOwnerManifest
	root, errRoot := os.OpenRoot(path)
	if errRoot != nil {
		return nil, nil, 0, 0, manifest, false, errRoot
	}
	info, files, bytes, manifest, valid, errValidate := validateChatGPTWebUsageCacheRoot(root, expectedID)
	if errValidate != nil || !valid || !chatGPTWebUsageCachePathMatches(path, info) {
		_ = root.Close()
		if errValidate != nil {
			return nil, info, files, bytes, manifest, false, errValidate
		}
		return nil, info, files, bytes, manifest, false,
			errors.New("usage cache directory changed while opening")
	}
	return root, info, files, bytes, manifest, true, nil
}

func validateChatGPTWebUsageCacheRoot(
	root *os.Root,
	expectedID string,
) (fs.FileInfo, int, int64, chatGPTWebUsageCacheOwnerManifest, bool, error) {
	var manifest chatGPTWebUsageCacheOwnerManifest
	if root == nil {
		return nil, 0, 0, manifest, false, errors.New("usage cache directory is unavailable")
	}
	info, errInfo := root.Lstat(".")
	if errInfo != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return info, 0, 0, manifest, false, errInfo
	}
	entries, errRead := fs.ReadDir(root.FS(), ".")
	if errRead != nil {
		return info, 0, 0, manifest, false, errRead
	}
	lockPresent := false
	manifestPresent := false
	files := 0
	var bytes int64
	for _, entry := range entries {
		entryInfo, errEntry := root.Lstat(entry.Name())
		if errEntry != nil || entryInfo.Mode()&os.ModeSymlink != 0 || !entryInfo.Mode().IsRegular() {
			return info, 0, 0, manifest, false, errEntry
		}
		switch entry.Name() {
		case chatGPTWebUsageCacheOwnerLockName:
			lockPresent = true
		case chatGPTWebUsageCacheOwnerManifestName:
			manifestPresent = true
		case chatGPTWebUsageCacheOrphanMarkerName:
			if entryInfo.Size() != 0 {
				return info, 0, 0, manifest, false, nil
			}
		default:
			if !chatGPTWebUsageCacheFilePattern.MatchString(entry.Name()) {
				return info, 0, 0, manifest, false, nil
			}
			files++
			bytes += entryInfo.Size()
		}
	}
	if !lockPresent || !manifestPresent {
		return info, 0, 0, manifest, false, nil
	}
	manifest, errManifest := readChatGPTWebUsageCacheOwnerManifestFromRoot(root)
	if errManifest != nil {
		return info, 0, 0, manifest, false, errManifest
	}
	if manifest.Version != chatGPTWebUsageCacheOwnerVersion ||
		manifest.InstanceID != expectedID ||
		manifest.CreatedAt.IsZero() {
		return info, 0, 0, manifest, false, nil
	}
	return info, files, bytes, manifest, true, nil
}

func chatGPTWebUsageCachePathMatches(path string, expected fs.FileInfo) bool {
	if expected == nil {
		return false
	}
	info, errInfo := os.Lstat(path)
	return errInfo == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 &&
		os.SameFile(expected, info)
}

func readChatGPTWebUsageCacheOwnerManifest(path string) (chatGPTWebUsageCacheOwnerManifest, error) {
	root, errRoot := os.OpenRoot(path)
	if errRoot != nil {
		return chatGPTWebUsageCacheOwnerManifest{}, errRoot
	}
	defer func() { _ = root.Close() }()
	return readChatGPTWebUsageCacheOwnerManifestFromRoot(root)
}

func readChatGPTWebUsageCacheOwnerManifestFromRoot(root *os.Root) (chatGPTWebUsageCacheOwnerManifest, error) {
	var manifest chatGPTWebUsageCacheOwnerManifest
	if root == nil {
		return manifest, errors.New("usage cache owner root is unavailable")
	}
	data, errRead := root.ReadFile(chatGPTWebUsageCacheOwnerManifestName)
	if errRead != nil {
		return manifest, errRead
	}
	if len(data) > 4096 {
		return manifest, errors.New("usage cache owner manifest is too large")
	}
	if errDecode := json.Unmarshal(data, &manifest); errDecode != nil {
		return manifest, errDecode
	}
	return manifest, nil
}

func ensureChatGPTWebUsageCacheOrphanMarker(path, expectedID string, now time.Time) (time.Time, error) {
	root, directoryInfo, _, _, _, valid, errOpen := openValidatedChatGPTWebUsageCacheRoot(path, expectedID)
	if errOpen != nil || !valid {
		if errOpen != nil {
			return time.Time{}, errOpen
		}
		return time.Time{}, errors.New("usage cache directory changed while marking orphan")
	}
	defer func() { _ = root.Close() }()
	lockFile, errOpen := root.OpenFile(chatGPTWebUsageCacheOwnerLockName, os.O_RDWR, 0)
	if errOpen != nil {
		return time.Time{}, errOpen
	}
	locked, errLock := lockChatGPTWebUsageFile(lockFile, true)
	if errLock != nil {
		_ = lockFile.Close()
		return time.Time{}, errLock
	}
	if !locked {
		_ = lockFile.Close()
		return time.Time{}, errors.New("usage cache directory is active")
	}
	defer func() {
		_ = unlockChatGPTWebUsageFile(lockFile)
		_ = lockFile.Close()
	}()
	if _, _, _, _, valid, errValidate := validateChatGPTWebUsageCacheRoot(root, expectedID); errValidate != nil ||
		!valid || !chatGPTWebUsageCachePathMatches(path, directoryInfo) {
		if errValidate != nil {
			return time.Time{}, errValidate
		}
		return time.Time{}, errors.New("usage cache directory changed while marking orphan")
	}
	file, errCreate := root.OpenFile(
		chatGPTWebUsageCacheOrphanMarkerName,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if errCreate == nil {
		if errClose := file.Close(); errClose != nil {
			return time.Time{}, errClose
		}
		markerPath := filepath.Join(path, chatGPTWebUsageCacheOrphanMarkerName)
		if errTimes := os.Chtimes(markerPath, now, now); errTimes != nil {
			return time.Time{}, errTimes
		}
	} else if !errors.Is(errCreate, fs.ErrExist) {
		return time.Time{}, errCreate
	}
	info, errInfo := root.Lstat(chatGPTWebUsageCacheOrphanMarkerName)
	if errInfo != nil {
		return time.Time{}, errInfo
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != 0 {
		return time.Time{}, errors.New("usage cache orphan marker is invalid")
	}
	return info.ModTime().UTC(), nil
}

func removeInactiveChatGPTWebUsageCacheDirectory(path, expectedID string) error {
	root, directoryInfo, _, _, _, valid, errOpen := openValidatedChatGPTWebUsageCacheRoot(path, expectedID)
	if errOpen != nil || !valid {
		if errOpen != nil {
			return errOpen
		}
		return errors.New("usage cache directory changed during cleanup")
	}
	lockFile, errOpen := root.OpenFile(chatGPTWebUsageCacheOwnerLockName, os.O_RDWR, 0)
	if errOpen != nil {
		_ = root.Close()
		return errOpen
	}
	locked, errLock := lockChatGPTWebUsageFile(lockFile, true)
	if errLock != nil {
		_ = lockFile.Close()
		_ = root.Close()
		return errLock
	}
	if !locked {
		_ = lockFile.Close()
		_ = root.Close()
		return errors.New("usage cache directory is active")
	}
	releaseLock := func() error {
		errUnlock := unlockChatGPTWebUsageFile(lockFile)
		errClose := lockFile.Close()
		errCloseRoot := root.Close()
		return errors.Join(errUnlock, errClose, errCloseRoot)
	}
	if _, _, _, _, valid, errValidate := validateChatGPTWebUsageCacheRoot(root, expectedID); errValidate != nil ||
		!valid || !chatGPTWebUsageCachePathMatches(path, directoryInfo) {
		errRelease := releaseLock()
		if errValidate != nil {
			return errors.Join(errValidate, errRelease)
		}
		return errors.Join(errors.New("usage cache directory changed during cleanup"), errRelease)
	}
	if errCloseRoot := root.Close(); errCloseRoot != nil {
		return errors.Join(errCloseRoot, unlockChatGPTWebUsageFile(lockFile), lockFile.Close())
	}
	root = nil
	quarantinePath, errQuarantine := quarantineChatGPTWebUsageCacheDirectory(path, directoryInfo, expectedID)
	if errQuarantine != nil {
		return errors.Join(errQuarantine, unlockChatGPTWebUsageFile(lockFile), lockFile.Close())
	}
	errRelease := errors.Join(unlockChatGPTWebUsageFile(lockFile), lockFile.Close())
	errRemove := removeQuarantinedChatGPTWebUsageCacheDirectory(quarantinePath, directoryInfo, expectedID)
	return errors.Join(errRelease, errRemove)
}

func removeInactiveChatGPTWebUsageCleanupDirectory(path, expectedID string) error {
	root, entries, valid, errOpen := openChatGPTWebUsageCleanupRoot(path, nil, expectedID)
	if errOpen != nil {
		return errOpen
	}
	if !valid {
		_ = root.Close()
		return errors.New("usage cache cleanup directory is invalid")
	}
	lockPresent := false
	for _, entry := range entries {
		if entry.Name() == chatGPTWebUsageCacheOwnerLockName {
			lockPresent = true
			break
		}
	}
	var lockFile *os.File
	if lockPresent {
		var errLockFile error
		lockFile, errLockFile = root.OpenFile(chatGPTWebUsageCacheOwnerLockName, os.O_RDWR, 0)
		if errLockFile != nil {
			_ = root.Close()
			return errLockFile
		}
		locked, errLock := lockChatGPTWebUsageFile(lockFile, true)
		if errLock != nil || !locked {
			_ = lockFile.Close()
			_ = root.Close()
			if errLock != nil {
				return errLock
			}
			return errors.New("usage cache cleanup directory is active")
		}
	}
	if lockFile != nil {
		if errUnlock := unlockChatGPTWebUsageFile(lockFile); errUnlock != nil {
			_ = lockFile.Close()
			_ = root.Close()
			return errUnlock
		}
		if errClose := lockFile.Close(); errClose != nil {
			_ = root.Close()
			return errClose
		}
	}
	errRemove := removeChatGPTWebUsageCleanupRoot(root, entries)
	errCloseRoot := root.Close()
	if errRemove != nil {
		return errors.Join(errRemove, errCloseRoot)
	}
	errRemoveDirectory := os.Remove(path)
	return errors.Join(errCloseRoot, errRemoveDirectory)
}

func removeChatGPTWebUsageCacheOwnedDirectory(owned *chatGPTWebUsageCacheOwnedDirectory) error {
	if owned == nil {
		return nil
	}
	var errRootClose, errUnlock, errClose error
	if owned.root != nil {
		errRootClose = owned.root.Close()
		owned.root = nil
	}
	quarantinePath, errQuarantine := quarantineChatGPTWebUsageCacheDirectory(owned.path, owned.info, owned.instanceID)
	if owned.lockFile != nil {
		errUnlock = unlockChatGPTWebUsageFile(owned.lockFile)
		errClose = owned.lockFile.Close()
		owned.lockFile = nil
	}
	if errors.Is(errQuarantine, fs.ErrNotExist) {
		errQuarantine = nil
	}
	if errQuarantine != nil {
		return errors.Join(errQuarantine, errRootClose, errUnlock, errClose)
	}
	errRemove := removeQuarantinedChatGPTWebUsageCacheDirectory(
		quarantinePath,
		owned.info,
		owned.instanceID,
	)
	if errors.Is(errRemove, fs.ErrNotExist) {
		errRemove = nil
	}
	return errors.Join(errRootClose, errUnlock, errClose, errRemove)
}

func quarantineChatGPTWebUsageCacheDirectory(path string, expected fs.FileInfo, instanceID string) (string, error) {
	info, errInfo := os.Lstat(path)
	if errInfo != nil {
		return "", errInfo
	}
	if expected == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(expected, info) {
		return "", errors.New("usage cache directory identity changed during cleanup")
	}
	randomID, errRandomID := newChatGPTWebUsageCacheRandomID()
	if errRandomID != nil {
		return "", errRandomID
	}
	quarantinePath := filepath.Join(
		filepath.Dir(path),
		chatGPTWebUsageCacheCleanupPrefix+instanceID+"-"+randomID,
	)
	if errRename := os.Rename(path, quarantinePath); errRename != nil {
		return "", errRename
	}
	movedInfo, errMovedInfo := os.Lstat(quarantinePath)
	if errMovedInfo == nil && movedInfo.IsDir() && movedInfo.Mode()&os.ModeSymlink == 0 && os.SameFile(expected, movedInfo) {
		root, errRoot := os.OpenRoot(quarantinePath)
		if errRoot == nil {
			errMarker := root.WriteFile(
				chatGPTWebUsageCacheCleanupMarkerName,
				[]byte(instanceID),
				0o600,
			)
			errCloseRoot := root.Close()
			if errMarker == nil && errCloseRoot == nil {
				return quarantinePath, nil
			}
			_ = os.Remove(filepath.Join(quarantinePath, chatGPTWebUsageCacheCleanupMarkerName))
			errRestore := os.Rename(quarantinePath, path)
			return "", errors.Join(errMarker, errCloseRoot, errRestore)
		}
		errRestore := os.Rename(quarantinePath, path)
		return "", errors.Join(errRoot, errRestore)
	}
	errRestore := os.Rename(quarantinePath, path)
	if errMovedInfo != nil {
		return "", errors.Join(errMovedInfo, errRestore)
	}
	return "", errors.Join(errors.New("usage cache directory identity changed during quarantine"), errRestore)
}

func removeQuarantinedChatGPTWebUsageCacheDirectory(path string, expected fs.FileInfo, instanceID string) error {
	root, entries, valid, errOpen := openChatGPTWebUsageCleanupRoot(path, expected, instanceID)
	if errOpen != nil {
		return errOpen
	}
	if !valid {
		_ = root.Close()
		return errors.New("usage cache quarantine contents changed during cleanup")
	}
	errRemove := removeChatGPTWebUsageCleanupRoot(root, entries)
	errCloseRoot := root.Close()
	if errRemove != nil {
		return errors.Join(errRemove, errCloseRoot)
	}
	errRemoveDirectory := os.Remove(path)
	return errors.Join(errCloseRoot, errRemoveDirectory)
}

func openChatGPTWebUsageCleanupRoot(
	path string,
	expected fs.FileInfo,
	instanceID string,
) (*os.Root, []fs.DirEntry, bool, error) {
	root, errRoot := os.OpenRoot(path)
	if errRoot != nil {
		return nil, nil, false, errRoot
	}
	info, errInfo := root.Lstat(".")
	if errInfo != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		(expected != nil && !os.SameFile(expected, info)) {
		_ = root.Close()
		if errInfo != nil {
			return nil, nil, false, errInfo
		}
		return nil, nil, false, errors.New("usage cache cleanup directory identity changed")
	}
	entries, errRead := fs.ReadDir(root.FS(), ".")
	if errRead != nil {
		_ = root.Close()
		return nil, nil, false, errRead
	}
	if len(entries) == 0 {
		return root, entries, true, nil
	}
	cleanupMarkerPresent := false
	manifestPresent := false
	for _, entry := range entries {
		entryInfo, errEntry := root.Lstat(entry.Name())
		if errEntry != nil || entryInfo.Mode()&os.ModeSymlink != 0 || !entryInfo.Mode().IsRegular() {
			_ = root.Close()
			return nil, nil, false, errEntry
		}
		switch entry.Name() {
		case chatGPTWebUsageCacheOwnerLockName,
			chatGPTWebUsageCacheOrphanMarkerName:
		case chatGPTWebUsageCacheOwnerManifestName:
			manifestPresent = true
		case chatGPTWebUsageCacheCleanupMarkerName:
			cleanupMarkerPresent = true
		default:
			if !chatGPTWebUsageCacheFilePattern.MatchString(entry.Name()) {
				_ = root.Close()
				return nil, nil, false, nil
			}
		}
	}
	if !cleanupMarkerPresent && !manifestPresent {
		_ = root.Close()
		return nil, nil, false, nil
	}
	markerValid := false
	if cleanupMarkerPresent {
		marker, errMarker := root.ReadFile(chatGPTWebUsageCacheCleanupMarkerName)
		if errMarker == nil && strings.TrimSpace(string(marker)) == instanceID {
			markerValid = true
		}
	}
	if manifestPresent {
		data, errReadManifest := root.ReadFile(chatGPTWebUsageCacheOwnerManifestName)
		if errReadManifest != nil || len(data) > 4096 {
			_ = root.Close()
			if errReadManifest != nil {
				return nil, nil, false, errReadManifest
			}
			return nil, nil, false, errors.New("usage cache owner manifest is too large")
		}
		var manifest chatGPTWebUsageCacheOwnerManifest
		if errDecode := json.Unmarshal(data, &manifest); errDecode != nil ||
			manifest.Version != chatGPTWebUsageCacheOwnerVersion ||
			manifest.InstanceID != instanceID {
			_ = root.Close()
			if errDecode != nil {
				return nil, nil, false, errDecode
			}
			return nil, nil, false, nil
		}
	}
	if !manifestPresent && !markerValid {
		_ = root.Close()
		return nil, nil, false, nil
	}
	return root, entries, true, nil
}

func removeChatGPTWebUsageCleanupRoot(root *os.Root, entries []fs.DirEntry) error {
	if root == nil {
		return errors.New("usage cache cleanup root is unavailable")
	}
	removeByName := func(name string) error {
		for _, entry := range entries {
			if entry.Name() == name {
				return root.Remove(name)
			}
		}
		return nil
	}
	for _, entry := range entries {
		if !chatGPTWebUsageCacheFilePattern.MatchString(entry.Name()) {
			continue
		}
		if errRemove := root.Remove(entry.Name()); errRemove != nil {
			return errRemove
		}
	}
	for _, name := range []string{
		chatGPTWebUsageCacheOrphanMarkerName,
		chatGPTWebUsageCacheOwnerManifestName,
		chatGPTWebUsageCacheOwnerLockName,
		chatGPTWebUsageCacheCleanupMarkerName,
	} {
		if errRemove := removeByName(name); errRemove != nil {
			return errRemove
		}
	}
	return nil
}

func countChatGPTWebUsageFiles(entries []fs.DirEntry) (int, int64) {
	files := 0
	var bytes int64
	for _, entry := range entries {
		if !chatGPTWebUsageCacheFilePattern.MatchString(entry.Name()) {
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		files++
		bytes += info.Size()
	}
	return files, bytes
}

func newChatGPTWebUsageCacheRandomID() (string, error) {
	var randomID [16]byte
	if _, errRandom := rand.Read(randomID[:]); errRandom != nil {
		return "", errRandom
	}
	return hex.EncodeToString(randomID[:]), nil
}

func countLegacyChatGPTWebUsageCacheFiles(path string) (int, int64) {
	entries, errRead := os.ReadDir(path)
	if errRead != nil {
		return 0, 0
	}
	files := 0
	var bytes int64
	for _, entry := range entries {
		if !chatGPTWebUsageCacheFilePattern.MatchString(entry.Name()) {
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		files++
		bytes += info.Size()
	}
	return files, bytes
}

func countRetainedChatGPTWebUsageCacheFiles(path string) (int, int64) {
	info, errInfo := os.Lstat(path)
	if errInfo != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return 0, 0
	}
	entries, errRead := os.ReadDir(path)
	if errRead != nil {
		return 0, 0
	}
	files := 0
	var bytes int64
	for _, entry := range entries {
		info, errInfo := entry.Info()
		if errInfo != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if entry.Name() == chatGPTWebUsageCacheOwnerLockName ||
			entry.Name() == chatGPTWebUsageCacheOwnerManifestName {
			continue
		}
		files++
		bytes += info.Size()
	}
	return files, bytes
}

func (cache *ChatGPTWebUsageCache) applyUsageCacheCleanupResult(base string, result chatGPTWebUsageCacheCleanupResult) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.inventoryByBase == nil {
		cache.inventoryByBase = make(map[string]chatGPTWebUsageCacheCleanupResult)
	}
	base = filepath.Clean(base)
	cache.inventoryByBase[base] = result
	cache.retainedOrphanBytes[base] = result.orphanBytes
	cache.stats.CleanupCount += result.cleanupCount
	cache.stats.CleanupErrors += result.cleanupErrors
	cache.recomputeUsageCacheInventoryStatsLocked()
	if !result.cleanedAt.IsZero() {
		cleanedAt := result.cleanedAt
		cache.stats.LastCleanupAt = &cleanedAt
	}
}

func (cache *ChatGPTWebUsageCache) recomputeUsageCacheInventoryStatsLocked() {
	cache.stats.OrphanDirectoryCount = 0
	cache.stats.OrphanFileCount = 0
	cache.stats.OrphanBytes = 0
	cache.stats.LegacyDirectoryCount = 0
	cache.stats.LegacyFileCount = 0
	cache.stats.LegacyBytes = 0
	for _, inventory := range cache.inventoryByBase {
		cache.stats.OrphanDirectoryCount += inventory.orphanDirectories
		cache.stats.OrphanFileCount += inventory.orphanFiles
		cache.stats.OrphanBytes += inventory.orphanBytes
		cache.stats.LegacyDirectoryCount += inventory.legacyDirectories
		cache.stats.LegacyFileCount += inventory.legacyFiles
		cache.stats.LegacyBytes += inventory.legacyBytes
	}
	cache.stats.RetainedOrphanBytes = 0
	for _, bytes := range cache.retainedOrphanBytes {
		cache.stats.RetainedOrphanBytes += bytes
	}
}

func (cache *ChatGPTWebUsageCache) recordUsageCacheCleanupError() {
	cache.mu.Lock()
	cache.stats.CleanupErrors++
	cache.mu.Unlock()
}
