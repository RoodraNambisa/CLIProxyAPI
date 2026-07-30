package helps

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/systemmetrics"
	"golang.org/x/sync/singleflight"
)

const (
	chatGPTWebUsageRecordChunkBytes      = 64 << 10
	chatGPTWebUsageTokenizerOverlapBytes = 8 << 10
	chatGPTWebUsageResourceProbeAttempts = 3
)

// ChatGPTWebUsageCacheOptions is an immutable per-request cache configuration.
type ChatGPTWebUsageCacheOptions struct {
	Enabled                  bool
	DiskThresholdBytes       int64
	MaxDiskBytes             int64
	ResourceGuardEnabled     bool
	MinAvailableDiskBytes    int64
	MaxFilesystemUsedPercent int
	Path                     string
	OrphanRetention          time.Duration
	AutoOutputQuality        string
}

// ChatGPTWebUsageCacheSnapshot reports active storage and cumulative outcomes.
type ChatGPTWebUsageCacheSnapshot struct {
	ActiveMemoryEntries    int        `json:"active_memory_entries"`
	ActiveMemoryBytes      int64      `json:"active_memory_bytes"`
	ActiveDiskEntries      int        `json:"active_disk_entries"`
	ActiveDiskBytes        int64      `json:"active_disk_bytes"`
	PeakDiskBytes          int64      `json:"peak_disk_bytes"`
	SuccessfulCalculations uint64     `json:"successful_calculations"`
	FailedDiscards         uint64     `json:"failed_discards"`
	CapacityRejections     uint64     `json:"capacity_rejections"`
	ResourceRejections     uint64     `json:"resource_rejections"`
	WriteErrors            uint64     `json:"write_errors"`
	InstanceDirectory      string     `json:"instance_directory"`
	OwnershipStatus        string     `json:"ownership_status"`
	OrphanDirectoryCount   int        `json:"orphan_directory_count"`
	OrphanFileCount        int        `json:"orphan_file_count"`
	OrphanBytes            int64      `json:"orphan_bytes"`
	LegacyDirectoryCount   int        `json:"legacy_directory_count"`
	LegacyFileCount        int        `json:"legacy_file_count"`
	LegacyBytes            int64      `json:"legacy_bytes"`
	CleanupCount           uint64     `json:"cleanup_count"`
	CleanupErrors          uint64     `json:"cleanup_errors"`
	LastCleanupAt          *time.Time `json:"last_cleanup_at"`
	RetainedOrphanBytes    int64      `json:"retained_orphan_bytes"`
}

// ChatGPTWebUsageCacheError identifies a local cache failure before upstream generation starts.
type ChatGPTWebUsageCacheError struct {
	Code    string
	Message string
}

func (err *ChatGPTWebUsageCacheError) Error() string {
	if err == nil {
		return "chatgpt web usage cache unavailable"
	}
	return err.Message
}

type chatGPTWebUsageTextRecord struct {
	separator bool
	text      string
}

type chatGPTWebUsageResourceState struct {
	snapshot      systemmetrics.FilesystemSnapshot
	reservedBytes int64
	pendingBytes  int64
}

type chatGPTWebUsageFilesystemProbe struct {
	snapshot         systemmetrics.FilesystemSnapshot
	resourceStateKey string
	generation       uint64
}

// ChatGPTWebUsageImage describes one decoded image without retaining its payload.
type ChatGPTWebUsageImage struct {
	Model   string
	Detail  string
	Use     string
	Width   int
	Height  int
	Quality string
}

// ChatGPTWebUsageCache owns compact per-request accounting projections.
type ChatGPTWebUsageCache struct {
	mu                   sync.Mutex
	resourceMu           sync.Mutex
	createWG             sync.WaitGroup
	ownershipWG          sync.WaitGroup
	retireWG             sync.WaitGroup
	handles              map[*ChatGPTWebUsageProjection]struct{}
	ownedDirectories     map[string]*chatGPTWebUsageCacheOwnedDirectory
	activeOwnedBase      string
	retainedOrphanBytes  map[string]int64
	inventoryByBase      map[string]chatGPTWebUsageCacheCleanupResult
	inventoriedBases     map[string]struct{}
	inventoryBaseInfo    map[string]fs.FileInfo
	cleanupPrepared      bool
	startedAt            time.Time
	closed               bool
	stats                ChatGPTWebUsageCacheSnapshot
	resourceStates       map[string]*chatGPTWebUsageResourceState
	resourcePathKeys     map[string]string
	resourceGenerations  map[string]uint64
	resourceProbeGroup   singleflight.Group
	prepareGroup         singleflight.Group
	collectFilesystem    func(string) systemmetrics.FilesystemSnapshot
	removeProjectionFile func(*os.Root, string) error
	removeOwnedDirectory func(*chatGPTWebUsageCacheOwnedDirectory) error
	closeOnce            sync.Once
}

// ChatGPTWebUsageProjection retains only data required for hybrid accounting.
type ChatGPTWebUsageProjection struct {
	mu                  sync.Mutex
	manager             *ChatGPTWebUsageCache
	model               string
	autoOutputQuality   string
	imageTool           bool
	precomputeOnRelease bool
	inputPrecomputed    bool
	inputTextTokens     int64
	inputImageTokens    int64
	precomputeErrors    []error
	records             []chatGPTWebUsageTextRecord
	images              []ChatGPTWebUsageImage
	filePath            string
	fileName            string
	ownedDirectory      *chatGPTWebUsageCacheOwnedDirectory
	cacheBase           string
	resourceStateKey    string
	memoryBytes         int64
	diskBytes           int64
	closeOnce           sync.Once
}

// NewChatGPTWebUsageCache creates an empty usage projection manager.
func NewChatGPTWebUsageCache() *ChatGPTWebUsageCache {
	return &ChatGPTWebUsageCache{
		handles:             make(map[*ChatGPTWebUsageProjection]struct{}),
		ownedDirectories:    make(map[string]*chatGPTWebUsageCacheOwnedDirectory),
		retainedOrphanBytes: make(map[string]int64),
		inventoryByBase:     make(map[string]chatGPTWebUsageCacheCleanupResult),
		inventoriedBases:    make(map[string]struct{}),
		inventoryBaseInfo:   make(map[string]fs.FileInfo),
		startedAt:           time.Now(),
		resourceStates:      make(map[string]*chatGPTWebUsageResourceState),
		resourcePathKeys:    make(map[string]string),
		resourceGenerations: make(map[string]uint64),
		collectFilesystem:   systemmetrics.CollectFilesystem,
		removeProjectionFile: func(root *os.Root, name string) error {
			return root.Remove(name)
		},
		removeOwnedDirectory: removeChatGPTWebUsageCacheOwnedDirectory,
	}
}

// NewProjection captures text records without tokenizing them.
func (cache *ChatGPTWebUsageCache) NewProjection(model string, request ChatGPTWebRequest, options ChatGPTWebUsageCacheOptions) (*ChatGPTWebUsageProjection, error) {
	if cache == nil {
		return nil, &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_unavailable", Message: "chatgpt web usage cache is unavailable"}
	}
	records := chatGPTWebUsageTextRecords(request)
	memoryBytes, diskBytes := chatGPTWebUsageRecordSizes(records)
	projection := &ChatGPTWebUsageProjection{
		manager:             cache,
		model:               strings.TrimSpace(model),
		autoOutputQuality:   normalizeChatGPTWebOutputQuality(options.AutoOutputQuality),
		imageTool:           request.Image != nil,
		precomputeOnRelease: options.Enabled,
		records:             records,
		memoryBytes:         memoryBytes,
	}
	spill := options.Enabled && options.DiskThresholdBytes > 0 && memoryBytes >= options.DiskThresholdBytes
	projection.precomputeOnRelease = projection.precomputeOnRelease && !spill
	if !spill {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		if cache.closed {
			return nil, &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_unavailable", Message: "chatgpt web usage cache is closed"}
		}
		cache.handles[projection] = struct{}{}
		cache.stats.ActiveMemoryEntries++
		cache.stats.ActiveMemoryBytes += memoryBytes
		return projection, nil
	}
	if options.MaxDiskBytes <= 0 || diskBytes > options.MaxDiskBytes {
		cache.mu.Lock()
		cache.stats.CapacityRejections++
		cache.mu.Unlock()
		return nil, &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_full", Message: "chatgpt web usage cache capacity is exhausted"}
	}
	if !cache.beginUsageCacheOwnershipOperation() {
		return nil, &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_unavailable", Message: "chatgpt web usage cache is closed"}
	}
	defer cache.ownershipWG.Done()
	ownedDirectory, errOwned := cache.acquireOwnedUsageCacheDirectory(options.Path, options.OrphanRetention)
	if errOwned != nil {
		cache.mu.Lock()
		cache.stats.WriteErrors++
		cache.mu.Unlock()
		return nil, &ChatGPTWebUsageCacheError{
			Code:    "chatgpt_web_usage_cache_unavailable",
			Message: "chatgpt web usage cache is unavailable",
		}
	}
	releaseOwned := true
	defer func() {
		if releaseOwned {
			cache.releaseOwnedUsageCacheDirectory(ownedDirectory)
		}
	}()
	if errInventory := cache.ensureUsageCacheBaseInventoryForOwned(ownedDirectory); errInventory != nil {
		cache.invalidateOwnedUsageCacheDirectory(ownedDirectory)
		return nil, &ChatGPTWebUsageCacheError{
			Code:    "chatgpt_web_usage_cache_storage_unavailable",
			Message: "chatgpt web usage cache storage changed during inventory",
		}
	}
	resourceStateKey, errReserve := cache.reserveDiskProjectionForOwned(options, ownedDirectory, diskBytes)
	if errReserve != nil {
		if !ownedDirectory.pathIdentityCurrent() {
			cache.invalidateOwnedUsageCacheDirectory(ownedDirectory)
		}
		return nil, errReserve
	}
	defer cache.createWG.Done()
	projection.resourceStateKey = resourceStateKey
	projection.cacheBase = ownedDirectory.base

	if !ownedDirectory.pathIdentityCurrent() {
		cache.rollbackDiskProjection(resourceStateKey, diskBytes)
		cache.invalidateOwnedUsageCacheDirectory(ownedDirectory)
		return nil, &ChatGPTWebUsageCacheError{
			Code:    "chatgpt_web_usage_cache_storage_unavailable",
			Message: "chatgpt web usage cache storage changed during validation",
		}
	}
	fileName, path, errWrite := cache.writeProjectionFile(ownedDirectory, records, diskBytes)
	if errWrite != nil {
		cache.rollbackDiskProjection(resourceStateKey, diskBytes)
		return nil, &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_unavailable", Message: "chatgpt web usage cache is unavailable"}
	}
	projection.filePath = path
	projection.fileName = fileName
	projection.ownedDirectory = ownedDirectory
	projection.diskBytes = diskBytes
	projection.records = nil
	projection.memoryBytes = 0
	releaseOwned = false

	cache.mu.Lock()
	if cache.closed {
		cache.stats.ActiveDiskBytes -= diskBytes
		cache.mu.Unlock()
		if errRemove := projection.removeDiskFile(); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
			cache.recordRetainedUsageCacheFile(projection.cacheBase, projection.diskBytes)
		}
		cache.releaseOwnedUsageCacheDirectory(ownedDirectory)
		cache.completeDiskReservation(resourceStateKey, diskBytes)
		return nil, &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_unavailable", Message: "chatgpt web usage cache is closed"}
	}
	cache.handles[projection] = struct{}{}
	cache.stats.ActiveDiskEntries++
	if cache.stats.ActiveDiskBytes > cache.stats.PeakDiskBytes {
		cache.stats.PeakDiskBytes = cache.stats.ActiveDiskBytes
	}
	cache.mu.Unlock()
	cache.completeDiskReservation(resourceStateKey, diskBytes)
	return projection, nil
}

func (cache *ChatGPTWebUsageCache) beginUsageCacheOwnershipOperation() bool {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.closed {
		return false
	}
	cache.ownershipWG.Add(1)
	return true
}

func (cache *ChatGPTWebUsageCache) rollbackDiskProjection(resourceStateKey string, diskBytes int64) {
	cache.mu.Lock()
	cache.stats.ActiveDiskBytes -= diskBytes
	if cache.stats.ActiveDiskBytes < 0 {
		cache.stats.ActiveDiskBytes = 0
	}
	cache.stats.WriteErrors++
	cache.mu.Unlock()
	cache.completeDiskReservation(resourceStateKey, diskBytes)
}

func (cache *ChatGPTWebUsageCache) reserveDiskProjection(options ChatGPTWebUsageCacheOptions, diskBytes int64) error {
	resourcePath := chatGPTWebUsageCacheResourcePath(options.Path)
	_, err := cache.reserveDiskProjectionAtPath(options, resourcePath, resourcePath, diskBytes, nil)
	return err
}

func (cache *ChatGPTWebUsageCache) reserveDiskProjectionAtPath(
	options ChatGPTWebUsageCacheOptions,
	resourcePath string,
	capacityBase string,
	diskBytes int64,
	validateIdentity func() bool,
) (string, error) {
	for range chatGPTWebUsageResourceProbeAttempts {
		if validateIdentity != nil && !validateIdentity() {
			return "", &ChatGPTWebUsageCacheError{
				Code:    "chatgpt_web_usage_cache_storage_unavailable",
				Message: "chatgpt web usage cache storage changed during validation",
			}
		}
		cache.resourceMu.Lock()
		cache.mu.Lock()
		if cache.closed {
			cache.mu.Unlock()
			cache.resourceMu.Unlock()
			return "", &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_unavailable", Message: "chatgpt web usage cache is closed"}
		}
		if cache.exceedsMaximumLocked(capacityBase, options.MaxDiskBytes, diskBytes) {
			cache.stats.CapacityRejections++
			cache.mu.Unlock()
			cache.resourceMu.Unlock()
			return "", &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_full", Message: "chatgpt web usage cache capacity is exhausted"}
		}
		if !options.ResourceGuardEnabled {
			cache.reserveDiskProjectionLocked(diskBytes)
			cache.mu.Unlock()
			cache.resourceMu.Unlock()
			return "", nil
		}
		if cache.resourceStates == nil {
			cache.resourceStates = make(map[string]*chatGPTWebUsageResourceState)
		}
		if cache.resourcePathKeys == nil {
			cache.resourcePathKeys = make(map[string]string)
		}
		if cache.resourceGenerations == nil {
			cache.resourceGenerations = make(map[string]uint64)
		}
		resourceStateKey := cache.resourcePathKeys[resourcePath]
		state := cache.resourceStates[resourceStateKey]
		if state != nil &&
			state.pendingBytes > 0 &&
			state.snapshot.Status == systemmetrics.FilesystemStatusOK {
			if errResource := chatGPTWebUsageCacheResourceError(
				state.snapshot,
				state.reservedBytes,
				diskBytes,
				options.MinAvailableDiskBytes,
				options.MaxFilesystemUsedPercent,
			); errResource != nil {
				cache.stats.ResourceRejections++
				cache.mu.Unlock()
				cache.resourceMu.Unlock()
				return "", errResource
			}
			cache.reserveDiskProjectionLocked(diskBytes)
			state.reservedBytes += diskBytes
			state.pendingBytes += diskBytes
			cache.mu.Unlock()
			cache.resourceMu.Unlock()
			return resourceStateKey, nil
		}
		delete(cache.resourcePathKeys, resourcePath)
		cache.mu.Unlock()
		cache.resourceMu.Unlock()

		probe := cache.collectFilesystemSnapshot(resourcePath)
		snapshot := probe.snapshot
		resourceStateKey = probe.resourceStateKey
		if validateIdentity != nil && !validateIdentity() {
			return "", &ChatGPTWebUsageCacheError{
				Code:    "chatgpt_web_usage_cache_storage_unavailable",
				Message: "chatgpt web usage cache storage changed during validation",
			}
		}

		cache.resourceMu.Lock()
		cache.mu.Lock()
		if cache.closed {
			cache.mu.Unlock()
			cache.resourceMu.Unlock()
			return "", &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_unavailable", Message: "chatgpt web usage cache is closed"}
		}
		if cache.exceedsMaximumLocked(capacityBase, options.MaxDiskBytes, diskBytes) {
			cache.stats.CapacityRejections++
			cache.mu.Unlock()
			cache.resourceMu.Unlock()
			return "", &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_full", Message: "chatgpt web usage cache capacity is exhausted"}
		}
		if state = cache.resourceStates[resourceStateKey]; state != nil &&
			state.pendingBytes > 0 &&
			state.snapshot.Status == systemmetrics.FilesystemStatusOK {
			if errResource := chatGPTWebUsageCacheResourceError(
				state.snapshot,
				state.reservedBytes,
				diskBytes,
				options.MinAvailableDiskBytes,
				options.MaxFilesystemUsedPercent,
			); errResource != nil {
				cache.stats.ResourceRejections++
				cache.mu.Unlock()
				cache.resourceMu.Unlock()
				return "", errResource
			}
			cache.reserveDiskProjectionLocked(diskBytes)
			state.reservedBytes += diskBytes
			state.pendingBytes += diskBytes
			cache.resourcePathKeys[resourcePath] = resourceStateKey
			cache.mu.Unlock()
			cache.resourceMu.Unlock()
			return resourceStateKey, nil
		}
		if cache.resourceGenerations[resourceStateKey] != probe.generation {
			cache.mu.Unlock()
			cache.resourceMu.Unlock()
			continue
		}
		if errResource := chatGPTWebUsageCacheResourceError(
			snapshot,
			0,
			diskBytes,
			options.MinAvailableDiskBytes,
			options.MaxFilesystemUsedPercent,
		); errResource != nil {
			cache.stats.ResourceRejections++
			cache.mu.Unlock()
			cache.resourceMu.Unlock()
			return "", errResource
		}
		cache.reserveDiskProjectionLocked(diskBytes)
		cache.resourceStates[resourceStateKey] = &chatGPTWebUsageResourceState{
			snapshot:      snapshot,
			reservedBytes: diskBytes,
			pendingBytes:  diskBytes,
		}
		cache.resourcePathKeys[resourcePath] = resourceStateKey
		cache.mu.Unlock()
		cache.resourceMu.Unlock()
		return resourceStateKey, nil
	}
	cache.mu.Lock()
	cache.stats.ResourceRejections++
	cache.mu.Unlock()
	return "", &ChatGPTWebUsageCacheError{
		Code:    "chatgpt_web_usage_cache_storage_unavailable",
		Message: "chatgpt web usage cache storage capacity changed during validation",
	}
}

func (cache *ChatGPTWebUsageCache) reserveDiskProjectionForOwned(
	options ChatGPTWebUsageCacheOptions,
	owned *chatGPTWebUsageCacheOwnedDirectory,
	diskBytes int64,
) (string, error) {
	if owned == nil {
		return "", &ChatGPTWebUsageCacheError{
			Code:    "chatgpt_web_usage_cache_storage_unavailable",
			Message: "chatgpt web usage cache storage is unavailable",
		}
	}
	return cache.reserveDiskProjectionAtPath(
		options,
		owned.path,
		owned.base,
		diskBytes,
		owned.pathIdentityCurrent,
	)
}

func (cache *ChatGPTWebUsageCache) collectFilesystemSnapshot(resourcePath string) chatGPTWebUsageFilesystemProbe {
	probe, _, _ := cache.resourceProbeGroup.Do(resourcePath, func() (any, error) {
		cache.resourceMu.Lock()
		generations := make(map[string]uint64, len(cache.resourceGenerations))
		for resourceStateKey, generation := range cache.resourceGenerations {
			generations[resourceStateKey] = generation
		}
		cache.resourceMu.Unlock()
		collect := cache.collectFilesystem
		if collect == nil {
			collect = systemmetrics.CollectFilesystem
		}
		snapshot := collect(resourcePath)
		resourceStateKey := chatGPTWebUsageCacheResourceStateKey(snapshot, resourcePath)
		return chatGPTWebUsageFilesystemProbe{
			snapshot:         snapshot,
			resourceStateKey: resourceStateKey,
			generation:       generations[resourceStateKey],
		}, nil
	})
	return probe.(chatGPTWebUsageFilesystemProbe)
}

func (cache *ChatGPTWebUsageCache) reserveDiskProjectionLocked(diskBytes int64) {
	cache.stats.ActiveDiskBytes += diskBytes
	cache.createWG.Add(1)
}

func chatGPTWebUsageCacheExceedsMaximum(
	stats ChatGPTWebUsageCacheSnapshot,
	maxDiskBytes int64,
	requestBytes int64,
) bool {
	if maxDiskBytes <= 0 || requestBytes < 0 || requestBytes > maxDiskBytes {
		return true
	}
	usedBytes := stats.ActiveDiskBytes + stats.RetainedOrphanBytes
	if usedBytes < stats.ActiveDiskBytes {
		return true
	}
	return usedBytes > maxDiskBytes-requestBytes
}

func (cache *ChatGPTWebUsageCache) exceedsMaximumLocked(resourcePath string, maxDiskBytes, requestBytes int64) bool {
	stats := cache.stats
	stats.RetainedOrphanBytes = cache.retainedOrphanBytes[filepath.Clean(resourcePath)]
	return chatGPTWebUsageCacheExceedsMaximum(stats, maxDiskBytes, requestBytes)
}

func (cache *ChatGPTWebUsageCache) completeDiskReservation(resourceStateKey string, diskBytes int64) {
	if cache == nil || resourceStateKey == "" || diskBytes <= 0 {
		return
	}
	cache.resourceMu.Lock()
	defer cache.resourceMu.Unlock()
	state := cache.resourceStates[resourceStateKey]
	if state == nil {
		if mappedKey := cache.resourcePathKeys[resourceStateKey]; mappedKey != "" {
			resourceStateKey = mappedKey
			state = cache.resourceStates[resourceStateKey]
		}
	}
	if state == nil {
		return
	}
	if cache.resourceGenerations == nil {
		cache.resourceGenerations = make(map[string]uint64)
	}
	cache.resourceGenerations[resourceStateKey]++
	state.pendingBytes -= diskBytes
	if state.pendingBytes <= 0 {
		delete(cache.resourceStates, resourceStateKey)
		for resourcePath, mappedKey := range cache.resourcePathKeys {
			if mappedKey == resourceStateKey {
				delete(cache.resourcePathKeys, resourcePath)
			}
		}
	}
}

func chatGPTWebUsageCacheResourcePath(configuredPath string) string {
	path := strings.TrimSpace(configuredPath)
	if path == "" {
		path = os.TempDir()
	}
	if absolutePath, err := filepath.Abs(path); err == nil {
		path = absolutePath
	}
	return filepath.Clean(path)
}

func chatGPTWebUsageCacheResourceStateKey(
	snapshot systemmetrics.FilesystemSnapshot,
	resourcePath string,
) string {
	if filesystemID := strings.TrimSpace(snapshot.FilesystemID); filesystemID != "" {
		return "filesystem:" + filesystemID
	}
	return "path:" + resourcePath
}

func chatGPTWebUsageCacheResourceError(
	snapshot systemmetrics.FilesystemSnapshot,
	pendingBytes int64,
	requestBytes int64,
	minAvailableBytes int64,
	maxUsedPercent int,
) error {
	if snapshot.Status != systemmetrics.FilesystemStatusOK || snapshot.TotalBytes == 0 {
		return &ChatGPTWebUsageCacheError{
			Code:    "chatgpt_web_usage_cache_storage_unavailable",
			Message: "chatgpt web usage cache storage capacity is unavailable",
		}
	}
	requiredBytes := nonNegativeUint64(pendingBytes) + nonNegativeUint64(requestBytes)
	if requiredBytes > snapshot.AvailableBytes ||
		snapshot.AvailableBytes-requiredBytes < nonNegativeUint64(minAvailableBytes) {
		return &ChatGPTWebUsageCacheError{
			Code:    "chatgpt_web_usage_cache_disk_pressure",
			Message: "chatgpt web usage cache disk resource threshold is exceeded",
		}
	}
	if maxUsedPercent > 0 {
		predictedUsedBytes := snapshot.UsedBytes + requiredBytes
		if predictedUsedBytes < snapshot.UsedBytes || predictedUsedBytes > snapshot.TotalBytes {
			predictedUsedBytes = snapshot.TotalBytes
		}
		if float64(predictedUsedBytes)/float64(snapshot.TotalBytes)*100 >= float64(maxUsedPercent) {
			return &ChatGPTWebUsageCacheError{
				Code:    "chatgpt_web_usage_cache_disk_pressure",
				Message: "chatgpt web usage cache disk resource threshold is exceeded",
			}
		}
	}
	return nil
}

func nonNegativeUint64(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

func chatGPTWebUsageTextRecords(request ChatGPTWebRequest) []chatGPTWebUsageTextRecord {
	segments := chatGPTWebTextTokenSegments(request)
	records := make([]chatGPTWebUsageTextRecord, 0, len(segments))
	for segmentIndex, segment := range segments {
		records = append(records, chatGPTWebUsageTextRecord{
			separator: segmentIndex > 0,
			text:      segment,
		})
	}
	return records
}

func chatGPTWebUsageRecordSizes(records []chatGPTWebUsageTextRecord) (memoryBytes, diskBytes int64) {
	for _, record := range records {
		memoryBytes += int64(len(record.text))
		_ = forEachChatGPTWebUsageRecordChunk(record, func(_ bool, text string) error {
			diskBytes += int64(5 + len(text))
			return nil
		})
	}
	return memoryBytes, diskBytes
}

func forEachChatGPTWebUsageRecordChunk(record chatGPTWebUsageTextRecord, visit func(bool, string) error) error {
	if visit == nil {
		return nil
	}
	firstChunk := true
	for len(record.text) > 0 {
		end := min(len(record.text), chatGPTWebUsageRecordChunkBytes)
		for end < len(record.text) && end > 0 && !isUTF8RuneStart(record.text[end]) {
			end--
		}
		if end == 0 {
			end = min(len(record.text), chatGPTWebUsageRecordChunkBytes)
		}
		if err := visit(record.separator && firstChunk, record.text[:end]); err != nil {
			return err
		}
		firstChunk = false
		record.text = record.text[end:]
	}
	return nil
}

func (cache *ChatGPTWebUsageCache) writeProjectionFile(
	owned *chatGPTWebUsageCacheOwnedDirectory,
	records []chatGPTWebUsageTextRecord,
	estimatedDiskBytes int64,
) (string, string, error) {
	if owned == nil || owned.root == nil || !owned.pathIdentityCurrent() {
		return "", "", errors.New("usage cache directory identity changed before write")
	}
	var (
		file      *os.File
		fileName  string
		errCreate error
	)
	for range 4 {
		randomID, errRandomID := newChatGPTWebUsageCacheRandomID()
		if errRandomID != nil {
			return "", "", errRandomID
		}
		fileName = "usage-" + randomID + ".bin"
		file, errCreate = owned.root.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if errCreate == nil {
			break
		}
		if !errors.Is(errCreate, fs.ErrExist) {
			return "", "", errCreate
		}
	}
	if file == nil {
		return "", "", errCreate
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
			cache.cleanupFailedProjectionFile(owned, fileName, estimatedDiskBytes)
		}
	}()
	if errChmod := file.Chmod(0o600); errChmod != nil {
		return "", "", errChmod
	}
	writer := bufio.NewWriterSize(file, 64<<10)
	var header [5]byte
	for _, record := range records {
		if errChunk := forEachChatGPTWebUsageRecordChunk(record, func(separator bool, text string) error {
			if separator {
				header[0] = 1
			} else {
				header[0] = 0
			}
			binary.BigEndian.PutUint32(header[1:], uint32(len(text)))
			if _, errWrite := writer.Write(header[:]); errWrite != nil {
				return errWrite
			}
			if _, errWrite := io.WriteString(writer, text); errWrite != nil {
				return errWrite
			}
			return nil
		}); errChunk != nil {
			return "", "", errChunk
		}
	}
	if errFlush := writer.Flush(); errFlush != nil {
		return "", "", errFlush
	}
	if errClose := file.Close(); errClose != nil {
		return "", "", errClose
	}
	cleanup = false
	return fileName, filepath.Join(owned.path, fileName), nil
}

func (cache *ChatGPTWebUsageCache) cleanupFailedProjectionFile(
	owned *chatGPTWebUsageCacheOwnedDirectory,
	fileName string,
	estimatedDiskBytes int64,
) {
	if cache == nil || owned == nil || owned.root == nil || fileName == "" {
		return
	}
	remove := cache.removeProjectionFile
	if remove == nil {
		remove = func(root *os.Root, name string) error {
			return root.Remove(name)
		}
	}
	if errRemove := remove(owned.root, fileName); errRemove == nil || errors.Is(errRemove, os.ErrNotExist) {
		return
	}
	retainedBytes := estimatedDiskBytes
	if info, errInfo := owned.root.Lstat(fileName); errInfo == nil && info.Mode().IsRegular() && info.Size() >= 0 {
		retainedBytes = info.Size()
	}
	cache.recordRetainedUsageCacheFile(owned.base, retainedBytes)
}

func (cache *ChatGPTWebUsageCache) recordRetainedUsageCacheFile(base string, bytes int64) {
	if cache == nil {
		return
	}
	if bytes < 0 {
		bytes = 0
	}
	base = filepath.Clean(base)
	cache.mu.Lock()
	cache.retainedOrphanBytes[base] += bytes
	inventory := cache.inventoryByBase[base]
	inventory.orphanDirectories = max(inventory.orphanDirectories, 1)
	inventory.orphanFiles++
	inventory.orphanBytes += bytes
	cache.inventoryByBase[base] = inventory
	cache.stats.CleanupErrors++
	cache.recomputeUsageCacheInventoryStatsLocked()
	cache.mu.Unlock()
}

// AddImage adds a compact image descriptor after upload or download validation.
func (projection *ChatGPTWebUsageProjection) AddImage(image ChatGPTWebUsageImage) {
	if projection == nil || image.Width <= 0 || image.Height <= 0 {
		return
	}
	projection.mu.Lock()
	defer projection.mu.Unlock()
	projection.images = append(projection.images, image)
}

// Model returns the route model captured with the projection.
func (projection *ChatGPTWebUsageProjection) Model() string {
	if projection == nil {
		return ""
	}
	projection.mu.Lock()
	defer projection.mu.Unlock()
	return projection.model
}

// PrecomputeInput converts small in-memory projections to token counts before
// the original request body is released. Disk-backed projections remain lazy.
func (projection *ChatGPTWebUsageProjection) PrecomputeInput() []error {
	if projection == nil {
		return nil
	}
	projection.mu.Lock()
	defer projection.mu.Unlock()
	if !projection.precomputeOnRelease || projection.inputPrecomputed {
		return nil
	}

	inputTextTokens, estimateErrors := projection.countInputTextTokensLocked()
	var inputImageTokens int64
	for _, image := range projection.images {
		inputImageTokens += ChatGPTWebImageTokenCount(image.Model, image.Detail, image.Width, image.Height)
	}
	projection.inputPrecomputed = true
	projection.inputTextTokens = inputTextTokens
	projection.inputImageTokens = inputImageTokens
	projection.precomputeErrors = append([]error(nil), estimateErrors...)
	projection.records = nil
	projection.images = nil
	oldMemoryBytes := projection.memoryBytes
	projection.memoryBytes = 0
	projection.manager.reduceProjectionMemory(projection, oldMemoryBytes)
	return append([]error(nil), estimateErrors...)
}

// Estimate calculates usage from compact records after a successful terminal response.
func (projection *ChatGPTWebUsageProjection) Estimate(outputText string, outputImages []ChatGPTWebUsageImage) (map[string]any, []error) {
	if projection == nil {
		return nil, nil
	}
	projection.mu.Lock()
	defer projection.mu.Unlock()

	var inputTextTokens, outputTextTokens int64
	errorsFound := append([]error(nil), projection.precomputeErrors...)
	if projection.inputPrecomputed {
		inputTextTokens = projection.inputTextTokens
	} else {
		inputTextTokens, errorsFound = projection.countInputTextTokensLocked()
	}
	if outputText != "" {
		encoder, errEncoder := TokenizerForModel(projection.model)
		if errEncoder != nil {
			errorsFound = append(errorsFound, errEncoder)
		} else if count, errCount := encoder.Count(outputText); errCount != nil {
			errorsFound = append(errorsFound, errCount)
		} else {
			outputTextTokens = int64(count)
		}
	}

	inputImageTokens := projection.inputImageTokens
	if !projection.inputPrecomputed {
		for _, image := range projection.images {
			inputImageTokens += ChatGPTWebImageTokenCount(image.Model, image.Detail, image.Width, image.Height)
		}
	}
	var outputImageTokens int64
	for _, image := range outputImages {
		tokens, errCount := ChatGPTWebImageOutputTokenCount(image.Model, image.Quality, projection.autoOutputQuality, image.Width, image.Height)
		if errCount != nil {
			errorsFound = append(errorsFound, errCount)
			continue
		}
		outputImageTokens += tokens
	}
	usage := chatGPTWebUsageMap(inputTextTokens, inputImageTokens, outputTextTokens, 0)
	if projection.imageTool {
		usage["tool_usage"] = map[string]any{
			"image_gen": ChatGPTWebImageUsageMap(inputTextTokens, inputImageTokens, 0, outputImageTokens),
		}
	}
	return usage, errorsFound
}

func (projection *ChatGPTWebUsageProjection) countInputTextTokensLocked() (int64, []error) {
	encoder, errEncoder := TokenizerForModel(projection.model)
	if errEncoder != nil {
		return 0, []error{errEncoder}
	}
	countText := func(text string) (int64, error) {
		count, errCount := encoder.Count(text)
		return int64(count), errCount
	}
	var inputTextTokens int64
	if projection.filePath == "" {
		for _, record := range projection.records {
			if record.separator {
				count, errCount := countText("\n")
				if errCount != nil {
					return inputTextTokens, []error{errCount}
				}
				inputTextTokens += count
			}
			count, errCount := countText(record.text)
			if errCount != nil {
				return inputTextTokens, []error{errCount}
			}
			inputTextTokens += count
		}
		return inputTextTokens, nil
	}

	pending := make([]byte, 0, chatGPTWebUsageRecordChunkBytes+chatGPTWebUsageTokenizerOverlapBytes)
	countPending := func(final bool) error {
		if len(pending) == 0 {
			return nil
		}
		ids, tokens, errEncode := encoder.Encode(string(pending))
		if errEncode != nil {
			return errEncode
		}
		if final {
			inputTextTokens += int64(len(ids))
			pending = pending[:0]
			return nil
		}
		cutoff := len(pending) - chatGPTWebUsageTokenizerOverlapBytes
		if cutoff <= 0 {
			return nil
		}
		safeBytes := 0
		safeTokens := 0
		consumedBytes := 0
		for index, token := range tokens {
			consumedBytes += len(token)
			if consumedBytes > cutoff {
				break
			}
			if utf8.Valid(pending[:consumedBytes]) {
				safeBytes = consumedBytes
				safeTokens = index + 1
			}
		}
		if safeBytes == 0 {
			return nil
		}
		inputTextTokens += int64(safeTokens)
		copy(pending, pending[safeBytes:])
		pending = pending[:len(pending)-safeBytes]
		return nil
	}
	errInput := projection.forEachTextRecord(func(separator bool, text string) error {
		if separator {
			if errFlush := countPending(true); errFlush != nil {
				return errFlush
			}
			count, errCount := countText("\n")
			if errCount != nil {
				return errCount
			}
			inputTextTokens += count
		}
		pending = append(pending, text...)
		return countPending(false)
	})
	if errInput != nil {
		return inputTextTokens, []error{errInput}
	}
	if errFlush := countPending(true); errFlush != nil {
		return inputTextTokens, []error{errFlush}
	}
	return inputTextTokens, nil
}

func chatGPTWebUsageMap(inputText, inputImage, outputText, outputImage int64) map[string]any {
	input := inputText + inputImage
	output := outputText + outputImage
	return map[string]any{
		"input_tokens":  input,
		"output_tokens": output,
		"total_tokens":  input + output,
		"input_tokens_details": map[string]any{
			"cached_tokens": 0,
			"text_tokens":   inputText,
			"image_tokens":  inputImage,
		},
		"output_tokens_details": map[string]any{
			"reasoning_tokens": 0,
			"text_tokens":      outputText,
			"image_tokens":     outputImage,
		},
	}
}

// ChatGPTWebImageUsageMap returns the usage shape emitted by the Images API.
func ChatGPTWebImageUsageMap(inputText, inputImage, outputText, outputImage int64) map[string]any {
	input := addChatGPTWebImageUsageTokens(inputText, inputImage)
	output := addChatGPTWebImageUsageTokens(outputText, outputImage)
	return map[string]any{
		"input_tokens":  input,
		"output_tokens": output,
		"total_tokens":  addChatGPTWebImageUsageTokens(input, output),
		"input_tokens_details": map[string]any{
			"text_tokens":  inputText,
			"image_tokens": inputImage,
		},
		"output_tokens_details": map[string]any{
			"text_tokens":  outputText,
			"image_tokens": outputImage,
		},
	}
}

func addChatGPTWebImageUsageTokens(left, right int64) int64 {
	if left < 0 {
		left = 0
	}
	if right < 0 {
		right = 0
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if left > maxInt64-right {
		return maxInt64
	}
	return left + right
}

func (projection *ChatGPTWebUsageProjection) forEachTextRecord(visit func(bool, string) error) error {
	if projection.filePath == "" {
		for _, record := range projection.records {
			if err := visit(record.separator, record.text); err != nil {
				return err
			}
		}
		return nil
	}
	file, errOpen := projection.openDiskFile()
	if errOpen != nil {
		return errOpen
	}
	defer func() { _ = file.Close() }()
	reader := bufio.NewReaderSize(file, 64<<10)
	var header [5]byte
	for {
		_, errRead := io.ReadFull(reader, header[:])
		if errors.Is(errRead, io.EOF) {
			return nil
		}
		if errRead != nil {
			return errRead
		}
		length := int(binary.BigEndian.Uint32(header[1:]))
		if length < 0 || length > chatGPTWebUsageRecordChunkBytes {
			return fmt.Errorf("chatgpt web usage cache record exceeds %d bytes", chatGPTWebUsageRecordChunkBytes)
		}
		data := make([]byte, length)
		if _, errRead = io.ReadFull(reader, data); errRead != nil {
			return errRead
		}
		if errVisit := visit(header[0] == 1, string(data)); errVisit != nil {
			return errVisit
		}
	}
}

// Complete releases projection storage and records a successful calculation.
func (projection *ChatGPTWebUsageProjection) Complete() {
	projection.finish(true)
}

// Discard releases projection storage without running the tokenizer.
func (projection *ChatGPTWebUsageProjection) Discard() {
	projection.finish(false)
}

func (projection *ChatGPTWebUsageProjection) finish(completed bool) {
	if projection == nil || projection.manager == nil {
		return
	}
	projection.closeOnce.Do(func() {
		projection.mu.Lock()
		defer projection.mu.Unlock()
		var removeErr error
		if projection.filePath != "" {
			removeErr = projection.removeDiskFile()
		}
		projection.records = nil
		projection.images = nil
		projection.manager.releaseProjection(projection, completed, removeErr)
	})
}

func (projection *ChatGPTWebUsageProjection) openDiskFile() (*os.File, error) {
	if projection == nil || projection.fileName == "" ||
		projection.ownedDirectory == nil || projection.ownedDirectory.root == nil {
		return nil, errors.New("chatgpt web usage cache file is unavailable")
	}
	return projection.ownedDirectory.root.Open(projection.fileName)
}

func (projection *ChatGPTWebUsageProjection) removeDiskFile() error {
	if projection == nil || projection.fileName == "" ||
		projection.ownedDirectory == nil || projection.ownedDirectory.root == nil {
		return errors.New("chatgpt web usage cache file is unavailable")
	}
	remove := projection.manager.removeProjectionFile
	if remove == nil {
		return projection.ownedDirectory.root.Remove(projection.fileName)
	}
	return remove(projection.ownedDirectory.root, projection.fileName)
}

func (cache *ChatGPTWebUsageCache) reduceProjectionMemory(projection *ChatGPTWebUsageProjection, releasedBytes int64) {
	if cache == nil || releasedBytes <= 0 {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, exists := cache.handles[projection]; !exists || projection.filePath != "" {
		return
	}
	cache.stats.ActiveMemoryBytes -= releasedBytes
	if cache.stats.ActiveMemoryBytes < 0 {
		cache.stats.ActiveMemoryBytes = 0
	}
}

func (cache *ChatGPTWebUsageCache) releaseProjection(
	projection *ChatGPTWebUsageProjection,
	completed bool,
	removeErr error,
) {
	cache.mu.Lock()
	if _, exists := cache.handles[projection]; !exists {
		cache.mu.Unlock()
		return
	}
	delete(cache.handles, projection)
	if projection.filePath != "" {
		cache.stats.ActiveDiskEntries--
		cache.stats.ActiveDiskBytes -= projection.diskBytes
		if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			cache.recordRetainedUsageCacheFileLocked(projection.cacheBase, projection.diskBytes)
		}
	} else {
		cache.stats.ActiveMemoryEntries--
		cache.stats.ActiveMemoryBytes -= projection.memoryBytes
	}
	if completed {
		cache.stats.SuccessfulCalculations++
	} else {
		cache.stats.FailedDiscards++
	}
	cache.mu.Unlock()
	cache.releaseOwnedUsageCacheDirectory(projection.ownedDirectory)
}

func (cache *ChatGPTWebUsageCache) recordRetainedUsageCacheFileLocked(base string, bytes int64) {
	if bytes < 0 {
		bytes = 0
	}
	base = filepath.Clean(base)
	cache.retainedOrphanBytes[base] += bytes
	inventory := cache.inventoryByBase[base]
	inventory.orphanDirectories = max(inventory.orphanDirectories, 1)
	inventory.orphanFiles++
	inventory.orphanBytes += bytes
	cache.inventoryByBase[base] = inventory
	cache.stats.CleanupErrors++
	cache.recomputeUsageCacheInventoryStatsLocked()
}

// Snapshot returns current cache usage and cumulative counters.
func (cache *ChatGPTWebUsageCache) Snapshot() ChatGPTWebUsageCacheSnapshot {
	if cache == nil {
		return ChatGPTWebUsageCacheSnapshot{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return cache.stats
}

// Close prevents new projections and removes all active temporary files.
func (cache *ChatGPTWebUsageCache) Close() {
	if cache == nil {
		return
	}
	cache.closeOnce.Do(func() {
		cache.mu.Lock()
		cache.closed = true
		cache.mu.Unlock()
		cache.ownershipWG.Wait()
		cache.createWG.Wait()
		cache.retireWG.Wait()
		cache.mu.Lock()
		handles := make([]*ChatGPTWebUsageProjection, 0, len(cache.handles))
		for handle := range cache.handles {
			handles = append(handles, handle)
		}
		ownedDirectories := make([]*chatGPTWebUsageCacheOwnedDirectory, 0, len(cache.ownedDirectories))
		for _, owned := range cache.ownedDirectories {
			ownedDirectories = append(ownedDirectories, owned)
		}
		cache.ownedDirectories = make(map[string]*chatGPTWebUsageCacheOwnedDirectory)
		cache.activeOwnedBase = ""
		cache.mu.Unlock()
		for _, handle := range handles {
			handle.Discard()
		}
		for _, owned := range ownedDirectories {
			if errRemove := cache.removeOwnedUsageCacheDirectory(owned); errRemove != nil {
				cache.recordUsageCacheCleanupError()
			}
		}
	})
}

func normalizeChatGPTWebOutputQuality(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "low", "medium", "high":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "medium"
	}
}
