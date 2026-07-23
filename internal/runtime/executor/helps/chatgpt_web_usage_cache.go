package helps

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const chatGPTWebUsageRecordChunkBytes = 64 << 10

// ChatGPTWebUsageCacheOptions is an immutable per-request cache configuration.
type ChatGPTWebUsageCacheOptions struct {
	Enabled            bool
	DiskThresholdBytes int64
	MaxDiskBytes       int64
	Path               string
	AutoOutputQuality  string
}

// ChatGPTWebUsageCacheSnapshot reports active storage and cumulative outcomes.
type ChatGPTWebUsageCacheSnapshot struct {
	ActiveMemoryEntries    int    `json:"active_memory_entries"`
	ActiveMemoryBytes      int64  `json:"active_memory_bytes"`
	ActiveDiskEntries      int    `json:"active_disk_entries"`
	ActiveDiskBytes        int64  `json:"active_disk_bytes"`
	PeakDiskBytes          int64  `json:"peak_disk_bytes"`
	SuccessfulCalculations uint64 `json:"successful_calculations"`
	FailedDiscards         uint64 `json:"failed_discards"`
	CapacityRejections     uint64 `json:"capacity_rejections"`
	WriteErrors            uint64 `json:"write_errors"`
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
	mu         sync.Mutex
	createWG   sync.WaitGroup
	handles    map[*ChatGPTWebUsageProjection]struct{}
	defaultDir string
	closed     bool
	stats      ChatGPTWebUsageCacheSnapshot
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
	memoryBytes         int64
	diskBytes           int64
	closeOnce           sync.Once
}

// NewChatGPTWebUsageCache creates an empty usage projection manager.
func NewChatGPTWebUsageCache() *ChatGPTWebUsageCache {
	return &ChatGPTWebUsageCache{handles: make(map[*ChatGPTWebUsageProjection]struct{})}
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

	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return nil, &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_unavailable", Message: "chatgpt web usage cache is closed"}
	}
	if cache.stats.ActiveDiskBytes > options.MaxDiskBytes-diskBytes {
		cache.stats.CapacityRejections++
		cache.mu.Unlock()
		return nil, &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_full", Message: "chatgpt web usage cache capacity is exhausted"}
	}
	cache.stats.ActiveDiskBytes += diskBytes
	cache.createWG.Add(1)
	cache.mu.Unlock()
	defer cache.createWG.Done()

	path, errWrite := cache.writeProjectionFile(options.Path, records)
	if errWrite != nil {
		cache.mu.Lock()
		cache.stats.ActiveDiskBytes -= diskBytes
		cache.stats.WriteErrors++
		cache.mu.Unlock()
		return nil, &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_unavailable", Message: "chatgpt web usage cache is unavailable"}
	}
	projection.filePath = path
	projection.diskBytes = diskBytes
	projection.records = nil
	projection.memoryBytes = 0

	cache.mu.Lock()
	if cache.closed {
		cache.stats.ActiveDiskBytes -= diskBytes
		cache.mu.Unlock()
		_ = os.Remove(path)
		return nil, &ChatGPTWebUsageCacheError{Code: "chatgpt_web_usage_cache_unavailable", Message: "chatgpt web usage cache is closed"}
	}
	cache.handles[projection] = struct{}{}
	cache.stats.ActiveDiskEntries++
	if cache.stats.ActiveDiskBytes > cache.stats.PeakDiskBytes {
		cache.stats.PeakDiskBytes = cache.stats.ActiveDiskBytes
	}
	cache.mu.Unlock()
	return projection, nil
}

func chatGPTWebUsageTextRecords(request ChatGPTWebRequest) []chatGPTWebUsageTextRecord {
	segments := chatGPTWebTextTokenSegments(request)
	records := make([]chatGPTWebUsageTextRecord, 0, len(segments))
	for segmentIndex, segment := range segments {
		firstChunk := true
		for len(segment) > 0 {
			end := min(len(segment), chatGPTWebUsageRecordChunkBytes)
			for end < len(segment) && end > 0 && !isUTF8RuneStart(segment[end]) {
				end--
			}
			if end == 0 {
				end = min(len(segment), chatGPTWebUsageRecordChunkBytes)
			}
			records = append(records, chatGPTWebUsageTextRecord{
				separator: segmentIndex > 0 && firstChunk,
				text:      segment[:end],
			})
			firstChunk = false
			segment = segment[end:]
		}
	}
	return records
}

func chatGPTWebUsageRecordSizes(records []chatGPTWebUsageTextRecord) (memoryBytes, diskBytes int64) {
	for _, record := range records {
		memoryBytes += int64(len(record.text))
		diskBytes += int64(5 + len(record.text))
	}
	return memoryBytes, diskBytes
}

func (cache *ChatGPTWebUsageCache) writeProjectionFile(configuredPath string, records []chatGPTWebUsageTextRecord) (string, error) {
	directory, errDir := cache.cacheDirectory(configuredPath)
	if errDir != nil {
		return "", errDir
	}
	file, errCreate := os.CreateTemp(directory, "usage-*.bin")
	if errCreate != nil {
		return "", errCreate
	}
	path := file.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	if errChmod := file.Chmod(0o600); errChmod != nil {
		return "", errChmod
	}
	writer := bufio.NewWriterSize(file, 64<<10)
	var header [5]byte
	for _, record := range records {
		if record.separator {
			header[0] = 1
		} else {
			header[0] = 0
		}
		binary.BigEndian.PutUint32(header[1:], uint32(len(record.text)))
		if _, errWrite := writer.Write(header[:]); errWrite != nil {
			return "", errWrite
		}
		if _, errWrite := io.WriteString(writer, record.text); errWrite != nil {
			return "", errWrite
		}
	}
	if errFlush := writer.Flush(); errFlush != nil {
		return "", errFlush
	}
	if errClose := file.Close(); errClose != nil {
		return "", errClose
	}
	cleanup = false
	return path, nil
}

func (cache *ChatGPTWebUsageCache) cacheDirectory(configuredPath string) (string, error) {
	configuredPath = strings.TrimSpace(configuredPath)
	if configuredPath != "" {
		if err := os.MkdirAll(configuredPath, 0o700); err != nil {
			return "", err
		}
		if err := os.Chmod(configuredPath, 0o700); err != nil {
			return "", err
		}
		return configuredPath, nil
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.defaultDir != "" {
		return cache.defaultDir, nil
	}
	directory, err := os.MkdirTemp("", "cli-proxy-api-chatgpt-web-usage-")
	if err != nil {
		return "", err
	}
	if err = os.Chmod(directory, 0o700); err != nil {
		_ = os.RemoveAll(directory)
		return "", err
	}
	cache.defaultDir = directory
	return directory, nil
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
			"image_gen": chatGPTWebUsageMap(inputTextTokens, inputImageTokens, 0, outputImageTokens),
		}
	}
	return usage, errorsFound
}

func (projection *ChatGPTWebUsageProjection) countInputTextTokensLocked() (int64, []error) {
	encoder, errEncoder := TokenizerForModel(projection.model)
	if errEncoder != nil {
		return 0, []error{errEncoder}
	}
	var inputTextTokens int64
	errInput := projection.forEachTextRecord(func(separator bool, text string) error {
		if separator {
			count, errCount := encoder.Count("\n")
			if errCount != nil {
				return errCount
			}
			inputTextTokens += int64(count)
		}
		count, errCount := encoder.Count(text)
		if errCount != nil {
			return errCount
		}
		inputTextTokens += int64(count)
		return nil
	})
	if errInput != nil {
		return inputTextTokens, []error{errInput}
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

func (projection *ChatGPTWebUsageProjection) forEachTextRecord(visit func(bool, string) error) error {
	if projection.filePath == "" {
		for _, record := range projection.records {
			if err := visit(record.separator, record.text); err != nil {
				return err
			}
		}
		return nil
	}
	file, errOpen := os.Open(projection.filePath)
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
		if projection.filePath != "" {
			_ = os.Remove(projection.filePath)
		}
		projection.records = nil
		projection.images = nil
		projection.manager.releaseProjection(projection, completed)
	})
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

func (cache *ChatGPTWebUsageCache) releaseProjection(projection *ChatGPTWebUsageProjection, completed bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, exists := cache.handles[projection]; !exists {
		return
	}
	delete(cache.handles, projection)
	if projection.filePath != "" {
		cache.stats.ActiveDiskEntries--
		cache.stats.ActiveDiskBytes -= projection.diskBytes
	} else {
		cache.stats.ActiveMemoryEntries--
		cache.stats.ActiveMemoryBytes -= projection.memoryBytes
	}
	if completed {
		cache.stats.SuccessfulCalculations++
	} else {
		cache.stats.FailedDiscards++
	}
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
	cache.mu.Lock()
	cache.closed = true
	cache.mu.Unlock()
	cache.createWG.Wait()
	cache.mu.Lock()
	handles := make([]*ChatGPTWebUsageProjection, 0, len(cache.handles))
	for handle := range cache.handles {
		handles = append(handles, handle)
	}
	defaultDir := cache.defaultDir
	cache.mu.Unlock()
	for _, handle := range handles {
		handle.Discard()
	}
	if defaultDir != "" {
		_ = os.RemoveAll(filepath.Clean(defaultDir))
	}
}

func normalizeChatGPTWebOutputQuality(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "low", "medium", "high":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "medium"
	}
}
