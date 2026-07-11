package cache

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/signature"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	// XAIReasoningReplayCacheTTL limits how long encrypted reasoning replay
	// items stay in process memory.
	XAIReasoningReplayCacheTTL = time.Hour

	// XAIReasoningReplayCacheMaxEntries bounds process memory for replay
	// continuity. Oldest entries are evicted first.
	XAIReasoningReplayCacheMaxEntries = 10240

	XAIReasoningReplayCacheMaxEntryBytes = 8 << 20
	XAIReasoningReplayCacheMaxTotalBytes = 256 << 20
)

type xaiReasoningReplayEntry struct {
	Items     [][]byte
	Timestamp time.Time
	Size      int64
}

var (
	xaiReasoningReplayMu      sync.Mutex
	xaiReasoningReplayEntries = make(map[string]xaiReasoningReplayEntry)
	xaiReasoningReplayBytes   int64
)

// CacheXAIReasoningReplayItem stores one final Grok reasoning item for replay.
func CacheXAIReasoningReplayItem(modelName, sessionKey string, item []byte) bool {
	return CacheXAIReasoningReplayItems(modelName, sessionKey, [][]byte{item})
}

// CacheXAIReasoningReplayItems stores the final Grok output items required by
// a stateless follow-up request.
func CacheXAIReasoningReplayItems(modelName, sessionKey string, items [][]byte) bool {
	return CacheXAIReasoningReplayItemsBestEffort(context.Background(), modelName, sessionKey, items)
}

// CacheXAIReasoningReplayItemsBestEffort stores replay items in process memory.
func CacheXAIReasoningReplayItemsBestEffort(_ context.Context, modelName, sessionKey string, items [][]byte) bool {
	key := xaiReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return false
	}
	normalized, ok := normalizeXAIReasoningReplayItems(items)
	if !ok {
		return false
	}
	size := xaiReasoningReplayItemsSize(normalized)
	if size <= 0 || size > XAIReasoningReplayCacheMaxEntryBytes {
		return false
	}

	cacheCleanupOnce.Do(startCacheCleanup)
	now := time.Now()
	xaiReasoningReplayMu.Lock()
	if previous, exists := xaiReasoningReplayEntries[key]; exists {
		xaiReasoningReplayBytes -= previous.Size
	}
	xaiReasoningReplayEntries[key] = xaiReasoningReplayEntry{Items: normalized, Timestamp: now, Size: size}
	xaiReasoningReplayBytes += size
	enforceXAIReasoningReplayLimitsLocked()
	xaiReasoningReplayMu.Unlock()
	return true
}

// GetXAIReasoningReplayItem retrieves one normalized reasoning replay item.
func GetXAIReasoningReplayItem(modelName, sessionKey string) ([]byte, bool) {
	items, ok := GetXAIReasoningReplayItems(modelName, sessionKey)
	if !ok || len(items) == 0 {
		return nil, false
	}
	return items[0], true
}

// GetXAIReasoningReplayItems retrieves normalized assistant output items.
func GetXAIReasoningReplayItems(modelName, sessionKey string) ([][]byte, bool) {
	items, ok, _ := GetXAIReasoningReplayItemsRequired(context.Background(), modelName, sessionKey)
	return items, ok
}

// GetXAIReasoningReplayItemsRequired retrieves replay items for request paths.
func GetXAIReasoningReplayItemsRequired(_ context.Context, modelName, sessionKey string) ([][]byte, bool, error) {
	key := xaiReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return nil, false, nil
	}

	cacheCleanupOnce.Do(startCacheCleanup)
	now := time.Now()
	xaiReasoningReplayMu.Lock()
	entry, ok := xaiReasoningReplayEntries[key]
	if !ok {
		xaiReasoningReplayMu.Unlock()
		return nil, false, nil
	}
	if now.Sub(entry.Timestamp) > XAIReasoningReplayCacheTTL {
		deleteXAIReasoningReplayEntryLocked(key)
		xaiReasoningReplayMu.Unlock()
		return nil, false, nil
	}
	entry.Timestamp = now
	xaiReasoningReplayEntries[key] = entry
	items := entry.Items
	xaiReasoningReplayMu.Unlock()
	return cloneXAIReasoningReplayItems(items), true, nil
}

// DeleteXAIReasoningReplayItem removes one replay session.
func DeleteXAIReasoningReplayItem(modelName, sessionKey string) {
	_ = DeleteXAIReasoningReplayItemRequired(context.Background(), modelName, sessionKey)
}

// DeleteXAIReasoningReplayItemRequired removes one replay session.
func DeleteXAIReasoningReplayItemRequired(_ context.Context, modelName, sessionKey string) error {
	key := xaiReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return nil
	}
	xaiReasoningReplayMu.Lock()
	deleteXAIReasoningReplayEntryLocked(key)
	xaiReasoningReplayMu.Unlock()
	return nil
}

// ClearXAIReasoningReplayCache clears all in-process xAI replay state.
func ClearXAIReasoningReplayCache() {
	xaiReasoningReplayMu.Lock()
	xaiReasoningReplayEntries = make(map[string]xaiReasoningReplayEntry)
	xaiReasoningReplayBytes = 0
	xaiReasoningReplayMu.Unlock()
}

func xaiReasoningReplayCacheKey(modelName, sessionKey string) string {
	modelName = strings.TrimSpace(modelName)
	sessionKey = strings.TrimSpace(sessionKey)
	if modelName == "" || sessionKey == "" {
		return ""
	}
	return strings.Join([]string{"xai-reasoning-replay", modelName, sessionKey}, "\x00")
}

func normalizeXAIReasoningReplayItems(items [][]byte) ([][]byte, bool) {
	normalized := make([][]byte, 0, len(items))
	for _, item := range items {
		if normalizedItem, ok := normalizeXAIReasoningReplayItem(item); ok {
			normalized = append(normalized, normalizedItem)
		}
	}
	return normalized, len(normalized) > 0
}

func normalizeXAIReasoningReplayItem(item []byte) ([]byte, bool) {
	itemResult := gjson.ParseBytes(item)
	switch strings.TrimSpace(itemResult.Get("type").String()) {
	case "reasoning":
		return normalizeXAIReasoningReplayReasoningItem(itemResult)
	case "function_call":
		return normalizeXAIReasoningReplayFunctionCallItem(itemResult)
	case "custom_tool_call":
		return normalizeXAIReasoningReplayCustomToolCallItem(itemResult)
	default:
		return nil, false
	}
}

func normalizeXAIReasoningReplayReasoningItem(itemResult gjson.Result) ([]byte, bool) {
	encrypted := itemResult.Get("encrypted_content")
	if encrypted.Type != gjson.String || encrypted.String() != strings.TrimSpace(encrypted.String()) {
		return nil, false
	}
	if _, err := signature.InspectGrokEncryptedContent(encrypted.String()); err != nil {
		return nil, false
	}
	normalized := []byte(`{"type":"reasoning","summary":[],"content":null}`)
	normalized, _ = sjson.SetBytes(normalized, "encrypted_content", encrypted.String())
	return normalized, true
}

func normalizeXAIReasoningReplayFunctionCallItem(itemResult gjson.Result) ([]byte, bool) {
	callID := strings.TrimSpace(itemResult.Get("call_id").String())
	name := strings.TrimSpace(itemResult.Get("name").String())
	arguments := itemResult.Get("arguments")
	if callID == "" || name == "" || arguments.Type != gjson.String {
		return nil, false
	}
	normalized := []byte(`{"type":"function_call"}`)
	normalized, _ = sjson.SetBytes(normalized, "call_id", callID)
	normalized, _ = sjson.SetBytes(normalized, "name", name)
	normalized, _ = sjson.SetBytes(normalized, "arguments", arguments.String())
	return normalized, true
}

func normalizeXAIReasoningReplayCustomToolCallItem(itemResult gjson.Result) ([]byte, bool) {
	callID := strings.TrimSpace(itemResult.Get("call_id").String())
	name := strings.TrimSpace(itemResult.Get("name").String())
	input := itemResult.Get("input")
	if callID == "" || name == "" || !input.Exists() {
		return nil, false
	}
	normalized := []byte(`{"type":"custom_tool_call","status":"completed"}`)
	if status := strings.TrimSpace(itemResult.Get("status").String()); status != "" {
		normalized, _ = sjson.SetBytes(normalized, "status", status)
	}
	normalized, _ = sjson.SetBytes(normalized, "call_id", callID)
	normalized, _ = sjson.SetBytes(normalized, "name", name)
	if input.Type == gjson.String {
		normalized, _ = sjson.SetBytes(normalized, "input", input.String())
	} else {
		normalized, _ = sjson.SetRawBytes(normalized, "input", []byte(input.Raw))
	}
	return normalized, true
}

func cloneXAIReasoningReplayItems(items [][]byte) [][]byte {
	cloned := make([][]byte, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, append([]byte(nil), item...))
	}
	return cloned
}

func xaiReasoningReplayItemsSize(items [][]byte) int64 {
	var size int64
	for _, item := range items {
		size += int64(len(item))
	}
	return size
}

func deleteXAIReasoningReplayEntryLocked(key string) {
	entry, ok := xaiReasoningReplayEntries[key]
	if !ok {
		return
	}
	delete(xaiReasoningReplayEntries, key)
	xaiReasoningReplayBytes -= entry.Size
	if xaiReasoningReplayBytes < 0 {
		xaiReasoningReplayBytes = 0
	}
}

func enforceXAIReasoningReplayLimitsLocked() {
	if len(xaiReasoningReplayEntries) <= XAIReasoningReplayCacheMaxEntries && xaiReasoningReplayBytes <= XAIReasoningReplayCacheMaxTotalBytes {
		return
	}
	for _, candidate := range xaiReasoningReplayEvictionCandidatesLocked() {
		if len(xaiReasoningReplayEntries) <= XAIReasoningReplayCacheMaxEntries && xaiReasoningReplayBytes <= XAIReasoningReplayCacheMaxTotalBytes {
			break
		}
		deleteXAIReasoningReplayEntryLocked(candidate.key)
	}
}

type xaiReasoningReplayEvictionCandidate struct {
	key       string
	timestamp time.Time
}

func xaiReasoningReplayEvictionCandidatesLocked() []xaiReasoningReplayEvictionCandidate {
	candidates := make([]xaiReasoningReplayEvictionCandidate, 0, len(xaiReasoningReplayEntries))
	for key, entry := range xaiReasoningReplayEntries {
		candidates = append(candidates, xaiReasoningReplayEvictionCandidate{key: key, timestamp: entry.Timestamp})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].timestamp.Before(candidates[j].timestamp) })
	return candidates
}

func evictOldestXAIReasoningReplayEntriesLocked(count int) {
	if count <= 0 || len(xaiReasoningReplayEntries) == 0 {
		return
	}
	candidates := xaiReasoningReplayEvictionCandidatesLocked()
	if count > len(candidates) {
		count = len(candidates)
	}
	for i := 0; i < count; i++ {
		deleteXAIReasoningReplayEntryLocked(candidates[i].key)
	}
}

func purgeExpiredXAIReasoningReplayCache(now time.Time) {
	xaiReasoningReplayMu.Lock()
	for key, entry := range xaiReasoningReplayEntries {
		if now.Sub(entry.Timestamp) > XAIReasoningReplayCacheTTL {
			deleteXAIReasoningReplayEntryLocked(key)
		}
	}
	xaiReasoningReplayMu.Unlock()
}
