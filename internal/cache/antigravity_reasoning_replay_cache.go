package cache

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	// AntigravityReasoningReplayCacheTTL limits how long encrypted reasoning replay
	// items stay in process memory.
	AntigravityReasoningReplayCacheTTL = 1 * time.Hour

	// AntigravityReasoningReplayCacheMaxEntries bounds process memory for replay
	// continuity. Oldest entries are evicted first.
	AntigravityReasoningReplayCacheMaxEntries = 10240

	AntigravityReasoningReplayCacheMaxEntryBytes = 8 << 20
	AntigravityReasoningReplayCacheMaxTotalBytes = 256 << 20

	minAntigravityThoughtSignatureReplayLen = 16
)

type antigravityReasoningReplayEntry struct {
	Items     [][]byte
	Timestamp time.Time
	Size      int64
}

var (
	antigravityReasoningReplayMu      sync.Mutex
	antigravityReasoningReplayEntries = make(map[string]antigravityReasoningReplayEntry)
	antigravityReasoningReplayBytes   int64
)

// CacheAntigravityReasoningReplayItem stores one Antigravity reasoning item for
// stateless replay. The stored item is normalized to the minimal upstream shape.
func CacheAntigravityReasoningReplayItem(modelName, sessionKey string, item []byte) bool {
	return CacheAntigravityReasoningReplayItems(modelName, sessionKey, [][]byte{item})
}

// CacheAntigravityReasoningReplayItems stores Antigravity assistant output items
// needed to replay a stateless next turn.
func CacheAntigravityReasoningReplayItems(modelName, sessionKey string, items [][]byte) bool {
	return CacheAntigravityReasoningReplayItemsBestEffort(context.Background(), modelName, sessionKey, items)
}

// CacheAntigravityReasoningReplayItemsBestEffort stores replay items for completed response paths.
func CacheAntigravityReasoningReplayItemsBestEffort(_ context.Context, modelName, sessionKey string, items [][]byte) bool {
	key := antigravityReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return false
	}
	normalized, ok := normalizeAntigravityReasoningReplayItems(items)
	if !ok {
		return false
	}
	size := antigravityReasoningReplayItemsSize(normalized)
	if size <= 0 || size > AntigravityReasoningReplayCacheMaxEntryBytes {
		return false
	}

	cacheCleanupOnce.Do(startCacheCleanup)
	now := time.Now()
	antigravityReasoningReplayMu.Lock()
	if previous, exists := antigravityReasoningReplayEntries[key]; exists {
		antigravityReasoningReplayBytes -= previous.Size
	}
	antigravityReasoningReplayEntries[key] = antigravityReasoningReplayEntry{
		Items:     normalized,
		Timestamp: now,
		Size:      size,
	}
	antigravityReasoningReplayBytes += size
	enforceAntigravityReasoningReplayLimitsLocked()
	antigravityReasoningReplayMu.Unlock()
	return true
}

// GetAntigravityReasoningReplayItem retrieves a normalized reasoning replay item.
func GetAntigravityReasoningReplayItem(modelName, sessionKey string) ([]byte, bool) {
	items, ok := GetAntigravityReasoningReplayItems(modelName, sessionKey)
	if !ok || len(items) == 0 {
		return nil, false
	}
	return items[0], true
}

// GetAntigravityReasoningReplayItems retrieves normalized assistant output items.
func GetAntigravityReasoningReplayItems(modelName, sessionKey string) ([][]byte, bool) {
	items, ok, err := GetAntigravityReasoningReplayItemsRequired(context.Background(), modelName, sessionKey)
	if err == nil {
		return items, ok
	}
	return nil, false
}

// GetAntigravityReasoningReplayItemsRequired retrieves replay items for request-time paths.
func GetAntigravityReasoningReplayItemsRequired(_ context.Context, modelName, sessionKey string) ([][]byte, bool, error) {
	key := antigravityReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return nil, false, nil
	}
	cacheCleanupOnce.Do(startCacheCleanup)
	now := time.Now()
	antigravityReasoningReplayMu.Lock()
	entry, ok := antigravityReasoningReplayEntries[key]
	if !ok {
		antigravityReasoningReplayMu.Unlock()
		return nil, false, nil
	}
	if now.Sub(entry.Timestamp) > AntigravityReasoningReplayCacheTTL {
		deleteAntigravityReasoningReplayEntryLocked(key)
		antigravityReasoningReplayMu.Unlock()
		return nil, false, nil
	}
	entry.Timestamp = now
	antigravityReasoningReplayEntries[key] = entry
	items := entry.Items
	antigravityReasoningReplayMu.Unlock()
	return cloneAntigravityReasoningReplayItems(items), true, nil
}

// DeleteAntigravityReasoningReplayItem removes one replay item after upstream rejects
// it or the caller otherwise knows it is stale.
func DeleteAntigravityReasoningReplayItem(modelName, sessionKey string) {
	if errDelete := DeleteAntigravityReasoningReplayItemRequired(context.Background(), modelName, sessionKey); errDelete != nil {
		return
	}
}

// DeleteAntigravityReasoningReplayItemRequired removes one replay item for request-time paths.
func DeleteAntigravityReasoningReplayItemRequired(_ context.Context, modelName, sessionKey string) error {
	key := antigravityReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return nil
	}
	antigravityReasoningReplayMu.Lock()
	deleteAntigravityReasoningReplayEntryLocked(key)
	antigravityReasoningReplayMu.Unlock()
	return nil
}

// ClearAntigravityReasoningReplayCache clears all Antigravity reasoning replay state.
func ClearAntigravityReasoningReplayCache() {
	antigravityReasoningReplayMu.Lock()
	antigravityReasoningReplayEntries = make(map[string]antigravityReasoningReplayEntry)
	antigravityReasoningReplayBytes = 0
	antigravityReasoningReplayMu.Unlock()
}

func antigravityReasoningReplayCacheKey(modelName, sessionKey string) string {
	modelName = strings.TrimSpace(modelName)
	sessionKey = strings.TrimSpace(sessionKey)
	if modelName == "" || sessionKey == "" {
		return ""
	}
	// The session key is the continuity boundary. Keep this independent from
	// the selected Antigravity credential so auth failover can preserve replay.
	return strings.Join([]string{"antigravity-reasoning-replay", modelName, sessionKey}, "\x00")
}

func normalizeAntigravityReasoningReplayItems(items [][]byte) ([][]byte, bool) {
	normalized := make([][]byte, 0, len(items))
	for _, item := range items {
		normalizedItem, ok := normalizeAntigravityReasoningReplayItem(item)
		if ok {
			normalized = append(normalized, normalizedItem)
		}
	}
	return normalized, len(normalized) > 0
}

func normalizeAntigravityReasoningReplayItem(item []byte) ([]byte, bool) {
	itemResult := gjson.ParseBytes(item)
	switch strings.TrimSpace(itemResult.Get("type").String()) {
	case "thought_signature":
		return normalizeAntigravityThoughtSignatureReplayItem(itemResult)
	case "function_call_part":
		return normalizeAntigravityFunctionCallPartReplayItem(itemResult)
	default:
		return nil, false
	}
}

func normalizeAntigravityThoughtSignatureReplayItem(itemResult gjson.Result) ([]byte, bool) {
	sig := strings.TrimSpace(itemResult.Get("thoughtSignature").String())
	if sig == "" {
		sig = strings.TrimSpace(itemResult.Get("thought_signature").String())
	}
	if sig == "" || len(sig) < minAntigravityThoughtSignatureReplayLen {
		return nil, false
	}
	normalized := []byte(`{"type":"thought_signature"}`)
	normalized, _ = sjson.SetBytes(normalized, "thoughtSignature", sig)
	if contentIndex := itemResult.Get("contentIndex"); contentIndex.Type == gjson.Number {
		normalized, _ = sjson.SetBytes(normalized, "contentIndex", contentIndex.Int())
	}
	if partIndex := itemResult.Get("partIndex"); partIndex.Type == gjson.Number {
		normalized, _ = sjson.SetBytes(normalized, "partIndex", partIndex.Int())
	}
	return normalized, true
}

func normalizeAntigravityFunctionCallPartReplayItem(itemResult gjson.Result) ([]byte, bool) {
	callID := strings.TrimSpace(itemResult.Get("call_id").String())
	if callID == "" {
		callID = strings.TrimSpace(itemResult.Get("id").String())
	}
	name := strings.TrimSpace(itemResult.Get("name").String())
	args := itemResult.Get("args")
	if name == "" || !args.Exists() {
		fc := itemResult.Get("functionCall")
		if fc.Exists() {
			if callID == "" {
				callID = strings.TrimSpace(fc.Get("id").String())
			}
			if name == "" {
				name = strings.TrimSpace(fc.Get("name").String())
			}
			if !args.Exists() {
				args = fc.Get("args")
			}
		}
	}
	if name == "" || !args.Exists() {
		return nil, false
	}
	normalized := []byte(`{"type":"function_call_part"}`)
	if callID != "" {
		normalized, _ = sjson.SetBytes(normalized, "call_id", callID)
	}
	normalized, _ = sjson.SetBytes(normalized, "name", name)
	if args.Type == gjson.String {
		normalized, _ = sjson.SetBytes(normalized, "args", args.String())
	} else {
		normalized, _ = sjson.SetRawBytes(normalized, "args", []byte(args.Raw))
	}
	sig := strings.TrimSpace(itemResult.Get("thoughtSignature").String())
	if sig != "" {
		normalized, _ = sjson.SetBytes(normalized, "thoughtSignature", sig)
	}
	if contentIndex := itemResult.Get("contentIndex"); contentIndex.Type == gjson.Number {
		normalized, _ = sjson.SetBytes(normalized, "contentIndex", contentIndex.Int())
	}
	if partIndex := itemResult.Get("partIndex"); partIndex.Type == gjson.Number {
		normalized, _ = sjson.SetBytes(normalized, "partIndex", partIndex.Int())
	}
	return normalized, true
}

func cloneAntigravityReasoningReplayItems(items [][]byte) [][]byte {
	cloned := make([][]byte, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, append([]byte(nil), item...))
	}
	return cloned
}

func antigravityReasoningReplayItemsSize(items [][]byte) int64 {
	var size int64
	for _, item := range items {
		size += int64(len(item))
	}
	return size
}

func deleteAntigravityReasoningReplayEntryLocked(key string) {
	entry, ok := antigravityReasoningReplayEntries[key]
	if !ok {
		return
	}
	delete(antigravityReasoningReplayEntries, key)
	antigravityReasoningReplayBytes -= entry.Size
	if antigravityReasoningReplayBytes < 0 {
		antigravityReasoningReplayBytes = 0
	}
}

func enforceAntigravityReasoningReplayLimitsLocked() {
	if len(antigravityReasoningReplayEntries) <= AntigravityReasoningReplayCacheMaxEntries && antigravityReasoningReplayBytes <= AntigravityReasoningReplayCacheMaxTotalBytes {
		return
	}
	for _, candidate := range antigravityReasoningReplayEvictionCandidatesLocked() {
		if len(antigravityReasoningReplayEntries) <= AntigravityReasoningReplayCacheMaxEntries && antigravityReasoningReplayBytes <= AntigravityReasoningReplayCacheMaxTotalBytes {
			break
		}
		deleteAntigravityReasoningReplayEntryLocked(candidate.key)
	}
}

type antigravityReasoningReplayEvictionCandidate struct {
	key       string
	timestamp time.Time
}

func antigravityReasoningReplayEvictionCandidatesLocked() []antigravityReasoningReplayEvictionCandidate {
	candidates := make([]antigravityReasoningReplayEvictionCandidate, 0, len(antigravityReasoningReplayEntries))
	for key, entry := range antigravityReasoningReplayEntries {
		candidates = append(candidates, antigravityReasoningReplayEvictionCandidate{key: key, timestamp: entry.Timestamp})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].timestamp.Before(candidates[j].timestamp)
	})
	return candidates
}

func purgeExpiredAntigravityReasoningReplayCache(now time.Time) {
	antigravityReasoningReplayMu.Lock()
	for key, entry := range antigravityReasoningReplayEntries {
		if now.Sub(entry.Timestamp) > AntigravityReasoningReplayCacheTTL {
			deleteAntigravityReasoningReplayEntryLocked(key)
		}
	}
	antigravityReasoningReplayMu.Unlock()
}
