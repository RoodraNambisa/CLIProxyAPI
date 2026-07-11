package cache

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"testing"
	"time"
)

func validXAIEncryptedContent(seed byte) string {
	raw := make([]byte, 256)
	for i := range raw {
		raw[i] = byte(i) ^ seed
	}
	return base64.RawStdEncoding.EncodeToString(raw)
}

func xaiReasoningItem(seed byte) []byte {
	return []byte(fmt.Sprintf(`{"type":"reasoning","encrypted_content":%q}`, validXAIEncryptedContent(seed)))
}

func TestXAIReasoningReplayCacheStoresClonesAndExpires(t *testing.T) {
	ClearXAIReasoningReplayCache()
	t.Cleanup(ClearXAIReasoningReplayCache)

	item := xaiReasoningItem(1)
	if !CacheXAIReasoningReplayItem("grok-4", "session-1", item) {
		t.Fatal("CacheXAIReasoningReplayItem() = false")
	}
	got, ok := GetXAIReasoningReplayItem("grok-4", "session-1")
	if !ok || len(got) == 0 {
		t.Fatal("GetXAIReasoningReplayItem() missed cached item")
	}
	got[0] = 'x'
	again, ok := GetXAIReasoningReplayItem("grok-4", "session-1")
	if !ok || len(again) == 0 || again[0] == 'x' {
		t.Fatal("cached item was not cloned")
	}

	key := xaiReasoningReplayCacheKey("grok-4", "session-1")
	xaiReasoningReplayMu.Lock()
	entry := xaiReasoningReplayEntries[key]
	entry.Timestamp = time.Now().Add(-XAIReasoningReplayCacheTTL - time.Second)
	xaiReasoningReplayEntries[key] = entry
	xaiReasoningReplayMu.Unlock()
	if _, ok = GetXAIReasoningReplayItem("grok-4", "session-1"); ok {
		t.Fatal("expired replay entry remained available")
	}
}

func TestXAIReasoningReplayCacheEvictsOldestAtCapacity(t *testing.T) {
	ClearXAIReasoningReplayCache()
	t.Cleanup(ClearXAIReasoningReplayCache)

	now := time.Now()
	xaiReasoningReplayMu.Lock()
	for i := 0; i < XAIReasoningReplayCacheMaxEntries; i++ {
		key := fmt.Sprintf("entry-%05d", i)
		item := []byte(`{"type":"function_call","call_id":"c","name":"f","arguments":"{}"}`)
		xaiReasoningReplayEntries[key] = xaiReasoningReplayEntry{
			Items:     [][]byte{item},
			Timestamp: now.Add(time.Duration(i) * time.Nanosecond),
			Size:      int64(len(item)),
		}
		xaiReasoningReplayBytes += int64(len(item))
	}
	xaiReasoningReplayEntries["newest"] = xaiReasoningReplayEntry{Timestamp: now.Add(time.Hour), Size: 1}
	xaiReasoningReplayBytes++
	evictOldestXAIReasoningReplayEntriesLocked(len(xaiReasoningReplayEntries) - XAIReasoningReplayCacheMaxEntries)
	_, oldestExists := xaiReasoningReplayEntries["entry-00000"]
	_, newestExists := xaiReasoningReplayEntries["newest"]
	size := len(xaiReasoningReplayEntries)
	xaiReasoningReplayMu.Unlock()

	if oldestExists || !newestExists || size != XAIReasoningReplayCacheMaxEntries {
		t.Fatalf("eviction oldest=%v newest=%v size=%d", oldestExists, newestExists, size)
	}
}

func TestXAIReasoningReplayCacheRejectsOversizedEntryWithoutReplacingExisting(t *testing.T) {
	ClearXAIReasoningReplayCache()
	t.Cleanup(ClearXAIReasoningReplayCache)

	if !CacheXAIReasoningReplayItem("grok-4", "session-1", xaiReasoningItem(2)) {
		t.Fatal("initial CacheXAIReasoningReplayItem() = false")
	}
	oversized := []byte(`{"type":"function_call","call_id":"c","name":"f","arguments":"`)
	oversized = append(oversized, bytes.Repeat([]byte("a"), XAIReasoningReplayCacheMaxEntryBytes)...)
	oversized = append(oversized, []byte(`"}`)...)
	if CacheXAIReasoningReplayItem("grok-4", "session-1", oversized) {
		t.Fatal("oversized replay entry was accepted")
	}
	if _, ok := GetXAIReasoningReplayItem("grok-4", "session-1"); !ok {
		t.Fatal("oversized replacement removed the existing entry")
	}
}

func TestXAIReasoningReplayCacheEnforcesTotalBytes(t *testing.T) {
	ClearXAIReasoningReplayCache()
	t.Cleanup(ClearXAIReasoningReplayCache)

	now := time.Now()
	xaiReasoningReplayMu.Lock()
	xaiReasoningReplayEntries["oldest"] = xaiReasoningReplayEntry{Timestamp: now, Size: XAIReasoningReplayCacheMaxTotalBytes}
	xaiReasoningReplayEntries["newest"] = xaiReasoningReplayEntry{Timestamp: now.Add(time.Second), Size: 1}
	xaiReasoningReplayBytes = XAIReasoningReplayCacheMaxTotalBytes + 1
	enforceXAIReasoningReplayLimitsLocked()
	_, oldestExists := xaiReasoningReplayEntries["oldest"]
	_, newestExists := xaiReasoningReplayEntries["newest"]
	totalBytes := xaiReasoningReplayBytes
	xaiReasoningReplayMu.Unlock()

	if oldestExists || !newestExists || totalBytes != 1 {
		t.Fatalf("total limit oldest=%v newest=%v bytes=%d", oldestExists, newestExists, totalBytes)
	}
}
