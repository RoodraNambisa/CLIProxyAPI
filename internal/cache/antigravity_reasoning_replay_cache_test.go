package cache

import (
	"bytes"
	"testing"
	"time"
)

func TestAntigravityReasoningReplayCacheStoresClonesAndExpires(t *testing.T) {
	ClearAntigravityReasoningReplayCache()
	t.Cleanup(ClearAntigravityReasoningReplayCache)

	item := []byte(`{"type":"thought_signature","thoughtSignature":"0123456789abcdef","contentIndex":1,"partIndex":0}`)
	if !CacheAntigravityReasoningReplayItem("gemini-3-flash", "session-1", item) {
		t.Fatal("CacheAntigravityReasoningReplayItem() = false")
	}
	got, ok := GetAntigravityReasoningReplayItem("gemini-3-flash", "session-1")
	if !ok {
		t.Fatal("GetAntigravityReasoningReplayItem() missed cached item")
	}
	got[0] = '{' + 1
	again, ok := GetAntigravityReasoningReplayItem("gemini-3-flash", "session-1")
	if !ok || bytes.Equal(got, again) {
		t.Fatal("cache returned mutable internal storage")
	}

	key := antigravityReasoningReplayCacheKey("gemini-3-flash", "session-1")
	antigravityReasoningReplayMu.Lock()
	entry := antigravityReasoningReplayEntries[key]
	entry.Timestamp = time.Now().Add(-AntigravityReasoningReplayCacheTTL - time.Second)
	antigravityReasoningReplayEntries[key] = entry
	antigravityReasoningReplayMu.Unlock()
	if _, ok = GetAntigravityReasoningReplayItem("gemini-3-flash", "session-1"); ok {
		t.Fatal("expired replay entry remained available")
	}
}

func TestAntigravityReasoningReplayCacheRejectsOversizedEntryWithoutReplacingExisting(t *testing.T) {
	ClearAntigravityReasoningReplayCache()
	t.Cleanup(ClearAntigravityReasoningReplayCache)

	valid := []byte(`{"type":"thought_signature","thoughtSignature":"0123456789abcdef","contentIndex":1,"partIndex":0}`)
	if !CacheAntigravityReasoningReplayItem("gemini-3-flash", "session-1", valid) {
		t.Fatal("initial cache store failed")
	}
	oversized := []byte(`{"type":"function_call_part","name":"tool","args":"`)
	oversized = append(oversized, bytes.Repeat([]byte("a"), AntigravityReasoningReplayCacheMaxEntryBytes)...)
	oversized = append(oversized, []byte(`"}`)...)
	if CacheAntigravityReasoningReplayItem("gemini-3-flash", "session-1", oversized) {
		t.Fatal("oversized replay entry was accepted")
	}
	if _, ok := GetAntigravityReasoningReplayItem("gemini-3-flash", "session-1"); !ok {
		t.Fatal("oversized replacement removed the existing entry")
	}
}

func TestAntigravityReasoningReplayCacheEnforcesTotalBytes(t *testing.T) {
	ClearAntigravityReasoningReplayCache()
	t.Cleanup(ClearAntigravityReasoningReplayCache)

	now := time.Now()
	antigravityReasoningReplayMu.Lock()
	antigravityReasoningReplayEntries["oldest"] = antigravityReasoningReplayEntry{Timestamp: now, Size: AntigravityReasoningReplayCacheMaxTotalBytes}
	antigravityReasoningReplayEntries["newest"] = antigravityReasoningReplayEntry{Timestamp: now.Add(time.Second), Size: 1}
	antigravityReasoningReplayBytes = AntigravityReasoningReplayCacheMaxTotalBytes + 1
	enforceAntigravityReasoningReplayLimitsLocked()
	_, oldestExists := antigravityReasoningReplayEntries["oldest"]
	_, newestExists := antigravityReasoningReplayEntries["newest"]
	totalBytes := antigravityReasoningReplayBytes
	antigravityReasoningReplayMu.Unlock()

	if oldestExists || !newestExists || totalBytes != 1 {
		t.Fatalf("limit enforcement: oldest=%v newest=%v bytes=%d", oldestExists, newestExists, totalBytes)
	}
}
