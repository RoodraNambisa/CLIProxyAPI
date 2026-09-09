package helps

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"
)

func replayTurnFixture(id int) codexReasoningReplayTurn {
	return codexReasoningReplayTurn{prefix: sha256.Sum256(fmt.Appendf(nil, "request-%d", id)), callIDs: []string{"call"}, items: [][]byte{fmt.Appendf(nil, `{"type":"reasoning","encrypted_content":"fixture-%d"}`, id)}}
}

func TestCodexReplayTurnStoreOwnsDataAndDeduplicates(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	t.Cleanup(resetCodexReasoningReplayStoreForTest)
	scope := CodexReasoningReplayScope{namespace: "tenant", modelName: "model", sessionKey: "session"}
	turn := replayTurnFixture(1)
	if !appendCodexReasoningReplayTurn(scope, turn) {
		t.Fatal("append failed")
	}
	turn.items[0][0] = '!'
	turn.callIDs[0] = "changed"
	got := getCodexReasoningReplayTurns(scope)
	if len(got) != 1 || got[0].items[0][0] != '{' || got[0].callIDs[0] != "call" {
		t.Fatal("store retained mutable source data")
	}
	got[0].items[0][0], got[0].callIDs[0] = '?', "changed again"
	appendCodexReasoningReplayTurn(scope, replayTurnFixture(1))
	got = getCodexReasoningReplayTurns(scope)
	if len(got) != 1 || got[0].items[0][0] != '{' || got[0].callIDs[0] != "call" {
		t.Fatal("duplicate or returned data changed stored turns")
	}
	withoutCalls := replayTurnFixture(2)
	withoutCalls.callIDs = nil
	appendCodexReasoningReplayTurn(scope, withoutCalls)
	withoutCalls.callIDs = []string{}
	appendCodexReasoningReplayTurn(scope, withoutCalls)
	got = getCodexReasoningReplayTurns(scope)
	if len(got) != 2 {
		t.Fatal("nil and empty call lists duplicated the same turn")
	}
	appendCodexReasoningReplayTurn(scope, got[1])
	if len(getCodexReasoningReplayTurns(scope)) != 2 {
		t.Fatal("reading and appending a turn changed its duplicate identity")
	}
	other := scope
	other.namespace = "another tenant"
	if len(getCodexReasoningReplayTurns(other)) != 0 {
		t.Fatal("turns crossed tenant boundaries")
	}
	if !setCodexReasoningReplayItems(scope, [][]byte{[]byte("legacy")}) || len(getCodexReasoningReplayTurns(scope)) != 0 {
		t.Fatal("legacy replacement retained cumulative turns")
	}
	items, ok := getCodexReasoningReplayItems(scope)
	if !ok || len(items) != 1 || string(items[0]) != "legacy" {
		t.Fatal("legacy cache contract changed")
	}
}

func TestCodexReplayTurnStoreMergesConcurrentCompletions(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	t.Cleanup(resetCodexReasoningReplayStoreForTest)
	scope := CodexReasoningReplayScope{namespace: "tenant", modelName: "model", sessionKey: "session"}
	var workers sync.WaitGroup
	for i := range 64 {
		workers.Go(func() {
			for range 3 {
				if !appendCodexReasoningReplayTurn(scope, replayTurnFixture(i)) {
					t.Error("concurrent append failed")
				}
				getCodexReasoningReplayTurns(scope)
			}
		})
	}
	workers.Wait()
	turns := getCodexReasoningReplayTurns(scope)
	if len(turns) != 64 {
		t.Fatalf("concurrent completions lost or duplicated turns: %d", len(turns))
	}
	var size int64
	for _, turn := range turns {
		size += turn.size
	}
	if codexReasoningReplayStore.totalBytes != size {
		t.Fatal("shared cache byte accounting drifted")
	}
}

func TestCodexReplayTurnStoreBoundsAndExpiry(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	t.Cleanup(resetCodexReasoningReplayStoreForTest)
	scope := CodexReasoningReplayScope{namespace: "tenant", modelName: "model", sessionKey: "session"}
	for i := range codexReasoningReplayMaxTurns + 1 {
		appendCodexReasoningReplayTurn(scope, replayTurnFixture(i))
	}
	turns := getCodexReasoningReplayTurns(scope)
	if len(turns) != codexReasoningReplayMaxTurns || turns[0].prefix != replayTurnFixture(1).prefix {
		t.Fatal("turn capacity did not evict the oldest turn")
	}
	deleteCodexReasoningReplay(scope)
	for i := range 3 {
		turn := replayTurnFixture(i)
		turn.items = [][]byte{bytes.Repeat([]byte{'a'}, 3<<20)}
		appendCodexReasoningReplayTurn(scope, turn)
	}
	if turns = getCodexReasoningReplayTurns(scope); len(turns) != 2 || codexReasoningReplayStore.totalBytes > CodexReasoningReplayCacheMaxEntryBytes {
		t.Fatal("byte capacity did not evict whole turns")
	}
	oversized := replayTurnFixture(9)
	oversized.items = [][]byte{make([]byte, CodexReasoningReplayCacheMaxEntryBytes+1)}
	if appendCodexReasoningReplayTurn(scope, oversized) || len(getCodexReasoningReplayTurns(scope)) != 2 {
		t.Fatal("oversized append replaced usable history")
	}
	codexReasoningReplayStore.Lock()
	entry := codexReasoningReplayStore.entries[codexReasoningReplayKey(scope)]
	entry.timestamp = time.Now().Add(-2 * CodexReasoningReplayCacheTTL)
	codexReasoningReplayStore.entries[codexReasoningReplayKey(scope)] = entry
	codexReasoningReplayStore.Unlock()
	if len(getCodexReasoningReplayTurns(scope)) != 0 || codexReasoningReplayStore.totalBytes != 0 {
		t.Fatal("expired turns remained in the cache or byte budget")
	}
}

func TestCodexReplayTurnStoreSharesGlobalBudget(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	t.Cleanup(resetCodexReasoningReplayStoreForTest)
	codexReasoningReplayStore.Lock()
	codexReasoningReplayStore.entries["older"] = codexReasoningReplayEntry{timestamp: time.Now().Add(-time.Minute), size: CodexReasoningReplayCacheMaxTotalBytes}
	codexReasoningReplayStore.totalBytes = CodexReasoningReplayCacheMaxTotalBytes
	codexReasoningReplayStore.Unlock()
	scope := CodexReasoningReplayScope{namespace: "tenant", modelName: "model", sessionKey: "session"}
	appendCodexReasoningReplayTurn(scope, replayTurnFixture(0))
	if _, exists := codexReasoningReplayStore.entries["older"]; exists || codexReasoningReplayStore.totalBytes > CodexReasoningReplayCacheMaxTotalBytes {
		t.Fatal("cumulative cache did not share the existing global budget")
	}
	deleteCodexReasoningReplay(scope)
	if codexReasoningReplayStore.totalBytes != 0 {
		t.Fatal("deletion leaked cumulative byte accounting")
	}
}
