package helps

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"time"
)

const codexReasoningReplayMaxTurns = 256

type codexReasoningReplayTurn struct {
	id        [sha256.Size]byte
	prefix    [sha256.Size]byte
	assistant [sha256.Size]byte
	callIDs   []string
	items     [][]byte
	size      int64
}

func cloneCodexReasoningReplayTurn(turn codexReasoningReplayTurn) codexReasoningReplayTurn {
	turn.items = cloneCodexReplayItems(turn.items)
	ids := make([]string, len(turn.callIDs))
	for index, id := range turn.callIDs {
		ids[index] = strings.Clone(id)
	}
	turn.callIDs = ids
	return turn
}

// The caller supplies normalized replay items. Storage owns all bytes and keeps
// the existing per-entry and process-wide budgets across legacy and turn caches.
func appendCodexReasoningReplayTurn(scope CodexReasoningReplayScope, turn codexReasoningReplayTurn, contexts ...context.Context) bool {
	key := codexReasoningReplayKey(scope)
	if key == "" || len(turn.items) == 0 {
		return false
	}
	turn.size = codexReplayItemsSize(turn.items) + 3*sha256.Size
	for _, id := range turn.callIDs {
		turn.size += int64(len(id))
	}
	if turn.size > CodexReasoningReplayCacheMaxEntryBytes {
		return false
	}
	turn = cloneCodexReasoningReplayTurn(turn)
	hasher := sha256.New()
	_, _ = hasher.Write(turn.prefix[:])
	_, _ = hasher.Write(turn.assistant[:])
	ids, _ := json.Marshal(turn.callIDs)
	_, _ = hasher.Write(ids)
	for _, item := range turn.items {
		_, _ = hasher.Write([]byte("\x00item\x00"))
		_, _ = hasher.Write(item)
	}
	hasher.Sum(turn.id[:0])
	now := time.Now()
	codexReasoningReplayStore.Lock()
	defer codexReasoningReplayStore.Unlock()
	for _, ctx := range contexts {
		if ctx != nil && ctx.Err() != nil {
			return false
		}
	}
	previous := codexReasoningReplayStore.entries[key]
	var turns []codexReasoningReplayTurn
	var size int64
	if now.Sub(previous.timestamp) <= CodexReasoningReplayCacheTTL {
		for _, old := range previous.turns {
			if old.id != turn.id {
				turns = append(turns, old)
				size += old.size
			}
		}
	}
	turns = append(turns, turn)
	size += turn.size
	start := 0
	for len(turns)-start > codexReasoningReplayMaxTurns || size > CodexReasoningReplayCacheMaxEntryBytes {
		size -= turns[start].size
		start++
	}
	if start > 0 {
		// Copy the suffix so evicted turns do not remain in the backing array.
		turns = append([]codexReasoningReplayTurn(nil), turns[start:]...)
	}
	publishCodexReasoningReplayEntryLocked(key, codexReasoningReplayEntry{items: turn.items, turns: turns, size: size}, now)
	return true
}

func getCodexReasoningReplayTurns(scope CodexReasoningReplayScope) []codexReasoningReplayTurn {
	key := codexReasoningReplayKey(scope)
	if key == "" {
		return nil
	}
	now := time.Now()
	codexReasoningReplayStore.Lock()
	entry, ok := codexReasoningReplayStore.entries[key]
	if ok && now.Sub(entry.timestamp) > CodexReasoningReplayCacheTTL {
		delete(codexReasoningReplayStore.entries, key)
		codexReasoningReplayStore.totalBytes -= entry.size
		ok = false
	} else if ok && len(entry.turns) > 0 {
		entry.timestamp = now
		codexReasoningReplayStore.entries[key] = entry
	}
	codexReasoningReplayStore.Unlock()
	if !ok {
		return nil
	}
	turns := make([]codexReasoningReplayTurn, len(entry.turns))
	for index, turn := range entry.turns {
		turns[index] = cloneCodexReasoningReplayTurn(turn)
	}
	return turns
}
