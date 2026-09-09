package chat_completions

import (
	"strconv"

	"github.com/tidwall/gjson"
)

type codexOpenAIPendingToolCall struct {
	sourceID, callID string
	custom, consumed bool
}

// A batch ends when another conversational message starts. Results cannot bind
// to stale calls, ambiguous IDs, or an already consumed call from that batch.
type codexOpenAIToolBatch struct {
	pending  []codexOpenAIPendingToolCall
	counts   map[string]int
	reserved map[string]bool
}

func (b *codexOpenAIToolBatch) begin(calls []gjson.Result) {
	b.pending = nil
	b.counts = make(map[string]int, len(calls))
	b.reserved = make(map[string]bool, len(calls))
	for _, call := range calls {
		id := call.Get("id").String()
		if id == "" {
			continue
		}
		b.reserved[id] = true
		if kind := call.Get("type").String(); kind == "function" || kind == "custom" {
			b.counts[id]++
		}
	}
}

func (b *codexOpenAIToolBatch) add(sourceID string, custom bool, messageIndex, callIndex int) (string, bool) {
	if sourceID != "" && b.counts[sourceID] > 1 {
		return "", false
	}
	id := sourceID
	if id == "" {
		base := "call_missing_" + strconv.Itoa(messageIndex) + "_" + strconv.Itoa(callIndex)
		id = base
		for suffix := 1; b.reserved[id]; suffix++ {
			id = base + "_" + strconv.Itoa(suffix)
		}
		b.reserved[id] = true
	}
	b.pending = append(b.pending, codexOpenAIPendingToolCall{sourceID: sourceID, callID: id, custom: custom})
	return id, true
}

func (b *codexOpenAIToolBatch) consume(sourceID string) (string, bool, bool) {
	if sourceID != "" && b.counts[sourceID] > 1 {
		return "", false, false
	}
	match := -1
	for index := range b.pending {
		call := &b.pending[index]
		if call.consumed || sourceID != "" && sourceID != call.sourceID && sourceID != call.callID {
			continue
		}
		if match >= 0 {
			// Missing result IDs are repaired only when the pending call is unique.
			return "", false, false
		}
		match = index
	}
	if match < 0 {
		return "", false, false
	}
	call := &b.pending[match]
	call.consumed = true
	return call.callID, call.custom, true
}
