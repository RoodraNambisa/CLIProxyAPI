package claude

import (
	"crypto/sha256"
	"fmt"
	"strconv"

	"github.com/tidwall/gjson"
)

func finishCodexReasoningItem(output []byte, params *ConvertCodexResponseToClaudeParams, event, item gjson.Result) []byte {
	signature := item.Get("encrypted_content").String()
	if signature == "" {
		signature = params.ThinkingSignature
	}
	keys := make([]string, 0, 2)
	itemID, eventID := item.Get("id").String(), event.Get("item_id").String()
	if itemID != "" && eventID != "" && itemID != eventID {
		return output
	}
	if itemID == "" {
		itemID = eventID
	}
	if itemID != "" {
		keys = append(keys, "item:"+itemID)
	}
	if index := event.Get("output_index"); index.Type == gjson.Number && index.Int() >= 0 && index.Float() == float64(index.Int()) {
		keys = append(keys, "index:"+strconv.FormatInt(index.Int(), 10))
	}
	if len(keys) == 0 && signature != "" {
		// Keep unlabelled duplicate detection bounded to a digest, never the opaque payload itself.
		keys = append(keys, fmt.Sprintf("signature:%x", sha256.Sum256([]byte(signature))))
	}
	for _, key := range keys {
		if _, finished := params.FinishedThinkingItems[key]; finished {
			return output
		}
	}
	for _, key := range keys {
		if params.FinishedThinkingItems == nil {
			params.FinishedThinkingItems = make(map[string]struct{})
		}
		params.FinishedThinkingItems[key] = struct{}{}
	}
	params.ThinkingSignature = signature
	if !params.ThinkingSummarySeen && !params.ThinkingBlockOpen && signature != "" {
		output = append(output, stopCodexTextBlock(params)...)
		output = append(output, startCodexThinkingBlock(params)...)
	}
	output = append(output, finalizeCodexThinkingBlock(params)...)
	params.ThinkingSignature = ""
	params.ThinkingSummarySeen = false
	return output
}
