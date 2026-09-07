package helps

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// These rewrites normalize declared collaboration message envelopes for opt-in
// multi-agent preparation. They do not decrypt their string values.
func rewriteCodexAgentMessageInput(payload []byte) []byte {
	if !gjson.ValidBytes(payload) {
		return payload
	}
	input := gjson.GetBytes(payload, "input")
	if !input.IsArray() {
		return payload
	}

	updated := rewriteCodexAgentMessageContent(payload)
	for itemIndex, item := range input.Array() {
		if strings.TrimSpace(item.Get("type").String()) != "agent_message" {
			continue
		}
		itemPath := fmt.Sprintf("input.%d", itemIndex)
		var errSet error
		updated, errSet = sjson.SetBytes(updated, itemPath+".role", "user")
		if errSet != nil {
			return payload
		}
		updated, errSet = sjson.SetBytes(updated, itemPath+".type", "message")
		if errSet != nil {
			return payload
		}
	}
	return updated
}

func rewriteCodexAgentMessageContent(payload []byte) []byte {
	if !gjson.ValidBytes(payload) {
		return payload
	}
	input := gjson.GetBytes(payload, "input")
	if !input.IsArray() {
		return payload
	}

	updated := payload
	for itemIndex, item := range input.Array() {
		if strings.TrimSpace(item.Get("type").String()) != "agent_message" {
			continue
		}
		content := item.Get("content")
		if !content.IsArray() {
			continue
		}
		for partIndex, part := range content.Array() {
			if strings.TrimSpace(part.Get("type").String()) != "encrypted_content" {
				continue
			}
			encryptedContent := part.Get("encrypted_content")
			if encryptedContent.Type != gjson.String {
				continue
			}
			partPath := fmt.Sprintf("input.%d.content.%d", itemIndex, partIndex)
			var errSet error
			updated, errSet = sjson.SetBytes(updated, partPath+".type", "input_text")
			if errSet != nil {
				return payload
			}
			updated, errSet = sjson.SetBytes(updated, partPath+".text", encryptedContent.String())
			if errSet != nil {
				return payload
			}
			updated, errSet = sjson.DeleteBytes(updated, partPath+".encrypted_content")
			if errSet != nil {
				return payload
			}
		}
	}
	return updated
}
