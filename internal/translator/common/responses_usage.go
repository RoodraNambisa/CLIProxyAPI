package common

import (
	"bytes"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// EnsureResponsesUsageDetails fills existing usage objects in JSON or SSE.
// Malformed JSON, absent usage and compaction remain intact.
func EnsureResponsesUsageDetails(payload []byte) []byte {
	if !bytes.Contains(payload, []byte(`"usage"`)) && !bytes.Contains(payload, []byte(`\u`)) {
		return payload
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) > 0 && trimmed[0] != '{' {
		return ensureResponsesSSEUsageDetails(payload)
	}
	return ensureResponsesJSONUsageDetails(payload)
}

func ensureResponsesJSONUsageDetails(payload []byte) []byte {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' || !gjson.ValidBytes(trimmed) {
		return payload
	}
	if gjson.GetBytes(trimmed, "object").String() == "response.compaction" {
		return payload
	}
	updated := ensureResponsesUsageDetailsAt(trimmed, "response.usage")
	updated = ensureResponsesUsageDetailsAt(updated, "usage")
	if bytes.Equal(updated, trimmed) {
		return payload
	}
	return updated
}

func ensureResponsesUsageDetailsAt(payload []byte, path string) []byte {
	usage := gjson.GetBytes(payload, path)
	if !usage.IsObject() {
		return payload
	}
	for _, field := range []struct{ parent, leaf string }{
		{"output_tokens_details", "reasoning_tokens"},
		{"input_tokens_details", "cached_tokens"},
	} {
		details := usage.Get(field.parent)
		if !details.IsObject() {
			payload, _ = sjson.SetRawBytes(payload, path+"."+field.parent, []byte(`{"`+field.leaf+`":0}`))
			continue
		}
		value := details.Get(field.leaf)
		if !value.Exists() || value.Type == gjson.Null {
			payload, _ = sjson.SetBytes(payload, path+"."+field.parent+"."+field.leaf, 0)
		}
	}
	return payload
}
