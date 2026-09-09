package common

import (
	"bytes"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// EnsureResponsesUsageDetails fills missing detail counters in existing JSON
// usage objects. It leaves malformed JSON, absent usage and compaction intact.
func EnsureResponsesUsageDetails(payload []byte) []byte {
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
