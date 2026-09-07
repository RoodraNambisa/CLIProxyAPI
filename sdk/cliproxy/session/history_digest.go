package session

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"hash"
	"strings"

	"github.com/tidwall/gjson"
)

const (
	historyMaxParts           = 256
	historyMaxDepth           = 32
	historyCanonicalJSONLimit = 16 * 1024
)

type historyTurnDigest struct {
	sum         [sha256.Size]byte
	parts       int
	userContent bool
	valid       bool
}

type historyDigestBuilder struct {
	hash        hash.Hash
	parts       int
	userContent bool
	valid       bool
}

// historyDigest never retains prompt text. Oversized JSON parts use their full
// bytes rather than a sparse sample; over-budget structures disable inference.
func historyDigest(role string, values ...gjson.Result) historyTurnDigest {
	b := historyDigestBuilder{hash: sha256.New(), valid: true}
	writeHistoryField(b.hash, "cpa-history-turn-v1")
	writeHistoryField(b.hash, role)
	for _, value := range values {
		b.add(value, 0)
	}
	result := historyTurnDigest{parts: b.parts, userContent: role == "user" && b.userContent, valid: b.valid}
	copy(result.sum[:], b.hash.Sum(nil))
	return result
}

func writeHistoryField(h hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write([]byte(value))
}

func (b *historyDigestBuilder) part(kind, value string, userContent bool) {
	if b.parts >= historyMaxParts {
		b.valid = false
		return
	}
	b.parts++
	b.userContent = b.userContent || userContent
	writeHistoryField(b.hash, kind)
	writeHistoryField(b.hash, value)
}

func (b *historyDigestBuilder) add(value gjson.Result, depth int) {
	if !b.valid || !value.Exists() {
		return
	}
	if depth > historyMaxDepth {
		b.valid = false
		return
	}
	if value.Type == gjson.String {
		if strings.TrimSpace(value.Str) != "" {
			b.part("text", value.Str, true)
		}
		return
	}
	if value.IsArray() {
		value.ForEach(func(_, item gjson.Result) bool { b.add(item, depth+1); return b.valid })
		return
	}
	kind, userContent := "value", false
	if value.IsObject() {
		typ := strings.ToLower(strings.TrimSpace(value.Get("type").String()))
		switch typ {
		case "thinking", "redacted_thinking", "reasoning", "thought":
			return
		}
		if value.Get("thought").Type == gjson.True {
			return
		}
		// Tool results may contain a field named content or text. Keep the
		// complete tool object and never treat its output as a new user anchor.
		switch typ {
		case "tool_use", "tool_result", "function", "function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output":
			b.part("tool:"+typ, canonicalHistoryJSON(value.Raw), false)
			return
		}
		for _, key := range []string{"functionCall", "function_call", "functionResponse", "function_response"} {
			if value.Get(key).Exists() {
				b.part("tool:"+key, canonicalHistoryJSON(value.Raw), false)
				return
			}
		}
		if typ == "" || typ == "text" || typ == "input_text" || typ == "output_text" {
			if text := value.Get("text"); text.Type == gjson.String {
				b.add(text, depth+1)
				return
			}
		}
		kind = "json"
		for _, key := range []string{"image_url", "inlineData", "inline_data", "fileData", "file_data", "source", "file_id"} {
			if value.Get(key).Exists() {
				kind = "media"
				userContent = true
				break
			}
		}
	}
	b.part(kind, canonicalHistoryJSON(value.Raw), userContent)
}

func canonicalHistoryJSON(raw string) string {
	if len(raw) > historyCanonicalJSONLimit {
		return raw
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return raw
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return raw
	}
	return string(encoded)
}
