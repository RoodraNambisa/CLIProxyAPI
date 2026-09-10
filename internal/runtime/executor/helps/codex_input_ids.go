package helps

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const codexInputItemIDLimit = 64

// SanitizeCodexInputItemIDs applies type prefixes and bounded deterministic IDs.
// Existing valid IDs reserve their names before replacements are allocated.
// call_id is a separate tool-pairing key and is never rewritten here.
func SanitizeCodexInputItemIDs(body []byte) []byte {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return body
	}
	items := input.Array()
	occupied := make(map[string]bool, len(items))
	preserved := make(map[string]bool, len(items))
	needsRewrite := false
	for _, item := range items {
		id := item.Get("id")
		if id.Type != gjson.String {
			continue
		}
		if dropCodexEncryptedReasoningID(item, id) {
			needsRewrite = true
			continue
		}
		normalized := normalizeCodexInputItemID(item, id.Str)
		tooLong := utf8.RuneCountInString(normalized) > codexInputItemIDLimit
		needsRewrite = needsRewrite || normalized != id.Str || tooLong
		if !tooLong {
			occupied[normalized] = true
		}
		if normalized == id.Str {
			preserved[normalized] = true
		}
	}
	if !needsRewrite {
		return body
	}
	// Prefix collisions must not share the shortening map with preserved IDs.
	mapped := make(map[string]string)
	collisionMapped := make(map[string]string)
	rebuilt := make([]string, 0, len(items))
	changed := false
	for _, item := range items {
		id := item.Get("id")
		if dropCodexEncryptedReasoningID(item, id) {
			changed = true
			continue
		}
		raw := item.Raw
		if id.Type == gjson.String {
			normalized := normalizeCodexInputItemID(item, id.Str)
			prefixCollision := normalized != id.Str && preserved[normalized]
			needsSuffix := utf8.RuneCountInString(normalized) > codexInputItemIDLimit || prefixCollision
			if needsSuffix {
				mappings := mapped
				if prefixCollision {
					mappings = collisionMapped
				}
				replacement, ok := mappings[normalized]
				if !ok {
					for attempt := 0; ; attempt++ {
						replacement = codexInputItemIDWithHashSuffix(normalized, attempt)
						if !occupied[replacement] {
							break
						}
					}
					mappings[normalized] = replacement
					occupied[replacement] = true
				}
				normalized = replacement
			}
			if normalized != id.Str {
				if updated, err := sjson.Set(raw, "id", normalized); err == nil {
					raw = updated
					changed = true
				}
			}
		}
		rebuilt = append(rebuilt, raw)
	}
	if !changed {
		return body
	}
	updated, err := sjson.SetRawBytes(body, "input", []byte("["+strings.Join(rebuilt, ",")+"]"))
	if err != nil {
		return body
	}
	return updated
}

func normalizeCodexInputItemID(item gjson.Result, id string) string {
	var prefix string
	switch item.Get("type").Str {
	case "message":
		prefix = "msg"
	case "reasoning":
		prefix = "rs"
	case "function_call":
		prefix = "fc"
	case "custom_tool_call":
		prefix = "ctc"
	case "custom_tool_call_output":
		prefix = "ctco"
	default:
		return id
	}
	if id == "" || strings.HasPrefix(id, prefix) {
		return id
	}
	return prefix + "_" + id
}

func dropCodexEncryptedReasoningID(item, id gjson.Result) bool {
	if id.Type != gjson.String || utf8.RuneCountInString(id.Str) <= codexInputItemIDLimit || item.Get("type").Str != "reasoning" {
		return false
	}
	encrypted := item.Get("encrypted_content")
	return encrypted.Type == gjson.String && encrypted.Str != ""
}

func codexInputItemIDWithHashSuffix(id string, attempt int) string {
	hashInput := id
	if attempt > 0 {
		hashInput += "\x00" + strconv.Itoa(attempt)
	}
	sum := sha256.Sum256([]byte(hashInput))
	suffix := "_" + hex.EncodeToString(sum[:8])
	runes := []rune(id)
	return string(runes[:min(len(runes), codexInputItemIDLimit-len(suffix))]) + suffix
}
