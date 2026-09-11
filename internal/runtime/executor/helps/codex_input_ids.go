package helps

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const codexInputItemIDLimit = 64

type codexInputItemIdentity struct {
	id     string
	kind   string
	callID string
}

func codexInputIdentity(item gjson.Result, id, kind string) codexInputItemIdentity {
	identity := codexInputItemIdentity{id: id, kind: kind}
	if identity.kind == "" && item.Get("role").Str != "" {
		identity.kind = "message"
	}
	switch identity.kind {
	case "function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output":
		identity.callID = item.Get("call_id").String()
	}
	return identity
}

// SanitizeCodexInputItemIDs applies type prefixes and bounded deterministic IDs.
// Existing valid IDs reserve their names before replacements are allocated.
// call_id is a separate tool-pairing key and is never rewritten here.
func SanitizeCodexInputItemIDs(body []byte) []byte {
	// A JSON key can spell id literally or use Unicode escapes. Without either
	// representation no item can need ID repair.
	if !util.JSONMayContainAnyField(body, "id") {
		return body
	}
	// Parsed views stay in this call; changed output is rebuilt into owned bytes.
	input := gjson.Get(unsafe.String(unsafe.SliceData(body), len(body)), "input")
	if !input.IsArray() {
		return body
	}
	items := input.Array()
	owners := make(map[string]codexInputItemIdentity, len(items))
	needsRewrite := false
	for _, item := range items {
		id := item.Get("id")
		if id.Type != gjson.String || id.Str == "" {
			continue
		}
		if dropCodexEncryptedReasoningID(item, id) {
			needsRewrite = true
			continue
		}
		kind := item.Get("type").Str
		normalized := normalizeCodexInputItemID(kind, id.Str)
		tooLong := utf8.RuneCountInString(normalized) > codexInputItemIDLimit
		needsRewrite = needsRewrite || normalized != id.Str || tooLong
		identity := codexInputIdentity(item, id.Str, kind)
		owner, exists := owners[normalized]
		if exists && owner != identity {
			needsRewrite = true
		}
		// Prefer an existing canonical ID over one that only gains this name
		// through prefix normalization. Otherwise the first identity owns it.
		if !exists || (owner.id != normalized && id.Str == normalized) {
			owners[normalized] = identity
		}
	}
	if !needsRewrite {
		return body
	}
	// Repeated occurrences of one item keep their mapping; distinct tool pairs
	// can reuse a source ID without being assigned the same replacement.
	mapped := make(map[codexInputItemIdentity]string)
	rebuilt := make([]string, 0, len(items))
	changed := false
	for _, item := range items {
		id := item.Get("id")
		if dropCodexEncryptedReasoningID(item, id) {
			changed = true
			continue
		}
		raw := item.Raw
		if id.Type == gjson.String && id.Str != "" {
			kind := item.Get("type").Str
			normalized := normalizeCodexInputItemID(kind, id.Str)
			identity := codexInputIdentity(item, id.Str, kind)
			needsSuffix := utf8.RuneCountInString(normalized) > codexInputItemIDLimit || owners[normalized] != identity
			if needsSuffix {
				replacement, ok := mapped[identity]
				if !ok {
					for attempt := 0; ; attempt++ {
						replacement = codexInputItemIDWithHashSuffix(normalized, attempt)
						if _, occupied := owners[replacement]; !occupied {
							break
						}
					}
					mapped[identity] = replacement
					owners[replacement] = identity
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

func normalizeCodexInputItemID(kind, id string) string {
	var prefix string
	switch kind {
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
