package helps

import (
	"encoding/json"
	"strconv"
)

// CodexProtocolIdentity is a detached Chat identity source shared by retries.
type CodexProtocolIdentity struct {
	Metadata []byte
	CacheKey string
}

// SnapshotCodexProtocolMetadata excludes messages, tool data and arbitrary metadata.
func SnapshotCodexProtocolMetadata(payload []byte) []byte {
	var source []byte
	_ = visitCodexTopLevelFieldsUntil(payload, func(key, value []byte) bool {
		if codexJSONKeyEquals(key, "client_metadata") {
			source = value
			return false
		}
		return true
	})
	if len(source) == 0 {
		return nil
	}
	var filter func([]byte, int) []byte
	filter = func(raw []byte, depth int) []byte {
		if depth > 4 {
			return nil
		}
		out := []byte{'{'}
		n := 0
		err := visitCodexTopLevelFields(raw, func(key, value []byte) {
			name, _ := strconv.Unquote(string(key))
			if (CodexIdentityRole(name) != "" && len(value) > 0 && value[0] == '"') || (name == "window_number" && len(value) > 0 && ((value[0] >= '0' && value[0] <= '9') || value[0] == '"')) {
				out = appendCodexRawJSONField(out, &n, key, value)
			} else if name == "x-codex-turn-metadata" {
				nested := value
				encoded := len(nested) > 0 && nested[0] == '"'
				if encoded {
					nested = []byte(exactCodexIdentityString(value))
				}
				if len(nested) > 0 && nested[0] == '{' {
					next := filter(nested, depth+1)
					if len(next) == 0 {
						return
					}
					if encoded {
						next, _ = json.Marshal(string(next))
					}
					out = appendCodexRawJSONField(out, &n, key, next)
				}
			}
		})
		if err != nil || n == 0 {
			return nil
		}
		return append(out, '}')
	}
	return filter(source, 0)
}
