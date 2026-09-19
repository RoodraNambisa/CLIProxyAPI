package helps

import (
	"bytes"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// CodexResponseIdentityReplacement applies only to a known protocol identity role.
type CodexResponseIdentityReplacement struct {
	From string
	To   string
	Role CodexResponseIdentityRole
}

// ReplaceCodexResponseIdentityFields preserves cache keys unless explicitly selected.
func ReplaceCodexResponseIdentityFields(payload []byte, from, to string, role CodexResponseIdentityRole) []byte {
	return ReplaceCodexResponseIdentities(payload, []CodexResponseIdentityReplacement{{From: from, To: to, Role: role}})
}

// ReplaceCodexResponseIdentities reads all replacements from the original event.
// It never scans business content or cascades one replacement into another.
func ReplaceCodexResponseIdentities(payload []byte, replacements []CodexResponseIdentityReplacement) []byte {
	if len(replacements) == 0 {
		return payload
	}
	return rewriteCodexSSEChunk(payload, func(original []byte) []byte {
		if !gjson.ValidBytes(original) {
			return original
		}
		out := original
		for _, prefix := range []string{"", "response."} {
			for _, field := range codexResponseIdentityFields {
				path := prefix + field
				out = replaceCodexIdentityValue(out, path, gjson.GetBytes(original, path), field, replacements)
				path = prefix + "client_metadata." + field
				out = replaceCodexIdentityValue(out, path, gjson.GetBytes(original, path), field, replacements)
			}
			path := prefix + "client_metadata.x-codex-turn-metadata"
			turn := gjson.GetBytes(original, path)
			var metadata []byte
			if turn.Type == gjson.String {
				metadata = []byte(turn.Str)
			} else if turn.IsObject() {
				metadata = []byte(turn.Raw)
			}
			if !gjson.ValidBytes(metadata) {
				continue
			}
			updated := metadata
			for _, field := range codexResponseIdentityFields {
				updated = replaceCodexIdentityValue(updated, field, gjson.GetBytes(metadata, field), field, replacements)
			}
			if bytes.Equal(updated, metadata) {
				continue
			}
			if turn.Type == gjson.String {
				out, _ = sjson.SetBytes(out, path, string(updated))
			} else {
				out, _ = sjson.SetRawBytes(out, path, updated)
			}
		}
		return out
	})
}

var codexResponseIdentityFields = []string{
	"session_id", "thread_id", "window_id", "x-codex-window-id", "prompt_cache_key",
	"turn_id", "root_turn_id", "parent_turn_id", "root_thread_id", "parent_thread_id",
	"forked_from_thread_id", "x-codex-parent-thread-id", "context_window_id",
}

func codexIdentityRoleContains(role CodexResponseIdentityRole, field string) bool {
	switch field {
	case "turn_id", "root_turn_id", "parent_turn_id":
		return role == CodexResponseTurnIdentity
	case "context_window_id":
		return role == CodexResponseContextWindowIdentity
	case "prompt_cache_key":
		return role == CodexResponseCacheAndSessionIdentity
	case "session_id":
		return role == CodexResponseSessionIdentity || role == CodexResponseCacheAndSessionIdentity
	default:
		return role == CodexResponseSessionIdentity || role == CodexResponseThreadAndWindowIdentity || role == CodexResponseCacheAndSessionIdentity
	}
}

func replaceCodexIdentityValue(payload []byte, path string, value gjson.Result, field string, replacements []CodexResponseIdentityReplacement) []byte {
	if value.Type != gjson.String {
		return payload
	}
	for _, replacement := range replacements {
		if replacement.From == "" || replacement.To == "" || replacement.From == replacement.To || !codexIdentityRoleContains(replacement.Role, field) {
			continue
		}
		text := replacement.To
		if value.Str != replacement.From {
			if field != "window_id" && field != "x-codex-window-id" {
				continue
			}
			thread, suffix, ok := strings.Cut(value.Str, ":")
			if !ok || thread != replacement.From || suffix == "" || strings.IndexFunc(suffix, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
				continue
			}
			text += ":" + suffix
		}
		if updated, err := sjson.SetBytes(payload, path, text); err == nil {
			return updated
		}
		return payload
	}
	return payload
}
