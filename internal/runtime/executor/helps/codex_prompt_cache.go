package helps

import (
	"bytes"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// CodexPromptCacheKeySnapshot owns only the explicit client key, never the request buffer.
// An empty Key leaves all legacy cache generation and identity behavior unchanged.
type CodexPromptCacheKeySnapshot struct {
	Key string
}

// CodexResponseIdentityRole keeps equal strings in session and turn roles independent.
type CodexResponseIdentityRole uint8

const (
	CodexResponseSessionIdentity CodexResponseIdentityRole = iota
	CodexResponseTurnIdentity
)

// SnapshotCodexPromptCacheKey captures the policy before translation, retries, or body release.
func SnapshotCodexPromptCacheKey(payload []byte, enabled bool) CodexPromptCacheKeySnapshot {
	if enabled {
		key := gjson.GetBytes(payload, "prompt_cache_key")
		if key.Type == gjson.String && strings.TrimSpace(key.Str) != "" {
			return CodexPromptCacheKeySnapshot{Key: strings.Clone(key.Str)}
		}
	}
	return CodexPromptCacheKeySnapshot{}
}

// Apply pins the cache role without assigning the key to any session identity field.
func (snapshot CodexPromptCacheKeySnapshot) Apply(payload []byte) []byte {
	if snapshot.Key == "" || gjson.GetBytes(payload, "prompt_cache_key").Str == snapshot.Key {
		return payload
	}
	if updated, err := sjson.SetBytes(payload, "prompt_cache_key", snapshot.Key); err == nil {
		return updated
	}
	return payload
}

// ReplaceCodexResponseIdentityFields rewrites protocol identity roles only. Cache keys,
// output text, tool arguments, item IDs, and arbitrary metadata are intentionally opaque.
// SSE framing is retained byte-for-byte apart from the identified JSON fields.
func ReplaceCodexResponseIdentityFields(payload []byte, from, to string, role CodexResponseIdentityRole) []byte {
	if from == "" || to == "" || from == to {
		return payload
	}
	if gjson.ValidBytes(payload) {
		return replaceCodexJSONIdentityFields(payload, from, to, role)
	}
	var out []byte
	start := 0
	for start < len(payload) {
		end := bytes.IndexAny(payload[start:], "\r\n")
		if end < 0 {
			end = len(payload)
		} else {
			end += start + 1
			if payload[end-1] == '\r' && end < len(payload) && payload[end] == '\n' {
				end++
			}
		}
		line := payload[start:end]
		trimmed := bytes.TrimSpace(line)
		if bytes.HasPrefix(trimmed, []byte("data:")) {
			data := bytes.TrimSpace(trimmed[len("data:"):])
			if gjson.ValidBytes(data) {
				updated := replaceCodexJSONIdentityFields(data, from, to, role)
				if !bytes.Equal(updated, data) {
					if out == nil {
						out = append(make([]byte, 0, len(payload)), payload[:start]...)
					}
					offset := bytes.Index(line, data)
					out = append(out, line[:offset]...)
					out = append(out, updated...)
					out = append(out, line[offset+len(data):]...)
					start = end
					continue
				}
			}
		}
		if out != nil {
			out = append(out, line...)
		}
		start = end
	}
	if out != nil {
		return out
	}
	return payload
}

func replaceCodexJSONIdentityFields(payload []byte, from, to string, role CodexResponseIdentityRole) []byte {
	fields := []string{"session_id", "thread_id", "window_id", "client_metadata.session_id", "client_metadata.thread_id", "client_metadata.x-codex-window-id"}
	turnFields := []string{"session_id", "thread_id", "window_id"}
	if role == CodexResponseTurnIdentity {
		fields = []string{"turn_id", "client_metadata.turn_id"}
		turnFields = []string{"turn_id"}
	}
	for _, prefix := range []string{"", "response."} {
		for _, field := range fields {
			payload = replaceCodexIdentityField(payload, prefix+field, from, to)
		}
		path := prefix + "client_metadata.x-codex-turn-metadata"
		turn := gjson.GetBytes(payload, path)
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
		for _, field := range turnFields {
			updated = replaceCodexIdentityField(updated, field, from, to)
		}
		if bytes.Equal(updated, metadata) {
			continue
		}
		if turn.Type == gjson.String {
			payload, _ = sjson.SetBytes(payload, path, string(updated))
		} else {
			payload, _ = sjson.SetRawBytes(payload, path, updated)
		}
	}
	return payload
}

func replaceCodexIdentityField(payload []byte, path, from, to string) []byte {
	value := gjson.GetBytes(payload, path)
	if value.Type != gjson.String {
		return payload
	}
	replacement := to
	if value.Str != from {
		if !strings.HasSuffix(path, "window_id") && !strings.HasSuffix(path, "x-codex-window-id") {
			return payload
		}
		if !strings.HasPrefix(value.Str, from+":") {
			return payload
		}
		replacement += strings.TrimPrefix(value.Str, from)
	}
	if updated, err := sjson.SetBytes(payload, path, replacement); err == nil {
		return updated
	}
	return payload
}
