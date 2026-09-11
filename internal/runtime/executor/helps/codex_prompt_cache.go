package helps

import (
	"bytes"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// CodexPromptCacheKeySnapshot owns client routing identifiers, never request buffers.
// Empty identifiers leave their corresponding legacy behavior unchanged.
type CodexPromptCacheKeySnapshot struct {
	Key       string
	SessionID string
}

// CodexResponseIdentityRole keeps equal strings in session and turn roles independent.
type CodexResponseIdentityRole uint8

const (
	CodexResponseSessionIdentity CodexResponseIdentityRole = iota
	CodexResponseTurnIdentity
	CodexResponseThreadAndWindowIdentity
)

// SnapshotCodexPromptCacheKey captures the policy before translation, retries, or body release.
func SnapshotCodexPromptCacheKey(payload []byte, enabled bool, headers ...http.Header) CodexPromptCacheKeySnapshot {
	if !enabled {
		return CodexPromptCacheKeySnapshot{}
	}
	snapshot := CodexPromptCacheKeySnapshot{Key: explicitCodexRoutingString(gjson.GetBytes(payload, "prompt_cache_key"))}
	if util.JSONMayContainAnyField(payload, "client_metadata", "session_id") {
		metadata := gjson.GetBytes(payload, "client_metadata")
		turn := codexRoutingTurnObject(metadata.Get("x-codex-turn-metadata"))
		snapshot.SessionID = firstExplicitCodexRoutingString(turn.Get("session_id"), metadata.Get("session_id"), gjson.GetBytes(payload, "session_id"))
	}
	for _, source := range headers {
		if snapshot.SessionID != "" {
			break
		}
		for _, name := range []string{"Session-Id", "session_id"} {
			if value := codexRoutingHeader(source, name); strings.TrimSpace(value) != "" {
				snapshot.SessionID = strings.Clone(value)
				break
			}
		}
		if snapshot.SessionID == "" {
			snapshot.SessionID = explicitCodexRoutingString(gjson.Get(codexRoutingHeader(source, "X-Codex-Turn-Metadata"), "session_id"))
		}
	}
	if snapshot.SessionID == "" {
		snapshot.SessionID = snapshot.Key
	}
	return snapshot
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
	} else if role == CodexResponseThreadAndWindowIdentity {
		fields = []string{"thread_id", "window_id", "client_metadata.thread_id", "client_metadata.x-codex-window-id"}
		turnFields = []string{"thread_id", "window_id"}
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
