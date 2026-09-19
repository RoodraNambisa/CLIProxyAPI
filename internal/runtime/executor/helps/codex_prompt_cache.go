package helps

import (
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
	CodexResponseCacheAndSessionIdentity
	CodexResponseContextWindowIdentity
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
			if value := codexRoutingHeader(source, name); value != "" {
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
