package helps

import (
	"net/http"

	"github.com/tidwall/gjson"
	"golang.org/x/net/http/httpguts"
)

// SyncCodexWebsocketRoutingHeaders updates existing compatibility aliases only
// after convergence and explicit routing protection have produced the final body.
func SyncCodexWebsocketRoutingHeaders(headers http.Header, payload []byte) {
	if headers == nil {
		return
	}
	if codexRoutingHeader(headers, "Conversation_id") != "" {
		key := gjson.GetBytes(payload, "prompt_cache_key")
		value := ""
		if key.Type == gjson.String && httpguts.ValidHeaderFieldValue(key.Str) {
			value = key.Str
		}
		setCodexRoutingHeader(headers, "Conversation_id", value)
		if value == "" {
			headers.Del("Conversation_id")
		}
	}
	session := codexRoutingHeader(headers, "Session-Id")
	if session == "" {
		metadata := gjson.GetBytes(payload, "client_metadata")
		session = firstExplicitCodexRoutingString(codexRoutingTurnObject(metadata.Get("x-codex-turn-metadata")).Get("session_id"), metadata.Get("session_id"))
	}
	if session != "" && httpguts.ValidHeaderFieldValue(session) && codexRoutingHeader(headers, "session_id") != "" {
		setCodexRoutingHeader(headers, "session_id", session)
	}
}

// RestoreCodexParentThreadHeader recovers an explicitly supplied reference when
// an intermediate client has omitted its compatibility header.
func RestoreCodexParentThreadHeader(headers http.Header, payload []byte) {
	if headers == nil || codexRoutingHeader(headers, "X-Codex-Parent-Thread-Id") != "" {
		return
	}
	metadata := gjson.GetBytes(payload, "client_metadata")
	parent := firstExplicitCodexRoutingString(codexRoutingTurnObject(metadata.Get("x-codex-turn-metadata")).Get("parent_thread_id"), metadata.Get("x-codex-parent-thread-id"), metadata.Get("parent_thread_id"))
	if parent != "" && httpguts.ValidHeaderFieldValue(parent) {
		setCodexRoutingHeader(headers, "X-Codex-Parent-Thread-Id", parent)
	}
}
