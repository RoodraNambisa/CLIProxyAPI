package helps

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// CodexSessionIdentity is the canonical session identity projected into a
// Codex request body and transport headers.
type CodexSessionIdentity struct {
	SessionID   string
	ThreadID    string
	TurnID      string
	WindowID    string
	RequestKind string
}

// CodexSessionIdentityHeaderSource contains identity values supplied through
// one header priority tier.
type CodexSessionIdentityHeaderSource struct {
	SessionID    string
	ThreadID     string
	WindowID     string
	TurnMetadata string
}

// ProjectCodexSessionIdentity merges identity sources into client_metadata and
// returns the same canonical turn metadata for the transport header.
func ProjectCodexSessionIdentity(
	payload []byte,
	admin CodexSessionIdentityHeaderSource,
	confused CodexSessionIdentityHeaderSource,
	client CodexSessionIdentityHeaderSource,
	defaults CodexSessionIdentity,
) ([]byte, CodexSessionIdentity, string, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return nil, CodexSessionIdentity{}, "", fmt.Errorf("Codex request payload must be a JSON object")
	}

	var rawClientMetadata []byte
	out := make([]byte, 0, len(payload)+512)
	out = append(out, '{')
	fields := 0
	err := visitCodexTopLevelFields(trimmed, func(rawKey, rawValue []byte) {
		if codexJSONKeyEquals(rawKey, "client_metadata") {
			rawClientMetadata = rawValue
			return
		}
		out = appendCodexRawJSONField(out, &fields, rawKey, rawValue)
	})
	if err != nil {
		return nil, CodexSessionIdentity{}, "", err
	}

	metadata, err := decodeCodexJSONObject(rawClientMetadata, "client_metadata", true)
	if err != nil {
		return nil, CodexSessionIdentity{}, "", err
	}
	bodyTurn, err := decodeCodexTurnMetadata(metadata["x-codex-turn-metadata"], "client_metadata.x-codex-turn-metadata", true)
	if err != nil {
		return nil, CodexSessionIdentity{}, "", err
	}
	adminTurn, err := decodeCodexTurnMetadata([]byte(admin.TurnMetadata), "credential X-Codex-Turn-Metadata", false)
	if err != nil {
		return nil, CodexSessionIdentity{}, "", err
	}
	clientTurn, err := decodeCodexTurnMetadata([]byte(client.TurnMetadata), "client X-Codex-Turn-Metadata", false)
	if err != nil {
		return nil, CodexSessionIdentity{}, "", err
	}

	bodySessionID := codexRawJSONString(metadata["session_id"])
	bodyThreadID := codexRawJSONString(metadata["thread_id"])
	bodyTurnID := codexRawJSONString(metadata["turn_id"])
	bodyWindowID := codexRawJSONString(metadata["x-codex-window-id"])

	identity := CodexSessionIdentity{
		SessionID: firstCodexIdentityValue(
			admin.SessionID, codexTurnString(adminTurn, "session_id"),
			codexTurnString(bodyTurn, "session_id"), bodySessionID,
			client.SessionID, codexTurnString(clientTurn, "session_id"),
			confused.SessionID, defaults.SessionID,
		),
		ThreadID: firstCodexIdentityValue(
			admin.ThreadID, codexTurnString(adminTurn, "thread_id"),
			codexTurnString(bodyTurn, "thread_id"), bodyThreadID,
			client.ThreadID, codexTurnString(clientTurn, "thread_id"),
			confused.ThreadID, defaults.ThreadID,
		),
		TurnID: firstCodexIdentityValue(
			codexTurnString(adminTurn, "turn_id"),
			codexTurnString(bodyTurn, "turn_id"), bodyTurnID,
			codexTurnString(clientTurn, "turn_id"), defaults.TurnID,
		),
		WindowID: firstCodexIdentityValue(
			admin.WindowID, codexTurnString(adminTurn, "window_id"),
			codexTurnString(bodyTurn, "window_id"), bodyWindowID,
			client.WindowID, codexTurnString(clientTurn, "window_id"),
			confused.WindowID, defaults.WindowID,
		),
		RequestKind: firstCodexIdentityValue(
			codexTurnString(adminTurn, "request_kind"), codexTurnString(bodyTurn, "request_kind"),
			codexTurnString(clientTurn, "request_kind"), defaults.RequestKind,
		),
	}
	if identity.ThreadID == "" {
		identity.ThreadID = identity.SessionID
	}
	if identity.SessionID == "" {
		identity.SessionID = identity.ThreadID
	}
	if identity.WindowID == "" && identity.ThreadID != "" {
		identity.WindowID = identity.ThreadID + ":0"
	}
	if identity.RequestKind == "" {
		identity.RequestKind = "turn"
	}
	if identity.SessionID == "" || identity.ThreadID == "" || identity.TurnID == "" || identity.WindowID == "" {
		return nil, CodexSessionIdentity{}, "", fmt.Errorf("Codex session identity defaults are incomplete")
	}

	turnMetadata := cloneCodexRawMap(clientTurn)
	overlayCodexRawMap(turnMetadata, bodyTurn)
	overlayCodexRawMap(turnMetadata, adminTurn)
	setCodexRawString(turnMetadata, "session_id", identity.SessionID)
	setCodexRawString(turnMetadata, "thread_id", identity.ThreadID)
	setCodexRawString(turnMetadata, "turn_id", identity.TurnID)
	setCodexRawString(turnMetadata, "window_id", identity.WindowID)
	setCodexRawString(turnMetadata, "request_kind", identity.RequestKind)
	turnMetadataJSON, err := json.Marshal(turnMetadata)
	if err != nil {
		return nil, CodexSessionIdentity{}, "", fmt.Errorf("encode Codex turn metadata: %w", err)
	}

	setCodexRawString(metadata, "session_id", identity.SessionID)
	setCodexRawString(metadata, "thread_id", identity.ThreadID)
	setCodexRawString(metadata, "turn_id", identity.TurnID)
	setCodexRawString(metadata, "x-codex-window-id", identity.WindowID)
	setCodexRawString(metadata, "x-codex-turn-metadata", string(turnMetadataJSON))
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, CodexSessionIdentity{}, "", fmt.Errorf("encode Codex client_metadata: %w", err)
	}
	out = appendCodexNamedJSONField(out, &fields, "client_metadata", string(metadataJSON))
	out = append(out, '}')
	return out, identity, string(turnMetadataJSON), nil
}

func decodeCodexJSONObject(raw []byte, field string, missingAllowed bool) (map[string]json.RawMessage, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 && missingAllowed {
		return make(map[string]json.RawMessage), nil
	}
	if len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return nil, fmt.Errorf("%s must be a JSON object", field)
	}
	decoded := make(map[string]json.RawMessage)
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("%s must be a valid JSON object: %w", field, err)
	}
	return decoded, nil
}

func decodeCodexTurnMetadata(raw []byte, field string, encoded bool) (map[string]json.RawMessage, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return make(map[string]json.RawMessage), nil
	}
	if encoded {
		if raw[0] == '"' {
			var value string
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, fmt.Errorf("%s must contain a valid JSON object: %w", field, err)
			}
			raw = []byte(strings.TrimSpace(value))
		}
	} else {
		raw = []byte(strings.TrimSpace(string(raw)))
	}
	return decodeCodexJSONObject(raw, field, false)
}

func codexJSONKeyEquals(rawKey []byte, expected string) bool {
	if bytes.Equal(rawKey, []byte(strconv.Quote(expected))) {
		return true
	}
	if !bytes.Contains(rawKey, []byte{'\\'}) {
		return false
	}
	key, err := strconv.Unquote(string(rawKey))
	return err == nil && key == expected
}

func codexRawJSONString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func codexTurnString(metadata map[string]json.RawMessage, key string) string {
	return codexRawJSONString(metadata[key])
}

func firstCodexIdentityValue(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func cloneCodexRawMap(source map[string]json.RawMessage) map[string]json.RawMessage {
	cloned := make(map[string]json.RawMessage, len(source)+5)
	overlayCodexRawMap(cloned, source)
	return cloned
}

func overlayCodexRawMap(target, source map[string]json.RawMessage) {
	for key, value := range source {
		target[key] = bytes.Clone(value)
	}
}

func setCodexRawString(target map[string]json.RawMessage, key, value string) {
	target[key] = json.RawMessage(strconv.Quote(value))
}
