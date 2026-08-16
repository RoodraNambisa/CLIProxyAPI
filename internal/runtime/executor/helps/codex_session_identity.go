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
	InstallationID string
	SessionID      string
	ThreadID       string
	TurnID         string
	WindowID       string
	RequestKind    string
}

// CodexSessionIdentityHeaderSource contains identity values supplied through
// one header priority tier.
type CodexSessionIdentityHeaderSource struct {
	InstallationID string
	SessionID      string
	ThreadID       string
	WindowID       string
	TurnMetadata   string
}

// CodexSessionIdentityProjection controls forced per-credential identity while
// preserving the existing fill-missing behavior for ordinary session spoofing.
type CodexSessionIdentityProjection struct {
	InstallationID string
	ForcedIdentity CodexSessionIdentity
	ProjectSession bool
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
	return ProjectCodexSessionIdentityWithProjection(
		payload, admin, confused, client, defaults,
		CodexSessionIdentityProjection{ProjectSession: true},
	)
}

// ProjectCodexSessionIdentityWithProjection applies optional per-credential
// convergence in the same top-level JSON rebuild used by session spoofing.
func ProjectCodexSessionIdentityWithProjection(
	payload []byte,
	admin CodexSessionIdentityHeaderSource,
	confused CodexSessionIdentityHeaderSource,
	client CodexSessionIdentityHeaderSource,
	defaults CodexSessionIdentity,
	projection CodexSessionIdentityProjection,
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

	installationID := firstCodexIdentityValue(
		admin.InstallationID, codexTurnString(adminTurn, "installation_id"),
		projection.InstallationID,
		confused.InstallationID,
		codexTurnString(bodyTurn, "installation_id"), codexRawJSONString(metadata["x-codex-installation-id"]),
		client.InstallationID, codexTurnString(clientTurn, "installation_id"),
		defaults.InstallationID,
	)
	if !projection.ProjectSession {
		if installationID == "" {
			return nil, CodexSessionIdentity{}, "", fmt.Errorf("Codex installation identity is incomplete")
		}
		setCodexRawString(metadata, "x-codex-installation-id", installationID)
		turnMetadataJSON := []byte(nil)
		if strings.TrimSpace(admin.TurnMetadata) != "" || strings.TrimSpace(client.TurnMetadata) != "" || len(metadata["x-codex-turn-metadata"]) > 0 {
			turnMetadata := cloneCodexRawMap(clientTurn)
			overlayCodexRawMap(turnMetadata, bodyTurn)
			overlayCodexRawMap(turnMetadata, adminTurn)
			setCodexRawString(turnMetadata, "installation_id", installationID)
			turnMetadataJSON, err = json.Marshal(turnMetadata)
			if err != nil {
				return nil, CodexSessionIdentity{}, "", fmt.Errorf("encode Codex turn metadata: %w", err)
			}
			setCodexRawString(metadata, "x-codex-turn-metadata", string(turnMetadataJSON))
		}
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return nil, CodexSessionIdentity{}, "", fmt.Errorf("encode Codex client_metadata: %w", err)
		}
		out = appendCodexNamedJSONField(out, &fields, "client_metadata", string(metadataJSON))
		out = append(out, '}')
		return out, CodexSessionIdentity{InstallationID: installationID}, string(turnMetadataJSON), nil
	}

	bodySessionID := codexRawJSONString(metadata["session_id"])
	bodyThreadID := codexRawJSONString(metadata["thread_id"])
	bodyTurnID := codexRawJSONString(metadata["turn_id"])
	bodyWindowID := codexRawJSONString(metadata["x-codex-window-id"])

	sessionID := firstCodexIdentityValue(
		admin.SessionID, codexTurnString(adminTurn, "session_id"),
		projection.ForcedIdentity.SessionID,
		confused.SessionID,
		codexTurnString(bodyTurn, "session_id"), bodySessionID,
		client.SessionID, codexTurnString(clientTurn, "session_id"),
	)
	threadID := firstCodexIdentityValue(
		admin.ThreadID, codexTurnString(adminTurn, "thread_id"),
		projection.ForcedIdentity.ThreadID,
		confused.ThreadID,
		codexTurnString(bodyTurn, "thread_id"), bodyThreadID,
		client.ThreadID, codexTurnString(clientTurn, "thread_id"),
	)
	usingDefaultIdentity := sessionID == "" && threadID == ""
	if usingDefaultIdentity {
		sessionID = firstCodexIdentityValue(defaults.SessionID)
		threadID = firstCodexIdentityValue(defaults.ThreadID)
	}
	if threadID == "" {
		threadID = sessionID
	}
	if sessionID == "" {
		sessionID = threadID
	}
	windowID := firstCodexIdentityValue(
		admin.WindowID, codexTurnString(adminTurn, "window_id"),
		projection.ForcedIdentity.WindowID,
		confused.WindowID,
		codexTurnString(bodyTurn, "window_id"), bodyWindowID,
		client.WindowID, codexTurnString(clientTurn, "window_id"),
	)
	if windowID == "" && usingDefaultIdentity {
		windowID = firstCodexIdentityValue(defaults.WindowID)
	}
	if windowID == "" && threadID != "" {
		windowID = threadID + ":0"
	}

	identity := CodexSessionIdentity{
		SessionID: sessionID,
		ThreadID:  threadID,
		TurnID: firstCodexIdentityValue(
			codexTurnString(adminTurn, "turn_id"),
			projection.ForcedIdentity.TurnID,
			codexTurnString(bodyTurn, "turn_id"), bodyTurnID,
			codexTurnString(clientTurn, "turn_id"), defaults.TurnID,
		),
		WindowID: windowID,
		RequestKind: firstCodexIdentityValue(
			codexTurnString(adminTurn, "request_kind"), codexTurnString(bodyTurn, "request_kind"),
			codexTurnString(clientTurn, "request_kind"), defaults.RequestKind,
		),
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
	if projection.InstallationID != "" {
		identity.InstallationID = installationID
		setCodexRawString(turnMetadata, "installation_id", installationID)
	}
	turnMetadataJSON, err := json.Marshal(turnMetadata)
	if err != nil {
		return nil, CodexSessionIdentity{}, "", fmt.Errorf("encode Codex turn metadata: %w", err)
	}

	setCodexRawString(metadata, "session_id", identity.SessionID)
	setCodexRawString(metadata, "thread_id", identity.ThreadID)
	setCodexRawString(metadata, "turn_id", identity.TurnID)
	setCodexRawString(metadata, "x-codex-window-id", identity.WindowID)
	setCodexRawString(metadata, "x-codex-turn-metadata", string(turnMetadataJSON))
	if projection.InstallationID != "" {
		setCodexRawString(metadata, "x-codex-installation-id", installationID)
	}
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
