package helps

import (
	"crypto/sha256"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
)

// MapCodexContextWindow preserves the UUID lifecycle independently of session slots.
func MapCodexContextWindow(scope, value string) string {
	parsed, err := uuid.Parse(value)
	if scope == "" || err != nil || parsed == uuid.Nil {
		return value
	}
	sum := sha256.Sum256([]byte("cli-proxy-api:codex-context-window:v1:" + scope + "\x00" + parsed.String()))
	var mapped uuid.UUID
	copy(mapped[:], sum[:16])
	if parsed.Version() == 7 {
		copy(mapped[:6], parsed[:6])
	}
	mapped[6] = (mapped[6] & 0x0f) | (parsed[6] & 0xf0)
	mapped[8] = (mapped[8] & 0x3f) | 0x80
	return mapped.String()
}

func projectCodexRelatedIdentities(metadata, turn map[string]json.RawMessage, admin, client CodexSessionIdentityHeaderSource, identity CodexSessionIdentity, projection CodexSessionIdentityProjection) string {
	parent := firstCodexIdentityValue(admin.ParentThreadID, codexTurnString(turn, "parent_thread_id"), codexTurnString(metadata, "x-codex-parent-thread-id"), codexTurnString(metadata, "parent_thread_id"), client.ParentThreadID)
	replacements := append([]CodexResponseIdentityReplacement(nil), projection.KnownReplacements...)
	add := func(from, to string, role CodexResponseIdentityRole) {
		if from != "" && to != "" && from != to {
			replacements = append(replacements, CodexResponseIdentityReplacement{From: from, To: to, Role: role})
		}
	}
	for _, source := range []map[string]json.RawMessage{turn, metadata} {
		add(codexTurnString(source, "thread_id"), identity.ThreadID, CodexResponseThreadAndWindowIdentity)
		add(codexTurnString(source, "turn_id"), identity.TurnID, CodexResponseTurnIdentity)
	}
	add(client.ThreadID, identity.ThreadID, CodexResponseThreadAndWindowIdentity)
	// Earlier confusion may already have rewritten the primary turn. Resolve its
	// original aliases directly to the final turn, without cascading replacements.
	for index := range replacements {
		if replacements[index].Role == CodexResponseTurnIdentity && identity.TurnID != "" && (replacements[index].To == codexTurnString(turn, "turn_id") || replacements[index].To == codexTurnString(metadata, "turn_id")) {
			replacements[index].To = identity.TurnID
		}
	}
	mapReference := func(value string, role CodexResponseIdentityRole) string {
		for _, replacement := range replacements {
			if replacement.Role == role && replacement.From == value && value != "" {
				return replacement.To
			}
		}
		return value
	}
	for _, source := range []map[string]json.RawMessage{metadata, turn} {
		for _, key := range []string{"root_turn_id", "parent_turn_id", "root_thread_id", "parent_thread_id", "forked_from_thread_id", "x-codex-parent-thread-id"} {
			value := codexTurnString(source, key)
			role := CodexResponseThreadAndWindowIdentity
			if key == "root_turn_id" || key == "parent_turn_id" {
				role = CodexResponseTurnIdentity
			}
			if mapped := mapReference(value, role); mapped != value {
				setCodexRawString(source, key, mapped)
			}
		}
		if projection.ForcedIdentity.WindowID != "" && identity.WindowID == projection.ForcedIdentity.WindowID && strings.HasSuffix(identity.WindowID, ":0") {
			if raw, exists := source["window_number"]; exists {
				if len(raw) > 0 && raw[0] == '"' {
					setCodexRawString(source, "window_number", "0")
				} else {
					source["window_number"] = json.RawMessage("0")
				}
			}
		}
	}
	contextID := firstCodexIdentityValue(codexTurnString(turn, "context_window_id"), codexTurnString(metadata, "context_window_id"))
	if mapped := MapCodexContextWindow(projection.ContextWindowScope, contextID); mapped != contextID {
		setCodexRawString(turn, "context_window_id", mapped)
		if _, exists := metadata["context_window_id"]; exists {
			setCodexRawString(metadata, "context_window_id", mapped)
		}
		if projection.ContextReplacements != nil {
			*projection.ContextReplacements = append(*projection.ContextReplacements, CodexResponseIdentityReplacement{From: contextID, To: mapped, Role: CodexResponseContextWindowIdentity})
		}
	}
	parent = mapReference(parent, CodexResponseThreadAndWindowIdentity)
	if parent != "" {
		setCodexRawString(turn, "parent_thread_id", parent)
		setCodexRawString(metadata, "x-codex-parent-thread-id", parent)
		if _, exists := metadata["parent_thread_id"]; exists {
			setCodexRawString(metadata, "parent_thread_id", parent)
		}
	}
	return parent
}
