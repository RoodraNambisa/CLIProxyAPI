package helps

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/cache"
)

// CodexIdentityTransform belongs to one execution attempt. The response inverse
// describes this request's inputs, independently of the shared alias registry.
type CodexIdentityTransform struct {
	SessionSlot                      string
	Scope, Generation                string
	Restore                          bool
	ProtectedCache, ProtectedSession string
	mappings                         []cache.CodexIdentityAlias
	fields                           []codexIdentityField
}
type codexIdentityField struct{ field, original, before, mapped string }

func CodexIdentityRole(field string) string {
	switch strings.ToLower(field) {
	case "installation_id", "installation-id", "x-codex-installation-id":
		return "device"
	case "session_id", "session-id", "conversation_id", "conversation-id", "thread_id", "thread-id", "root_thread_id", "parent_thread_id", "forked_from_thread_id", "forked_from_id", "x-codex-parent-thread-id":
		return "conversation"
	case "prompt_cache_key":
		return "cache"
	case "x-client-request-id":
		return "request"
	case "turn_id", "turn-id", "parent_turn_id", "root_turn_id":
		return "turn"
	case "window_id", "window-id", "x-codex-window-id":
		return "window"
	case "context_window_id":
		return "context"
	}
	return ""
}

func StableCodexIdentity(scope, role, value string) string {
	if scope == "" || role == "" || value == "" {
		return value
	}
	if role == "window" {
		if thread, suffix, ok := strings.Cut(value, ":"); ok && suffix != "" && strings.IndexFunc(suffix, func(r rune) bool { return r < '0' || r > '9' }) < 0 {
			return StableCodexIdentity(scope, "conversation", thread) + ":" + suffix
		}
	}
	sum := sha256.Sum256([]byte("cli-proxy-api:codex-identity:v2:" + scope + "\x00" + role + "\x00" + value))
	var id uuid.UUID
	copy(id[:], sum[:16])
	id[6] = (id[6] & 15) | 0x50
	if source, err := uuid.Parse(value); err == nil && source != uuid.Nil {
		id[6] = (id[6] & 15) | (source[6] & 0xf0)
		if source.Version() == 7 {
			copy(id[:6], source[:6])
		}
	}
	id[8] = (id[8] & 0x3f) | 0x80
	return id.String()
}

func (t *CodexIdentityTransform) Map(field, value string) string {
	if t == nil || value == "" {
		return value
	}
	role := t.role(field, value)
	if role == "" {
		return value
	}
	if (field == "prompt_cache_key" && t.ProtectedCache != "") || ((strings.EqualFold(field, "session_id") || strings.EqualFold(field, "Session-Id")) && t.ProtectedSession != "") {
		return value
	}
	for _, v := range t.mappings {
		if v.Role == role && (v.Original == value || v.Mapped == value) {
			if v.Original == value {
				t.rememberField(field, value, v.Mapped)
			}
			return v.Mapped
		}
	}
	for _, f := range t.fields {
		if CodexIdentityRole(f.field) == role && (f.before == value || f.mapped == value) {
			return f.mapped
		}
	}
	original := value
	alias, known := cache.GlobalCodexIdentityAliases().Get(role, value)
	if !known && role == "conversation" {
		alias, known = cache.GlobalCodexIdentityAliases().Get("cache", value)
		if known {
			role = alias.Role
		}
	}
	if known {
		if alias.Scope == t.Scope && alias.Generation == t.Generation {
			t.mappings = append(t.mappings, alias)
			t.rememberField(field, value, alias.Mapped)
			return alias.Mapped
		}
		if !alias.Converged {
			original = alias.Original
		}
	}
	mapped := ""
	if role == "window" {
		if thread, suffix, ok := strings.Cut(original, ":"); ok && suffix != "" && strings.IndexFunc(suffix, func(r rune) bool { return r < '0' || r > '9' }) < 0 {
			mapped = t.Map("thread_id", thread) + ":" + suffix
		}
	}
	if mapped == "" {
		mapped = StableCodexIdentity(t.Scope, role, original)
	}
	t.Record(role, original, mapped, false)
	t.rememberField(field, value, mapped)
	return mapped
}

func (t *CodexIdentityTransform) Record(role, original, mapped string, converged bool) {
	if t == nil || original == "" || mapped == "" {
		return
	}
	for i := range t.mappings {
		v := &t.mappings[i]
		if v.Role == role && (v.Original == original || v.Mapped == original) {
			v.Mapped = strings.Clone(mapped)
			v.Converged = v.Converged || converged
			return
		}
	}
	t.mappings = append(t.mappings, cache.CodexIdentityAlias{Original: strings.Clone(original), Mapped: strings.Clone(mapped), Scope: t.Scope, Generation: t.Generation, Role: role, Converged: converged})
}

func (t *CodexIdentityTransform) MapHeaders(h http.Header) {
	if t == nil {
		return
	}
	for key, values := range h {
		if CodexIdentityRole(key) != "" && len(values) > 0 {
			h[key] = []string{t.Map(key, values[0])}
		}
	}
	if raw := h.Get("X-Codex-Turn-Metadata"); raw != "" {
		h.Set("X-Codex-Turn-Metadata", string(RewriteCodexIdentityEnvelope([]byte(raw), t.Map)))
	}
}

func (t *CodexIdentityTransform) Response(payload []byte) []byte {
	if t == nil || !t.Restore || len(t.mappings) == 0 {
		return payload
	}
	return rewriteCodexSSEChunk(payload, func(data []byte) []byte {
		return RewriteCodexIdentityEnvelope(data, func(field, value string) string {
			for _, f := range t.fields {
				if f.field == field && f.mapped == value {
					return f.original
				}
			}
			role := t.role(field, value)
			original := ""
			for _, v := range t.mappings {
				if v.Role == role && v.Mapped == value {
					if original != "" && original != v.Original {
						return value
					}
					original = v.Original
				}
			}
			if original != "" {
				return original
			}
			return value
		})
	})
}

// ObserveReturned registers only identifiers in payloads handed to the downstream
// flow. Buffered bootstrap events and discarded translator fields are excluded.
func (t *CodexIdentityTransform) ObserveReturned(payload []byte) {
	if t == nil || t.Restore || len(t.mappings) == 0 {
		return
	}
	rewriteCodexSSEChunk(payload, func(data []byte) []byte {
		return RewriteCodexIdentityEnvelope(data, func(field, value string) string {
			role := t.role(field, value)
			for _, v := range t.mappings {
				if v.Role == role && v.Mapped == value && v.Mapped != v.Original {
					v.SessionSlot = t.SessionSlot
					cache.GlobalCodexIdentityAliases().Put(v)
				}
			}
			return value
		})
	})
}

func (t *CodexIdentityTransform) Converge(field, before, after string) {
	if t == nil || after == "" {
		return
	}
	role := CodexIdentityRole(field)
	if before == "" {
		t.Record(role, after, after, true)
		return
	}
	found := false
	for i := range t.fields {
		f := &t.fields[i]
		if CodexIdentityRole(f.field) == role && f.mapped == before {
			f.mapped = after
			if f.field == field {
				found = true
			}
		}
	}
	if !found {
		t.fields = append(t.fields, codexIdentityField{field, before, before, after})
	}
	t.Record(role, before, after, before != after)
}
func (t *CodexIdentityTransform) rememberField(field, original, mapped string) {
	for _, f := range t.fields {
		if f.field == field && (f.original == original || f.mapped == original) {
			return
		}
	}
	t.fields = append(t.fields, codexIdentityField{field, original, mapped, mapped})
}
func (t *CodexIdentityTransform) source(s CodexSessionIdentityHeaderSource) CodexSessionIdentityHeaderSource {
	if t == nil {
		return s
	}
	s.InstallationID = t.Map("installation_id", s.InstallationID)
	s.SessionID = t.Map("session_id", s.SessionID)
	s.ThreadID = t.Map("thread_id", s.ThreadID)
	s.WindowID = t.Map("window_id", s.WindowID)
	s.ParentThreadID = t.Map("parent_thread_id", s.ParentThreadID)
	if s.TurnMetadata != "" {
		s.TurnMetadata = string(RewriteCodexIdentityEnvelope([]byte(s.TurnMetadata), t.Map))
	}
	return s
}
func (t *CodexIdentityTransform) object(m map[string]json.RawMessage) {
	if t == nil {
		return
	}
	// Seed conversation aliases first; map iteration order must not change whether
	// an explicit cache/request value aliases the supplied session or thread.
	for key, raw := range m {
		if CodexIdentityRole(key) == "conversation" {
			t.Map(key, codexRawJSONString(raw))
		}
	}
	for key, raw := range m {
		if CodexIdentityRole(key) == "" {
			continue
		}
		value := codexRawJSONString(raw)
		if key == "prompt_cache_key" {
			value = exactCodexIdentityString(raw)
		}
		if next := t.Map(key, value); next != value {
			setCodexRawString(m, key, next)
		}
	}
}

func (t *CodexIdentityTransform) role(field, value string) string {
	role := CodexIdentityRole(field)
	if role == "cache" {
		if parsed, err := uuid.Parse(value); err == nil && parsed != uuid.Nil {
			role = "conversation"
		}
	}
	if t != nil && (role == "cache" || role == "request" || role == "conversation") {
		for _, v := range t.mappings {
			if v.Role == "conversation" && (v.Original == value || v.Mapped == value) {
				return "conversation"
			}
			if role == "conversation" && v.Role == "cache" && v.Mapped == value {
				return "cache"
			}
		}
	}
	return role
}
func exactCodexIdentityString(raw []byte) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

type codexIdentityEdit struct {
	start, end  int
	replacement []byte
}

// RewriteCodexIdentityEnvelope changes only protocol objects and splices the
// enclosing buffer once. Business input/output/tool subtrees remain opaque.
func RewriteCodexIdentityEnvelope(payload []byte, transform func(string, string) string) []byte {
	return rewriteCodexIdentityObject(payload, transform, 0)
}
func rewriteCodexIdentityObject(payload []byte, transform func(string, string) string, depth int) []byte {
	var edits []codexIdentityEdit
	if err := collectCodexIdentityEdits(payload, transform, depth, 0, &edits); err != nil || len(edits) == 0 {
		return payload
	}
	size := len(payload)
	for _, e := range edits {
		size += len(e.replacement) - (e.end - e.start)
	}
	out := make([]byte, 0, size)
	last := 0
	for _, e := range edits {
		out = append(out, payload[last:e.start]...)
		out = append(out, e.replacement...)
		last = e.end
	}
	return append(out, payload[last:]...)
}

// Nested JSON edits retain absolute offsets. Only encoded turn metadata needs
// its own small rebuild; large response/output envelopes are copied once.
func collectCodexIdentityEdits(payload []byte, transform func(string, string) string, depth, base int, edits *[]codexIdentityEdit) error {
	raw := bytes.TrimSpace(payload)
	if depth > 4 || len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return nil
	}
	base += len(payload) - len(bytes.TrimLeft(payload, " \t\r\n"))
	var nestedErr error
	err := visitCodexTopLevelFieldsOffsets(raw, func(rawKey, value []byte, start int) bool {
		key := codexProtocolIdentityKey(rawKey)
		if key == "" {
			return true
		}
		absolute := base + start
		if CodexIdentityRole(key) != "" && len(value) > 0 && value[0] == '"' {
			var original string
			if json.Unmarshal(value, &original) == nil {
				if mapped := transform(key, original); mapped != original {
					replacement, _ := json.Marshal(mapped)
					*edits = append(*edits, codexIdentityEdit{absolute, absolute + len(value), replacement})
				}
			}
			return true
		}
		if key == "response" || key == "client_metadata" || key == "error" || (key == "x-codex-turn-metadata" && len(value) > 0 && value[0] == '{') {
			nestedErr = collectCodexIdentityEdits(value, transform, depth+1, absolute, edits)
			return nestedErr == nil
		}
		if key == "x-codex-turn-metadata" && len(value) > 0 && value[0] == '"' {
			var text string
			if json.Unmarshal(value, &text) == nil {
				changed := rewriteCodexIdentityObject([]byte(text), transform, depth+1)
				if string(changed) != text {
					replacement, _ := json.Marshal(string(changed))
					*edits = append(*edits, codexIdentityEdit{absolute, absolute + len(value), replacement})
				}
			}
		}
		return true
	})
	if err != nil {
		return err
	}
	return nestedErr
}

func codexProtocolIdentityKey(raw []byte) string {
	switch string(raw) {
	case `"installation_id"`:
		return "installation_id"
	case `"installation-id"`:
		return "installation-id"
	case `"x-codex-installation-id"`:
		return "x-codex-installation-id"
	case `"session_id"`:
		return "session_id"
	case `"session-id"`:
		return "session-id"
	case `"conversation_id"`:
		return "conversation_id"
	case `"conversation-id"`:
		return "conversation-id"
	case `"thread_id"`:
		return "thread_id"
	case `"thread-id"`:
		return "thread-id"
	case `"root_thread_id"`:
		return "root_thread_id"
	case `"parent_thread_id"`:
		return "parent_thread_id"
	case `"forked_from_thread_id"`:
		return "forked_from_thread_id"
	case `"forked_from_id"`:
		return "forked_from_id"
	case `"x-codex-parent-thread-id"`:
		return "x-codex-parent-thread-id"
	case `"prompt_cache_key"`:
		return "prompt_cache_key"
	case `"x-client-request-id"`:
		return "x-client-request-id"
	case `"turn_id"`:
		return "turn_id"
	case `"turn-id"`:
		return "turn-id"
	case `"parent_turn_id"`:
		return "parent_turn_id"
	case `"root_turn_id"`:
		return "root_turn_id"
	case `"window_id"`:
		return "window_id"
	case `"window-id"`:
		return "window-id"
	case `"x-codex-window-id"`:
		return "x-codex-window-id"
	case `"context_window_id"`:
		return "context_window_id"
	case `"response"`:
		return "response"
	case `"client_metadata"`:
		return "client_metadata"
	case `"error"`:
		return "error"
	case `"x-codex-turn-metadata"`:
		return "x-codex-turn-metadata"
	}
	if bytes.IndexByte(raw, '\\') >= 0 {
		if key, err := strconv.Unquote(string(raw)); err == nil && (CodexIdentityRole(key) != "" || key == "response" || key == "client_metadata" || key == "error" || key == "x-codex-turn-metadata") {
			return key
		}
	}
	return ""
}
