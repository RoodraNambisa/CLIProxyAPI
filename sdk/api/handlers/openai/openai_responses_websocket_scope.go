package openai

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type responsesWebsocketConversation struct {
	scope               helps.CodexRequestScope
	sharedKey           string
	tools               *websocketToolPairState
	lastRequest         []byte
	lastResponseOutput  []byte
	lastResponseID      string
	pinnedAuthID        string
	lastAttemptedAuthID string
	model               string
}

const (
	responsesWebsocketConversationLimit = 32
	responsesWebsocketHistoryBytesLimit = 64 << 20
)

// Conversation histories stay local to a connection. Only committed tool pairs
// may be shared by authenticated callers with the same original thread and kind.
type responsesWebsocketConversations struct {
	states        map[string]*responsesWebsocketConversation
	order         []string
	lastScope     helps.CodexRequestScope
	prewarmID     string
	prewarmScope  helps.CodexRequestScope
	upstreamScope string
}

func (states *responsesWebsocketConversations) selectRequest(c *gin.Context, payload []byte) (*responsesWebsocketConversation, []byte) {
	var headers http.Header
	if c != nil && c.Request != nil {
		headers = c.Request.Header
	}
	scope := states.requestScope(payload, headers)
	if scope.Kind == "turn" && states.prewarmID != "" && gjson.GetBytes(payload, "previous_response_id").Str == states.prewarmID && scope.ThreadID == states.prewarmScope.ThreadID && scope.SessionID == states.prewarmScope.SessionID {
		payload, _ = sjson.DeleteBytes(payload, "previous_response_id")
	}
	key := scope.Key()
	if states.states == nil {
		states.states = make(map[string]*responsesWebsocketConversation)
	}
	state := states.states[key]
	if state == nil {
		if len(states.order) >= responsesWebsocketConversationLimit {
			oldKey := states.order[0]
			releaseResponsesWebsocketToolPairState(states.states[oldKey].sharedKey)
			delete(states.states, oldKey)
			states.order = states.order[1:]
		}
		sharedKey := ""
		if scope.ThreadID != "" || scope.SessionID != "" {
			sharedKey = websocketToolPairScopeKey(c, key)
		}
		state = &responsesWebsocketConversation{scope: scope, sharedKey: sharedKey, tools: acquireResponsesWebsocketToolPairState(sharedKey), lastResponseOutput: []byte("[]")}
		states.states[key] = state
		states.order = append(states.order, key)
	}
	if gjson.GetBytes(payload, "model").String() == "" {
		model := gjson.GetBytes(state.lastRequest, "model").String()
		if model == "" {
			model = state.model
		}
		if model == "" && scope.Kind == "turn" && scope.ThreadID == states.prewarmScope.ThreadID && scope.SessionID == states.prewarmScope.SessionID {
			if warm := states.states[states.prewarmScope.Key()]; warm != nil {
				model = warm.model
			}
		}
		if model != "" {
			payload, _ = sjson.SetBytes(payload, "model", model)
		}
	}
	states.lastScope = scope
	return state, carryResponsesWebsocketScope(payload, headers, scope)
}

func (states *responsesWebsocketConversations) responseOwner(responseID string) *responsesWebsocketConversation {
	if responseID == "" {
		return nil
	}
	var owner *responsesWebsocketConversation
	for _, candidate := range states.states {
		if candidate.lastResponseID != responseID {
			continue
		}
		if owner != nil {
			return nil
		}
		owner = candidate
	}
	return owner
}

func (states *responsesWebsocketConversations) requestScope(payload []byte, headers http.Header) helps.CodexRequestScope {
	explicit := helps.SnapshotCodexRequestScope(payload)
	baseline := helps.SnapshotCodexRequestScope(nil, headers)
	fallback := states.lastScope
	if fallback.Kind == "" {
		fallback = baseline
	}
	metadata := gjson.GetBytes(payload, "client_metadata")
	turn := metadata.Get("x-codex-turn-metadata")
	if turn.Type == gjson.String {
		turn = gjson.Parse(turn.Str)
	}
	generate := gjson.GetBytes(payload, "generate")
	hasKind := strings.TrimSpace(turn.Get("request_kind").String()) != "" || strings.TrimSpace(metadata.Get("request_kind").String()) != "" || (generate.Exists() && !generate.Bool())
	owner := states.responseOwner(gjson.GetBytes(payload, "previous_response_id").Str)
	compatibleOwner := owner != nil && (explicit.ThreadID == "" || explicit.ThreadID == owner.scope.ThreadID) && (explicit.SessionID == "" || explicit.SessionID == owner.scope.SessionID)
	if compatibleOwner {
		fallback = owner.scope
	}
	scope := explicit
	if scope.ThreadID == "" && (scope.SessionID == "" || scope.SessionID == fallback.SessionID) {
		scope.ThreadID = fallback.ThreadID
	}
	if scope.ThreadID == "" {
		scope.ThreadID = baseline.ThreadID
	}
	if scope.SessionID == "" {
		if scope.ThreadID == fallback.ThreadID {
			scope.SessionID = fallback.SessionID
		} else {
			scope.SessionID = baseline.SessionID
		}
	}
	if !hasKind {
		switch {
		case compatibleOwner && owner.scope.Kind != "prewarm":
			scope.Kind = owner.scope.Kind
		case gjson.GetBytes(payload, "type").Str == wsRequestTypeAppend && fallback.Kind != "prewarm":
			scope.Kind = fallback.Kind
		case len(states.states) == 0:
			scope.Kind = baseline.Kind
		}
	}
	return scope
}

// Carry only resolved protocol scope fields when a continuation omits them.
// This prevents the executor from falling back to stale handshake metadata.
func carryResponsesWebsocketScope(payload []byte, headers http.Header, scope helps.CodexRequestScope) []byte {
	seen := helps.SnapshotCodexRequestScope(payload, headers)
	if seen == scope {
		return payload
	}
	metadata := gjson.GetBytes(payload, "client_metadata")
	if metadata.Exists() && !metadata.IsObject() {
		return payload
	}
	turn := metadata.Get("x-codex-turn-metadata")
	raw := []byte(`{}`)
	if turn.Type == gjson.String {
		raw = []byte(turn.Str)
	} else if turn.Exists() {
		raw = []byte(turn.Raw)
	}
	if !gjson.ValidBytes(raw) || !gjson.ParseBytes(raw).IsObject() {
		return payload
	}
	for _, field := range []struct{ name, from, to string }{{"thread_id", seen.ThreadID, scope.ThreadID}, {"session_id", seen.SessionID, scope.SessionID}, {"request_kind", seen.Kind, scope.Kind}} {
		if field.from != field.to && field.to != "" {
			raw, _ = sjson.SetBytes(raw, field.name, field.to)
		}
	}
	if turn.IsObject() {
		payload, _ = sjson.SetRawBytes(payload, "client_metadata.x-codex-turn-metadata", raw)
	} else {
		payload, _ = sjson.SetBytes(payload, "client_metadata.x-codex-turn-metadata", string(raw))
	}
	return payload
}

// Bound histories retained while one socket alternates between background and
// user threads. Eviction requires a full replay instead of borrowing another scope.
func (states *responsesWebsocketConversations) trimHistory(current *responsesWebsocketConversation, limit int) {
	total := 0
	for _, state := range states.states {
		total += len(state.lastRequest) + len(state.lastResponseOutput)
	}
	for _, key := range states.order {
		if total <= limit {
			return
		}
		state := states.states[key]
		if state == current {
			continue
		}
		total -= len(state.lastRequest) + len(state.lastResponseOutput)
		state.lastRequest, state.lastResponseOutput, state.lastResponseID, state.pinnedAuthID = nil, nil, "", ""
	}
}

func (states *responsesWebsocketConversations) close() {
	for _, state := range states.states {
		releaseResponsesWebsocketToolPairState(state.sharedKey)
	}
	states.states = nil
	states.order = nil
}

func (state *responsesWebsocketConversation) modelChanged(payload []byte) bool {
	previous := strings.TrimSpace(gjson.GetBytes(state.lastRequest, "model").String())
	current := strings.TrimSpace(gjson.GetBytes(payload, "model").String())
	return previous != "" && current != "" && previous != current
}
