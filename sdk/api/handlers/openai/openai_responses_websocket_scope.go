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
	model         string
	prewarmID     string
	prewarmScope  helps.CodexRequestScope
	upstreamScope string
}

func (states *responsesWebsocketConversations) selectRequest(c *gin.Context, payload []byte) (*responsesWebsocketConversation, []byte) {
	var headers http.Header
	if c != nil && c.Request != nil {
		headers = c.Request.Header
	}
	scope := helps.SnapshotCodexRequestScope(payload, headers)
	if scope.ThreadID == "" && scope.SessionID == states.lastScope.SessionID {
		scope.ThreadID = states.lastScope.ThreadID
	}
	if scope.ThreadID == "" && scope.SessionID == "" {
		scope.ThreadID, scope.SessionID = states.lastScope.ThreadID, states.lastScope.SessionID
	}
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
			model = states.model
		}
		if model != "" {
			payload, _ = sjson.SetBytes(payload, "model", model)
		}
	}
	states.lastScope = scope
	return state, payload
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
