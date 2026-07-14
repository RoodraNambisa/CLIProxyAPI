// Package executor provides runtime execution capabilities for various AI service providers.
// This file implements an xAI executor that uses the Responses API WebSocket transport.
package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/xai"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var errXAIWebsocketSessionTerminated = errors.New("xai websockets executor: session terminated")

func enqueueXAIWebsocketTerminalChunk(ctx context.Context, out chan cliproxyexecutor.StreamChunk, chunk cliproxyexecutor.StreamChunk) bool {
	if ctx == nil {
		out <- chunk
		return true
	}
	select {
	case out <- chunk:
		return true
	case <-ctx.Done():
		return false
	}
}

func discardXAIWebsocketStreamChunks(ch <-chan cliproxyexecutor.StreamChunk) {
	if ch == nil {
		return
	}
	go func() {
		for range ch {
		}
	}()
}

// XAIWebsocketsExecutor executes xAI Responses requests using a WebSocket transport.
type XAIWebsocketsExecutor struct {
	*XAIExecutor

	store   *codexWebsocketSessionStore
	idStore *xaiWebsocketIDStateStore
}

var _ cliproxyauth.AuthExecutionSessionCloser = (*XAIWebsocketsExecutor)(nil)
var _ cliproxyauth.AuthInstanceExecutionSessionCloser = (*XAIWebsocketsExecutor)(nil)

var globalXAIWebsocketSessionStore = &codexWebsocketSessionStore{
	sessions: make(map[string]*codexWebsocketSession),
}

var globalXAIWebsocketIDStates = &xaiWebsocketIDStateStore{
	sessions: make(map[string]*xaiWebsocketIDState),
}

func selectedAuthInstanceRetired(opts cliproxyexecutor.Options) bool {
	if len(opts.Metadata) == 0 {
		return false
	}
	state, _ := opts.Metadata[cliproxyexecutor.SelectedAuthInstanceRetirementMetadataKey].(cliproxyexecutor.AuthInstanceRetirement)
	return state != nil && state.Retired()
}

func xaiAuthExecutionCurrent(auth *cliproxyauth.Auth) bool {
	return auth == nil || !auth.RuntimeInstanceRetired()
}

func writeCurrentXAIWebsocketMessage(ctx context.Context, auth *cliproxyauth.Auth, sess *codexWebsocketSession, conn *websocket.Conn, payload []byte) error {
	if (ctx != nil && ctx.Err() != nil) || !xaiAuthExecutionCurrent(auth) {
		return errXAIWebsocketSessionTerminated
	}
	return writeCodexWebsocketMessage(sess, conn, payload)
}

type xaiWebsocketIDStateStore struct {
	mu       sync.Mutex
	sessions map[string]*xaiWebsocketIDState
}

type xaiWebsocketIDState struct {
	mu                   sync.Mutex
	downstreamToUpstream map[string]string
	sequence             int
	transcriptInput      []json.RawMessage
}

type xaiWebsocketRequestIDMapper struct {
	state                *xaiWebsocketIDState
	downstreamPreviousID string
	upstreamPreviousID   string
	upstreamResponseID   string
	downstreamResponseID string
}

func NewXAIWebsocketsExecutor(cfg *config.Config) *XAIWebsocketsExecutor {
	return &XAIWebsocketsExecutor{
		XAIExecutor: NewXAIExecutor(cfg),
		store:       globalXAIWebsocketSessionStore,
		idStore:     globalXAIWebsocketIDStates,
	}
}

func getXAIWebsocketIDState(store *xaiWebsocketIDStateStore, sessionID string) *xaiWebsocketIDState {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.sessions == nil {
		store.sessions = make(map[string]*xaiWebsocketIDState)
	}
	if state := store.sessions[sessionID]; state != nil {
		return state
	}
	state := &xaiWebsocketIDState{
		downstreamToUpstream: make(map[string]string),
	}
	store.sessions[sessionID] = state
	return state
}

func deleteXAIWebsocketIDState(store *xaiWebsocketIDStateStore, sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || store == nil {
		return
	}
	store.mu.Lock()
	delete(store.sessions, sessionID)
	store.mu.Unlock()
}

func deleteXAIWebsocketIDStateForSession(store *codexWebsocketSessionStore, idStore *xaiWebsocketIDStateStore, sessionID string, stateSessionID string, expected *codexWebsocketSession) {
	sessionID = strings.TrimSpace(sessionID)
	stateSessionID = strings.TrimSpace(stateSessionID)
	if store == nil || idStore == nil || sessionID == "" || stateSessionID == "" || expected == nil {
		return
	}
	store.mu.Lock()
	idStore.mu.Lock()
	if store.sessions[sessionID] == expected {
		delete(idStore.sessions, stateSessionID)
	}
	idStore.mu.Unlock()
	store.mu.Unlock()
}

func newXAIWebsocketRequestIDMapper(store *xaiWebsocketIDStateStore, sessionID string, downstreamRequest []byte) *xaiWebsocketRequestIDMapper {
	state := getXAIWebsocketIDState(store, sessionID)
	if state == nil {
		return nil
	}
	downstreamPreviousID := strings.TrimSpace(gjson.GetBytes(downstreamRequest, "previous_response_id").String())
	upstreamPreviousID := downstreamPreviousID
	if downstreamPreviousID != "" {
		upstreamPreviousID = state.upstreamIDForDownstream(downstreamPreviousID)
	}
	return &xaiWebsocketRequestIDMapper{
		state:                state,
		downstreamPreviousID: downstreamPreviousID,
		upstreamPreviousID:   upstreamPreviousID,
	}
}

func (s *xaiWebsocketIDState) upstreamIDForDownstream(downstreamID string) string {
	downstreamID = strings.TrimSpace(downstreamID)
	if s == nil || downstreamID == "" {
		return downstreamID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if upstreamID, ok := s.downstreamToUpstream[downstreamID]; ok {
		return strings.TrimSpace(upstreamID)
	}
	return downstreamID
}

func (s *xaiWebsocketIDState) mapDownstreamToUpstream(downstreamID string, upstreamID string) {
	downstreamID = strings.TrimSpace(downstreamID)
	if s == nil || downstreamID == "" {
		return
	}
	s.mu.Lock()
	if s.downstreamToUpstream == nil {
		s.downstreamToUpstream = make(map[string]string)
	}
	s.downstreamToUpstream[downstreamID] = strings.TrimSpace(upstreamID)
	s.mu.Unlock()
}

func (s *xaiWebsocketIDState) snapshotTranscriptInput() []byte {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.transcriptInput) == 0 {
		return nil
	}
	return xaiMarshalRawMessages(s.transcriptInput)
}

func (s *xaiWebsocketIDState) prependTranscriptInput(payload []byte) []byte {
	if s == nil || len(payload) == 0 {
		return payload
	}
	s.mu.Lock()
	prefix := make([]json.RawMessage, 0, len(s.transcriptInput))
	for _, item := range s.transcriptInput {
		prefix = append(prefix, bytes.Clone(item))
	}
	s.mu.Unlock()
	if len(prefix) == 0 {
		return payload
	}
	current := xaiJSONRawMessages(gjson.GetBytes(payload, "input"))
	merged := append(prefix, current...)
	out, errSet := sjson.SetRawBytes(payload, "input", xaiMarshalRawMessages(merged))
	if errSet != nil {
		return payload
	}
	return out
}

func (s *xaiWebsocketIDState) recordTranscriptTurn(requestPayload []byte, completedPayload []byte) {
	if s == nil || len(requestPayload) == 0 || len(completedPayload) == 0 {
		return
	}
	inputItems := xaiJSONRawMessages(gjson.GetBytes(requestPayload, "input"))
	outputItems := xaiJSONRawMessages(gjson.GetBytes(completedPayload, "response.output"))
	if len(inputItems) == 0 && len(outputItems) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(gjson.GetBytes(requestPayload, "previous_response_id").String()) == "" {
		s.transcriptInput = nil
	}
	s.transcriptInput = append(s.transcriptInput, inputItems...)
	s.transcriptInput = append(s.transcriptInput, outputItems...)
}

func (s *xaiWebsocketIDState) replaceTranscriptWithItems(items ...[]byte) {
	if s == nil {
		return
	}
	next := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		item = bytes.TrimSpace(item)
		if len(item) == 0 || !json.Valid(item) {
			continue
		}
		next = append(next, bytes.Clone(item))
	}
	s.mu.Lock()
	s.transcriptInput = next
	s.mu.Unlock()
}

func xaiJSONRawMessages(result gjson.Result) []json.RawMessage {
	if !result.Exists() || !result.IsArray() {
		return nil
	}
	items := result.Array()
	out := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		raw := bytes.TrimSpace([]byte(item.Raw))
		if len(raw) == 0 || !json.Valid(raw) {
			continue
		}
		out = append(out, bytes.Clone(raw))
	}
	return out
}

func xaiMarshalRawMessages(items []json.RawMessage) []byte {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, item := range items {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(bytes.TrimSpace(item))
	}
	buf.WriteByte(']')
	return buf.Bytes()
}

func (m *xaiWebsocketRequestIDMapper) upstreamRequestPayload(payload []byte) []byte {
	if m == nil || len(payload) == 0 || m.downstreamPreviousID == m.upstreamPreviousID {
		return payload
	}
	if m.upstreamPreviousID == "" {
		out, errDelete := sjson.DeleteBytes(payload, "previous_response_id")
		if errDelete == nil {
			if m.downstreamPreviousID != "" && m.state != nil {
				out = m.state.prependTranscriptInput(out)
			}
			return out
		}
		return payload
	}
	out, errSet := sjson.SetBytes(payload, "previous_response_id", m.upstreamPreviousID)
	if errSet != nil {
		return payload
	}
	return out
}

func (m *xaiWebsocketRequestIDMapper) downstreamResponsePayload(payload []byte) []byte {
	if m == nil || len(payload) == 0 {
		return payload
	}
	upstreamResponseID := strings.TrimSpace(gjson.GetBytes(payload, "response.id").String())
	downstreamResponseID := m.downstreamIDForUpstreamResponse(upstreamResponseID)
	if downstreamResponseID == "" {
		return payload
	}
	return rewriteXAIWebsocketDownstreamIDs(payload, m.upstreamResponseID, downstreamResponseID, m.upstreamPreviousID, m.downstreamPreviousID)
}

func (m *xaiWebsocketRequestIDMapper) downstreamIDForUpstreamResponse(upstreamResponseID string) string {
	upstreamResponseID = strings.TrimSpace(upstreamResponseID)
	if m == nil || m.state == nil {
		return upstreamResponseID
	}
	if m.upstreamResponseID != "" {
		return m.downstreamResponseID
	}
	if upstreamResponseID == "" {
		return ""
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.upstreamResponseID = upstreamResponseID
	m.downstreamResponseID = upstreamResponseID
	if m.state.downstreamToUpstream == nil {
		m.state.downstreamToUpstream = make(map[string]string)
	}
	_, upstreamResponseIDSeen := m.state.downstreamToUpstream[upstreamResponseID]
	if (m.downstreamPreviousID != "" && m.upstreamPreviousID != "" && upstreamResponseID == m.upstreamPreviousID) || upstreamResponseIDSeen {
		m.state.sequence++
		m.downstreamResponseID = fmt.Sprintf("%s-xai-%d", upstreamResponseID, m.state.sequence)
	}
	m.state.downstreamToUpstream[upstreamResponseID] = upstreamResponseID
	m.state.downstreamToUpstream[m.downstreamResponseID] = upstreamResponseID
	return m.downstreamResponseID
}

func rewriteXAIWebsocketDownstreamIDs(payload []byte, upstreamResponseID string, downstreamResponseID string, upstreamPreviousID string, downstreamPreviousID string) []byte {
	upstreamResponseID = strings.TrimSpace(upstreamResponseID)
	downstreamResponseID = strings.TrimSpace(downstreamResponseID)
	upstreamPreviousID = strings.TrimSpace(upstreamPreviousID)
	downstreamPreviousID = strings.TrimSpace(downstreamPreviousID)
	if len(payload) == 0 || (upstreamResponseID == downstreamResponseID && upstreamPreviousID == downstreamPreviousID) {
		return payload
	}

	var value any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if errDecode := decoder.Decode(&value); errDecode != nil {
		return payload
	}
	if !rewriteXAIWebsocketDownstreamIDValue(value, upstreamResponseID, downstreamResponseID, upstreamPreviousID, downstreamPreviousID, "") {
		return payload
	}
	out, errMarshal := json.Marshal(value)
	if errMarshal != nil {
		return payload
	}
	return out
}

func rewriteXAIWebsocketDownstreamIDValue(value any, upstreamResponseID string, downstreamResponseID string, upstreamPreviousID string, downstreamPreviousID string, key string) bool {
	switch typed := value.(type) {
	case map[string]any:
		changed := false
		for childKey, childValue := range typed {
			if childString, ok := childValue.(string); ok {
				replaced := rewriteXAIWebsocketDownstreamIDString(childString, childKey, upstreamResponseID, downstreamResponseID, upstreamPreviousID, downstreamPreviousID)
				if replaced != childString {
					typed[childKey] = replaced
					changed = true
				}
				continue
			}
			if rewriteXAIWebsocketDownstreamIDValue(childValue, upstreamResponseID, downstreamResponseID, upstreamPreviousID, downstreamPreviousID, childKey) {
				changed = true
			}
		}
		return changed
	case []any:
		changed := false
		for i := range typed {
			if rewriteXAIWebsocketDownstreamIDValue(typed[i], upstreamResponseID, downstreamResponseID, upstreamPreviousID, downstreamPreviousID, key) {
				changed = true
			}
		}
		return changed
	default:
		return false
	}
}

func rewriteXAIWebsocketDownstreamIDString(value string, key string, upstreamResponseID string, downstreamResponseID string, upstreamPreviousID string, downstreamPreviousID string) string {
	switch key {
	case "id", "item_id":
		if upstreamResponseID != "" && downstreamResponseID != "" && downstreamResponseID != upstreamResponseID && strings.Contains(value, upstreamResponseID) {
			return strings.ReplaceAll(value, upstreamResponseID, downstreamResponseID)
		}
	case "previous_response_id":
		if upstreamPreviousID != "" && downstreamPreviousID != "" && value == upstreamPreviousID {
			return downstreamPreviousID
		}
	}
	return value
}

func (e *XAIWebsocketsExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e == nil || e.XAIExecutor == nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("xai websockets executor: executor is nil")
	}
	return e.XAIExecutor.Execute(ctx, auth, req, opts)
}

func (e *XAIWebsocketsExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runtimeCtx, releaseExecution, active := auth.BeginRuntimeExecution(ctx)
	if !active {
		return nil, errXAIWebsocketSessionTerminated
	}
	result, err := e.executeStream(runtimeCtx, auth, req, opts)
	if err != nil || result == nil || result.Chunks == nil {
		if releaseExecution() {
			return nil, errXAIWebsocketSessionTerminated
		}
		return result, err
	}
	out := make(chan cliproxyexecutor.StreamChunk, cliproxyexecutor.StreamBufferSize)
	go relayXAIWebsocketStream(ctx, runtimeCtx, auth, result.Chunks, out, releaseExecution)
	return &cliproxyexecutor.StreamResult{Headers: result.Headers, Chunks: out}, nil
}

func relayXAIWebsocketStream(ctx context.Context, runtimeCtx context.Context, auth *cliproxyauth.Auth, in <-chan cliproxyexecutor.StreamChunk, out chan cliproxyexecutor.StreamChunk, releaseExecution func() bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if runtimeCtx == nil {
		runtimeCtx = ctx
	}
	retiredErrorSent := false
	terminalSent := false
	defer func() {
		retiredAtRelease := false
		if releaseExecution != nil {
			retiredAtRelease = releaseExecution()
		}
		if (retiredAtRelease || auth.RuntimeInstanceRetired()) && !retiredErrorSent && !terminalSent {
			enqueueXAIWebsocketTerminalChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: errXAIWebsocketSessionTerminated})
		}
		close(out)
	}()

	runtimeDone := runtimeCtx.Done()
	for {
		var (
			chunk cliproxyexecutor.StreamChunk
			ok    bool
		)
		select {
		case chunk, ok = <-in:
		default:
			select {
			case <-ctx.Done():
				discardXAIWebsocketStreamChunks(in)
				return
			case <-runtimeDone:
				select {
				case chunk, ok = <-in:
				default:
					discardXAIWebsocketStreamChunks(in)
					return
				}
			case chunk, ok = <-in:
			}
		}
		if !ok {
			return
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			if !enqueueXAIWebsocketTerminalChunk(ctx, out, chunk) {
				discardXAIWebsocketStreamChunks(in)
				return
			}
			terminalSent = true
			discardXAIWebsocketStreamChunks(in)
			return
		}
		retiredChunk := false
		if chunk.Err != nil && auth.RuntimeInstanceRetired() {
			chunk.Err = errXAIWebsocketSessionTerminated
			retiredChunk = true
		}
		select {
		case out <- chunk:
			terminalSent = chunk.Err != nil
			if retiredChunk {
				retiredErrorSent = true
			}
			if terminalSent {
				discardXAIWebsocketStreamChunks(in)
				return
			}
		case <-ctx.Done():
			discardXAIWebsocketStreamChunks(in)
			return
		}
	}
}

func (e *XAIWebsocketsExecutor) executeStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	if e == nil || e.XAIExecutor == nil {
		return nil, fmt.Errorf("xai websockets executor: executor is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if selectedAuthInstanceRetired(opts) {
		return nil, errXAIWebsocketSessionTerminated
	}
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "streaming not supported for /responses/compact"}
	}
	executionSessionID := executionSessionIDFromOptions(opts)
	stateSessionID := xaiExecutionSessionID(req, opts)
	if stateSessionID == "" {
		stateSessionID = executionSessionID
	}
	if xaiInputHasItemType(req.Payload, "compaction_trigger") {
		idMapper := newXAIWebsocketRequestIDMapper(e.idStore, stateSessionID, req.Payload)
		return e.executeCompactionTriggerFromWebsocketContext(ctx, auth, req, opts, idMapper)
	}

	// Keep websocket on the official API base URL (or an explicit non-default
	// base_url). Do not reuse xaiChatBaseURL: cli-chat-proxy only accepts HTTP
	// POST and returns 405 for websocket upgrades.
	token, baseURL := xaiCreds(auth)
	if baseURL == "" {
		baseURL = xaiauth.DefaultAPIBaseURL
	}

	prepared, err := e.prepareResponsesWebsocketRequest(ctx, auth, req, opts)
	if err != nil {
		return nil, err
	}
	reporter := helps.NewExecutorUsageReporter(ctx, e, prepared.baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)
	reporter.SetTranslatedReasoningEffort(prepared.body, e.Identifier())
	reporter.SetRequestServiceTierFromPayload(prepared.body)

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildXAIResponsesWebsocketURL(httpURL)
	if err != nil {
		return nil, err
	}
	wsHeaders := applyXAIWebsocketHeaders(http.Header{}, auth, token, prepared.sessionID)

	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	if selectedAuthInstanceRetired(opts) {
		return nil, errXAIWebsocketSessionTerminated
	}
	var sess *codexWebsocketSession
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		if sess != nil {
			sess.reqMu.Lock()
			if !xaiAuthExecutionCurrent(auth) {
				sess.reqMu.Unlock()
				return nil, errXAIWebsocketSessionTerminated
			}
		}
	}
	if sess != nil && xaiWebsocketSessionIdentityChanged(sess, authID, auth.RuntimeInstanceID(), wsURL) {
		deleteXAIWebsocketIDStateForSession(e.store, e.idStore, executionSessionID, stateSessionID, sess)
	}
	idMapper := newXAIWebsocketRequestIDMapper(e.idStore, stateSessionID, req.Payload)
	if idMapper != nil {
		prepared.body = idMapper.upstreamRequestPayload(prepared.body)
	}
	wsReqBody := buildXAIWebsocketRequestBody(prepared.body)
	warmupRequest := xaiWebsocketGenerateFalse(wsReqBody)

	wsReqLog := helps.UpstreamRequestLog{
		URL:       wsURL,
		Method:    "WEBSOCKET",
		Headers:   wsHeaders.Clone(),
		Body:      wsReqBody,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	}
	helps.RecordAPIWebsocketRequest(ctx, e.cfg, wsReqLog)
	logXAIWebsocketRequest(executionSessionID, authID, wsURL, wsReqBody)

	conn, respHS, errDial := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
	var upstreamHeaders http.Header
	if respHS != nil {
		upstreamHeaders = respHS.Header.Clone()
	}
	if errDial != nil {
		bodyErr := websocketHandshakeBody(respHS)
		if respHS != nil {
			helps.RecordAPIWebsocketUpgradeRejection(ctx, e.cfg, websocketUpgradeRequestLog(wsReqLog), respHS.StatusCode, respHS.Header.Clone(), bodyErr)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			if sess != nil {
				sess.reqMu.Unlock()
			}
			return nil, xaiStatusErrFromResponse(respHS.StatusCode, bodyErr, respHS.Header, time.Now())
		}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "dial", errDial)
		if sess != nil {
			sess.reqMu.Unlock()
		}
		return nil, errDial
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	reporter.StartResponseTTFT()

	if sess == nil {
		logXAIWebsocketConnected(executionSessionID, authID, wsURL)
	}
	stopContextWatch := func() {}
	contextWatchTransferred := false
	if sess == nil {
		watchDone := make(chan struct{})
		var stopOnce sync.Once
		stopContextWatch = func() {
			stopOnce.Do(func() { close(watchDone) })
		}
		go func() {
			select {
			case <-ctx.Done():
				_ = conn.Close()
			case <-watchDone:
			}
		}()
		defer func() {
			if !contextWatchTransferred {
				stopContextWatch()
			}
		}()
	}

	var readCh chan codexWebsocketRead
	if sess != nil {
		readCh = make(chan codexWebsocketRead, 4096)
		sess.setActiveForConn(readCh, conn)
	}

	if errSend := writeCurrentXAIWebsocketMessage(ctx, auth, sess, conn, wsReqBody); errSend != nil {
		helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
		if sess != nil {
			sess.clearActiveForConn(readCh, conn)
			e.invalidateUpstreamConn(sess, conn, "send_error", errSend)
			connRetry, respHSRetry, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
			if errDialRetry != nil || connRetry == nil {
				bodyErrRetry := websocketHandshakeBody(respHSRetry)
				closeHTTPResponseBody(respHSRetry, "xai websockets executor: close handshake response body error")
				helps.RecordAPIWebsocketError(ctx, e.cfg, "dial_retry", errDialRetry)
				sess.clearActive(readCh)
				sess.reqMu.Unlock()
				if respHSRetry != nil && respHSRetry.StatusCode > 0 {
					return nil, xaiStatusErrFromResponse(respHSRetry.StatusCode, bodyErrRetry, respHSRetry.Header, time.Now())
				}
				return nil, errDialRetry
			}
			readCh = make(chan codexWebsocketRead, 4096)
			sess.setActiveForConn(readCh, connRetry)
			wsReqBodyRetry := buildXAIWebsocketRequestBody(prepared.body)
			helps.RecordAPIWebsocketRequest(ctx, e.cfg, helps.UpstreamRequestLog{
				URL:       wsURL,
				Method:    "WEBSOCKET",
				Headers:   wsHeaders.Clone(),
				Body:      wsReqBodyRetry,
				Provider:  e.Identifier(),
				AuthID:    authID,
				AuthLabel: authLabel,
				AuthType:  authType,
				AuthValue: authValue,
			})
			logXAIWebsocketRequest(executionSessionID, authID, wsURL, wsReqBodyRetry)
			recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
			reporter.StartResponseTTFT()
			if errSendRetry := writeCurrentXAIWebsocketMessage(ctx, auth, sess, connRetry, wsReqBodyRetry); errSendRetry != nil {
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
				e.invalidateUpstreamConn(sess, connRetry, "send_error", errSendRetry)
				sess.clearActive(readCh)
				sess.reqMu.Unlock()
				return nil, errSendRetry
			}
			conn = connRetry
			wsReqBody = wsReqBodyRetry
		} else {
			logXAIWebsocketDisconnected(executionSessionID, authID, wsURL, "send_error", errSend)
			if errClose := conn.Close(); errClose != nil {
				log.Errorf("xai websockets executor: close websocket error: %v", errClose)
			}
			return nil, errSend
		}
	}

	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		terminateReason := "completed"
		var terminateErr error

		defer close(out)
		defer func() {
			if sess != nil {
				sess.clearActiveForConn(readCh, conn)
				sess.reqMu.Unlock()
				return
			}
			stopContextWatch()
			logXAIWebsocketDisconnected(executionSessionID, authID, wsURL, terminateReason, terminateErr)
			if errClose := conn.Close(); errClose != nil {
				log.Errorf("xai websockets executor: close websocket error: %v", errClose)
			}
		}()

		send := func(chunk cliproxyexecutor.StreamChunk) bool {
			if ctx == nil {
				out <- chunk
				return true
			}
			select {
			case out <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}
		emitSuccessfulTerminal := func() {
			reporter.EnsurePublished(ctx)
			if metadataBool(opts.Metadata, cliproxyexecutor.StreamTerminalMarkerMetadataKey) {
				if !send(cliproxyexecutor.SuccessfulStreamTerminalChunk()) {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
				}
			}
		}

		var param any
		outputItemsByIndex := make(map[int64][]byte)
		var outputItemsFallback [][]byte
		recordedTranscript := false
		for {
			if ctx != nil && ctx.Err() != nil {
				terminateReason = "context_done"
				terminateErr = ctx.Err()
				reporter.PublishFailure(ctx, ctx.Err())
				_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
				return
			}
			msgType, payload, errRead := readXAIWebsocketMessage(ctx, sess, conn, readCh)
			if errRead != nil {
				if sess != nil && ctx != nil && ctx.Err() != nil {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					reporter.PublishFailure(ctx, ctx.Err())
					_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
					return
				}
				terminateReason = "read_error"
				terminateErr = errRead
				helps.RecordAPIWebsocketError(ctx, e.cfg, "read", errRead)
				reporter.PublishFailure(ctx, errRead)
				_ = send(cliproxyexecutor.StreamChunk{Err: errRead})
				return
			}
			if msgType != websocket.TextMessage {
				if msgType == websocket.BinaryMessage {
					errBinary := fmt.Errorf("xai websockets executor: unexpected binary message")
					terminateReason = "unexpected_binary"
					terminateErr = errBinary
					helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", errBinary)
					reporter.PublishFailure(ctx, errBinary)
					if sess != nil {
						e.invalidateUpstreamConn(sess, conn, "unexpected_binary", errBinary)
					}
					_ = send(cliproxyexecutor.StreamChunk{Err: errBinary})
					return
				}
				continue
			}

			payload = bytes.TrimSpace(payload)
			if len(payload) == 0 {
				continue
			}
			reporter.MarkFirstResponseByte()
			helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)

			if wsErr, ok := parseXAIWebsocketError(payload); ok {
				statusCode := http.StatusInternalServerError
				if statusProvider, okStatus := wsErr.(interface{ StatusCode() int }); okStatus && statusProvider.StatusCode() > 0 {
					statusCode = statusProvider.StatusCode()
				}
				clearXAIReasoningReplayOnInvalidSignature(ctx, prepared.replayScope, statusCode, payload)
				terminateReason = "upstream_error"
				terminateErr = wsErr
				helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", wsErr)
				reporter.PublishFailure(ctx, wsErr)
				if sess != nil {
					e.invalidateUpstreamConnWithoutDisconnectNotify(sess, conn, "upstream_error", wsErr)
				}
				_ = send(cliproxyexecutor.StreamChunk{Err: wsErr})
				return
			}

			for _, payload := range xaiNormalizeReasoningSummaryDataEvents(payload) {
				payload = xaiNormalizeResponseDone(payload)
				eventType := gjson.GetBytes(payload, "type").String()
				if terminalErr, ok := xaiTerminalStreamError(payload); ok {
					clearXAIReasoningReplayOnInvalidSignature(ctx, prepared.replayScope, terminalErr.code, payload)
					terminateReason = "upstream_error"
					terminateErr = terminalErr
					helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", terminalErr)
					reporter.PublishFailure(ctx, terminalErr)
					_ = send(cliproxyexecutor.StreamChunk{Err: terminalErr})
					return
				}
				isTerminalEvent := eventType == "response.completed" || eventType == "response.done" || eventType == "error"
				isSuccessfulTerminalEvent := eventType == "response.completed" || eventType == "response.done"
				warmupCompletedPayload := []byte(nil)
				switch eventType {
				case "response.created":
					if warmupRequest {
						warmupCompletedPayload = buildXAIWebsocketWarmupCompletedPayload(payload)
						logXAIWebsocketWarmupCompleted(executionSessionID, authID, wsURL, payload)
					}
				case "response.output_item.done":
					xaiCollectOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
				case "response.completed":
					logXAIWebsocketTerminalResponse(executionSessionID, authID, wsURL, eventType, payload)
					if detail, ok := helps.ParseCodexUsage(payload); ok {
						reporter.Observe(detail)
					}
					payload = xaiPatchCompletedOutput(payload, outputItemsByIndex, outputItemsFallback)
					payload = xaiNormalizeReasoningSummaryData(payload)
					cacheXAIReasoningReplayFromCompleted(ctx, prepared.replayScope, payload)
					if !warmupRequest && idMapper != nil && idMapper.state != nil && !recordedTranscript {
						idMapper.state.recordTranscriptTurn(wsReqBody, payload)
						recordedTranscript = true
					}
				}

				if cliproxyexecutor.DownstreamWebsocket(ctx) {
					downstreamPayload := payload
					downstreamWarmupCompletedPayload := warmupCompletedPayload
					if idMapper != nil {
						downstreamPayload = idMapper.downstreamResponsePayload(payload)
						if len(warmupCompletedPayload) > 0 {
							downstreamWarmupCompletedPayload = idMapper.downstreamResponsePayload(warmupCompletedPayload)
						}
					}
					if !send(cliproxyexecutor.StreamChunk{Payload: downstreamPayload}) {
						terminateReason = "context_done"
						terminateErr = ctx.Err()
						reporter.PublishFailure(ctx, terminateErr)
						return
					}
					if len(downstreamWarmupCompletedPayload) > 0 {
						if !send(cliproxyexecutor.StreamChunk{Payload: downstreamWarmupCompletedPayload}) {
							terminateReason = "context_done"
							terminateErr = ctx.Err()
							reporter.PublishFailure(ctx, terminateErr)
							return
						}
						emitSuccessfulTerminal()
						return
					}
					if isTerminalEvent {
						if isSuccessfulTerminalEvent {
							emitSuccessfulTerminal()
						}
						return
					}
					continue
				}

				payload = normalizeCodexWebsocketCompletion(payload)
				line := encodeCodexWebsocketAsSSE(payload)
				chunks := sdktranslator.TranslateStream(ctx, prepared.to, prepared.responseFormat, req.Model, prepared.originalPayload, prepared.body, line, &param)
				for i := range chunks {
					if !send(cliproxyexecutor.StreamChunk{Payload: chunks[i]}) {
						terminateReason = "context_done"
						terminateErr = ctx.Err()
						reporter.PublishFailure(ctx, terminateErr)
						return
					}
				}
				if len(warmupCompletedPayload) > 0 {
					line = encodeCodexWebsocketAsSSE(warmupCompletedPayload)
					chunks = sdktranslator.TranslateStream(ctx, prepared.to, prepared.responseFormat, req.Model, prepared.originalPayload, prepared.body, line, &param)
					for i := range chunks {
						if !send(cliproxyexecutor.StreamChunk{Payload: chunks[i]}) {
							terminateReason = "context_done"
							terminateErr = ctx.Err()
							reporter.PublishFailure(ctx, terminateErr)
							return
						}
					}
					emitSuccessfulTerminal()
					return
				}
				if eventType == "response.completed" || eventType == "response.done" {
					emitSuccessfulTerminal()
					return
				}
			}
		}
	}()
	contextWatchTransferred = true
	return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
}

func (e *XAIWebsocketsExecutor) executeCompactionTriggerFromWebsocketContext(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, idMapper *xaiWebsocketRequestIDMapper) (*cliproxyexecutor.StreamResult, error) {
	if idMapper == nil || idMapper.state == nil {
		return nil, statusErr{code: http.StatusBadRequest, msg: "xai websocket compaction context is unavailable"}
	}
	transcriptInput := idMapper.state.snapshotTranscriptInput()
	if len(transcriptInput) == 0 {
		return nil, statusErr{code: http.StatusBadRequest, msg: "xai websocket compaction context is empty"}
	}
	authID := ""
	if auth != nil {
		authID = auth.ID
	}
	log.Infof(
		"xai websockets: compact fallback session=%s auth=%s input_items=%d",
		xaiExecutionSessionID(req, opts),
		strings.TrimSpace(authID),
		len(gjson.ParseBytes(transcriptInput).Array()),
	)
	compactPayload, err := buildXAIWebsocketCompactionPayload(req.Payload, transcriptInput)
	if err != nil {
		return nil, err
	}
	compactReq := req
	compactReq.Payload = compactPayload

	prepared, data, headers, err := e.XAIExecutor.executeCompactRequest(ctx, auth, compactReq, opts)
	if err != nil {
		return nil, err
	}

	responseID := xaiCompactionResponseID(data)
	idMapper.state.replaceTranscriptWithItems(xaiCompactionOutputItem(data, responseID))
	idMapper.state.mapDownstreamToUpstream(responseID, "")

	headers = headers.Clone()
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("Content-Type", "text/event-stream")

	chunks := xaiBuildCompactionTriggerStreamChunks(prepared, data)
	markerEnabled := metadataBool(opts.Metadata, cliproxyexecutor.StreamTerminalMarkerMetadataKey)
	bufferSize := len(chunks)
	if markerEnabled {
		bufferSize++
	}
	if bufferSize == 0 {
		bufferSize = 1
	}
	out := make(chan cliproxyexecutor.StreamChunk, bufferSize)
	for _, chunk := range chunks {
		out <- cliproxyexecutor.StreamChunk{Payload: chunk}
	}
	if markerEnabled {
		enqueueXAIWebsocketTerminalChunk(ctx, out, cliproxyexecutor.SuccessfulStreamTerminalChunk())
	}
	close(out)
	return &cliproxyexecutor.StreamResult{Headers: headers, Chunks: out}, nil
}

func buildXAIWebsocketCompactionPayload(payload []byte, transcriptInput []byte) ([]byte, error) {
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	if len(transcriptInput) == 0 {
		transcriptInput = []byte("[]")
	}
	out := bytes.Clone(payload)
	var err error
	out, err = sjson.SetRawBytes(out, "input", transcriptInput)
	if err != nil {
		return nil, err
	}
	out, _ = sjson.DeleteBytes(out, "previous_response_id")
	return out, nil
}

func xaiWebsocketGenerateFalse(payload []byte) bool {
	generate := gjson.GetBytes(payload, "generate")
	return generate.Exists() && !generate.Bool()
}

func buildXAIWebsocketWarmupCompletedPayload(createdPayload []byte) []byte {
	completed := []byte(`{"type":"response.completed","response":{"output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
	if sequence := gjson.GetBytes(createdPayload, "sequence_number"); sequence.Exists() {
		completed, _ = sjson.SetBytes(completed, "sequence_number", sequence.Int()+1)
	}
	if response := gjson.GetBytes(createdPayload, "response"); response.Exists() && response.IsObject() {
		responsePayload := []byte(response.Raw)
		responsePayload, _ = sjson.SetBytes(responsePayload, "status", "completed")
		if !gjson.GetBytes(responsePayload, "output").Exists() {
			responsePayload, _ = sjson.SetRawBytes(responsePayload, "output", []byte("[]"))
		}
		if !gjson.GetBytes(responsePayload, "usage").Exists() {
			responsePayload, _ = sjson.SetRawBytes(responsePayload, "usage", []byte(`{"input_tokens":0,"output_tokens":0,"total_tokens":0}`))
		}
		completed, _ = sjson.SetRawBytes(completed, "response", responsePayload)
	}
	return completed
}

func parseXAIWebsocketError(payload []byte) (error, bool) {
	if wsErr, ok := parseCodexWebsocketError(payload); ok {
		if statusError, okStatus := wsErr.(statusErrWithHeaders); okStatus {
			xaiError := xaiStatusErrFromResponse(statusError.code, payload, statusError.headers, time.Now())
			if xaiError.retryAfter != nil {
				statusError.retryAfter = xaiError.retryAfter
			}
			return statusError, true
		}
		return wsErr, true
	}
	if len(payload) == 0 || !gjson.GetBytes(payload, "error").Exists() {
		return nil, false
	}
	status := int(gjson.GetBytes(payload, "status").Int())
	if status <= 0 {
		status = int(gjson.GetBytes(payload, "status_code").Int())
	}
	if status <= 0 {
		status = xaiBareWebsocketErrorStatus(payload)
	}
	out := []byte(`{}`)
	out, _ = sjson.SetBytes(out, "type", "error")
	out, _ = sjson.SetBytes(out, "status", status)
	if errNode := gjson.GetBytes(payload, "error"); errNode.Exists() {
		out, _ = sjson.SetRawBytes(out, "error", []byte(errNode.Raw))
	}
	return xaiStatusErrFromResponse(status, out, parseCodexWebsocketErrorHeaders(payload), time.Now()), true
}

func xaiBareWebsocketErrorStatus(payload []byte) int {
	for _, path := range []string{"error.code", "error.status", "code"} {
		raw := strings.TrimSpace(gjson.GetBytes(payload, path).String())
		if raw == "" {
			continue
		}
		status, errAtoi := strconv.Atoi(raw)
		if errAtoi == nil && status > 0 {
			return status
		}
	}
	message := strings.TrimSpace(gjson.GetBytes(payload, "error.message").String())
	if strings.Contains(message, `"code":"400"`) || strings.Contains(message, "Request validation error") {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func (e *XAIWebsocketsExecutor) prepareResponsesWebsocketRequest(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*xaiPreparedRequest, error) {
	prepared, err := e.prepareResponsesRequest(ctx, auth, req, opts, true)
	if err != nil {
		return nil, err
	}
	if previousResponseID := strings.TrimSpace(gjson.GetBytes(req.Payload, "previous_response_id").String()); previousResponseID != "" {
		prepared.body, _ = sjson.SetBytes(prepared.body, "previous_response_id", previousResponseID)
	}
	return prepared, nil
}

func (e *XAIWebsocketsExecutor) dialXAIWebsocket(ctx context.Context, auth *cliproxyauth.Auth, wsURL string, headers http.Header) (*websocket.Conn, *http.Response, error) {
	dialer := newProxyAwareWebsocketDialer(ctx, e.cfg, auth)
	dialer.HandshakeTimeout = codexResponsesWebsocketHandshakeTO
	dialer.EnableCompression = true
	if ctx == nil {
		ctx = context.Background()
	}
	conn, resp, err := dialer.DialContext(ctx, wsURL, headers)
	if conn != nil {
		// Avoid gorilla/websocket flate tail validation issues on some upstreams/Go versions.
		conn.EnableWriteCompression(false)
	}
	return conn, resp, err
}

func (e *XAIWebsocketsExecutor) getOrCreateSession(sessionID string) *codexWebsocketSession {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || e == nil {
		return nil
	}
	store := e.store
	if store == nil {
		store = globalXAIWebsocketSessionStore
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.sessions == nil {
		store.sessions = make(map[string]*codexWebsocketSession)
	}
	if sess, ok := store.sessions[sessionID]; ok && sess != nil {
		return sess
	}
	sess := &codexWebsocketSession{
		sessionID:            sessionID,
		upstreamDisconnectCh: make(chan error, 1),
	}
	store.sessions[sessionID] = sess
	return sess
}

func (e *XAIWebsocketsExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	sess := e.getOrCreateSession(sessionID)
	if sess == nil {
		return nil
	}
	return sess.upstreamDisconnectCh
}

func (e *XAIWebsocketsExecutor) ensureUpstreamConn(ctx context.Context, auth *cliproxyauth.Auth, sess *codexWebsocketSession, authID string, wsURL string, headers http.Header) (*websocket.Conn, *http.Response, error) {
	if !xaiAuthExecutionCurrent(auth) {
		return nil, nil, errXAIWebsocketSessionTerminated
	}
	if sess == nil {
		conn, resp, errDial := e.dialXAIWebsocket(ctx, auth, wsURL, headers)
		if errDial != nil || xaiAuthExecutionCurrent(auth) {
			return conn, resp, errDial
		}
		if conn != nil {
			if errClose := conn.Close(); errClose != nil {
				log.Errorf("xai websockets executor: close websocket error: %v", errClose)
			}
		}
		closeHTTPResponseBody(resp, "xai websockets executor: close stale handshake response body error")
		return nil, nil, errXAIWebsocketSessionTerminated
	}

	sess.connMu.Lock()
	if sess.terminated || !xaiAuthExecutionCurrent(auth) {
		sess.connMu.Unlock()
		return nil, nil, errXAIWebsocketSessionTerminated
	}
	conn := sess.conn
	readerConn := sess.readerConn
	currentAuthID := sess.authID
	currentAuthInstanceID := sess.authInstanceID
	currentWSURL := sess.wsURL
	sess.connMu.Unlock()
	if conn != nil && (strings.TrimSpace(currentAuthID) != strings.TrimSpace(authID) || strings.TrimSpace(currentAuthInstanceID) != auth.RuntimeInstanceID() || strings.TrimSpace(currentWSURL) != strings.TrimSpace(wsURL)) {
		e.invalidateUpstreamConnWithoutDisconnectNotify(sess, conn, "auth_changed", nil)
		conn = nil
		readerConn = nil
	}
	if conn != nil {
		if readerConn != conn {
			sess.connMu.Lock()
			sess.readerConn = conn
			sess.connMu.Unlock()
			configureXAIWebsocketConn(sess, conn)
			go e.readUpstreamLoop(sess, conn)
		}
		return conn, nil, nil
	}

	sess.connMu.Lock()
	if sess.terminated || !xaiAuthExecutionCurrent(auth) {
		sess.connMu.Unlock()
		return nil, nil, errXAIWebsocketSessionTerminated
	}
	if sess.conn != nil {
		previous := sess.conn
		sess.connMu.Unlock()
		return previous, nil, nil
	}
	sess.dialGeneration++
	dialGeneration := sess.dialGeneration
	sess.pendingAuthID = strings.TrimSpace(authID)
	sess.pendingAuthInstanceID = auth.RuntimeInstanceID()
	sess.connMu.Unlock()

	conn, resp, errDial := e.dialXAIWebsocket(ctx, auth, wsURL, headers)
	if errDial != nil {
		clearXAIPendingWebsocketDial(sess, dialGeneration)
		return nil, resp, errDial
	}

	sess.connMu.Lock()
	if sess.terminated || !xaiAuthExecutionCurrent(auth) || sess.dialGeneration != dialGeneration || sess.pendingAuthID != strings.TrimSpace(authID) || sess.pendingAuthInstanceID != auth.RuntimeInstanceID() {
		sess.connMu.Unlock()
		if errClose := conn.Close(); errClose != nil {
			log.Errorf("xai websockets executor: close websocket error: %v", errClose)
		}
		closeHTTPResponseBody(resp, "xai websockets executor: close terminated handshake response body error")
		return nil, nil, errXAIWebsocketSessionTerminated
	}
	if sess.conn != nil {
		previous := sess.conn
		sess.pendingAuthID = ""
		sess.pendingAuthInstanceID = ""
		sess.connMu.Unlock()
		if errClose := conn.Close(); errClose != nil {
			log.Errorf("xai websockets executor: close websocket error: %v", errClose)
		}
		return previous, nil, nil
	}
	sess.conn = conn
	sess.wsURL = wsURL
	sess.authID = authID
	sess.authInstanceID = auth.RuntimeInstanceID()
	sess.pendingAuthID = ""
	sess.pendingAuthInstanceID = ""
	sess.readerConn = conn
	sess.connMu.Unlock()

	configureXAIWebsocketConn(sess, conn)
	go e.readUpstreamLoop(sess, conn)
	logXAIWebsocketConnected(sess.sessionID, authID, wsURL)
	return conn, resp, nil
}

func clearXAIPendingWebsocketDial(sess *codexWebsocketSession, generation uint64) {
	if sess == nil || generation == 0 {
		return
	}
	sess.connMu.Lock()
	if sess.dialGeneration == generation {
		sess.pendingAuthID = ""
		sess.pendingAuthInstanceID = ""
	}
	sess.connMu.Unlock()
}

func xaiWebsocketSessionIdentityChanged(sess *codexWebsocketSession, authID, authInstanceID, wsURL string) bool {
	if sess == nil {
		return false
	}
	sess.connMu.Lock()
	currentAuthID := strings.TrimSpace(sess.authID)
	currentAuthInstanceID := strings.TrimSpace(sess.authInstanceID)
	currentWSURL := strings.TrimSpace(sess.wsURL)
	sess.connMu.Unlock()
	authID = strings.TrimSpace(authID)
	authInstanceID = strings.TrimSpace(authInstanceID)
	wsURL = strings.TrimSpace(wsURL)
	return currentAuthID != "" && currentAuthID != authID || currentAuthInstanceID != "" && currentAuthInstanceID != authInstanceID || currentWSURL != "" && currentWSURL != wsURL
}

func configureXAIWebsocketConn(sess *codexWebsocketSession, conn *websocket.Conn) {
	if sess == nil || conn == nil {
		return
	}
	conn.SetPingHandler(func(appData string) error {
		sess.writeMu.Lock()
		defer sess.writeMu.Unlock()
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Time{})
	})
}

func readXAIWebsocketMessage(ctx context.Context, sess *codexWebsocketSession, conn *websocket.Conn, readCh chan codexWebsocketRead) (int, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if sess == nil {
		if conn == nil {
			return 0, nil, fmt.Errorf("xai websockets executor: websocket conn is nil")
		}
		msgType, payload, errRead := conn.ReadMessage()
		return msgType, payload, errRead
	}
	if conn == nil {
		return 0, nil, fmt.Errorf("xai websockets executor: websocket conn is nil")
	}
	if readCh == nil {
		return 0, nil, fmt.Errorf("xai websockets executor: session read channel is nil")
	}
	for {
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case ev, ok := <-readCh:
			if !ok {
				return 0, nil, fmt.Errorf("xai websockets executor: session read channel closed")
			}
			if ev.conn != conn {
				continue
			}
			if ev.err != nil {
				return 0, nil, ev.err
			}
			return ev.msgType, ev.payload, nil
		}
	}
}

func (e *XAIWebsocketsExecutor) readUpstreamLoop(sess *codexWebsocketSession, conn *websocket.Conn) {
	if e == nil || sess == nil || conn == nil {
		return
	}
	for {
		msgType, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			ch, done := sess.activeForConn(conn)
			if ch != nil {
				select {
				case ch <- codexWebsocketRead{conn: conn, err: errRead}:
				case <-done:
				default:
				}
				if sess.clearActiveForConn(ch, conn) {
					close(ch)
				}
			}
			e.invalidateUpstreamConn(sess, conn, "upstream_disconnected", errRead)
			return
		}

		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				errBinary := fmt.Errorf("xai websockets executor: unexpected binary message")
				ch, done := sess.activeForConn(conn)
				if ch != nil {
					select {
					case ch <- codexWebsocketRead{conn: conn, err: errBinary}:
					case <-done:
					default:
					}
					if sess.clearActiveForConn(ch, conn) {
						close(ch)
					}
				}
				e.invalidateUpstreamConn(sess, conn, "unexpected_binary", errBinary)
				return
			}
			continue
		}

		ch, done := sess.activeForConn(conn)
		if ch == nil {
			continue
		}
		select {
		case ch <- codexWebsocketRead{conn: conn, msgType: msgType, payload: payload}:
		case <-done:
		}
	}
}

func (e *XAIWebsocketsExecutor) invalidateUpstreamConn(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error) {
	e.invalidateUpstreamConnWithNotify(sess, conn, reason, err, true)
}

func (e *XAIWebsocketsExecutor) invalidateUpstreamConnWithoutDisconnectNotify(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error) {
	e.invalidateUpstreamConnWithNotify(sess, conn, reason, err, false)
}

func (e *XAIWebsocketsExecutor) invalidateUpstreamConnWithNotify(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error, notify bool) {
	if sess == nil || conn == nil {
		return
	}

	sess.connMu.Lock()
	current := sess.conn
	authID := sess.authID
	wsURL := sess.wsURL
	sessionID := sess.sessionID
	if current == nil || current != conn {
		sess.connMu.Unlock()
		return
	}
	sess.conn = nil
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sess.connMu.Unlock()

	logXAIWebsocketDisconnected(sessionID, authID, wsURL, reason, err)
	if notify {
		sess.notifyUpstreamDisconnect(err)
	}
	if errClose := conn.Close(); errClose != nil {
		log.Errorf("xai websockets executor: close websocket error: %v", errClose)
	}
}

func (e *XAIWebsocketsExecutor) CloseExecutionSession(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if e == nil || sessionID == "" {
		return
	}
	if sessionID == cliproxyauth.CloseAllExecutionSessionsID {
		return
	}

	store := e.store
	if store == nil {
		store = globalXAIWebsocketSessionStore
	}
	idStore := e.idStore
	if idStore == nil {
		idStore = globalXAIWebsocketIDStates
	}
	sess := detachXAIWebsocketSession(store, idStore, sessionID)

	e.closeExecutionSession(sess, "session_closed")
}

// CloseAuthExecutionSessions closes all xAI websocket sessions using one auth.
func (e *XAIWebsocketsExecutor) CloseAuthExecutionSessions(authID string, reason string) {
	e.closeAuthExecutionSessions(authID, "", reason)
}

// CloseAuthInstanceExecutionSessions closes xAI websocket sessions for one auth runtime instance.
func (e *XAIWebsocketsExecutor) CloseAuthInstanceExecutionSessions(authID string, authInstanceID string, reason string) {
	e.closeAuthExecutionSessions(authID, authInstanceID, reason)
}

func (e *XAIWebsocketsExecutor) closeAuthExecutionSessions(authID string, authInstanceID string, reason string) {
	if e == nil {
		return
	}
	store := e.store
	if store == nil {
		store = globalXAIWebsocketSessionStore
	}
	idStore := e.idStore
	if idStore == nil {
		idStore = globalXAIWebsocketIDStates
	}
	closeXAIWebsocketSessionsForAuth(store, idStore, authID, authInstanceID, reason)
}

func (e *XAIWebsocketsExecutor) closeExecutionSession(sess *codexWebsocketSession, reason string) {
	closeXAIWebsocketSession(sess, reason)
}

func closeXAIWebsocketSession(sess *codexWebsocketSession, reason string) {
	if sess == nil {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "session_closed"
	}

	sess.connMu.Lock()
	conn := sess.conn
	authID := sess.authID
	wsURL := sess.wsURL
	sess.conn = nil
	if !sess.terminated {
		sess.terminated = true
		sess.dialGeneration++
	}
	sess.pendingAuthID = ""
	sess.pendingAuthInstanceID = ""
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sessionID := sess.sessionID
	sess.connMu.Unlock()

	if conn == nil {
		return
	}
	logXAIWebsocketDisconnected(sessionID, authID, wsURL, reason, nil)
	if errClose := conn.Close(); errClose != nil {
		log.Errorf("xai websockets executor: close websocket error: %v", errClose)
	}
}

func buildXAIWebsocketRequestBody(body []byte) []byte {
	if len(body) == 0 {
		return nil
	}
	wsReqBody := bytes.Clone(body)
	wsReqBody, _ = sjson.SetBytes(wsReqBody, "type", "response.create")
	wsReqBody, _ = sjson.DeleteBytes(wsReqBody, "stream")
	wsReqBody, _ = sjson.DeleteBytes(wsReqBody, "stream_options")
	wsReqBody, _ = sjson.DeleteBytes(wsReqBody, "background")
	wsReqBody, _ = sjson.SetBytes(wsReqBody, "store", true)
	if strings.TrimSpace(gjson.GetBytes(wsReqBody, "previous_response_id").String()) != "" {
		wsReqBody, _ = sjson.DeleteBytes(wsReqBody, "instructions")
	}
	return wsReqBody
}

func buildXAIResponsesWebsocketURL(httpURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(httpURL))
	if err != nil {
		return "", err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("xai websockets executor: unsupported responses websocket URL scheme %q", parsed.Scheme)
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", fmt.Errorf("xai websockets executor: responses websocket URL host is empty")
	}
	return parsed.String(), nil
}

func applyXAIWebsocketHeaders(headers http.Header, auth *cliproxyauth.Auth, token string, sessionID string) http.Header {
	if headers == nil {
		headers = http.Header{}
	}
	headers.Set("Content-Type", "application/json")
	if strings.TrimSpace(token) != "" {
		headers.Set("Authorization", "Bearer "+token)
	}
	if sessionID != "" {
		headers.Set("x-grok-conv-id", sessionID)
	}
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(&http.Request{Header: headers}, attrs)
	return headers
}

func logXAIWebsocketConnected(sessionID string, authID string, wsURL string) {
	log.Infof("xai websockets: upstream connected session=%s auth=%s url=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL))
}

func logXAIWebsocketRequest(sessionID string, authID string, wsURL string, payload []byte) {
	if len(payload) == 0 {
		log.Infof("xai websockets: upstream request sent session=%s auth=%s url=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL))
		return
	}
	generateValue := "default"
	if generate := gjson.GetBytes(payload, "generate"); generate.Exists() {
		generateValue = strings.TrimSpace(generate.Raw)
	}
	log.Infof(
		"xai websockets: upstream request sent session=%s auth=%s url=%s event=%s previous_response_id=%s generate=%s input_items=%d",
		strings.TrimSpace(sessionID),
		strings.TrimSpace(authID),
		strings.TrimSpace(wsURL),
		strings.TrimSpace(gjson.GetBytes(payload, "type").String()),
		strings.TrimSpace(gjson.GetBytes(payload, "previous_response_id").String()),
		generateValue,
		len(gjson.GetBytes(payload, "input").Array()),
	)
}

func logXAIWebsocketWarmupCompleted(sessionID string, authID string, wsURL string, payload []byte) {
	log.Infof(
		"xai websockets: upstream warmup completed session=%s auth=%s url=%s response_id=%s",
		strings.TrimSpace(sessionID),
		strings.TrimSpace(authID),
		strings.TrimSpace(wsURL),
		strings.TrimSpace(gjson.GetBytes(payload, "response.id").String()),
	)
}

func logXAIWebsocketTerminalResponse(sessionID string, authID string, wsURL string, eventType string, payload []byte) {
	log.Infof(
		"xai websockets: upstream terminal response session=%s auth=%s url=%s event=%s response_id=%s previous_response_id=%s",
		strings.TrimSpace(sessionID),
		strings.TrimSpace(authID),
		strings.TrimSpace(wsURL),
		strings.TrimSpace(eventType),
		strings.TrimSpace(gjson.GetBytes(payload, "response.id").String()),
		strings.TrimSpace(gjson.GetBytes(payload, "response.previous_response_id").String()),
	)
}

func logXAIWebsocketDisconnected(sessionID string, authID string, wsURL string, reason string, err error) {
	if err != nil {
		log.Infof("xai websockets: upstream disconnected session=%s auth=%s url=%s reason=%s err=%v", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL), strings.TrimSpace(reason), err)
		return
	}
	log.Infof("xai websockets: upstream disconnected session=%s auth=%s url=%s reason=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL), strings.TrimSpace(reason))
}

type detachedXAIWebsocketSession struct {
	sessionID string
	authID    string
	wsURL     string
	conn      *websocket.Conn
}

func detachXAIWebsocketSession(store *codexWebsocketSessionStore, idStore *xaiWebsocketIDStateStore, sessionID string) *codexWebsocketSession {
	if store == nil || sessionID == "" {
		return nil
	}
	store.mu.Lock()
	if idStore != nil {
		idStore.mu.Lock()
	}
	sess := store.sessions[sessionID]
	if sess != nil {
		sess.connMu.Lock()
		sess.terminated = true
		sess.dialGeneration++
		sess.pendingAuthID = ""
		sess.pendingAuthInstanceID = ""
		sess.connMu.Unlock()
	}
	delete(store.sessions, sessionID)
	if idStore != nil {
		delete(idStore.sessions, sessionID)
		idStore.mu.Unlock()
	}
	store.mu.Unlock()
	return sess
}

func detachXAIWebsocketSessionForAuth(store *codexWebsocketSessionStore, idStore *xaiWebsocketIDStateStore, sessionID string, expected *codexWebsocketSession, authID string, authInstanceID string) (detachedXAIWebsocketSession, bool) {
	if store == nil || expected == nil || sessionID == "" || authID == "" {
		return detachedXAIWebsocketSession{}, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if idStore != nil {
		idStore.mu.Lock()
		defer idStore.mu.Unlock()
	}
	current, ok := store.sessions[sessionID]
	if !ok || current == nil || current != expected {
		return detachedXAIWebsocketSession{}, false
	}
	current.connMu.Lock()
	defer current.connMu.Unlock()
	currentAuthID := strings.TrimSpace(current.authID)
	currentAuthInstanceID := strings.TrimSpace(current.authInstanceID)
	pendingAuthID := strings.TrimSpace(current.pendingAuthID)
	pendingAuthInstanceID := strings.TrimSpace(current.pendingAuthInstanceID)
	activeMatch := currentAuthID == authID && (authInstanceID == "" || currentAuthInstanceID == authInstanceID)
	pendingMatch := pendingAuthID == authID && (authInstanceID == "" || pendingAuthInstanceID == authInstanceID)
	if !activeMatch && !pendingMatch {
		return detachedXAIWebsocketSession{}, false
	}
	current.terminated = true
	current.dialGeneration++
	current.pendingAuthID = ""
	current.pendingAuthInstanceID = ""
	detached := detachedXAIWebsocketSession{
		sessionID: current.sessionID,
		authID:    currentAuthID,
		wsURL:     current.wsURL,
		conn:      current.conn,
	}
	current.conn = nil
	if current.readerConn == detached.conn {
		current.readerConn = nil
	}
	delete(store.sessions, sessionID)
	if idStore != nil {
		delete(idStore.sessions, sessionID)
	}
	return detached, true
}

// CloseXAIWebsocketSessionsForAuthID closes all active xAI upstream websocket sessions
// associated with the supplied auth ID.
func CloseXAIWebsocketSessionsForAuthID(authID string, reason string) {
	closeXAIWebsocketSessionsForAuth(globalXAIWebsocketSessionStore, globalXAIWebsocketIDStates, authID, "", reason)
}

func closeXAIWebsocketSessionsForAuth(store *codexWebsocketSessionStore, idStore *xaiWebsocketIDStateStore, authID string, authInstanceID string, reason string) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "auth_removed"
	}

	if store == nil {
		return
	}

	type sessionItem struct {
		sessionID string
		sess      *codexWebsocketSession
	}

	store.mu.Lock()
	items := make([]sessionItem, 0, len(store.sessions))
	for sessionID, sess := range store.sessions {
		items = append(items, sessionItem{sessionID: sessionID, sess: sess})
	}
	store.mu.Unlock()

	toClose := make([]detachedXAIWebsocketSession, 0)
	for i := range items {
		detached, ok := detachXAIWebsocketSessionForAuth(store, idStore, items[i].sessionID, items[i].sess, authID, strings.TrimSpace(authInstanceID))
		if !ok {
			continue
		}
		toClose = append(toClose, detached)
	}

	for i := range toClose {
		if toClose[i].conn == nil {
			continue
		}
		logXAIWebsocketDisconnected(toClose[i].sessionID, toClose[i].authID, toClose[i].wsURL, reason, nil)
		if errClose := toClose[i].conn.Close(); errClose != nil {
			log.Errorf("xai websockets executor: close websocket error: %v", errClose)
		}
	}
}

// XAIAutoExecutor routes xAI stream requests to the websocket transport only
// when the downstream transport is websocket and the selected auth enables
// websockets. Non-stream requests keep using the HTTP implementation.
type XAIAutoExecutor struct {
	httpExec *XAIExecutor
	wsExec   *XAIWebsocketsExecutor
}

var _ cliproxyauth.AuthExecutionSessionCloser = (*XAIAutoExecutor)(nil)
var _ cliproxyauth.AuthInstanceExecutionSessionCloser = (*XAIAutoExecutor)(nil)

func NewXAIAutoExecutor(cfg *config.Config) *XAIAutoExecutor {
	return &XAIAutoExecutor{
		httpExec: NewXAIExecutor(cfg),
		wsExec:   NewXAIWebsocketsExecutor(cfg),
	}
}

func (e *XAIAutoExecutor) Identifier() string { return "xai" }

func (e *XAIAutoExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if e == nil || e.httpExec == nil {
		return nil
	}
	return e.httpExec.PrepareRequest(req, auth)
}

func (e *XAIAutoExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if e == nil || e.httpExec == nil {
		return nil, fmt.Errorf("xai auto executor: http executor is nil")
	}
	return e.httpExec.HttpRequest(ctx, auth, req)
}

func (e *XAIAutoExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e == nil || e.httpExec == nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("xai auto executor: executor is nil")
	}
	return e.httpExec.Execute(ctx, auth, req, opts)
}

func (e *XAIAutoExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if e == nil || e.httpExec == nil || e.wsExec == nil {
		return nil, fmt.Errorf("xai auto executor: executor is nil")
	}
	if cliproxyexecutor.DownstreamWebsocket(ctx) && xaiWebsocketsEnabled(auth) {
		return e.wsExec.ExecuteStream(ctx, auth, req, opts)
	}
	return e.httpExec.ExecuteStream(ctx, auth, req, opts)
}

func (e *XAIAutoExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if e == nil || e.httpExec == nil {
		return nil, fmt.Errorf("xai auto executor: http executor is nil")
	}
	return e.httpExec.Refresh(ctx, auth)
}

func (e *XAIAutoExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e == nil || e.httpExec == nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("xai auto executor: http executor is nil")
	}
	return e.httpExec.CountTokens(ctx, auth, req, opts)
}

func (e *XAIAutoExecutor) CloseExecutionSession(sessionID string) {
	if e == nil || e.wsExec == nil {
		return
	}
	e.wsExec.CloseExecutionSession(sessionID)
}

// CloseAuthExecutionSessions closes all xAI websocket sessions using one auth.
func (e *XAIAutoExecutor) CloseAuthExecutionSessions(authID string, reason string) {
	if e == nil || e.wsExec == nil {
		return
	}
	e.wsExec.CloseAuthExecutionSessions(authID, reason)
}

// CloseAuthInstanceExecutionSessions closes xAI websocket sessions for one auth runtime instance.
func (e *XAIAutoExecutor) CloseAuthInstanceExecutionSessions(authID string, authInstanceID string, reason string) {
	if e == nil || e.wsExec == nil {
		return
	}
	e.wsExec.CloseAuthInstanceExecutionSessions(authID, authInstanceID, reason)
}

func (e *XAIAutoExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	if e == nil || e.wsExec == nil {
		return nil
	}
	return e.wsExec.UpstreamDisconnectChan(sessionID)
}

func xaiWebsocketsEnabled(auth *cliproxyauth.Auth) bool {
	if auth == nil {
		return false
	}
	if len(auth.Attributes) > 0 {
		if raw := strings.TrimSpace(auth.Attributes["websockets"]); raw != "" {
			parsed, errParse := strconv.ParseBool(raw)
			if errParse == nil {
				return parsed
			}
		}
	}
	if len(auth.Metadata) == 0 {
		return false
	}
	raw, ok := auth.Metadata["websockets"]
	if !ok || raw == nil {
		return false
	}
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(v))
		if errParse == nil {
			return parsed
		}
	default:
	}
	return false
}
