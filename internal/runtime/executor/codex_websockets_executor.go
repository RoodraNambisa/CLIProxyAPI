// Package executor provides runtime execution capabilities for various AI service providers.
// This file implements a Codex executor that uses the Responses API WebSocket transport.
package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	codexResponsesWebsocketBetaHeaderValue = "responses_websockets=2026-02-06"
	codexResponsesWebsocketIdleTimeout     = 5 * time.Minute
	codexResponsesWebsocketHandshakeTO     = 30 * time.Second
)

var errCodexWebsocketSessionTerminated = errors.New("codex websockets executor: session terminated")

// CodexWebsocketsExecutor executes Codex Responses requests using a WebSocket transport.
//
// It preserves the existing CodexExecutor HTTP implementation as a fallback for endpoints
// not available over WebSocket (e.g. /responses/compact) and for websocket upgrade failures.
type CodexWebsocketsExecutor struct {
	*CodexExecutor

	store *codexWebsocketSessionStore
}

type codexWebsocketSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*codexWebsocketSession
}

var globalCodexWebsocketSessionStore = &codexWebsocketSessionStore{
	sessions: make(map[string]*codexWebsocketSession),
}

type codexWebsocketSession struct {
	sessionID string

	reqMu sync.Mutex

	connMu         sync.Mutex
	conn           *websocket.Conn
	wsURL          string
	authID         string
	authInstanceID string
	proxyBindingID string
	proxyIdentity  string
	// pendingAuthID and dialGeneration identify an in-flight Codex dial before a
	// connection has been installed on the session.
	pendingAuthID         string
	pendingAuthInstanceID string
	pendingProxyBindingID string
	pendingProxyIdentity  string
	dialGeneration        uint64
	// terminated prevents Codex requests that already captured this session from
	// reconnecting after the session has been removed from its store.
	terminated bool

	writeMu sync.Mutex

	activeMu     sync.Mutex
	activeCh     chan codexWebsocketRead
	activeConn   *websocket.Conn
	activeDone   <-chan struct{}
	activeCancel context.CancelFunc

	readerConn *websocket.Conn

	upstreamDisconnectOnce sync.Once
	upstreamDisconnectCh   chan error
}

func NewCodexWebsocketsExecutor(cfg *config.Config) *CodexWebsocketsExecutor {
	return &CodexWebsocketsExecutor{
		CodexExecutor: NewCodexExecutor(cfg),
		store:         globalCodexWebsocketSessionStore,
	}
}

type codexWebsocketRead struct {
	conn    *websocket.Conn
	msgType int
	payload []byte
	err     error
}

func (s *codexWebsocketSession) setActive(ch chan codexWebsocketRead) {
	s.setActiveForConn(ch, nil)
}

func (s *codexWebsocketSession) setActiveForConn(ch chan codexWebsocketRead, conn *websocket.Conn) {
	if s == nil {
		return
	}
	s.activeMu.Lock()
	if s.activeCancel != nil {
		s.activeCancel()
		s.activeCancel = nil
		s.activeDone = nil
	}
	s.activeCh = ch
	s.activeConn = conn
	if ch != nil {
		activeCtx, activeCancel := context.WithCancel(context.Background())
		s.activeDone = activeCtx.Done()
		s.activeCancel = activeCancel
	}
	s.activeMu.Unlock()
}

func (s *codexWebsocketSession) replaceActiveForConn(ch chan codexWebsocketRead, oldConn, newConn *websocket.Conn) chan codexWebsocketRead {
	if s == nil {
		return nil
	}
	s.clearActiveForConn(ch, oldConn)
	next := make(chan codexWebsocketRead, 4096)
	s.setActiveForConn(next, newConn)
	return next
}

func (s *codexWebsocketSession) clearActive(ch chan codexWebsocketRead) bool {
	return s.clearActiveForConn(ch, nil)
}

func (s *codexWebsocketSession) clearActiveForConn(ch chan codexWebsocketRead, conn *websocket.Conn) bool {
	if s == nil {
		return false
	}
	s.activeMu.Lock()
	cleared := s.activeCh == ch && (conn == nil || s.activeConn == conn)
	if cleared {
		s.activeCh = nil
		s.activeConn = nil
		if s.activeCancel != nil {
			s.activeCancel()
		}
		s.activeCancel = nil
		s.activeDone = nil
	}
	s.activeMu.Unlock()
	return cleared
}

func (s *codexWebsocketSession) activeForConn(conn *websocket.Conn) (chan codexWebsocketRead, <-chan struct{}) {
	if s == nil {
		return nil, nil
	}
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	if s.activeConn != nil && s.activeConn != conn {
		return nil, nil
	}
	return s.activeCh, s.activeDone
}

func (s *codexWebsocketSession) writeMessage(conn *websocket.Conn, msgType int, payload []byte) error {
	if s == nil {
		return fmt.Errorf("codex websockets executor: session is nil")
	}
	if conn == nil {
		return fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return conn.WriteMessage(msgType, payload)
}

func (s *codexWebsocketSession) configureConn(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	conn.SetPingHandler(func(appData string) error {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		// Reply pongs from the same write lock to avoid concurrent writes.
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(10*time.Second))
	})
}

func (s *codexWebsocketSession) notifyUpstreamDisconnect(err error) {
	if s == nil {
		return
	}
	s.upstreamDisconnectOnce.Do(func() {
		if s.upstreamDisconnectCh == nil {
			return
		}
		select {
		case s.upstreamDisconnectCh <- err:
		default:
		}
		close(s.upstreamDisconnectCh)
	})
}

func (e *CodexWebsocketsExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = contextWithCodexFingerprintPersona(ctx, e.cfg, auth)
	if opts.Alt == "responses/compact" {
		return e.CodexExecutor.executeCompact(ctx, auth, req, opts)
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	to := sdktranslator.FromString("codex")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayload := originalPayloadSource
	originalTranslated := sdktranslator.TranslateRequest(from, to, baseModel, originalPayload, false)
	body := sdktranslator.TranslateRequest(from, to, baseModel, req.Payload, false)

	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return resp, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, originalTranslated, requestedModel)
	body, err = sjson.SetBytes(body, "model", baseModel)
	if err != nil {
		return resp, fmt.Errorf("codex websockets executor: set base model in request body: %w", err)
	}
	body, _ = sjson.SetBytes(body, "stream", true)
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	if !gjson.GetBytes(body, "instructions").Exists() {
		body, _ = sjson.SetBytes(body, "instructions", "")
	}
	body = helps.SanitizeCodexReasoningEncryptedContent(ctx, "codex websockets executor", body)
	body = helps.NormalizeCodexToolSelection(body)
	reporter.SetRequestServiceTierFromPayload(body)

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildCodexResponsesWebsocketURL(httpURL)
	if err != nil {
		return resp, err
	}

	body, wsHeaders := applyCodexPromptCacheHeaders(from, req, body)
	clientBody := body
	upstreamBody, identityState := applyCodexIdentityConfuseBody(e.cfg, auth, originalPayloadSource, body)
	wsHeaders, err = prepareCodexWebsocketHeadersForURL(ctx, wsHeaders, auth, apiKey, e.cfg, parsedURLOrNil(wsURL))
	if err != nil {
		return resp, err
	}
	applyCodexIdentityConfuseHeaders(wsHeaders, &identityState)
	releasedOriginalPayload := slimCodexOriginalPayloadForTranslation(from, originalPayload)
	releasedClientBody := slimCodexBodyForStreamUsage(clientBody)
	originalRef, clientBodyRef, unregisterClientBodies := codexStreamBodyRefs(ctx, opts, originalPayload, clientBody, releasedOriginalPayload, releasedClientBody)
	_, upstreamRef, unregisterUpstreamBody := codexStreamBodyRefs(ctx, opts, nil, upstreamBody, nil, slimCodexBodyForStreamUsage(upstreamBody))
	unregisterRawBodyCleanup := cliproxyexecutor.RegisterRequestBodyReleaseCleanup(ctx, &req, &opts)
	defer unregisterRawBodyCleanup()
	defer unregisterClientBodies()
	defer unregisterUpstreamBody()
	defer originalRef.Release()
	defer clientBodyRef.Release()
	defer upstreamRef.Release()
	originalPayloadSource = nil
	originalPayload = nil
	originalTranslated = nil
	body = nil
	clientBody = nil
	upstreamBody = nil

	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}

	executionSessionID := executionSessionIDFromOptions(opts)
	var sess *codexWebsocketSession
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		sess.reqMu.Lock()
		defer sess.reqMu.Unlock()
	}

	wsReqBody := buildCodexWebsocketRequestBody(upstreamRef.Bytes())
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

	conn, respHS, errDial := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
	if errDial != nil {
		bodyErr := websocketHandshakeBody(respHS)
		bodyErr = codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), bodyErr)
		if respHS != nil {
			helps.RecordAPIWebsocketUpgradeRejection(ctx, e.cfg, websocketUpgradeRequestLog(wsReqLog), respHS.StatusCode, respHS.Header.Clone(), bodyErr)
		}
		if respHS != nil && respHS.StatusCode == http.StatusUpgradeRequired {
			if !helps.RequestBodyReplayable(ctx, opts) {
				return resp, newCodexWebsocketHandshakeStatusErr(respHS.StatusCode, bodyErr, respHS.Header)
			}
			return e.CodexExecutor.Execute(ctx, auth, req, opts)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			return resp, newCodexWebsocketHandshakeStatusErr(respHS.StatusCode, bodyErr, respHS.Header)
		}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "dial", errDial)
		return resp, errDial
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	dropCodexRawRequestCopies(&req, &opts)
	if sess == nil {
		logCodexWebsocketConnected(executionSessionID, authID, wsURL)
		defer func() {
			reason := "completed"
			if err != nil {
				reason = "error"
			}
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, reason, err)
			if errClose := conn.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
		}()
	}

	var readCh chan codexWebsocketRead
	if sess != nil {
		readCh = make(chan codexWebsocketRead, 4096)
		sess.setActiveForConn(readCh, conn)
		defer func() { sess.clearActiveForConn(readCh, conn) }()
	}

	if errSend := writeCurrentCodexWebsocketMessage(ctx, auth, sess, conn, wsReqBody); errSend != nil {
		if sess != nil {
			e.invalidateUpstreamConn(sess, conn, "send_error", errSend)
			if !helps.RequestBodyReplayable(ctx, opts) {
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
				return resp, errSend
			}

			// Retry once with a fresh websocket connection. This is mainly to handle
			// upstream closing the socket between sequential requests within the same
			// execution session.
			connRetry, respHSRetry, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
			if errDialRetry == nil && connRetry != nil {
				readCh = sess.replaceActiveForConn(readCh, conn, connRetry)
				wsReqBodyRetry := buildCodexWebsocketRequestBody(upstreamRef.Bytes())
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
				recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
				if errSendRetry := writeCurrentCodexWebsocketMessage(ctx, auth, sess, connRetry, wsReqBodyRetry); errSendRetry == nil {
					conn = connRetry
					wsReqBodyRetry = nil
				} else {
					e.invalidateUpstreamConn(sess, connRetry, "send_error", errSendRetry)
					helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
					return resp, errSendRetry
				}
			} else {
				closeHTTPResponseBody(respHSRetry, "codex websockets executor: close handshake response body error")
				helps.RecordAPIWebsocketError(ctx, e.cfg, "dial_retry", errDialRetry)
				return resp, errDialRetry
			}
		} else {
			helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
			return resp, errSend
		}
	}
	wsReqBody = nil

	for {
		if ctx != nil && ctx.Err() != nil {
			return resp, ctx.Err()
		}
		msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
		if errRead != nil {
			mappedErr := mapCodexWebsocketReadError(errRead)
			helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
			return resp, mappedErr
		}
		if msgType != websocket.TextMessage {
			if msgType == websocket.BinaryMessage {
				err = fmt.Errorf("codex websockets executor: unexpected binary message")
				if sess != nil {
					e.invalidateUpstreamConn(sess, conn, "unexpected_binary", err)
				}
				helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", err)
				return resp, err
			}
			continue
		}

		payload = bytes.TrimSpace(payload)
		if len(payload) == 0 {
			continue
		}
		payload = applyCodexIdentityConfuseResponsePayload(payload, identityState)
		normalizedPayload := normalizeCodexCompletion(payload)
		clientPayload := applyCodexIdentityExposeResponsePayload(normalizedPayload, identityState)
		if originalWSErr, ok := parseCodexWebsocketError(clientPayload); ok {
			payload = codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), payload)
			helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)
			normalizedPayload = normalizeCodexCompletion(payload)
			clientPayload = applyCodexIdentityExposeResponsePayload(normalizedPayload, identityState)
			wsErr := codexWebsocketErrorWithSanitizedMessage(originalWSErr, payload)
			if sanitizedWSErr, parsed := parseCodexWebsocketError(clientPayload); parsed {
				wsErr = sanitizedWSErr
			}
			if sess != nil {
				e.invalidateUpstreamConn(sess, conn, "upstream_error", wsErr)
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", wsErr)
			return resp, wsErr
		}
		if originalTerminalErr, ok := codexTerminalStreamError(clientPayload); ok {
			payload = codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), payload)
			helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)
			normalizedPayload = normalizeCodexCompletion(payload)
			clientPayload = applyCodexIdentityExposeResponsePayload(normalizedPayload, identityState)
			terminalErr := originalTerminalErr
			if sanitizedTerminalErr, parsed := codexTerminalStreamError(clientPayload); parsed {
				terminalErr = sanitizedTerminalErr
			} else {
				terminalErr.msg = string(payload)
			}
			helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_response_error", terminalErr)
			return resp, terminalErr
		}
		helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)
		payload = normalizedPayload

		eventType := gjson.GetBytes(payload, "type").String()
		if eventType == "response.completed" {
			if detail, ok := helps.ParseCodexUsage(payload); ok {
				reporter.Publish(ctx, detail)
			}
			var param any
			out := sdktranslator.TranslateNonStream(ctx, to, from, req.Model, originalRef.Bytes(), clientBodyRef.Bytes(), clientPayload, &param)
			resp = cliproxyexecutor.Response{Payload: out}
			return resp, nil
		}
	}
}

func (e *CodexWebsocketsExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	log.Debugf("Executing Codex Websockets stream request with auth ID: %s, model: %s", auth.ID, req.Model)
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = contextWithCodexFingerprintPersona(ctx, e.cfg, auth)
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "streaming not supported for /responses/compact"}
	}

	baseModel := thinking.ParseSuffix(req.Model).ModelName
	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	to := sdktranslator.FromString("codex")
	body := req.Payload
	userPayload := req.Payload
	if len(opts.OriginalRequest) > 0 {
		userPayload = opts.OriginalRequest
	}

	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, body, requestedModel)
	body, err = sjson.SetBytes(body, "model", baseModel)
	if err != nil {
		return nil, fmt.Errorf("codex websockets executor: set base model in request body: %w", err)
	}
	body, err = e.applyDisabledImageGenerationToolPolicy(auth, body)
	if err != nil {
		return nil, err
	}
	body = helps.SanitizeCodexReasoningEncryptedContent(ctx, "codex websockets executor", body)
	body = helps.NormalizeCodexToolSelection(body)
	reporter.SetRequestServiceTierFromPayload(body)

	httpURL := strings.TrimSuffix(baseURL, "/") + "/responses"
	wsURL, err := buildCodexResponsesWebsocketURL(httpURL)
	if err != nil {
		return nil, err
	}

	body, wsHeaders := applyCodexPromptCacheHeaders(from, req, body)
	clientBody := body
	upstreamBody, identityState := applyCodexIdentityConfuseBody(e.cfg, auth, userPayload, body)
	wsHeaders, err = prepareCodexWebsocketHeadersForURL(ctx, wsHeaders, auth, apiKey, e.cfg, parsedURLOrNil(wsURL))
	if err != nil {
		return nil, err
	}
	applyCodexIdentityConfuseHeaders(wsHeaders, &identityState)
	releasedOriginalPayload := slimCodexOriginalPayloadForTranslation(from, userPayload)
	releasedClientBody := slimCodexBodyForStreamUsage(clientBody)
	originalRef, clientBodyRef, unregisterClientBodies := codexStreamBodyRefs(ctx, opts, userPayload, clientBody, releasedOriginalPayload, releasedClientBody)
	_, upstreamRef, unregisterUpstreamBody := codexStreamBodyRefs(ctx, opts, nil, upstreamBody, nil, slimCodexBodyForStreamUsage(upstreamBody))
	unregisterRawBodyCleanup := cliproxyexecutor.RegisterRequestBodyReleaseCleanup(ctx, &req, &opts)
	cleanupBodies := func() {
		unregisterRawBodyCleanup()
		unregisterClientBodies()
		unregisterUpstreamBody()
		originalRef.Release()
		clientBodyRef.Release()
		upstreamRef.Release()
	}
	body = nil
	clientBody = nil
	upstreamBody = nil
	userPayload = nil

	var authID, authLabel, authType, authValue string
	authID = auth.ID
	authLabel = auth.Label
	authType, authValue = auth.AccountInfo()

	executionSessionID := executionSessionIDFromOptions(opts)
	var sess *codexWebsocketSession
	if executionSessionID != "" {
		sess = e.getOrCreateSession(executionSessionID)
		if sess != nil {
			sess.reqMu.Lock()
		}
	}

	wsReqBody := buildCodexWebsocketRequestBody(upstreamRef.Bytes())
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

	conn, respHS, errDial := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
	var upstreamHeaders http.Header
	if respHS != nil {
		upstreamHeaders = respHS.Header.Clone()
	}
	if errDial != nil {
		if sess != nil {
			sess.reqMu.Unlock()
		}
		bodyErr := websocketHandshakeBody(respHS)
		bodyErr = codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), bodyErr)
		if respHS != nil {
			helps.RecordAPIWebsocketUpgradeRejection(ctx, e.cfg, websocketUpgradeRequestLog(wsReqLog), respHS.StatusCode, respHS.Header.Clone(), bodyErr)
		}
		if respHS != nil && respHS.StatusCode == http.StatusUpgradeRequired {
			if !helps.RequestBodyReplayable(ctx, opts) {
				cleanupBodies()
				return nil, newCodexWebsocketHandshakeStatusErr(respHS.StatusCode, bodyErr, respHS.Header)
			}
			cleanupBodies()
			return e.CodexExecutor.ExecuteStream(ctx, auth, req, opts)
		}
		if respHS != nil && respHS.StatusCode > 0 {
			cleanupBodies()
			return nil, newCodexWebsocketHandshakeStatusErr(respHS.StatusCode, bodyErr, respHS.Header)
		}
		helps.RecordAPIWebsocketError(ctx, e.cfg, "dial", errDial)
		cleanupBodies()
		return nil, errDial
	}
	recordAPIWebsocketHandshake(ctx, e.cfg, respHS)
	dropCodexRawRequestCopies(&req, &opts)

	if sess == nil {
		logCodexWebsocketConnected(executionSessionID, authID, wsURL)
	}

	var readCh chan codexWebsocketRead
	if sess != nil {
		readCh = make(chan codexWebsocketRead, 4096)
		sess.setActiveForConn(readCh, conn)
	}

	if errSend := writeCurrentCodexWebsocketMessage(ctx, auth, sess, conn, wsReqBody); errSend != nil {
		helps.RecordAPIWebsocketError(ctx, e.cfg, "send", errSend)
		if sess != nil {
			e.invalidateUpstreamConn(sess, conn, "send_error", errSend)
			if !helps.RequestBodyReplayable(ctx, opts) {
				sess.clearActiveForConn(readCh, conn)
				sess.reqMu.Unlock()
				cleanupBodies()
				return nil, errSend
			}

			// Retry once with a new websocket connection for the same execution session.
			connRetry, respHSRetry, errDialRetry := e.ensureUpstreamConn(ctx, auth, sess, authID, wsURL, wsHeaders)
			if errDialRetry != nil || connRetry == nil {
				closeHTTPResponseBody(respHSRetry, "codex websockets executor: close handshake response body error")
				helps.RecordAPIWebsocketError(ctx, e.cfg, "dial_retry", errDialRetry)
				sess.clearActiveForConn(readCh, conn)
				sess.reqMu.Unlock()
				cleanupBodies()
				return nil, errDialRetry
			}
			readCh = sess.replaceActiveForConn(readCh, conn, connRetry)
			wsReqBodyRetry := buildCodexWebsocketRequestBody(upstreamRef.Bytes())
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
			recordAPIWebsocketHandshake(ctx, e.cfg, respHSRetry)
			if errSendRetry := writeCurrentCodexWebsocketMessage(ctx, auth, sess, connRetry, wsReqBodyRetry); errSendRetry != nil {
				helps.RecordAPIWebsocketError(ctx, e.cfg, "send_retry", errSendRetry)
				e.invalidateUpstreamConn(sess, connRetry, "send_error", errSendRetry)
				sess.clearActiveForConn(readCh, connRetry)
				sess.reqMu.Unlock()
				cleanupBodies()
				return nil, errSendRetry
			}
			conn = connRetry
			wsReqBodyRetry = nil
		} else {
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, "send_error", errSend)
			if errClose := conn.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
			}
			cleanupBodies()
			return nil, errSend
		}
	}
	wsReqBody = nil

	out := make(chan cliproxyexecutor.StreamChunk, cliproxyexecutor.StreamBufferSize)
	go func() {
		terminateReason := "completed"
		var terminateErr error

		defer close(out)
		defer cleanupBodies()
		defer func() {
			if sess != nil {
				sess.clearActiveForConn(readCh, conn)
				sess.reqMu.Unlock()
				return
			}
			logCodexWebsocketDisconnected(executionSessionID, authID, wsURL, terminateReason, terminateErr)
			if errClose := conn.Close(); errClose != nil {
				log.Errorf("codex websockets executor: close websocket error: %v", errClose)
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

		var param any
		streamEstablished := false
		for {
			if ctx != nil && ctx.Err() != nil {
				terminateReason = "context_done"
				terminateErr = ctx.Err()
				reporter.PublishFailure(ctx, ctx.Err())
				_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
				return
			}
			msgType, payload, errRead := readCodexWebsocketMessage(ctx, sess, conn, readCh)
			if errRead != nil {
				if sess != nil && ctx != nil && ctx.Err() != nil {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					reporter.PublishFailure(ctx, ctx.Err())
					_ = send(cliproxyexecutor.StreamChunk{Err: ctx.Err()})
					return
				}
				mappedErr := mapCodexWebsocketReadError(errRead)
				terminateReason = "read_error"
				terminateErr = mappedErr
				helps.RecordAPIWebsocketError(ctx, e.cfg, "read", mappedErr)
				reporter.PublishFailure(ctx)
				_ = send(cliproxyexecutor.StreamChunk{Err: mappedErr})
				return
			}
			if msgType != websocket.TextMessage {
				if msgType == websocket.BinaryMessage {
					err = fmt.Errorf("codex websockets executor: unexpected binary message")
					terminateReason = "unexpected_binary"
					terminateErr = err
					helps.RecordAPIWebsocketError(ctx, e.cfg, "unexpected_binary", err)
					reporter.PublishFailure(ctx)
					if sess != nil {
						e.invalidateUpstreamConn(sess, conn, "unexpected_binary", err)
					}
					_ = send(cliproxyexecutor.StreamChunk{Err: err})
					return
				}
				continue
			}

			payload = bytes.TrimSpace(payload)
			if len(payload) == 0 {
				continue
			}
			payload = applyCodexIdentityConfuseResponsePayload(payload, identityState)
			normalizedPayload := normalizeCodexCompletion(payload)
			clientPayload := applyCodexIdentityExposeResponsePayload(normalizedPayload, identityState)
			if originalWSErr, ok := parseCodexWebsocketError(clientPayload); ok {
				payload = codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), payload)
				helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)
				normalizedPayload = normalizeCodexCompletion(payload)
				clientPayload = applyCodexIdentityExposeResponsePayload(normalizedPayload, identityState)
				wsErr := codexWebsocketErrorWithSanitizedMessage(originalWSErr, payload)
				if sanitizedWSErr, parsed := parseCodexWebsocketError(clientPayload); parsed {
					wsErr = sanitizedWSErr
				}
				terminateReason = "upstream_error"
				terminateErr = wsErr
				helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_error", wsErr)
				reporter.PublishFailure(ctx)
				if sess != nil {
					e.invalidateUpstreamConn(sess, conn, "upstream_error", wsErr)
				}
				_ = send(cliproxyexecutor.StreamChunk{Err: wsErr})
				return
			}
			if originalTerminalErr, ok := codexTerminalStreamError(clientPayload); ok {
				payload = codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), payload)
				helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)
				normalizedPayload = normalizeCodexCompletion(payload)
				clientPayload = applyCodexIdentityExposeResponsePayload(normalizedPayload, identityState)
				terminalErr := originalTerminalErr
				if sanitizedTerminalErr, parsed := codexTerminalStreamError(clientPayload); parsed {
					terminalErr = sanitizedTerminalErr
				} else {
					terminalErr.msg = string(payload)
				}
				terminateReason = "upstream_response_error"
				terminateErr = terminalErr
				helps.RecordAPIWebsocketError(ctx, e.cfg, "upstream_response_error", terminalErr)
				reporter.PublishFailure(ctx, terminalErr)
				_ = send(cliproxyexecutor.StreamChunk{Err: terminalErr})
				return
			}
			helps.AppendAPIWebsocketResponse(ctx, e.cfg, payload)
			payload = normalizedPayload

			eventType := gjson.GetBytes(payload, "type").String()
			if !streamEstablished && eventType != "error" {
				helps.ReleaseRequestBodyAfterStreamEstablished(ctx, opts)
				streamEstablished = true
			}
			if eventType == "response.completed" || eventType == "response.done" {
				if detail, ok := helps.ParseCodexUsage(payload); ok {
					reporter.Observe(detail)
				}
			}

			line := encodeCodexWebsocketAsSSE(clientPayload)
			chunks := sdktranslator.TranslateStream(ctx, to, from, req.Model, originalRef.Bytes(), clientBodyRef.Bytes(), line, &param)
			for i := range chunks {
				if !send(cliproxyexecutor.StreamChunk{Payload: chunks[i]}) {
					terminateReason = "context_done"
					terminateErr = ctx.Err()
					return
				}
			}
			if isCodexSuccessfulCompletion(payload) {
				reporter.EnsurePublished(ctx)
				if metadataBool(opts.Metadata, cliproxyexecutor.StreamTerminalMarkerMetadataKey) {
					if !send(cliproxyexecutor.SuccessfulStreamTerminalChunk()) {
						terminateReason = "context_done"
						terminateErr = ctx.Err()
					}
				}
				return
			}
		}
	}()

	return &cliproxyexecutor.StreamResult{Headers: upstreamHeaders, Chunks: out}, nil
}

func (e *CodexWebsocketsExecutor) dialCodexWebsocket(ctx context.Context, auth *cliproxyauth.Auth, wsURL string, headers http.Header) (*websocket.Conn, *http.Response, error) {
	if codexauth.IsAgentIdentityMetadata(authMetadata(auth)) {
		headers = headers.Clone()
		if headers == nil {
			headers = make(http.Header)
		}
		authorization, errAuthorization := codexauth.AuthorizationHeader(authMetadata(auth), "", time.Now())
		if errAuthorization != nil {
			return nil, nil, fmt.Errorf("codex websockets executor: build dial authorization: %w", errAuthorization)
		}
		headers.Set("Authorization", authorization)
	}
	dialer := newProxyAwareWebsocketDialer(ctx, e.cfg, auth)
	dialer.HandshakeTimeout = codexResponsesWebsocketHandshakeTO
	dialer.EnableCompression = true
	if ctx == nil {
		ctx = context.Background()
	}
	conn, resp, err := dialer.DialContext(ctx, wsURL, headers)
	if conn != nil {
		// Avoid gorilla/websocket flate tail validation issues on some upstreams/Go versions.
		// Negotiating permessage-deflate is fine; we just don't compress outbound messages.
		conn.EnableWriteCompression(false)
	}
	return conn, resp, err
}

func writeCodexWebsocketMessage(sess *codexWebsocketSession, conn *websocket.Conn, payload []byte) error {
	if sess != nil {
		return sess.writeMessage(conn, websocket.TextMessage, payload)
	}
	if conn == nil {
		return fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func writeCurrentCodexWebsocketMessage(ctx context.Context, auth *cliproxyauth.Auth, sess *codexWebsocketSession, conn *websocket.Conn, payload []byte) error {
	if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent != nil {
		return errCurrent
	}
	return writeCodexWebsocketMessage(sess, conn, payload)
}

func mapCodexWebsocketReadError(err error) error {
	if err == nil {
		return nil
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) && closeErr.Code == websocket.CloseMessageTooBig {
		return statusErr{code: http.StatusRequestEntityTooLarge, msg: `{"error":{"message":"upstream websocket message too big","type":"invalid_request_error","code":"message_too_big"}}`}
	}
	return err
}

func buildCodexWebsocketRequestBody(body []byte) []byte {
	if len(body) == 0 {
		return nil
	}

	// Match codex-rs websocket v2 semantics: every request is `response.create`.
	// Incremental follow-up turns continue on the same websocket using
	// `previous_response_id` + incremental `input`, not `response.append`.
	wsReqBody, errSet := sjson.SetBytes(bytes.Clone(body), "type", "response.create")
	if errSet == nil && len(wsReqBody) > 0 {
		return wsReqBody
	}
	fallback := bytes.Clone(body)
	fallback, _ = sjson.SetBytes(fallback, "type", "response.create")
	return fallback
}

func readCodexWebsocketMessage(ctx context.Context, sess *codexWebsocketSession, conn *websocket.Conn, readCh chan codexWebsocketRead) (int, []byte, error) {
	if sess == nil {
		if conn == nil {
			return 0, nil, fmt.Errorf("codex websockets executor: websocket conn is nil")
		}
		stopCancelClose := func() bool { return true }
		if ctx != nil {
			stopCancelClose = context.AfterFunc(ctx, func() {
				if errClose := conn.Close(); errClose != nil {
					log.Debugf("codex websockets executor: close canceled websocket error: %v", errClose)
				}
			})
		}
		defer stopCancelClose()
		_ = conn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
		msgType, payload, errRead := conn.ReadMessage()
		if errRead != nil && ctx != nil && ctx.Err() != nil {
			if cause := context.Cause(ctx); cause != nil {
				return 0, nil, cause
			}
		}
		return msgType, payload, errRead
	}
	if conn == nil {
		return 0, nil, fmt.Errorf("codex websockets executor: websocket conn is nil")
	}
	if readCh == nil {
		return 0, nil, fmt.Errorf("codex websockets executor: session read channel is nil")
	}
	for {
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case ev, ok := <-readCh:
			if !ok {
				return 0, nil, fmt.Errorf("codex websockets executor: session read channel closed")
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

func newProxyAwareWebsocketDialer(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) *websocket.Dialer {
	dialer := &websocket.Dialer{
		Proxy:             http.ProxyFromEnvironment,
		HandshakeTimeout:  codexResponsesWebsocketHandshakeTO,
		EnableCompression: true,
		NetDialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	proxyURL := effectiveWebsocketProxyURL(cfg, auth)
	if proxyURL == "" {
		return enableCodexWebsocketUTLS(dialer, ctx, cfg, auth, proxyURL)
	}

	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		log.Errorf("codex websockets executor: %v", errParse)
		dialer.Proxy = nil
		dialer.NetDialContext = func(context.Context, string, string) (net.Conn, error) {
			return nil, errParse
		}
		return enableCodexWebsocketUTLS(dialer, ctx, cfg, auth, proxyURL)
	}

	switch setting.Mode {
	case proxyutil.ModeDirect:
		dialer.Proxy = nil
		return enableCodexWebsocketUTLS(dialer, ctx, cfg, auth, proxyURL)
	case proxyutil.ModeProxy:
	default:
		return enableCodexWebsocketUTLS(dialer, ctx, cfg, auth, "")
	}

	proxyDialer, _, errBuild := proxyutil.BuildContextDialer(proxyURL)
	if errBuild != nil {
		log.Errorf("codex websockets executor: configure proxy dialer failed: %v", errBuild)
		dialer.Proxy = nil
		dialer.NetDialContext = func(context.Context, string, string) (net.Conn, error) {
			return nil, errBuild
		}
		return enableCodexWebsocketUTLS(dialer, ctx, cfg, auth, proxyURL)
	}
	dialer.Proxy = nil
	dialer.NetDialContext = proxyDialer.DialContext

	return enableCodexWebsocketUTLS(dialer, ctx, cfg, auth, proxyURL)
}

func effectiveWebsocketProxyURL(cfg *config.Config, auth *cliproxyauth.Auth) string {
	if auth != nil {
		if proxyURL := strings.TrimSpace(auth.EffectiveProxyURL()); proxyURL != "" {
			return proxyURL
		}
	}
	if cfg != nil {
		return strings.TrimSpace(cfg.ProxyURL)
	}
	return ""
}

func websocketProxyIdentity(cfg *config.Config, auth *cliproxyauth.Auth) string {
	proxyURL := effectiveWebsocketProxyURL(cfg, auth)
	if proxyURL == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(proxyURL))
	return fmt.Sprintf("%x", sum[:])
}

func enableCodexWebsocketUTLS(dialer *websocket.Dialer, ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, proxyURL string) *websocket.Dialer {
	if dialer == nil || !codexFingerprintJA3Enabled(cfg) {
		return dialer
	}
	dialer.Proxy = nil
	dialer.NetDialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		serverName := addr
		if host, _, errSplit := net.SplitHostPort(addr); errSplit == nil && strings.TrimSpace(host) != "" {
			serverName = host
		}
		conn, err := helps.DialCodexNativeTLSContext(ctx, network, addr, serverName, proxyURL)
		if err != nil {
			return nil, err
		}
		return helps.WrapCodexWebsocketHeaderOrder(conn), nil
	}
	return dialer
}

func buildCodexResponsesWebsocketURL(httpURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(httpURL))
	if err != nil {
		return "", err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	default:
		return "", fmt.Errorf("codex websockets executor: unsupported responses websocket URL scheme %q", parsed.Scheme)
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return "", fmt.Errorf("codex websockets executor: responses websocket URL host is empty")
	}
	return parsed.String(), nil
}

func applyCodexPromptCacheHeaders(from sdktranslator.Format, req cliproxyexecutor.Request, rawJSON []byte) ([]byte, http.Header) {
	headers := http.Header{}
	if len(rawJSON) == 0 {
		return rawJSON, headers
	}

	var cache helps.CodexCache
	if from == "claude" {
		userIDResult := gjson.GetBytes(req.Payload, "metadata.user_id")
		if userIDResult.Exists() {
			key := fmt.Sprintf("%s-%s", req.Model, userIDResult.String())
			if cached, ok := helps.GetCodexCache(key); ok {
				cache = cached
			} else {
				cache = helps.CodexCache{
					ID:     uuid.New().String(),
					Expire: time.Now().Add(1 * time.Hour),
				}
				helps.SetCodexCache(key, cache)
			}
		}
	} else if from == "openai-response" {
		if promptCacheKey := gjson.GetBytes(req.Payload, "prompt_cache_key"); promptCacheKey.Exists() {
			cache.ID = promptCacheKey.String()
		}
	}

	if cache.ID != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "prompt_cache_key", cache.ID)
		setHeaderCasePreserved(headers, "session_id", cache.ID)
		headers.Set("Conversation_id", cache.ID)
	}

	return rawJSON, headers
}

func applyCodexWebsocketHeaders(ctx context.Context, headers http.Header, auth *cliproxyauth.Auth, token string, cfg *config.Config) http.Header {
	prepared, _ := prepareCodexWebsocketHeadersForURL(ctx, headers, auth, token, cfg, nil)
	return prepared
}

func applyCodexWebsocketHeadersForURL(ctx context.Context, headers http.Header, auth *cliproxyauth.Auth, token string, cfg *config.Config, target *url.URL) http.Header {
	prepared, _ := prepareCodexWebsocketHeadersForURL(ctx, headers, auth, token, cfg, target)
	return prepared
}

func prepareCodexWebsocketHeadersForURL(ctx context.Context, headers http.Header, auth *cliproxyauth.Auth, token string, cfg *config.Config, target *url.URL) (http.Header, error) {
	if headers == nil {
		headers = http.Header{}
	}

	var ginHeaders http.Header
	if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
		ginHeaders = ginCtx.Request.Header.Clone()
	}

	isAPIKey := codexAuthUsesAPIKey(auth)
	cfgUserAgent, cfgOriginator, cfgBetaFeatures := codexHeaderDefaults(cfg, auth)
	ensureHeaderWithPriority(headers, ginHeaders, "x-codex-beta-features", cfgBetaFeatures, "")
	misc.EnsureHeader(headers, ginHeaders, "x-codex-turn-state", "")
	misc.EnsureHeader(headers, ginHeaders, "x-codex-turn-metadata", "")
	misc.EnsureHeader(headers, ginHeaders, "x-client-request-id", "")
	misc.EnsureHeader(headers, ginHeaders, "x-responsesapi-include-timing-metrics", "")
	misc.EnsureHeader(headers, ginHeaders, "Version", "")
	if isAPIKey {
		ensureHeaderWithPriority(headers, ginHeaders, "User-Agent", "", "")
	} else {
		ensureHeaderWithConfigPrecedence(headers, ginHeaders, "User-Agent", cfgUserAgent, codexUserAgent)
	}

	betaHeader := strings.TrimSpace(headers.Get("OpenAI-Beta"))
	if betaHeader == "" && ginHeaders != nil {
		betaHeader = strings.TrimSpace(ginHeaders.Get("OpenAI-Beta"))
	}
	if betaHeader == "" || !strings.Contains(betaHeader, "responses_websockets=") {
		betaHeader = codexResponsesWebsocketBetaHeaderValue
	}
	headers.Set("OpenAI-Beta", betaHeader)
	if strings.Contains(headers.Get("User-Agent"), "Mac OS") {
		ensureHeaderCasePreserved(headers, ginHeaders, "session_id", "", uuid.NewString())
	}
	ensureHeaderCasePreserved(headers, ginHeaders, "session_id", "", "")
	if originator := strings.TrimSpace(ginHeaders.Get("Originator")); originator != "" {
		headers.Set("Originator", originator)
	} else if cfgOriginator != "" {
		headers.Set("Originator", cfgOriginator)
	} else if !isAPIKey {
		headers.Set("Originator", codexOriginator)
	}
	if !isAPIKey {
		metadata := authMetadata(auth)
		if accountID := codexauth.EffectiveRequestAccountID(metadata); accountID != "" {
			setHeaderCasePreserved(headers, "ChatGPT-Account-ID", accountID)
		}
		if codexauth.ChatGPTAccountIsFedRAMP(metadata) {
			headers.Set("X-OpenAI-Fedramp", "true")
		}
	}

	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(&http.Request{Header: headers}, attrs)
	authorization, err := codexauth.AuthorizationHeader(authMetadata(auth), token, time.Now())
	if err != nil {
		return nil, fmt.Errorf("codex websockets executor: build authorization: %w", err)
	}
	if authorization != "" {
		headers.Set("Authorization", authorization)
	}

	return headers, nil
}

func codexAuthUsesAPIKey(auth *cliproxyauth.Auth) bool {
	if auth == nil || auth.Attributes == nil {
		return false
	}
	return strings.TrimSpace(auth.Attributes["api_key"]) != ""
}

func ensureHeaderCasePreserved(target http.Header, source http.Header, key, configValue, fallbackValue string) {
	if target == nil {
		return
	}
	if strings.TrimSpace(headerValueCaseInsensitive(target, key)) != "" {
		return
	}
	if source != nil {
		if val := strings.TrimSpace(headerValueCaseInsensitive(source, key)); val != "" {
			setHeaderCasePreserved(target, key, val)
			return
		}
	}
	if val := strings.TrimSpace(configValue); val != "" {
		setHeaderCasePreserved(target, key, val)
		return
	}
	if val := strings.TrimSpace(fallbackValue); val != "" {
		setHeaderCasePreserved(target, key, val)
	}
}

func setHeaderCasePreserved(headers http.Header, key string, value string) {
	if headers == nil {
		return
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return
	}
	deleteHeaderCaseInsensitive(headers, key)
	headers[key] = []string{value}
}

func headerValueCaseInsensitive(headers http.Header, key string) string {
	key = strings.TrimSpace(key)
	if headers == nil || key == "" {
		return ""
	}
	if val := strings.TrimSpace(headers.Get(key)); val != "" {
		return val
	}
	for existingKey, values := range headers {
		if !strings.EqualFold(existingKey, key) {
			continue
		}
		for _, value := range values {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func deleteHeaderCaseInsensitive(headers http.Header, key string) {
	for existingKey := range headers {
		if strings.EqualFold(existingKey, key) {
			delete(headers, existingKey)
		}
	}
}

func codexHeaderDefaults(cfg *config.Config, auth *cliproxyauth.Auth) (string, string, string) {
	if cfg == nil || auth == nil {
		return "", "", ""
	}
	if auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes["api_key"]); v != "" {
			return "", "", ""
		}
	}
	return strings.TrimSpace(cfg.CodexHeaderDefaults.UserAgent),
		strings.TrimSpace(cfg.CodexHeaderDefaults.Originator),
		strings.TrimSpace(cfg.CodexHeaderDefaults.BetaFeatures)
}

func ensureHeaderWithPriority(target http.Header, source http.Header, key, configValue, fallbackValue string) {
	if target == nil {
		return
	}
	if strings.TrimSpace(target.Get(key)) != "" {
		return
	}
	if source != nil {
		if val := strings.TrimSpace(source.Get(key)); val != "" {
			target.Set(key, val)
			return
		}
	}
	if val := strings.TrimSpace(configValue); val != "" {
		target.Set(key, val)
		return
	}
	if val := strings.TrimSpace(fallbackValue); val != "" {
		target.Set(key, val)
	}
}

func ensureHeaderWithConfigPrecedence(target http.Header, source http.Header, key, configValue, fallbackValue string) {
	if target == nil {
		return
	}
	if strings.TrimSpace(target.Get(key)) != "" {
		return
	}
	if val := strings.TrimSpace(configValue); val != "" {
		target.Set(key, val)
		return
	}
	if source != nil {
		if val := strings.TrimSpace(source.Get(key)); val != "" {
			target.Set(key, val)
			return
		}
	}
	if val := strings.TrimSpace(fallbackValue); val != "" {
		target.Set(key, val)
	}
}

type statusErrWithHeaders struct {
	statusErr
	headers http.Header
}

func codexWebsocketErrorWithSanitizedMessage(original error, payload []byte) error {
	switch typed := original.(type) {
	case statusErrWithHeaders:
		typed.statusErr.msg = string(payload)
		return typed
	case statusErr:
		typed.msg = string(payload)
		return typed
	default:
		return fmt.Errorf("codex websocket upstream error: %s", payload)
	}
}

func newCodexWebsocketStatusErr(status int, body []byte) statusErr {
	return statusErr{
		code:           status,
		msg:            string(body),
		skipAuthResult: isCodexContextTooLargeRequestError(status, body),
	}
}

func newCodexWebsocketHandshakeStatusErr(status int, body []byte, headers http.Header) statusErrWithHeaders {
	statusError := newCodexWebsocketStatusErr(status, body)
	if retryAfter := parseCodexRetryAfter(status, body, time.Now()); retryAfter != nil {
		statusError.retryAfter = retryAfter
	} else if status == http.StatusTooManyRequests {
		statusError.retryAfter = parseXAIRetryAfterHeader(headers.Get("Retry-After"), time.Now())
	}
	return statusErrWithHeaders{statusErr: statusError, headers: headers.Clone()}
}

func (e statusErrWithHeaders) Headers() http.Header {
	if e.headers == nil {
		return nil
	}
	return e.headers.Clone()
}

func parseCodexWebsocketError(payload []byte) (error, bool) {
	if len(payload) == 0 {
		return nil, false
	}
	if strings.TrimSpace(gjson.GetBytes(payload, "type").String()) != "error" {
		return nil, false
	}
	status := int(gjson.GetBytes(payload, "status").Int())
	if status == 0 {
		status = int(gjson.GetBytes(payload, "status_code").Int())
	}
	if status <= 0 {
		return nil, false
	}

	out := buildCodexWebsocketErrorPayload(payload, status)
	headers := parseCodexWebsocketErrorHeaders(payload)
	statusError := newCodexWebsocketStatusErr(status, out)
	if retryAfter := parseCodexRetryAfter(status, out, time.Now()); retryAfter != nil {
		statusError.retryAfter = retryAfter
	} else if isCodexWebsocketConnectionLimitError(payload) {
		retryAfter := time.Duration(0)
		statusError.retryAfter = &retryAfter
	}
	return statusErrWithHeaders{
		statusErr: statusError,
		headers:   headers,
	}, true
}

func buildCodexWebsocketErrorPayload(payload []byte, status int) []byte {
	out := []byte(`{}`)
	out, _ = sjson.SetBytes(out, "status", status)

	if bodyNode := gjson.GetBytes(payload, "body"); bodyNode.Exists() {
		out, _ = sjson.SetRawBytes(out, "body", []byte(bodyNode.Raw))
		if bodyErrorNode := bodyNode.Get("error"); bodyErrorNode.Exists() {
			out, _ = sjson.SetRawBytes(out, "error", []byte(bodyErrorNode.Raw))
			return out
		}
	}

	if errNode := gjson.GetBytes(payload, "error"); errNode.Exists() {
		out, _ = sjson.SetRawBytes(out, "error", []byte(errNode.Raw))
		return out
	}

	out, _ = sjson.SetBytes(out, "error.type", "server_error")
	out, _ = sjson.SetBytes(out, "error.message", http.StatusText(status))
	return out
}

func isCodexWebsocketConnectionLimitError(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	for _, path := range []string{"error.code", "error.type", "body.error.code", "body.error.type", "code", "error"} {
		if strings.TrimSpace(gjson.GetBytes(payload, path).String()) == "websocket_connection_limit_reached" {
			return true
		}
	}
	return false
}

func parseCodexWebsocketErrorHeaders(payload []byte) http.Header {
	headersNode := gjson.GetBytes(payload, "headers")
	if !headersNode.Exists() || !headersNode.IsObject() {
		return nil
	}
	mapped := make(http.Header)
	headersNode.ForEach(func(key, value gjson.Result) bool {
		name := strings.TrimSpace(key.String())
		if name == "" {
			return true
		}
		switch value.Type {
		case gjson.String:
			if v := strings.TrimSpace(value.String()); v != "" {
				mapped.Set(name, v)
			}
		case gjson.Number, gjson.True, gjson.False:
			if v := strings.TrimSpace(value.Raw); v != "" {
				mapped.Set(name, v)
			}
		default:
		}
		return true
	})
	if len(mapped) == 0 {
		return nil
	}
	return mapped
}

func normalizeCodexWebsocketCompletion(payload []byte) []byte {
	return normalizeCodexCompletion(payload)
}

func encodeCodexWebsocketAsSSE(payload []byte) []byte {
	if len(payload) == 0 {
		return nil
	}
	line := make([]byte, 0, len("data: ")+len(payload))
	line = append(line, []byte("data: ")...)
	line = append(line, payload...)
	return line
}

func websocketUpgradeRequestLog(info helps.UpstreamRequestLog) helps.UpstreamRequestLog {
	upgradeInfo := info
	upgradeInfo.URL = helps.WebsocketUpgradeRequestURL(info.URL)
	upgradeInfo.Method = http.MethodGet
	upgradeInfo.Body = nil
	upgradeInfo.Headers = info.Headers.Clone()
	if upgradeInfo.Headers == nil {
		upgradeInfo.Headers = make(http.Header)
	}
	if strings.TrimSpace(upgradeInfo.Headers.Get("Connection")) == "" {
		upgradeInfo.Headers.Set("Connection", "Upgrade")
	}
	if strings.TrimSpace(upgradeInfo.Headers.Get("Upgrade")) == "" {
		upgradeInfo.Headers.Set("Upgrade", "websocket")
	}
	return upgradeInfo
}

func recordAPIWebsocketHandshake(ctx context.Context, cfg *config.Config, resp *http.Response) {
	if resp == nil {
		return
	}
	helps.RecordAPIWebsocketHandshake(ctx, cfg, resp.StatusCode, resp.Header.Clone())
	closeHTTPResponseBody(resp, "codex websockets executor: close handshake response body error")
}

func websocketHandshakeBody(resp *http.Response) []byte {
	if resp == nil || resp.Body == nil {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	closeHTTPResponseBody(resp, "codex websockets executor: close handshake response body error")
	if len(body) == 0 {
		return nil
	}
	return body
}

func closeHTTPResponseBody(resp *http.Response, logPrefix string) {
	if resp == nil || resp.Body == nil {
		return
	}
	if errClose := resp.Body.Close(); errClose != nil {
		log.Errorf("%s: %v", logPrefix, errClose)
	}
}

func executionSessionIDFromOptions(opts cliproxyexecutor.Options) string {
	if len(opts.Metadata) == 0 {
		return ""
	}
	raw, ok := opts.Metadata[cliproxyexecutor.ExecutionSessionMetadataKey]
	if !ok || raw == nil {
		return ""
	}
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	default:
		return ""
	}
}

func (e *CodexWebsocketsExecutor) getOrCreateSession(sessionID string) *codexWebsocketSession {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	if e == nil {
		return nil
	}
	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
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

func (e *CodexWebsocketsExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	sess := e.getOrCreateSession(sessionID)
	if sess == nil {
		return nil
	}
	return sess.upstreamDisconnectCh
}

func (e *CodexWebsocketsExecutor) ensureUpstreamConn(ctx context.Context, auth *cliproxyauth.Auth, sess *codexWebsocketSession, authID string, wsURL string, headers http.Header) (*websocket.Conn, *http.Response, error) {
	if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent != nil {
		return nil, nil, errCurrent
	}
	if sess == nil {
		conn, resp, errDial := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
		if errDial != nil {
			if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent != nil {
				return nil, resp, errCurrent
			}
			return nil, resp, errDial
		}
		if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent == nil {
			return conn, resp, nil
		} else {
			if conn != nil {
				if errClose := conn.Close(); errClose != nil {
					log.Errorf("codex websockets executor: close websocket error: %v", errClose)
				}
			}
			closeHTTPResponseBody(resp, "codex websockets executor: close terminated handshake response body error")
			return nil, nil, errCurrent
		}
	}

	sess.connMu.Lock()
	if sess.terminated {
		sess.connMu.Unlock()
		return nil, nil, errCodexWebsocketSessionTerminated
	}
	if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent != nil {
		sess.connMu.Unlock()
		return nil, nil, errCurrent
	}
	conn := sess.conn
	readerConn := sess.readerConn
	currentAuthID := strings.TrimSpace(sess.authID)
	currentAuthInstanceID := strings.TrimSpace(sess.authInstanceID)
	currentProxyBindingID := strings.TrimSpace(sess.proxyBindingID)
	currentProxyIdentity := strings.TrimSpace(sess.proxyIdentity)
	currentWSURL := strings.TrimSpace(sess.wsURL)
	sess.connMu.Unlock()
	requestedAuthID := strings.TrimSpace(authID)
	requestedAuthInstanceID := auth.RuntimeInstanceID()
	requestedProxyBindingID := auth.EffectiveProxyBindingID()
	requestedProxyIdentity := websocketProxyIdentity(e.cfg, auth)
	requestedWSURL := strings.TrimSpace(wsURL)
	if conn != nil && (currentAuthID != requestedAuthID || currentAuthInstanceID != requestedAuthInstanceID || currentProxyBindingID != requestedProxyBindingID || currentProxyIdentity != requestedProxyIdentity || currentWSURL != requestedWSURL) {
		e.invalidateUpstreamConnWithoutDisconnectNotify(sess, conn, "auth_changed", nil)
		conn = nil
		readerConn = nil
	}
	if conn != nil {
		if readerConn != conn {
			sess.connMu.Lock()
			sess.readerConn = conn
			sess.connMu.Unlock()
			sess.configureConn(conn)
			go e.readUpstreamLoop(sess, conn)
		}
		return conn, nil, nil
	}

	sess.connMu.Lock()
	if sess.terminated {
		sess.connMu.Unlock()
		return nil, nil, errCodexWebsocketSessionTerminated
	}
	if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent != nil {
		sess.connMu.Unlock()
		return nil, nil, errCurrent
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
	sess.pendingProxyBindingID = requestedProxyBindingID
	sess.pendingProxyIdentity = requestedProxyIdentity
	sess.connMu.Unlock()

	conn, resp, errDial := e.dialCodexWebsocket(ctx, auth, wsURL, headers)
	if errDial != nil {
		clearCodexPendingWebsocketDial(sess, dialGeneration)
		if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent != nil {
			return nil, resp, errCurrent
		}
		return nil, resp, errDial
	}

	sess.connMu.Lock()
	if sess.terminated || sess.dialGeneration != dialGeneration || sess.pendingAuthID != requestedAuthID || sess.pendingAuthInstanceID != requestedAuthInstanceID || sess.pendingProxyBindingID != requestedProxyBindingID || sess.pendingProxyIdentity != requestedProxyIdentity {
		sess.connMu.Unlock()
		if errClose := conn.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
		closeHTTPResponseBody(resp, "codex websockets executor: close terminated handshake response body error")
		return nil, nil, errCodexWebsocketSessionTerminated
	}
	if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent != nil {
		sess.pendingAuthID = ""
		sess.pendingAuthInstanceID = ""
		sess.pendingProxyBindingID = ""
		sess.pendingProxyIdentity = ""
		sess.connMu.Unlock()
		if errClose := conn.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
		closeHTTPResponseBody(resp, "codex websockets executor: close retired handshake response body error")
		return nil, nil, errCurrent
	}
	if sess.conn != nil {
		previous := sess.conn
		sess.pendingAuthID = ""
		sess.pendingAuthInstanceID = ""
		sess.pendingProxyBindingID = ""
		sess.pendingProxyIdentity = ""
		sess.connMu.Unlock()
		if errClose := conn.Close(); errClose != nil {
			log.Errorf("codex websockets executor: close websocket error: %v", errClose)
		}
		return previous, nil, nil
	}
	sess.conn = conn
	sess.wsURL = wsURL
	sess.authID = authID
	sess.authInstanceID = auth.RuntimeInstanceID()
	sess.proxyBindingID = requestedProxyBindingID
	sess.proxyIdentity = requestedProxyIdentity
	sess.pendingAuthID = ""
	sess.pendingAuthInstanceID = ""
	sess.pendingProxyBindingID = ""
	sess.pendingProxyIdentity = ""
	sess.readerConn = conn
	sess.connMu.Unlock()

	sess.configureConn(conn)
	go e.readUpstreamLoop(sess, conn)
	logCodexWebsocketConnected(sess.sessionID, authID, wsURL)
	return conn, resp, nil
}

func codexWebsocketExecutionStateError(ctx context.Context, auth *cliproxyauth.Auth) error {
	if auth != nil && auth.RuntimeInstanceRetired() {
		return errCodexWebsocketSessionTerminated
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func clearCodexPendingWebsocketDial(sess *codexWebsocketSession, generation uint64) {
	if sess == nil || generation == 0 {
		return
	}
	sess.connMu.Lock()
	if sess.dialGeneration == generation {
		sess.pendingAuthID = ""
		sess.pendingAuthInstanceID = ""
		sess.pendingProxyBindingID = ""
		sess.pendingProxyIdentity = ""
	}
	sess.connMu.Unlock()
}

func (e *CodexWebsocketsExecutor) readUpstreamLoop(sess *codexWebsocketSession, conn *websocket.Conn) {
	if e == nil || sess == nil || conn == nil {
		return
	}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(codexResponsesWebsocketIdleTimeout))
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
				errBinary := fmt.Errorf("codex websockets executor: unexpected binary message")
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

func (e *CodexWebsocketsExecutor) invalidateUpstreamConn(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error) {
	e.invalidateUpstreamConnWithNotify(sess, conn, reason, err, true)
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConnWithoutDisconnectNotify(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error) {
	e.invalidateUpstreamConnWithNotify(sess, conn, reason, err, false)
}

func (e *CodexWebsocketsExecutor) invalidateUpstreamConnWithNotify(sess *codexWebsocketSession, conn *websocket.Conn, reason string, err error, notify bool) {
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
	sess.authID = ""
	sess.authInstanceID = ""
	sess.proxyBindingID = ""
	sess.proxyIdentity = ""
	sess.wsURL = ""
	sess.connMu.Unlock()

	logCodexWebsocketDisconnected(sessionID, authID, wsURL, reason, err)
	if notify {
		sess.notifyUpstreamDisconnect(err)
	}
	if errClose := conn.Close(); errClose != nil {
		log.Errorf("codex websockets executor: close websocket error: %v", errClose)
	}
}

func (e *CodexWebsocketsExecutor) CloseExecutionSession(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if e == nil {
		return
	}
	if sessionID == "" {
		return
	}
	if sessionID == cliproxyauth.CloseAllExecutionSessionsID {
		// Executor replacement can happen during hot reload (config/credential changes).
		// Do not force-close upstream websocket sessions here, otherwise in-flight
		// downstream websocket requests get interrupted.
		return
	}

	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	sess := store.sessions[sessionID]
	delete(store.sessions, sessionID)
	store.mu.Unlock()

	e.closeExecutionSession(sess, "session_closed")
}

// CloseAuthExecutionSessions closes all websocket sessions using an auth ID.
func (e *CodexWebsocketsExecutor) CloseAuthExecutionSessions(authID string, reason string) {
	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	closeCodexWebsocketSessionsForAuth(store, authID, "", reason)
}

// CloseAuthInstanceExecutionSessions closes websocket sessions using one runtime auth instance.
func (e *CodexWebsocketsExecutor) CloseAuthInstanceExecutionSessions(authID string, authInstanceID string, reason string) {
	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	closeCodexWebsocketSessionsForAuth(store, authID, authInstanceID, reason)
}

func (e *CodexWebsocketsExecutor) closeAllExecutionSessions(reason string) {
	if e == nil {
		return
	}

	store := e.store
	if store == nil {
		store = globalCodexWebsocketSessionStore
	}
	store.mu.Lock()
	sessions := make([]*codexWebsocketSession, 0, len(store.sessions))
	for sessionID, sess := range store.sessions {
		delete(store.sessions, sessionID)
		if sess != nil {
			sessions = append(sessions, sess)
		}
	}
	store.mu.Unlock()

	for i := range sessions {
		e.closeExecutionSession(sessions[i], reason)
	}
}

func (e *CodexWebsocketsExecutor) closeExecutionSession(sess *codexWebsocketSession, reason string) {
	closeCodexWebsocketSession(sess, reason)
}

func closeCodexWebsocketSession(sess *codexWebsocketSession, reason string) {
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
	sess.pendingProxyBindingID = ""
	sess.pendingProxyIdentity = ""
	if sess.readerConn == conn {
		sess.readerConn = nil
	}
	sessionID := sess.sessionID
	sess.connMu.Unlock()

	if conn == nil {
		return
	}
	logCodexWebsocketDisconnected(sessionID, authID, wsURL, reason, nil)
	if errClose := conn.Close(); errClose != nil {
		log.Errorf("codex websockets executor: close websocket error: %v", errClose)
	}
}

func logCodexWebsocketConnected(sessionID string, authID string, wsURL string) {
	log.Infof("codex websockets: upstream connected session=%s auth=%s url=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL))
}

func logCodexWebsocketDisconnected(sessionID string, authID string, wsURL string, reason string, err error) {
	if err != nil {
		log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s reason=%s err=%v", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL), strings.TrimSpace(reason), err)
		return
	}
	log.Infof("codex websockets: upstream disconnected session=%s auth=%s url=%s reason=%s", strings.TrimSpace(sessionID), strings.TrimSpace(authID), strings.TrimSpace(wsURL), strings.TrimSpace(reason))
}

// CloseCodexWebsocketSessionsForAuthID closes all active Codex upstream websocket sessions
// associated with the supplied auth ID.
func CloseCodexWebsocketSessionsForAuthID(authID string, reason string) {
	closeCodexWebsocketSessionsForAuth(globalCodexWebsocketSessionStore, authID, "", reason)
}

func closeCodexWebsocketSessionsForAuth(store *codexWebsocketSessionStore, authID string, authInstanceID string, reason string) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	authInstanceID = strings.TrimSpace(authInstanceID)
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

	toClose := make([]*codexWebsocketSession, 0)
	for i := range items {
		store.mu.Lock()
		current, ok := store.sessions[items[i].sessionID]
		if !ok || current == nil || current != items[i].sess {
			store.mu.Unlock()
			continue
		}
		current.connMu.Lock()
		activeMatch := strings.TrimSpace(current.authID) == authID && (authInstanceID == "" || strings.TrimSpace(current.authInstanceID) == authInstanceID)
		pendingMatch := strings.TrimSpace(current.pendingAuthID) == authID && (authInstanceID == "" || strings.TrimSpace(current.pendingAuthInstanceID) == authInstanceID)
		if !activeMatch && !pendingMatch {
			current.connMu.Unlock()
			store.mu.Unlock()
			continue
		}
		current.terminated = true
		current.dialGeneration++
		current.pendingAuthID = ""
		current.pendingAuthInstanceID = ""
		current.pendingProxyBindingID = ""
		current.pendingProxyIdentity = ""
		delete(store.sessions, items[i].sessionID)
		current.connMu.Unlock()
		store.mu.Unlock()
		toClose = append(toClose, current)
	}

	for i := range toClose {
		closeCodexWebsocketSession(toClose[i], reason)
	}
}

// CodexAutoExecutor routes Codex requests to the websocket transport only when:
//  1. The downstream transport is websocket, and
//  2. The selected auth enables websockets.
//
// For non-websocket downstream requests, it always uses the legacy HTTP implementation.
type CodexAutoExecutor struct {
	httpExec *CodexExecutor
	wsExec   *CodexWebsocketsExecutor
}

func NewCodexAutoExecutor(cfg *config.Config) *CodexAutoExecutor {
	return &CodexAutoExecutor{
		httpExec: NewCodexExecutor(cfg),
		wsExec:   NewCodexWebsocketsExecutor(cfg),
	}
}

func (e *CodexAutoExecutor) Identifier() string { return "codex" }

func (e *CodexAutoExecutor) ShouldPrepareRequestAuth(auth *cliproxyauth.Auth) bool {
	return e != nil && e.httpExec != nil && e.httpExec.ShouldPrepareRequestAuth(auth)
}

func (e *CodexAutoExecutor) PrepareRequestAuth(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if e == nil || e.httpExec == nil {
		return nil, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.PrepareRequestAuth(ctx, auth)
}

func (e *CodexAutoExecutor) ShouldRecoverUnauthorized(auth *cliproxyauth.Auth, err error) bool {
	return e != nil && e.httpExec != nil && e.httpExec.ShouldRecoverUnauthorized(auth, err)
}

func (e *CodexAutoExecutor) RecoverUnauthorized(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if e == nil || e.httpExec == nil {
		return nil, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.RecoverUnauthorized(ctx, auth)
}

func (e *CodexAutoExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if e == nil || e.httpExec == nil {
		return nil
	}
	return e.httpExec.PrepareRequest(req, auth)
}

func (e *CodexAutoExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if e == nil || e.httpExec == nil {
		return nil, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.HttpRequest(ctx, auth, req)
}

func (e *CodexAutoExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e == nil || e.httpExec == nil || e.wsExec == nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("codex auto executor: executor is nil")
	}
	if cliproxyexecutor.DownstreamWebsocket(ctx) && codexWebsocketsEnabled(auth) {
		return e.wsExec.Execute(ctx, auth, req, opts)
	}
	return e.httpExec.Execute(ctx, auth, req, opts)
}

func (e *CodexAutoExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if e == nil || e.httpExec == nil || e.wsExec == nil {
		return nil, fmt.Errorf("codex auto executor: executor is nil")
	}
	if cliproxyexecutor.DownstreamWebsocket(ctx) && codexWebsocketsEnabled(auth) {
		return e.wsExec.ExecuteStream(ctx, auth, req, opts)
	}
	return e.httpExec.ExecuteStream(ctx, auth, req, opts)
}

func (e *CodexAutoExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if e == nil || e.httpExec == nil {
		return nil, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.Refresh(ctx, auth)
}

func (e *CodexAutoExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e == nil || e.httpExec == nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("codex auto executor: http executor is nil")
	}
	return e.httpExec.CountTokens(ctx, auth, req, opts)
}

func (e *CodexAutoExecutor) CloseExecutionSession(sessionID string) {
	if e == nil || e.wsExec == nil {
		return
	}
	e.wsExec.CloseExecutionSession(sessionID)
}

func (e *CodexAutoExecutor) CloseAuthExecutionSessions(authID string, reason string) {
	if e == nil || e.wsExec == nil {
		return
	}
	e.wsExec.CloseAuthExecutionSessions(authID, reason)
}

func (e *CodexAutoExecutor) CloseAuthInstanceExecutionSessions(authID string, authInstanceID string, reason string) {
	if e == nil || e.wsExec == nil {
		return
	}
	e.wsExec.CloseAuthInstanceExecutionSessions(authID, authInstanceID, reason)
}

func (e *CodexAutoExecutor) UpstreamDisconnectChan(sessionID string) <-chan error {
	if e == nil || e.wsExec == nil {
		return nil
	}
	return e.wsExec.UpstreamDisconnectChan(sessionID)
}

func codexWebsocketsEnabled(auth *cliproxyauth.Auth) bool {
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
