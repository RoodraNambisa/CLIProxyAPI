package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	wsRequestTypeCreate   = "response.create"
	wsRequestTypeAppend   = "response.append"
	wsEventTypeError      = "error"
	wsEventTypeCompleted  = "response.completed"
	wsEventTypeDone       = "response.done"
	wsEventTypeFailed     = "response.failed"
	wsEventTypeIncomplete = "response.incomplete"
	wsDoneMarker          = "[DONE]"
	wsTurnStateHeader     = "x-codex-turn-state"
	wsTimelineBodyKey     = "WEBSOCKET_TIMELINE_OVERRIDE"
)

var responsesWebsocketUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// ResponsesWebsocket handles websocket requests for /v1/responses.
// It accepts `response.create` and `response.append` requests and streams
// response events back as JSON websocket text messages.
func (h *OpenAIResponsesAPIHandler) ResponsesWebsocket(c *gin.Context) {
	conn, err := responsesWebsocketUpgrader.Upgrade(c.Writer, c.Request, websocketUpgradeHeaders(c.Request))
	if err != nil {
		return
	}
	writer := newResponsesWebsocketWriter(conn)
	readCtx, stopReading := context.WithCancelCause(c.Request.Context())
	c.Request = c.Request.WithContext(readCtx)
	stopCloseOnCancel := context.AfterFunc(readCtx, func() { _ = writer.Close() })
	defer stopCloseOnCancel()
	incoming, readerDone := readResponsesWebsocketRequests(readCtx, stopReading, conn)
	defer func() { stopReading(context.Canceled); _ = writer.Close(); <-readerDone }()
	passthroughSessionID := uuid.NewString()
	downstreamSessionKey := websocketToolPairScopeKey(c, websocketDownstreamSessionKey(c.Request))
	toolPairState := acquireResponsesWebsocketToolPairState(downstreamSessionKey)
	clientIP := websocketClientAddress(c)
	log.Infof("responses websocket: client connected id=%s remote=%s", executorhelps.CodexWebsocketSessionLogID(passthroughSessionID), clientIP)

	wsDone := make(chan struct{})
	var disconnectHandler atomic.Pointer[handlers.BaseAPIHandler]
	disconnectHandler.Store(handlers.NewBaseAPIHandlers(h.ConfigSnapshot(), h.AuthManager))
	disconnectCause := make(chan error, 1)
	var disconnectObserverDone chan struct{}
	defer func() {
		close(wsDone)
		if disconnectObserverDone != nil {
			<-disconnectObserverDone
		}
	}()

	if h != nil && h.AuthManager != nil {
		if exec, ok := h.AuthManager.Executor("codex"); ok && exec != nil {
			type upstreamDisconnectSubscriber interface {
				UpstreamDisconnectChan(sessionID string) <-chan error
			}
			if subscriber, ok := exec.(upstreamDisconnectSubscriber); ok && subscriber != nil {
				disconnectCh := subscriber.UpstreamDisconnectChan(passthroughSessionID)
				if disconnectCh != nil {
					disconnectObserverDone = make(chan struct{})
					go func() {
						defer close(disconnectObserverDone)
						select {
						case <-wsDone:
							return
						case disconnectErr := <-disconnectCh:
							disconnectCause <- disconnectErr
							writer.closeForUpstreamDisconnect(disconnectErr, func(err error) ([]byte, error) {
								base := disconnectHandler.Load()
								projected := base.RewriteExecutionErrorResponseForGin(c, handlers.ExecutionErrorMessage(err))
								projected = base.ProjectChatGPTWebImageErrorResponse(c, projected)
								return buildResponsesWebsocketErrorPayload(projected)
							})
						}
					}()
				}
			}
		}
	}

	var wsTerminateErr error
	var wsTimelineLog strings.Builder
	defer func() {
		releaseResponsesWebsocketToolPairState(downstreamSessionKey)
		select {
		case upstreamErr := <-disconnectCause:
			if upstreamErr != nil {
				wsTerminateErr = upstreamErr
			}
		default:
		}
		if wsTerminateErr != nil {
			appendWebsocketTimelineDisconnect(&wsTimelineLog, wsTerminateErr, time.Now(), util.PromptCacheLogForGin(c))
			// log.Infof("responses websocket: session closing id=%s reason=%v", passthroughSessionID, wsTerminateErr)
		} else {
			log.Infof("responses websocket: session closing id=%s", executorhelps.CodexWebsocketSessionLogID(passthroughSessionID))
		}
		if h != nil && h.AuthManager != nil {
			h.AuthManager.CloseExecutionSession(passthroughSessionID)
			log.Infof("responses websocket: upstream execution session closed id=%s", executorhelps.CodexWebsocketSessionLogID(passthroughSessionID))
		}
		setWebsocketTimelineBody(c, wsTimelineLog.String())
		if errClose := writer.Close(); errClose != nil {
			log.Warnf("responses websocket: close connection error: %v", executorhelps.CodexWebsocketLogError(errClose, util.PromptCacheLogForGin(c)))
		}
	}()

	var lastRequest []byte
	lastResponseOutput := []byte("[]")
	lastResponseID := ""
	pinnedAuthID := ""
	lastAttemptedAuthID := ""

	for {
		var message responsesWebsocketRequestMessage
		select {
		case <-readCtx.Done():
		case message = <-incoming:
		}
		if errReadMessage := context.Cause(readCtx); errReadMessage != nil {
			wsTerminateErr = errReadMessage
			if websocket.IsCloseError(errReadMessage, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseNoStatusReceived) {
				log.Infof("responses websocket: client disconnected id=%s error=%v", executorhelps.CodexWebsocketSessionLogID(passthroughSessionID), executorhelps.CodexWebsocketLogError(errReadMessage, util.PromptCacheLogForGin(c)))
			} else {
				// log.Warnf("responses websocket: read message failed id=%s error=%v", passthroughSessionID, errReadMessage)
			}
			return
		}
		msgType, payload := message.kind, message.payload
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}
		// Freeze handler policy once for this logical turn, including a potentially
		// slow credential/bootstrap phase before the first upstream output.
		h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(h.ConfigSnapshot(), h.AuthManager))
		writer.beginTurn()
		disconnectHandler.Store(h.BaseAPIHandler)
		util.RegisterPromptCacheLogPolicy(c, payload)
		h.BeginChatGPTWebImageErrorSanitization(c, false)
		// log.Infof(
		// 	"responses websocket: downstream_in id=%s type=%d event=%s payload=%s",
		// 	passthroughSessionID,
		// 	msgType,
		// 	websocketPayloadEventType(payload),
		// 	websocketPayloadPreview(payload),
		// )
		appendWebsocketTimelineEvent(&wsTimelineLog, "request", payload, time.Now(), util.PromptCacheLogForGin(c))

		allowIncrementalInputWithPreviousResponseID := false
		requestPinnedAuthID := pinnedAuthID
		if requestPinnedAuthID != "" && h != nil && h.AuthManager != nil {
			modelName := strings.TrimSpace(gjson.GetBytes(payload, "model").String())
			if modelName == "" {
				modelName = strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
			}
			modelName = util.ResolveAutoModel(thinking.ParseSuffix(modelName).ModelName)
			if pinnedAuth, ok := h.AuthManager.GetByID(requestPinnedAuthID); ok && h.AuthManager.AuthSupportsRouteModel(pinnedAuth, modelName) {
				allowIncrementalInputWithPreviousResponseID = websocketUpstreamSupportsIncrementalInput(pinnedAuth.Attributes, pinnedAuth.Metadata)
			} else {
				// A different model may require a different credential. Normalize
				// replayable history before selecting it, without carrying a foreign ID.
				requestPinnedAuthID = ""
			}
		}

		var requestJSON []byte
		var updatedLastRequest []byte
		var errMsg *interfaces.ErrorMessage
		requestJSON, updatedLastRequest, errMsg = normalizeResponsesWebsocketRequestWithContext(
			payload,
			lastRequest,
			lastResponseOutput,
			lastResponseID,
			allowIncrementalInputWithPreviousResponseID,
		)
		if errMsg != nil {
			h.LoggingAPIResponseError(context.WithValue(context.Background(), "gin", c), errMsg)
			markAPIResponseTimestamp(c)
			errorPayload, errWrite := h.writePublicResponsesWebsocketError(c, writer, &wsTimelineLog, errMsg)
			log.Infof(
				"responses websocket: downstream_out id=%s type=%d event=%s payload=%s",
				executorhelps.CodexWebsocketSessionLogID(passthroughSessionID),
				websocket.TextMessage,
				websocketPayloadEventType(errorPayload),
				websocketPayloadPreview(errorPayload, util.PromptCacheLogForGin(c)),
			)
			if errWrite != nil {
				log.Warnf(
					"responses websocket: downstream_out write failed id=%s event=%s error=%v",
					executorhelps.CodexWebsocketSessionLogID(passthroughSessionID),
					websocketPayloadEventType(errorPayload),
					executorhelps.CodexWebsocketLogError(errWrite, util.PromptCacheLogForGin(c)),
				)
				return
			}
			continue
		}
		if shouldHandleResponsesWebsocketPrewarmLocally(payload, lastRequest, allowIncrementalInputWithPreviousResponseID) {
			accessContext := context.WithValue(context.Background(), "gin", c)
			modelName := gjson.GetBytes(requestJSON, "model").String()
			if accessError := h.ValidateModelProviderAccess(accessContext, h.HandlerType(), modelName); accessError != nil {
				h.LoggingAPIResponseError(accessContext, accessError)
				markAPIResponseTimestamp(c)
				errorPayload, errWrite := h.writePublicResponsesWebsocketError(c, writer, &wsTimelineLog, accessError)
				log.Infof(
					"responses websocket: downstream_out id=%s type=%d event=%s payload=%s",
					executorhelps.CodexWebsocketSessionLogID(passthroughSessionID),
					websocket.TextMessage,
					websocketPayloadEventType(errorPayload),
					websocketPayloadPreview(errorPayload, util.PromptCacheLogForGin(c)),
				)
				if errWrite != nil {
					wsTerminateErr = errWrite
					return
				}
				continue
			}
			if updated, errDelete := sjson.DeleteBytes(requestJSON, "generate"); errDelete == nil {
				requestJSON = updated
			}
			if updated, errDelete := sjson.DeleteBytes(updatedLastRequest, "generate"); errDelete == nil {
				updatedLastRequest = updated
			}
			lastRequest = updatedLastRequest
			lastResponseOutput = []byte("[]")
			if errWrite := writeResponsesWebsocketSyntheticPrewarm(c, writer, requestJSON, &wsTimelineLog, passthroughSessionID, &lastResponseID); errWrite != nil {
				wsTerminateErr = errWrite
				return
			}
			continue
		}

		requestJSON = h.prepareOrphanDelegation(c, requestJSON, func(callID string) bool {
			_, exists := toolPairState.getCall(callID)
			return exists
		})
		requestJSON = h.prepareCodexMultiAgentV2(c, requestJSON)
		toolCacheTurn := newResponsesWebsocketToolCacheTurn(toolPairState)
		requestJSON = toolCacheTurn.repairRequest(requestJSON)
		requestJSON = dedupeResponsesWebsocketInputItemsByID(requestJSON)
		h.PinChatGPTWebImageErrorSanitization(c, coreauth.PayloadHasImageGenerationTool(requestJSON))
		updatedLastRequest = bytes.Clone(requestJSON)

		modelName := gjson.GetBytes(requestJSON, "model").String()
		cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
		cliCtx = cliproxyexecutor.WithDownstreamWebsocket(cliCtx)
		if strings.TrimSpace(gjson.GetBytes(requestJSON, "previous_response_id").String()) != "" {
			cliCtx = cliproxyexecutor.WithRequiredUpstreamWebsocket(cliCtx)
		}
		cliCtx = handlers.WithExecutionSessionID(cliCtx, passthroughSessionID)
		lastAttemptedAuthID = ""
		if requestPinnedAuthID != "" {
			lastAttemptedAuthID = requestPinnedAuthID
			cliCtx = handlers.WithPinnedAuthID(cliCtx, requestPinnedAuthID)
		} else {
			cliCtx = handlers.WithSelectedAuthIDCallback(cliCtx, func(authID string) {
				authID = strings.TrimSpace(authID)
				if authID == "" || h == nil || h.AuthManager == nil {
					return
				}
				lastAttemptedAuthID = authID
				selectedAuth, ok := h.AuthManager.GetByID(authID)
				if !ok || selectedAuth == nil {
					return
				}
				if websocketUpstreamSupportsIncrementalInput(selectedAuth.Attributes, selectedAuth.Metadata) {
					pinnedAuthID = authID
				}
			})
		}
		dataChan, errChan, streamDone := h.startResponsesWebsocketStream(cliCtx, modelName, requestJSON)

		completedOutput, forwardErrMsg, errForward := h.forwardResponsesWebsocket(c, writer, cliCancel, dataChan, errChan, &wsTimelineLog, passthroughSessionID, toolPairState, toolCacheTurn)
		cliCancel(nil)
		<-streamDone
		if errForward != nil {
			wsTerminateErr = errForward
			log.Warnf("responses websocket: forward failed id=%s error=%v", executorhelps.CodexWebsocketSessionLogID(passthroughSessionID), executorhelps.CodexWebsocketLogError(errForward, util.PromptCacheLogForGin(c)))
			return
		}
		if shouldClearResponsesWebsocketPinnedAuth(pinnedAuthID, lastAttemptedAuthID, forwardErrMsg) {
			pinnedAuthID = ""
		}
		if forwardErrMsg == nil && toolCacheTurn.succeeded {
			toolCacheTurn.commit()
			lastRequest = updatedLastRequest
			lastResponseOutput = completedOutput
			lastResponseID = toolCacheTurn.responseID
		}
	}
}

func websocketClientAddress(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return ""
	}
	return strings.TrimSpace(c.ClientIP())
}

func websocketUpgradeHeaders(req *http.Request) http.Header {
	headers := http.Header{}
	if req == nil {
		return headers
	}

	// Keep the same sticky turn-state across reconnects when provided by the client.
	turnState := strings.TrimSpace(req.Header.Get(wsTurnStateHeader))
	if turnState != "" {
		headers.Set(wsTurnStateHeader, turnState)
	}
	return headers
}

func normalizeResponsesWebsocketRequest(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte) ([]byte, []byte, *interfaces.ErrorMessage) {
	return normalizeResponsesWebsocketRequestWithMode(rawJSON, lastRequest, lastResponseOutput, true)
}

func normalizeResponsesWebsocketRequestWithMode(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, allowIncrementalInputWithPreviousResponseID bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	requestType := strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String())
	switch requestType {
	case wsRequestTypeCreate:
		// log.Infof("responses websocket: response.create request")
		if len(lastRequest) == 0 {
			return normalizeResponseCreateRequest(rawJSON)
		}
		return normalizeResponseSubsequentRequest(rawJSON, lastRequest, lastResponseOutput, allowIncrementalInputWithPreviousResponseID)
	case wsRequestTypeAppend:
		// log.Infof("responses websocket: response.append request")
		return normalizeResponseSubsequentRequest(rawJSON, lastRequest, lastResponseOutput, allowIncrementalInputWithPreviousResponseID)
	default:
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("unsupported websocket request type: %s", requestType),
		}
	}
}

func normalizeResponseCreateRequest(rawJSON []byte) ([]byte, []byte, *interfaces.ErrorMessage) {
	normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
	if errDelete != nil {
		normalized = bytes.Clone(rawJSON)
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	if !gjson.GetBytes(normalized, "input").Exists() {
		normalized, _ = sjson.SetRawBytes(normalized, "input", []byte("[]"))
	}

	modelName := strings.TrimSpace(gjson.GetBytes(normalized, "model").String())
	if modelName == "" {
		return nil, nil, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("missing model in response.create request"),
		}
	}
	return normalized, bytes.Clone(normalized), nil
}

func normalizeResponseSubsequentRequest(rawJSON []byte, lastRequest []byte, lastResponseOutput []byte, allowIncrementalInputWithPreviousResponseID bool) ([]byte, []byte, *interfaces.ErrorMessage) {
	if len(lastRequest) == 0 {
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("websocket request received before response.create"),
		}
	}

	nextInput := gjson.GetBytes(rawJSON, "input")
	if !nextInput.Exists() || !nextInput.IsArray() {
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("websocket request requires array field: input"),
		}
	}

	// Compaction can cause clients to replace local websocket history with a new
	// compact transcript on the next `response.create`. When the input already
	// contains historical model output items, treating it as an incremental append
	// duplicates stale turn-state and can leave late orphaned function_call items.
	if shouldReplaceWebsocketTranscript(rawJSON, nextInput) {
		normalized := normalizeResponseTranscriptReplacement(rawJSON, lastRequest)
		return normalized, bytes.Clone(normalized), nil
	}

	// Websocket v2 mode uses response.create with previous_response_id + incremental input.
	// Do not expand it into a full input transcript; upstream expects the incremental payload.
	if allowIncrementalInputWithPreviousResponseID {
		if prev := strings.TrimSpace(gjson.GetBytes(rawJSON, "previous_response_id").String()); prev != "" {
			normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
			if errDelete != nil {
				normalized = bytes.Clone(rawJSON)
			}
			if !gjson.GetBytes(normalized, "model").Exists() {
				modelName := strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
				if modelName != "" {
					normalized, _ = sjson.SetBytes(normalized, "model", modelName)
				}
			}
			if !gjson.GetBytes(normalized, "instructions").Exists() {
				instructions := gjson.GetBytes(lastRequest, "instructions")
				if instructions.Exists() {
					normalized, _ = sjson.SetRawBytes(normalized, "instructions", []byte(instructions.Raw))
				}
			}
			normalized, _ = sjson.SetBytes(normalized, "stream", true)
			return normalized, bytes.Clone(normalized), nil
		}
	}

	existingInput := gjson.GetBytes(lastRequest, "input")
	existingRaw := dropConsumedWebsocketCompactionTriggers(existingInput, gjson.ParseBytes(lastResponseOutput))
	mergedInput, errMerge := mergeJSONArrayRaw(existingRaw, normalizeJSONArrayRaw(lastResponseOutput))
	if errMerge != nil {
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("invalid previous response output: %w", errMerge),
		}
	}

	mergedInput, errMerge = mergeJSONArrayRaw(mergedInput, nextInput.Raw)
	if errMerge != nil {
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("invalid request input: %w", errMerge),
		}
	}
	dedupedInput, errDedupeFunctionCalls := dedupeFunctionCallsByCallID(mergedInput)
	if errDedupeFunctionCalls == nil {
		mergedInput = dedupedInput
	}
	dedupedInput, errDedupeItems := dedupeInputItemsByID(mergedInput)
	if errDedupeItems == nil {
		mergedInput = dedupedInput
	}

	normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
	if errDelete != nil {
		normalized = bytes.Clone(rawJSON)
	}
	normalized, _ = sjson.DeleteBytes(normalized, "previous_response_id")
	var errSet error
	normalized, errSet = sjson.SetRawBytes(normalized, "input", []byte(mergedInput))
	if errSet != nil {
		return nil, lastRequest, &interfaces.ErrorMessage{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("failed to merge websocket input: %w", errSet),
		}
	}
	if !gjson.GetBytes(normalized, "model").Exists() {
		modelName := strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
		if modelName != "" {
			normalized, _ = sjson.SetBytes(normalized, "model", modelName)
		}
	}
	if !gjson.GetBytes(normalized, "instructions").Exists() {
		instructions := gjson.GetBytes(lastRequest, "instructions")
		if instructions.Exists() {
			normalized, _ = sjson.SetRawBytes(normalized, "instructions", []byte(instructions.Raw))
		}
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	return normalized, bytes.Clone(normalized), nil
}

func shouldReplaceWebsocketTranscript(rawJSON []byte, nextInput gjson.Result) bool {
	requestType := strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String())
	if requestType != wsRequestTypeCreate && requestType != wsRequestTypeAppend {
		return false
	}
	previousResponseID := gjson.GetBytes(rawJSON, "previous_response_id")
	if strings.TrimSpace(previousResponseID.String()) != "" {
		return false
	}
	if !nextInput.Exists() || !nextInput.IsArray() {
		return false
	}
	if requestType == wsRequestTypeCreate && !previousResponseID.Exists() && inputHasCodexLocalCompactionSummary(nextInput) {
		return true
	}

	for _, item := range nextInput.Array() {
		switch strings.TrimSpace(item.Get("type").String()) {
		case "function_call", "custom_tool_call", "compaction", "compaction_summary":
			return true
		case "message":
			role := strings.TrimSpace(item.Get("role").String())
			if role == "assistant" {
				return true
			}
		}
	}

	return false
}

func normalizeResponseTranscriptReplacement(rawJSON []byte, lastRequest []byte) []byte {
	normalized, errDelete := sjson.DeleteBytes(rawJSON, "type")
	if errDelete != nil {
		normalized = bytes.Clone(rawJSON)
	}
	normalized, _ = sjson.DeleteBytes(normalized, "previous_response_id")
	if !gjson.GetBytes(normalized, "model").Exists() {
		modelName := strings.TrimSpace(gjson.GetBytes(lastRequest, "model").String())
		if modelName != "" {
			normalized, _ = sjson.SetBytes(normalized, "model", modelName)
		}
	}
	if !gjson.GetBytes(normalized, "instructions").Exists() {
		instructions := gjson.GetBytes(lastRequest, "instructions")
		if instructions.Exists() {
			normalized, _ = sjson.SetRawBytes(normalized, "instructions", []byte(instructions.Raw))
		}
	}
	normalized, _ = sjson.SetBytes(normalized, "stream", true)
	return bytes.Clone(normalized)
}

func dedupeFunctionCallsByCallID(rawArray string) (string, error) {
	rawArray = strings.TrimSpace(rawArray)
	if rawArray == "" {
		return "[]", nil
	}
	var items []json.RawMessage
	if errUnmarshal := json.Unmarshal([]byte(rawArray), &items); errUnmarshal != nil {
		return "", errUnmarshal
	}

	seenCallIDs := make(map[string]struct{}, len(items))
	filtered := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		if len(item) == 0 {
			continue
		}
		itemType := strings.TrimSpace(gjson.GetBytes(item, "type").String())
		if isResponsesToolCallType(itemType) {
			callID := strings.TrimSpace(gjson.GetBytes(item, "call_id").String())
			if callID != "" {
				if _, ok := seenCallIDs[callID]; ok {
					continue
				}
				seenCallIDs[callID] = struct{}{}
			}
		}
		filtered = append(filtered, item)
	}

	out, errMarshal := json.Marshal(filtered)
	if errMarshal != nil {
		return "", errMarshal
	}
	return string(out), nil
}

func dedupeResponsesWebsocketInputItemsByID(payload []byte) []byte {
	input := gjson.GetBytes(payload, "input")
	if !input.Exists() || !input.IsArray() {
		return payload
	}
	deduped, errDedupe := dedupeInputItemsByID(input.Raw)
	if errDedupe != nil {
		return payload
	}
	updated, errSet := sjson.SetRawBytes(payload, "input", []byte(deduped))
	if errSet != nil {
		return payload
	}
	return updated
}

func dedupeInputItemsByID(rawArray string) (string, error) {
	rawArray = strings.TrimSpace(rawArray)
	if rawArray == "" {
		return "[]", nil
	}
	var items []json.RawMessage
	if errUnmarshal := json.Unmarshal([]byte(rawArray), &items); errUnmarshal != nil {
		return "", errUnmarshal
	}

	type inputIdentity struct {
		kind   string
		id     string
		callID string
	}
	seenIDs := make(map[inputIdentity]struct{}, len(items))
	filtered := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		if len(item) == 0 {
			continue
		}
		id := strings.TrimSpace(gjson.GetBytes(item, "id").String())
		if id != "" {
			kind := strings.TrimSpace(gjson.GetBytes(item, "type").String())
			if kind == "" && strings.TrimSpace(gjson.GetBytes(item, "role").String()) != "" {
				kind = "message"
			}
			identity := inputIdentity{kind: kind, id: id}
			if isResponsesToolCallType(kind) || isResponsesToolCallOutputType(kind) {
				identity.callID = strings.TrimSpace(gjson.GetBytes(item, "call_id").String())
			}
			// Item IDs can collide before provider normalization. Distinct tool
			// pairs and item types must survive until their IDs can be repaired.
			if _, ok := seenIDs[identity]; ok {
				continue
			}
			seenIDs[identity] = struct{}{}
		}
		filtered = append(filtered, item)
	}

	out, errMarshal := json.Marshal(filtered)
	if errMarshal != nil {
		return "", errMarshal
	}
	return string(out), nil
}

func websocketUpstreamSupportsIncrementalInput(attributes map[string]string, metadata map[string]any) bool {
	if len(attributes) > 0 {
		if raw := strings.TrimSpace(attributes["websockets"]); raw != "" {
			parsed, errParse := strconv.ParseBool(raw)
			if errParse == nil {
				return parsed
			}
		}
	}
	if len(metadata) == 0 {
		return false
	}
	raw, ok := metadata["websockets"]
	if !ok || raw == nil {
		return false
	}
	switch value := raw.(type) {
	case bool:
		return value
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(value))
		if errParse == nil {
			return parsed
		}
	default:
	}
	return false
}

func (h *OpenAIResponsesAPIHandler) websocketUpstreamSupportsIncrementalInputForModel(modelName string) bool {
	if h == nil || h.AuthManager == nil {
		return false
	}

	resolvedModelName := modelName
	initialSuffix := thinking.ParseSuffix(modelName)
	if initialSuffix.ModelName == "auto" {
		resolvedBase := util.ResolveAutoModel(initialSuffix.ModelName)
		if initialSuffix.HasSuffix {
			resolvedModelName = fmt.Sprintf("%s(%s)", resolvedBase, initialSuffix.RawSuffix)
		} else {
			resolvedModelName = resolvedBase
		}
	} else {
		resolvedModelName = util.ResolveAutoModel(modelName)
	}

	parsed := thinking.ParseSuffix(resolvedModelName)
	baseModel := strings.TrimSpace(parsed.ModelName)
	providers := util.GetProviderName(baseModel)
	if len(providers) == 0 && baseModel != resolvedModelName {
		providers = util.GetProviderName(resolvedModelName)
	}
	if len(providers) == 0 {
		return false
	}

	providerSet := make(map[string]struct{}, len(providers))
	for i := 0; i < len(providers); i++ {
		providerKey := strings.TrimSpace(strings.ToLower(providers[i]))
		if providerKey == "" {
			continue
		}
		providerSet[providerKey] = struct{}{}
	}
	if len(providerSet) == 0 {
		return false
	}

	modelKey := baseModel
	if modelKey == "" {
		modelKey = strings.TrimSpace(resolvedModelName)
	}
	registryRef := registry.GetGlobalRegistry()
	now := time.Now()
	auths := h.AuthManager.AuthsForProviders(providers...)
	for i := 0; i < len(auths); i++ {
		auth := auths[i]
		if auth == nil {
			continue
		}
		providerKey := strings.TrimSpace(strings.ToLower(auth.Provider))
		if _, ok := providerSet[providerKey]; !ok {
			continue
		}
		if modelKey != "" && registryRef != nil && !registryRef.ClientSupportsModel(auth.ID, modelKey) {
			continue
		}
		if !responsesWebsocketAuthAvailableForModel(auth, modelKey, now) {
			continue
		}
		if websocketUpstreamSupportsIncrementalInput(auth.Attributes, auth.Metadata) {
			return true
		}
	}
	return false
}

func responsesWebsocketAuthAvailableForModel(auth *coreauth.Auth, modelName string, now time.Time) bool {
	if auth == nil {
		return false
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return false
	}
	if modelName != "" && len(auth.ModelStates) > 0 {
		state, ok := auth.ModelStates[modelName]
		if (!ok || state == nil) && modelName != "" {
			baseModel := strings.TrimSpace(thinking.ParseSuffix(modelName).ModelName)
			if baseModel != "" && baseModel != modelName {
				state, ok = auth.ModelStates[baseModel]
			}
		}
		if ok && state != nil {
			if state.Status == coreauth.StatusDisabled {
				return false
			}
			if state.Unavailable && !state.NextRetryAfter.IsZero() && state.NextRetryAfter.After(now) {
				return false
			}
			return true
		}
	}
	if auth.Unavailable && !auth.NextRetryAfter.IsZero() && auth.NextRetryAfter.After(now) {
		return false
	}
	return true
}

func shouldHandleResponsesWebsocketPrewarmLocally(rawJSON []byte, lastRequest []byte, allowIncrementalInputWithPreviousResponseID bool) bool {
	if allowIncrementalInputWithPreviousResponseID || len(lastRequest) != 0 {
		return false
	}
	if strings.TrimSpace(gjson.GetBytes(rawJSON, "type").String()) != wsRequestTypeCreate {
		return false
	}
	generateResult := gjson.GetBytes(rawJSON, "generate")
	return generateResult.Exists() && !generateResult.Bool()
}

func writeResponsesWebsocketSyntheticPrewarm(
	c *gin.Context,
	conn responsesWebsocketOutput,
	requestJSON []byte,
	wsTimelineLog *strings.Builder,
	sessionID string,
	responseIDs ...*string,
) error {
	payloads, errPayloads := syntheticResponsesWebsocketPrewarmPayloads(requestJSON)
	if errPayloads != nil {
		return errPayloads
	}
	for i := 0; i < len(payloads); i++ {
		markAPIResponseTimestamp(c)
		// log.Infof(
		// 	"responses websocket: downstream_out id=%s type=%d event=%s payload=%s",
		// 	sessionID,
		// 	websocket.TextMessage,
		// 	websocketPayloadEventType(payloads[i]),
		// 	websocketPayloadPreview(payloads[i]),
		// )
		if errWrite := writeResponsesWebsocketPayload(conn, wsTimelineLog, payloads[i], time.Now(), util.PromptCacheLogForGin(c)); errWrite != nil {
			log.Warnf(
				"responses websocket: downstream_out write failed id=%s event=%s error=%v",
				executorhelps.CodexWebsocketSessionLogID(sessionID),
				websocketPayloadEventType(payloads[i]),
				executorhelps.CodexWebsocketLogError(errWrite, util.PromptCacheLogForGin(c)),
			)
			return errWrite
		}
	}
	if len(responseIDs) > 0 && responseIDs[0] != nil {
		*responseIDs[0] = strings.Clone(gjson.GetBytes(payloads[len(payloads)-1], "response.id").String())
	}
	return nil
}

func syntheticResponsesWebsocketPrewarmPayloads(requestJSON []byte) ([][]byte, error) {
	responseID := "resp_prewarm_" + uuid.NewString()
	createdAt := time.Now().Unix()
	modelName := strings.TrimSpace(gjson.GetBytes(requestJSON, "model").String())

	createdPayload := []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"","object":"response","created_at":0,"status":"in_progress","background":false,"error":null,"output":[]}}`)
	var errSet error
	createdPayload, errSet = sjson.SetBytes(createdPayload, "response.id", responseID)
	if errSet != nil {
		return nil, errSet
	}
	createdPayload, errSet = sjson.SetBytes(createdPayload, "response.created_at", createdAt)
	if errSet != nil {
		return nil, errSet
	}
	if modelName != "" {
		createdPayload, errSet = sjson.SetBytes(createdPayload, "response.model", modelName)
		if errSet != nil {
			return nil, errSet
		}
	}

	completedPayload := []byte(`{"type":"response.completed","sequence_number":1,"response":{"id":"","object":"response","created_at":0,"status":"completed","background":false,"error":null,"output":[],"usage":{"input_tokens":0,"input_tokens_details":{"cached_tokens":0},"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":0}}}`)
	completedPayload, errSet = sjson.SetBytes(completedPayload, "response.id", responseID)
	if errSet != nil {
		return nil, errSet
	}
	completedPayload, errSet = sjson.SetBytes(completedPayload, "response.created_at", createdAt)
	if errSet != nil {
		return nil, errSet
	}
	if modelName != "" {
		completedPayload, errSet = sjson.SetBytes(completedPayload, "response.model", modelName)
		if errSet != nil {
			return nil, errSet
		}
	}

	return [][]byte{createdPayload, completedPayload}, nil
}

func mergeJSONArrayRaw(existingRaw, appendRaw string) (string, error) {
	existingRaw = strings.TrimSpace(existingRaw)
	appendRaw = strings.TrimSpace(appendRaw)
	if existingRaw == "" {
		existingRaw = "[]"
	}
	if appendRaw == "" {
		appendRaw = "[]"
	}

	var existing []json.RawMessage
	if err := json.Unmarshal([]byte(existingRaw), &existing); err != nil {
		return "", err
	}
	var appendItems []json.RawMessage
	if err := json.Unmarshal([]byte(appendRaw), &appendItems); err != nil {
		return "", err
	}

	merged := append(existing, appendItems...)
	out, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func normalizeJSONArrayRaw(raw []byte) string {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return "[]"
	}
	result := gjson.Parse(trimmed)
	if result.Type == gjson.JSON && result.IsArray() {
		return trimmed
	}
	return "[]"
}

func (h *OpenAIResponsesAPIHandler) forwardResponsesWebsocket(
	c *gin.Context,
	conn responsesWebsocketOutput,
	cancel handlers.APIHandlerCancelFunc,
	data <-chan []byte,
	errs <-chan *interfaces.ErrorMessage,
	wsTimelineLog *strings.Builder,
	sessionID string,
	toolPairState *websocketToolPairState,
	turns ...*responsesWebsocketToolCacheTurn,
) ([]byte, *interfaces.ErrorMessage, error) {
	writer := newResponsesWebsocketWriter(conn)
	conn = writer
	toolCacheTurn := newResponsesWebsocketToolCacheTurn(toolPairState)
	if len(turns) > 0 && turns[0] != nil {
		toolCacheTurn = turns[0]
	}
	defer func() {
		if len(turns) == 0 {
			toolCacheTurn.commit()
		}
	}()
	completed := false
	completedSuccessfully := false
	keepAliveInterval := time.Duration(0)
	if h != nil && h.BaseAPIHandler != nil {
		keepAliveInterval = handlers.StreamingKeepAliveInterval(h.ConfigSnapshot())
	}
	var keepAlive *time.Timer
	var keepAliveC <-chan time.Time
	if keepAliveInterval > 0 {
		keepAlive = time.NewTimer(keepAliveInterval)
		defer keepAlive.Stop()
		keepAliveC = keepAlive.C
	}
	completedOutput := []byte("[]")
	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte
	forwardError := func(errMsg *interfaces.ErrorMessage, warnWriteFailure bool) (*interfaces.ErrorMessage, error) {
		if errMsg == nil {
			cancel(nil)
			return nil, nil
		}
		h.LoggingAPIResponseError(context.WithValue(context.Background(), "gin", c), errMsg)
		markAPIResponseTimestamp(c)
		if matched, errClose := writer.closeForUpstreamError(errMsg.Error); matched {
			cancel(errMsg.Error)
			if errClose != nil {
				return errMsg, errClose
			}
			return errMsg, websocket.ErrCloseSent
		}
		errorPayload, errWrite := h.writePublicResponsesWebsocketError(c, conn, wsTimelineLog, errMsg)
		log.Infof(
			"responses websocket: downstream_out id=%s type=%d event=%s payload=%s",
			executorhelps.CodexWebsocketSessionLogID(sessionID),
			websocket.TextMessage,
			websocketPayloadEventType(errorPayload),
			websocketPayloadPreview(errorPayload, util.PromptCacheLogForGin(c)),
		)
		if errWrite != nil {
			if warnWriteFailure {
				log.Warnf(
					"responses websocket: downstream_out write failed id=%s event=%s error=%v",
					executorhelps.CodexWebsocketSessionLogID(sessionID),
					websocketPayloadEventType(errorPayload),
					executorhelps.CodexWebsocketLogError(errWrite, util.PromptCacheLogForGin(c)),
				)
			}
			cancel(errMsg.Error)
			return errMsg, errWrite
		}
		cancel(errMsg.Error)
		return errMsg, nil
	}

	for {
		select {
		case <-c.Request.Context().Done():
			cancel(c.Request.Context().Err())
			return completedOutput, nil, c.Request.Context().Err()
		case <-keepAliveC:
			// All application data and proactive Ping frames use this writer loop.
			// A zero deadline preserves the existing no-network-timeout contract.
			if errPing := conn.WriteControl(websocket.PingMessage, nil, time.Time{}); errPing != nil {
				cancel(errPing)
				return completedOutput, nil, errPing
			}
			keepAlive.Reset(keepAliveInterval)
		case errMsg, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if completed {
				// A completed response remains authoritative over late transport errors.
				toolCacheTurn.succeeded = completedSuccessfully && c.Request.Context().Err() == nil
				cancel(nil)
				return completedOutput, nil, nil
			}
			forwardErrMsg, errForward := forwardError(errMsg, false)
			return completedOutput, forwardErrMsg, errForward
		case chunk, ok := <-data:
			if !ok {
				if !completed {
					if errMsg := takePendingStreamError(errs); errMsg != nil {
						forwardErrMsg, errForward := forwardError(errMsg, false)
						return completedOutput, forwardErrMsg, errForward
					}
					errMsg := &interfaces.ErrorMessage{
						StatusCode: http.StatusRequestTimeout,
						Error:      fmt.Errorf("stream closed before response.completed"),
					}
					forwardErrMsg, errForward := forwardError(errMsg, true)
					return completedOutput, forwardErrMsg, errForward
				}
				toolCacheTurn.succeeded = completedSuccessfully && c.Request.Context().Err() == nil
				cancel(nil)
				return completedOutput, nil, nil
			}

			payloads := websocketJSONPayloadsFromChunk(chunk)
			for i := range payloads {
				if completed {
					continue
				}
				collectResponsesWebsocketOutputItem(payloads[i], outputItemsByIndex, &outputItemsFallback)
				eventType := gjson.GetBytes(payloads[i], "type").String()
				if responsesWebsocketCompletionEvent(eventType) {
					payloads[i] = restoreResponsesWebsocketCompletionOutput(payloads[i], outputItemsByIndex, outputItemsFallback)
				}
				toolCacheTurn.recordResponse(payloads[i])
				var payloadErrMsg *interfaces.ErrorMessage
				if responsesWebsocketTerminalEvent(eventType) {
					completed = true
					if responsesWebsocketCompletionEvent(eventType) {
						completedOutput = responseCompletedOutputFromPayloadWithFallback(payloads[i], outputItemsByIndex, outputItemsFallback)
					} else if eventType == wsEventTypeError || eventType == wsEventTypeFailed {
						payloadErrMsg = responsesWebsocketErrorMessageFromPayload(payloads[i])
						projected := payloadErrMsg
						if h != nil && h.BaseAPIHandler != nil {
							projected = h.RewriteExecutionErrorResponseForGin(c, payloadErrMsg)
							projected = h.ProjectChatGPTWebImageErrorResponse(c, projected, payloads[i])
						}
						if projected != payloadErrMsg {
							var errRewrite error
							payloads[i], errRewrite = rewriteResponsesWebsocketTerminalErrorPayload(payloads[i], projected)
							if errRewrite != nil {
								cancel(errRewrite)
								return completedOutput, payloadErrMsg, errRewrite
							}
						}
					}
				}
				markAPIResponseTimestamp(c)
				// log.Infof(
				// 	"responses websocket: downstream_out id=%s type=%d event=%s payload=%s",
				// 	sessionID,
				// 	websocket.TextMessage,
				// 	websocketPayloadEventType(payloads[i]),
				// 	websocketPayloadPreview(payloads[i]),
				// )
				writePayload := writeResponsesWebsocketPayload
				if responsesWebsocketTerminalEvent(eventType) {
					writePayload = writeResponsesWebsocketTerminalPayload
				}
				if errWrite := writePayload(conn, wsTimelineLog, payloads[i], time.Now(), util.PromptCacheLogForGin(c)); errWrite != nil {
					log.Warnf(
						"responses websocket: downstream_out write failed id=%s event=%s error=%v",
						executorhelps.CodexWebsocketSessionLogID(sessionID),
						websocketPayloadEventType(payloads[i]),
						executorhelps.CodexWebsocketLogError(errWrite, util.PromptCacheLogForGin(c)),
					)
					cancel(errWrite)
					return completedOutput, nil, errWrite
				}
				if keepAlive != nil {
					keepAlive.Reset(keepAliveInterval)
				}
				if payloadErrMsg != nil {
					cancel(payloadErrMsg.Error)
					return completedOutput, payloadErrMsg, nil
				}
				if responsesWebsocketTerminalEvent(eventType) {
					status := gjson.GetBytes(payloads[i], "response.status").String()
					completedSuccessfully = responsesWebsocketCompletionEvent(eventType) && (status == "" || status == "completed")
				}
			}
		}
	}
}

func responsesWebsocketTerminalEvent(eventType string) bool {
	switch eventType {
	case wsEventTypeCompleted, wsEventTypeDone, wsEventTypeFailed, wsEventTypeIncomplete, wsEventTypeError:
		return true
	default:
		return false
	}
}

func responsesWebsocketCompletionEvent(eventType string) bool {
	return eventType == wsEventTypeCompleted || eventType == wsEventTypeDone
}

type responsesWebsocketPayloadError struct {
	status  int
	payload string
	message string
}

func (e *responsesWebsocketPayloadError) Error() string   { return e.payload }
func (e *responsesWebsocketPayloadError) StatusCode() int { return e.status }

// ErrorResponseMatchText preserves the existing message-only rewrite contract.
func (e *responsesWebsocketPayloadError) ErrorResponseMatchText() string { return e.message }

func responsesWebsocketErrorMessageFromPayload(payload []byte) *interfaces.ErrorMessage {
	eventType := gjson.GetBytes(payload, "type").String()
	if eventType != wsEventTypeError && eventType != wsEventTypeFailed {
		return nil
	}
	status := int(gjson.GetBytes(payload, "status").Int())
	if status <= 0 {
		status = int(gjson.GetBytes(payload, "error.status").Int())
	}
	if status <= 0 {
		status = int(gjson.GetBytes(payload, "response.status_code").Int())
	}
	if status <= 0 {
		status = int(gjson.GetBytes(payload, "response.error.status").Int())
	}
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	message := strings.TrimSpace(gjson.GetBytes(payload, "error.message").String())
	if message == "" {
		message = strings.TrimSpace(gjson.GetBytes(payload, "response.error.message").String())
	}
	if message == "" {
		message = strings.TrimSpace(gjson.GetBytes(payload, "message").String())
	}
	if message == "" {
		message = http.StatusText(status)
	}
	code := strings.TrimSpace(gjson.GetBytes(payload, "error.code").String())
	if code == "" {
		code = strings.TrimSpace(gjson.GetBytes(payload, "response.error.code").String())
	}
	if code != "" && !strings.Contains(strings.ToLower(message), strings.ToLower(code)) {
		message = code + ": " + message
	}
	// The error owns its text so source buffers can be released or reused safely.
	return &interfaces.ErrorMessage{StatusCode: status, Error: &responsesWebsocketPayloadError{status: status, payload: string(bytes.TrimSpace(payload)), message: message}}
}

func shouldClearResponsesWebsocketPinnedAuth(pinnedAuthID, lastAttemptedAuthID string, errMsg *interfaces.ErrorMessage) bool {
	pinnedAuthID = strings.TrimSpace(pinnedAuthID)
	if pinnedAuthID == "" {
		return false
	}
	lastAttemptedAuthID = strings.TrimSpace(lastAttemptedAuthID)
	if lastAttemptedAuthID != "" && lastAttemptedAuthID != pinnedAuthID {
		return true
	}
	return shouldReleaseResponsesWebsocketPinnedAuth(errMsg)
}

func shouldReleaseResponsesWebsocketPinnedAuth(errMsg *interfaces.ErrorMessage) bool {
	if errMsg == nil {
		return false
	}
	if errMsg.Error != nil {
		var skipAuthResult interface{ SkipAuthResult() bool }
		if errors.As(errMsg.Error, &skipAuthResult) && skipAuthResult.SkipAuthResult() {
			var retryOtherAuth interface{ RetryOtherAuth() bool }
			if !errors.As(errMsg.Error, &retryOtherAuth) || !retryOtherAuth.RetryOtherAuth() {
				return false
			}
		}
	}
	switch handlers.OriginalErrorStatusCode(errMsg) {
	case http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	message := strings.ToLower(strings.TrimSpace(handlers.OriginalErrorText(errMsg)))
	if message == "" {
		return false
	}
	retryableMarkers := []string{
		"stream closed before response.completed",
		"previous_response_not_found",
		"ws_failed",
		"upstream stream closed before first payload",
		"empty_stream",
	}
	for _, marker := range retryableMarkers {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func rewriteResponsesWebsocketTerminalErrorPayload(payload []byte, errMsg *interfaces.ErrorMessage) ([]byte, error) {
	if errMsg == nil {
		return payload, nil
	}
	status := errMsg.StatusCode
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	updated := bytes.Clone(payload)
	hadErrorStatus := gjson.GetBytes(updated, "error.status").Exists()
	hadResponseErrorStatus := gjson.GetBytes(updated, "response.error.status").Exists()
	rewrittenStatus, rewriteStatus := handlers.RewrittenErrorResponseStatus(errMsg)
	if rewriteStatus {
		status = rewrittenStatus
		var errSet error
		updated, errSet = sjson.SetBytes(updated, "status", status)
		if errSet != nil {
			return nil, errSet
		}
		if gjson.GetBytes(updated, "type").String() == wsEventTypeFailed {
			updated, errSet = sjson.SetBytes(updated, "response.status_code", status)
			if errSet != nil {
				return nil, errSet
			}
		}
	}
	_, rewriteBody := handlers.RewrittenErrorResponseBody(errMsg)
	if body, ok := handlers.RewrittenErrorResponseBody(errMsg); ok {
		errorBody := body
		if errorNode := gjson.GetBytes(body, "error"); errorNode.Exists() {
			errorBody = []byte(errorNode.Raw)
		}
		var errSet error
		switch gjson.GetBytes(updated, "type").String() {
		case wsEventTypeFailed:
			updated, errSet = sjson.SetRawBytes(updated, "response.error", errorBody)
		case wsEventTypeError:
			updated, errSet = sjson.SetRawBytes(updated, "error", errorBody)
			// Flat error fields are mirrors of the replaced error, not metadata.
			// Retaining them would expose the original message beside the rewrite.
			for _, field := range []string{"message", "code", "param"} {
				if errSet == nil {
					updated, errSet = sjson.DeleteBytes(updated, field)
				}
			}
		}
		if errSet != nil {
			return nil, errSet
		}
	}
	if rewriteStatus {
		var errSet error
		if hadErrorStatus {
			updated, errSet = sjson.SetBytes(updated, "error.status", status)
			if errSet != nil {
				return nil, errSet
			}
		}
		if hadResponseErrorStatus {
			updated, errSet = sjson.SetBytes(updated, "response.error.status", status)
			if errSet != nil {
				return nil, errSet
			}
		}
	}
	var errFilter error
	updated, errFilter = filterResponsesWebsocketHeaders(updated, status, rewriteBody, errMsg)
	if errFilter != nil {
		return nil, errFilter
	}
	if handlers.IsChatGPTWebImageErrorResponseSanitized(errMsg) {
		updated, errFilter = handlers.SanitizeChatGPTWebImageProtocolPayload(updated)
		if errFilter != nil {
			return nil, errFilter
		}
	}
	return updated, nil
}

func filterResponsesWebsocketHeaders(payload []byte, status int, bodyRewritten bool, errMsg *interfaces.ErrorMessage) ([]byte, error) {
	updated := payload
	headers := gjson.GetBytes(updated, "headers")
	if !headers.IsObject() {
		return updated, nil
	}
	for key := range headers.Map() {
		value := headers.Get(key).String()
		remove := handlers.ShouldRemoveRewrittenErrorHeader(key, status, bodyRewritten)
		if handlers.IsChatGPTWebImageErrorResponseSanitized(errMsg) {
			remove = !handlers.ShouldForwardSanitizedImageErrorHeader(errMsg, key, value)
		}
		if !remove {
			continue
		}
		headerPath := strings.ReplaceAll(strings.ReplaceAll(key, `\`, `\\`), ".", `\.`)
		var errDelete error
		updated, errDelete = sjson.DeleteBytes(updated, "headers."+headerPath)
		if errDelete != nil {
			return nil, errDelete
		}
	}
	return updated, nil
}

func responseCompletedOutputFromPayload(payload []byte) []byte {
	return responseCompletedOutputFromPayloadWithFallback(payload, nil, nil)
}

func collectResponsesWebsocketOutputItem(payload []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback *[][]byte) {
	if gjson.GetBytes(payload, "type").String() != "response.output_item.done" {
		return
	}
	item := gjson.GetBytes(payload, "item")
	if !item.Exists() || !item.IsObject() {
		return
	}
	outputIndex := gjson.GetBytes(payload, "output_index")
	if outputIndex.Exists() {
		outputItemsByIndex[outputIndex.Int()] = bytes.Clone([]byte(item.Raw))
		return
	}
	*outputItemsFallback = append(*outputItemsFallback, bytes.Clone([]byte(item.Raw)))
}

func restoreResponsesWebsocketCompletionOutput(payload []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) []byte {
	output := gjson.GetBytes(payload, "response.output")
	if output.Exists() && output.IsArray() && len(output.Array()) > 0 {
		if repaired, changed := reconcileResponsesWebsocketCompletionToolCalls(output, outputItemsByIndex, outputItemsFallback); changed {
			if updated, errSet := sjson.SetRawBytes(payload, "response.output", repaired); errSet == nil {
				return updated
			}
		}
		return payload
	}
	if len(outputItemsByIndex) == 0 && len(outputItemsFallback) == 0 {
		return payload
	}
	restored, errSet := sjson.SetRawBytes(payload, "response.output", responseCompletedOutputFromPayloadWithFallback(payload, outputItemsByIndex, outputItemsFallback))
	if errSet != nil {
		return payload
	}
	return restored
}

func responseCompletedOutputFromPayloadWithFallback(payload []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) []byte {
	output := gjson.GetBytes(payload, "response.output")
	if output.Exists() && output.IsArray() && len(output.Array()) > 0 {
		return bytes.Clone([]byte(output.Raw))
	}
	if len(outputItemsByIndex) == 0 && len(outputItemsFallback) == 0 {
		return []byte("[]")
	}

	indexes := make([]int64, 0, len(outputItemsByIndex))
	for index := range outputItemsByIndex {
		indexes = append(indexes, index)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })

	items := make([]json.RawMessage, 0, len(outputItemsByIndex)+len(outputItemsFallback))
	appendItem := func(raw []byte) {
		item := gjson.ParseBytes(raw)
		if isResponsesToolCallType(item.Get("type").String()) && !isCompleteResponsesWebsocketToolCall(item) {
			return
		}
		items = append(items, json.RawMessage(raw))
	}
	for _, index := range indexes {
		appendItem(outputItemsByIndex[index])
	}
	for _, item := range outputItemsFallback {
		appendItem(item)
	}
	marshaled, errMarshal := json.Marshal(items)
	if errMarshal != nil {
		return []byte("[]")
	}
	return marshaled
}

func websocketJSONPayloadsFromChunk(chunk []byte) [][]byte {
	payloads := make([][]byte, 0, 2)
	lines := bytes.Split(chunk, []byte("\n"))
	for i := range lines {
		line := bytes.TrimSpace(lines[i])
		if len(line) == 0 || bytes.HasPrefix(line, []byte("event:")) {
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			line = bytes.TrimSpace(line[len("data:"):])
		}
		if len(line) == 0 || bytes.Equal(line, []byte(wsDoneMarker)) {
			continue
		}
		if json.Valid(line) {
			payloads = append(payloads, bytes.Clone(line))
		}
	}

	if len(payloads) > 0 {
		return payloads
	}

	trimmed := bytes.TrimSpace(chunk)
	if bytes.HasPrefix(trimmed, []byte("data:")) {
		trimmed = bytes.TrimSpace(trimmed[len("data:"):])
	}
	if len(trimmed) > 0 && !bytes.Equal(trimmed, []byte(wsDoneMarker)) && json.Valid(trimmed) {
		payloads = append(payloads, bytes.Clone(trimmed))
	}
	return payloads
}

func writeResponsesWebsocketError(conn responsesWebsocketOutput, wsTimelineLog *strings.Builder, errMsg *interfaces.ErrorMessage, redactors ...*util.PromptCacheLogRedactor) ([]byte, error) {
	payload, errBuild := buildResponsesWebsocketErrorPayload(errMsg)
	if errBuild != nil {
		return nil, errBuild
	}
	return payload, writeResponsesWebsocketTerminalPayload(conn, wsTimelineLog, payload, time.Now(), redactors...)
}

func (h *OpenAIResponsesAPIHandler) writePublicResponsesWebsocketError(c *gin.Context, conn responsesWebsocketOutput, wsTimelineLog *strings.Builder, errMsg *interfaces.ErrorMessage) ([]byte, error) {
	if h != nil && h.BaseAPIHandler != nil {
		errMsg = h.ProjectChatGPTWebImageErrorResponse(c, errMsg)
	}
	return writeResponsesWebsocketError(conn, wsTimelineLog, errMsg, util.PromptCacheLogForGin(c))
}

func buildResponsesWebsocketErrorPayload(errMsg *interfaces.ErrorMessage) ([]byte, error) {
	status := http.StatusInternalServerError
	errText := http.StatusText(status)
	if errMsg != nil {
		if errMsg.StatusCode > 0 {
			status = errMsg.StatusCode
			errText = http.StatusText(status)
		}
		if errMsg.Error != nil && strings.TrimSpace(errMsg.Error.Error()) != "" {
			errText = errMsg.Error.Error()
		}
	}

	body, rewriteBody := handlers.RewrittenErrorResponseBody(errMsg)
	if !rewriteBody {
		bodyStatus := status
		bodyText := errText
		if handlers.IsErrorResponseRewritten(errMsg) {
			bodyStatus = handlers.OriginalErrorStatusCode(errMsg)
			bodyText = handlers.OriginalErrorText(errMsg)
		}
		body = handlers.BuildErrorResponseBody(bodyStatus, bodyText)
	}
	payload := []byte(`{}`)
	var errSet error
	payload, errSet = sjson.SetBytes(payload, "type", wsEventTypeError)
	if errSet != nil {
		return nil, errSet
	}
	payload, errSet = sjson.SetBytes(payload, "status", status)
	if errSet != nil {
		return nil, errSet
	}

	if errMsg != nil && errMsg.Addon != nil {
		headers := []byte(`{}`)
		hasHeaders := false
		for key, values := range errMsg.Addon {
			if len(values) == 0 {
				continue
			}
			headerPath := strings.ReplaceAll(strings.ReplaceAll(key, `\\`, `\\\\`), ".", `\\.`)
			headers, errSet = sjson.SetBytes(headers, headerPath, values[0])
			if errSet != nil {
				return nil, errSet
			}
			hasHeaders = true
		}
		if hasHeaders {
			payload, errSet = sjson.SetRawBytes(payload, "headers", headers)
			if errSet != nil {
				return nil, errSet
			}
		}
	}

	if len(body) > 0 && json.Valid(body) {
		errorNode := gjson.GetBytes(body, "error")
		if errorNode.Exists() {
			payload, errSet = sjson.SetRawBytes(payload, "error", []byte(errorNode.Raw))
		} else {
			payload, errSet = sjson.SetRawBytes(payload, "error", body)
		}
		if errSet != nil {
			return nil, errSet
		}
	}

	if !gjson.GetBytes(payload, "error").Exists() {
		payload, errSet = sjson.SetBytes(payload, "error.type", "server_error")
		if errSet != nil {
			return nil, errSet
		}
		payload, errSet = sjson.SetBytes(payload, "error.message", errText)
		if errSet != nil {
			return nil, errSet
		}
	}

	return payload, nil
}

func appendWebsocketEvent(builder *strings.Builder, eventType string, payload []byte) {
	if builder == nil {
		return
	}
	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) == 0 {
		return
	}
	if builder.Len() > 0 {
		builder.WriteString("\n")
	}
	builder.WriteString("websocket.")
	builder.WriteString(eventType)
	builder.WriteString("\n")
	builder.Write(trimmedPayload)
	builder.WriteString("\n")
}

func websocketPayloadEventType(payload []byte) string {
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	if eventType == "" {
		return "-"
	}
	return eventType
}

func websocketPayloadPreview(payload []byte, redactors ...*util.PromptCacheLogRedactor) string {
	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) == 0 {
		return "<empty>"
	}
	text := string(trimmedPayload)
	if len(redactors) > 0 {
		text = redactors[0].Redact(text)
	}
	previewText := strings.ReplaceAll(text, "\n", "\\n")
	previewText = strings.ReplaceAll(previewText, "\r", "\\r")
	return previewText
}

func setWebsocketTimelineBody(c *gin.Context, body string) {
	setWebsocketBody(c, wsTimelineBodyKey, body)
}

func setWebsocketBody(c *gin.Context, key string, body string) {
	if c == nil {
		return
	}
	trimmedBody := strings.TrimSpace(body)
	if trimmedBody == "" {
		return
	}
	c.Set(key, []byte(trimmedBody))
}

func writeResponsesWebsocketPayload(conn responsesWebsocketOutput, wsTimelineLog *strings.Builder, payload []byte, timestamp time.Time, redactors ...*util.PromptCacheLogRedactor) error {
	appendWebsocketTimelineEvent(wsTimelineLog, "response", payload, timestamp, redactors...)
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func writeResponsesWebsocketTerminalPayload(conn responsesWebsocketOutput, wsTimelineLog *strings.Builder, payload []byte, timestamp time.Time, redactors ...*util.PromptCacheLogRedactor) error {
	appendWebsocketTimelineEvent(wsTimelineLog, "response", payload, timestamp, redactors...)
	if writer, ok := conn.(*responsesWebsocketWriter); ok {
		return writer.writeMessage(websocket.TextMessage, payload, true)
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func appendWebsocketTimelineDisconnect(builder *strings.Builder, err error, timestamp time.Time, redactors ...*util.PromptCacheLogRedactor) {
	if err == nil {
		return
	}
	detail := err.Error()
	if len(redactors) > 0 {
		detail = executorhelps.CodexWebsocketLogError(err, redactors[0])
	}
	appendWebsocketTimelineEvent(builder, "disconnect", []byte(detail), timestamp, redactors...)
}

func appendWebsocketTimelineEvent(builder *strings.Builder, eventType string, payload []byte, timestamp time.Time, redactors ...*util.PromptCacheLogRedactor) {
	if builder == nil {
		return
	}
	trimmedPayload := bytes.TrimSpace(payload)
	if len(trimmedPayload) == 0 {
		return
	}
	if builder.Len() > 0 {
		builder.WriteString("\n")
	}
	builder.WriteString("Timestamp: ")
	builder.WriteString(timestamp.Format(time.RFC3339Nano))
	builder.WriteString("\n")
	builder.WriteString("Event: websocket.")
	builder.WriteString(eventType)
	builder.WriteString("\n")
	if len(redactors) > 0 {
		builder.WriteString(redactors[0].Redact(string(trimmedPayload)))
	} else {
		builder.Write(trimmedPayload)
	}
	builder.WriteString("\n")
}

func markAPIResponseTimestamp(c *gin.Context) {
	if c == nil {
		return
	}
	if _, exists := c.Get("API_RESPONSE_TIMESTAMP"); exists {
		return
	}
	c.Set("API_RESPONSE_TIMESTAMP", time.Now())
}
