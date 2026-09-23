package executor

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/client/grokbuild"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/tiktoken-go/tokenizer"
	"golang.org/x/net/http/httpguts"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	codexUserAgent             = codexauth.DefaultUserAgent
	codexOriginator            = codexauth.DefaultOriginator
	codexDefaultImageToolModel = "gpt-image-2"
	codexSSEMaxFrameBytes      = 52_428_800
)

var dataTag = []byte("data:")

// Streamed Codex responses may emit response.output_item.done events while leaving
// response.completed.response.output empty. Keep the stream path aligned with the
// already-patched non-stream path by reconstructing response.output from those items.
func collectCodexOutputItemDone(eventData []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback *[][]byte) {
	helps.CollectCodexOutputItemDone(eventData, outputItemsByIndex, outputItemsFallback)
}

func patchCodexCompletedOutput(eventData []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) []byte {
	outputResult := gjson.GetBytes(eventData, "response.output")
	if outputResult.IsArray() && len(outputResult.Array()) > 0 {
		return helps.HydrateCodexOutputItemIDs(eventData, outputItemsByIndex)
	}
	shouldPatchOutput := (!outputResult.Exists() || !outputResult.IsArray() || len(outputResult.Array()) == 0) && (len(outputItemsByIndex) > 0 || len(outputItemsFallback) > 0)
	if !shouldPatchOutput {
		return eventData
	}

	items := make([][]byte, 0, len(outputItemsByIndex)+len(outputItemsFallback))
	if len(outputItemsByIndex) > 0 {
		indexes := make([]int64, 0, len(outputItemsByIndex))
		for idx := range outputItemsByIndex {
			indexes = append(indexes, idx)
		}
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i] < indexes[j]
		})
		for _, idx := range indexes {
			items = append(items, outputItemsByIndex[idx])
		}
	}
	items = append(items, outputItemsFallback...)

	outputArray := []byte("[]")
	if len(items) > 0 {
		var buf bytes.Buffer
		totalLen := 2
		for _, item := range items {
			totalLen += len(item)
		}
		if len(items) > 1 {
			totalLen += len(items) - 1
		}
		buf.Grow(totalLen)
		buf.WriteByte('[')
		for i, item := range items {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.Write(item)
		}
		buf.WriteByte(']')
		outputArray = buf.Bytes()
	}

	completedDataPatched, _ := sjson.SetRawBytes(eventData, "response.output", outputArray)
	return completedDataPatched
}

func codexTerminalStreamError(eventData []byte) (result statusErr, terminal bool) {
	eventType := gjson.GetBytes(eventData, "type").String()
	defer func() {
		if terminal {
			result.responseBody = string(eventData)
			if helps.IsCodexUsageLimitError(eventData) {
				result.code = http.StatusTooManyRequests
				result.skipAuthResult = false
			}
			if result.code == http.StatusTooManyRequests {
				result.retryAfter = parseCodexRetryAfter(result.code, eventData, time.Now())
			}
		}
	}()
	if isCodexCompletionType(eventType) && !gjson.ValidBytes(eventData) {
		return codexStreamStatusErr(http.StatusBadGateway, "invalid upstream Codex terminal JSON", "invalid_response", "server_error", nil), true
	}
	switch eventType {
	case "response.failed":
		return codexResponseFailedError(eventData), true
	case "response.incomplete":
		if helps.IsCodexPartialResponse(eventData) {
			return statusErr{}, false
		}
		return codexResponseIncompleteError(eventData), true
	case "response.completed", "response.done":
		if helps.CodexTerminalErrorNode(eventData).Exists() || helps.CodexTerminalHTTPStatus(eventData) != 0 {
			return codexResponseFailedError(eventData), true
		}
		switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(eventData, "response.status").String())) {
		case "failed":
			return codexResponseFailedError(eventData), true
		case "incomplete":
			if helps.IsCodexPartialResponse(eventData) {
				return statusErr{}, false
			}
			return codexResponseIncompleteError(eventData), true
		case "cancelled", "canceled":
			return codexResponseCancelledError(), true
		}
	case "error":
		return codexStreamErrorEventError(eventData), true
	default:
		return statusErr{}, false
	}
	return statusErr{}, false
}

func normalizeCodexCompletion(payload []byte) []byte {
	if helps.IsCodexPartialResponse(payload) {
		if gjson.GetBytes(payload, "type").String() != "response.incomplete" {
			if updated, err := sjson.SetBytes(payload, "type", "response.incomplete"); err == nil {
				return updated
			}
		}
		return payload
	}
	if strings.TrimSpace(gjson.GetBytes(payload, "type").String()) == "response.done" && isCodexSuccessfulCompletion(payload) {
		updated, err := sjson.SetBytes(payload, "type", "response.completed")
		if err == nil && len(updated) > 0 {
			return updated
		}
	}
	return payload
}

func isCodexSuccessfulCompletion(payload []byte) bool {
	if !isCodexCompletionType(gjson.GetBytes(payload, "type").String()) || !gjson.ValidBytes(payload) {
		return false
	}
	status := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "response.status").String()))
	if status != "" && status != "completed" {
		return false
	}
	return !helps.CodexTerminalErrorNode(payload).Exists() && helps.CodexTerminalHTTPStatus(payload) == 0
}

func isCodexCompletionType(eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	return eventType == "response.completed" || eventType == "response.done"
}

func codexSSEDataCompletion(line []byte) bool {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, dataTag) {
		return false
	}
	payload := bytes.TrimSpace(line[len(dataTag):])
	return isCodexSuccessfulCompletion(payload) || helps.IsCodexPartialResponse(payload)
}

func codexSSEEventCompletion(line []byte) bool {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, []byte("event:")) {
		return false
	}
	kind := string(bytes.TrimSpace(line[len("event:"):]))
	return isCodexCompletionType(kind) || kind == "response.incomplete"
}

func codexResponseFailedError(eventData []byte) statusErr {
	node := helps.CodexTerminalErrorNode(eventData)
	code := strings.TrimSpace(node.Get("code").String())
	errType := strings.TrimSpace(node.Get("type").String())
	status := codexStreamErrorStatus(code, codexStreamErrorStatus(errType, http.StatusInternalServerError))
	if strings.EqualFold(code, "server_error") || strings.EqualFold(code, "internal_server_error") {
		status = codexStreamErrorStatus(errType, status)
	}
	if explicitStatus := helps.CodexTerminalHTTPStatus(eventData); explicitStatus != 0 {
		status = explicitStatus
	}
	message := strings.TrimSpace(node.Get("message").String())
	if message == "" && node.Type == gjson.String {
		message = strings.TrimSpace(node.String())
	}
	if message == "" {
		message = "response failed"
	}
	if code == "" {
		code = "server_error"
	}
	if errType == "" {
		errType = codexStreamErrorType(code)
	}
	var param *gjson.Result
	if value := node.Get("param"); value.Exists() {
		param = &value
	}
	return codexStreamStatusErr(status, message, code, errType, param)
}

func codexResponseIncompleteError(eventData []byte) statusErr {
	if helps.CodexTerminalErrorNode(eventData).Exists() || helps.CodexTerminalHTTPStatus(eventData) != 0 {
		return codexResponseFailedError(eventData)
	}
	reason := strings.TrimSpace(gjson.GetBytes(eventData, "response.incomplete_details.reason").String())
	message := strings.TrimSpace(gjson.GetBytes(eventData, "response.error.message").String())
	if message == "" && reason != "" {
		message = fmt.Sprintf("response incomplete: %s", reason)
	}
	if message == "" {
		message = "response incomplete"
	}
	code := reason
	if code == "" {
		code = "response_incomplete"
	}
	err := codexStreamStatusErr(codexStreamErrorStatus(code, http.StatusBadGateway), message, code, "server_error", nil)
	switch strings.ToLower(reason) {
	case "max_tokens", "max_output_tokens", "content_filter":
		err.skipAuthResult = true
	}
	return err
}

func codexResponseCancelledError() statusErr {
	err := codexStreamStatusErr(http.StatusBadGateway, "response cancelled", "response_cancelled", "server_error", nil)
	err.skipAuthResult = true
	return err
}

func codexStreamErrorEventError(eventData []byte) statusErr {
	code := strings.TrimSpace(gjson.GetBytes(eventData, "error.code").String())
	if code == "" {
		code = strings.TrimSpace(gjson.GetBytes(eventData, "code").String())
	}
	message := strings.TrimSpace(gjson.GetBytes(eventData, "error.message").String())
	if message == "" {
		message = strings.TrimSpace(gjson.GetBytes(eventData, "message").String())
	}
	if message == "" {
		message = "stream error"
	}
	if code == "" {
		code = "stream_error"
	}
	errType := strings.TrimSpace(gjson.GetBytes(eventData, "error.type").String())
	status := codexStreamErrorStatus(code, codexStreamErrorStatus(errType, http.StatusInternalServerError))
	if strings.EqualFold(code, "server_error") || strings.EqualFold(code, "internal_server_error") {
		status = codexStreamErrorStatus(errType, status)
	}
	if explicitStatus := helps.CodexTerminalHTTPStatus(eventData); explicitStatus != 0 {
		status = explicitStatus
	}
	var param *gjson.Result
	if paramResult := gjson.GetBytes(eventData, "error.param"); paramResult.Exists() {
		param = &paramResult
	} else if paramResult := gjson.GetBytes(eventData, "param"); paramResult.Exists() {
		param = &paramResult
	}
	return codexStreamStatusErr(status, message, code, errType, param)
}

func codexStreamErrorStatus(code string, fallback int) int {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "invalid_api_key", "authentication_error", "unauthorized", "invalid_task_id", "task_not_found", "task_expired":
		return http.StatusUnauthorized
	case "permission_error", "permission_denied", "forbidden":
		return http.StatusForbidden
	case "rate_limit_exceeded", "rate_limit_error", "quota_exceeded", "insufficient_quota", "usage_limit_reached":
		return http.StatusTooManyRequests
	case "not_found", "model_not_found":
		return http.StatusNotFound
	case "bad_request", "invalid_request", "invalid_request_error", "invalid_prompt", "invalid_value", "invalid_encrypted_content", "invalid_signature", "context_length_exceeded", "context_too_large":
		return http.StatusBadRequest
	case "server_error", "internal_server_error":
		return http.StatusInternalServerError
	case "overloaded", "service_unavailable":
		return http.StatusServiceUnavailable
	default:
		return fallback
	}
}

func codexStreamErrorType(code string) string {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "invalid_api_key", "authentication_error", "unauthorized", "invalid_task_id", "task_not_found", "task_expired":
		return "authentication_error"
	case "permission_error", "permission_denied", "forbidden":
		return "permission_error"
	case "rate_limit_exceeded", "quota_exceeded", "insufficient_quota", "usage_limit_reached":
		return "rate_limit_error"
	case "server_error", "internal_server_error", "overloaded", "service_unavailable":
		return "server_error"
	default:
		return "invalid_request_error"
	}
}

func codexStreamStatusErr(status int, message, code, errType string, param *gjson.Result) statusErr {
	if status <= 0 {
		status = http.StatusInternalServerError
	}
	if strings.TrimSpace(message) == "" {
		message = http.StatusText(status)
	}
	if strings.TrimSpace(errType) == "" {
		errType = codexStreamErrorType(code)
	}
	body := []byte(`{"error":{}}`)
	body, _ = sjson.SetBytes(body, "error.message", message)
	body, _ = sjson.SetBytes(body, "error.type", errType)
	if strings.TrimSpace(code) != "" {
		body, _ = sjson.SetBytes(body, "error.code", code)
	}
	if param != nil {
		body, _ = sjson.SetRawBytes(body, "error.param", []byte(param.Raw))
	}
	err := statusErr{code: status, msg: string(body)}
	if isCodexContextTooLargeRequestError(status, body) {
		err.skipAuthResult = true
	}
	return err
}

// CodexExecutor is a stateless executor for Codex (OpenAI Responses API entrypoint).
// If api_key is unavailable on auth, it falls back to legacy via ClientAdapter.
type CodexExecutor struct {
	cfg                     *config.Config
	agentIdentityBaseURL    string
	agentIdentityHTTPClient func(context.Context, *cliproxyauth.Auth) *http.Client
}

func NewCodexExecutor(cfg *config.Config) *CodexExecutor { return &CodexExecutor{cfg: cfg} }

func (e *CodexExecutor) Identifier() string { return "codex" }

// DeferAuthRequestCommitUntilUpstream excludes local validation and policy rejections.
func (*CodexExecutor) DeferAuthRequestCommitUntilUpstream() bool { return true }

type codexPreparedSessionIdentity struct {
	RequestScope                  helps.CodexRequestScope
	IdentityConfuseEnabled        bool
	IdentityPolicyFrozen          bool
	MultiAgentV2                  helps.CodexMultiAgentPolicy
	StreamBootstrapBuffering      bool
	ClaudeInputTokensEstimate     int64
	PromptCacheLog                *util.PromptCacheLogRedactor
	PromptCacheKey                helps.CodexPromptCacheKeySnapshot
	ResponsesLite                 helps.CodexResponsesLiteSnapshot
	OrphanDelegationCompatibility bool
	Enabled                       bool
	SessionID                     string
	ThreadID                      string
	TurnID                        string
	WindowID                      string
	RequestKind                   string
	AffinityKind                  string
	AffinityDigest                string
	TenantDigest                  string
	ClientThreadID                string
}

// PrepareProviderRequest creates one immutable identity fallback shared by all
// credential retries and transport fallbacks for the logical request.
func (e *CodexExecutor) PrepareProviderRequest(ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, operation cliproxyexecutor.RequestOperation) (any, error) {
	if opts.SourceFormat == sdktranslator.FormatCodexLive {
		return nil, cliproxyexecutor.NewGlobalProviderRequestPreparationError(helps.CodexLiveNativeRouteError{})
	}
	if opts.SourceFormat == sdktranslator.FormatCodexAlphaSearch {
		if operation != cliproxyexecutor.RequestOperationExecute || opts.Stream {
			return nil, cliproxyexecutor.NewGlobalProviderRequestPreparationError(statusErr{code: http.StatusNotImplemented, msg: "Codex search supports non-streaming HTTP requests only", skipAuthResult: true})
		}
		if _, err := helps.ParseCodexAlphaSearchRouting(req.Payload); err != nil {
			code := http.StatusBadRequest
			if len(req.Payload) > helps.CodexAlphaSearchMaxRequestBytes {
				code = http.StatusRequestEntityTooLarge
			}
			return nil, cliproxyexecutor.NewGlobalProviderRequestPreparationError(statusErr{code: code, msg: err.Error(), skipAuthResult: true})
		}
		return nil, nil
	}
	if operation == cliproxyexecutor.RequestOperationCount {
		return nil, nil
	}
	payload := req.Payload
	if len(opts.OriginalRequest) > 0 {
		payload = opts.OriginalRequest
	}
	turnID, err := helps.NewCodexUUIDv7()
	if err != nil {
		return nil, err
	}
	affinityKind, affinityDigest, tenantDigest, clientThreadID, spoofThreadID := codexPreparedRequestAffinity(ctx, opts, payload, turnID)
	if shared := cliproxyauth.SessionAffinityFingerprint(opts); shared != "" {
		// Reuse routing's pre-translation identity only for pool selection. Caller
		// authorization, turn state and client thread projection stay independent.
		affinityKind, affinityDigest, tenantDigest = "session_affinity", shared, "shared"
	}
	var incomingHeaders http.Header
	if ctx != nil {
		if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
			incomingHeaders = ginCtx.Request.Header
		}
	}
	liteHeaders := http.Header{}
	if value := firstCodexHeaderValue(helps.CodexResponsesLiteHeader, opts.Headers, incomingHeaders); value != "" {
		liteHeaders.Set(helps.CodexResponsesLiteHeader, value)
	}
	requestScope := helps.SnapshotCodexRequestScope(payload, opts.Headers, incomingHeaders)
	if strings.EqualFold(strings.Trim(strings.TrimSpace(opts.Alt), "/"), "responses/compact") {
		requestScope.Kind = "compaction"
	}
	prepared := codexPreparedSessionIdentity{
		RequestScope:                  requestScope,
		IdentityConfuseEnabled:        codexIdentityConfuseEnabled(e.cfg),
		IdentityPolicyFrozen:          true,
		MultiAgentV2:                  helps.SnapshotCodexMultiAgentPolicy(ctx, opts.Headers, e.cfg != nil && e.cfg.Codex.OptimizeMultiAgentV2),
		StreamBootstrapBuffering:      e.cfg != nil && e.cfg.Codex.StreamBootstrapBuffering,
		PromptCacheLog:                helps.SnapshotCodexPromptCacheLog(ctx, payload),
		PromptCacheKey:                helps.SnapshotCodexPromptCacheKey(payload, e.cfg != nil && e.cfg.Codex.PassthroughPromptCacheKey, opts.Headers, incomingHeaders),
		ResponsesLite:                 helps.SnapshotCodexResponsesLite(payload, liteHeaders, cliproxyexecutor.DownstreamWebsocket(ctx)),
		OrphanDelegationCompatibility: helps.CodexOrphanDelegationEnabled(ctx, opts.Headers, e.cfg != nil && e.cfg.Codex.OrphanDelegationCompatibility),
		Enabled:                       codexSpoofSessionIdentityEnabled(e.cfg),
		TurnID:                        turnID,
		RequestKind:                   requestScope.Kind,
		AffinityKind:                  affinityKind,
		AffinityDigest:                affinityDigest,
		TenantDigest:                  tenantDigest,
		ClientThreadID:                clientThreadID,
	}
	if errValidate := prepared.PromptCacheKey.Validate(); errValidate != nil {
		invalid := codexStreamStatusErr(http.StatusInternalServerError, errValidate.Error(), "invalid_prompt_cache_key", "invalid_request_error", nil)
		invalid.skipAuthResult = true
		return nil, cliproxyexecutor.NewGlobalProviderRequestPreparationError(invalid)
	}
	if opts.SourceFormat != sdktranslator.FormatCodex && opts.SourceFormat != sdktranslator.FormatOpenAIResponse {
		prepared.MultiAgentV2 = helps.CodexMultiAgentPolicy{}
	}
	if operation == cliproxyexecutor.RequestOperationStream && opts.SourceFormat == sdktranslator.FormatClaude &&
		cliproxyexecutor.ResponseFormatOrSource(opts) == sdktranslator.FormatClaude && e.cfg != nil && e.cfg.Codex.EstimateClaudeInputTokens {
		prepared.ClaudeInputTokensEstimate = helps.EstimateClaudeInputTokens(ctx, payload)
	}
	if !prepared.Enabled {
		return prepared, nil
	}
	if spoofThreadID == "" {
		spoofThreadID, err = helps.NewCodexUUIDv7()
		if err != nil {
			return nil, err
		}
	}
	prepared.SessionID = spoofThreadID
	prepared.ThreadID = spoofThreadID
	prepared.WindowID = spoofThreadID + ":0"
	return prepared, nil
}

func codexPreparedRequestAffinity(ctx context.Context, opts cliproxyexecutor.Options, payload []byte, turnID string) (string, string, string, string, string) {
	client := codexClientSessionIdentitySource(ctx, opts)
	var bodyTurnMetadata, bodyThreadID, bodySessionID string
	if util.JSONMayContainAnyField(payload, "client_metadata") {
		bodyTurnMetadata = gjson.GetBytes(payload, "client_metadata.x-codex-turn-metadata").String()
		bodyThreadID = gjson.GetBytes(payload, "client_metadata.thread_id").String()
		bodySessionID = gjson.GetBytes(payload, "client_metadata.session_id").String()
	}
	clientTurnMetadata := client.TurnMetadata
	threadID := firstCodexPreparedIdentityValue(
		gjson.Get(bodyTurnMetadata, "thread_id").String(),
		bodyThreadID,
		gjson.Get(clientTurnMetadata, "thread_id").String(),
		client.ThreadID,
	)
	sessionID := firstCodexPreparedIdentityValue(
		gjson.Get(bodyTurnMetadata, "session_id").String(),
		bodySessionID,
		gjson.Get(clientTurnMetadata, "session_id").String(),
		client.SessionID,
	)
	promptCacheKey := ""
	if util.JSONMayContainAnyField(payload, "prompt_cache_key") {
		promptCacheKey = strings.TrimSpace(gjson.GetBytes(payload, "prompt_cache_key").String())
	}
	executionSessionID := executionSessionIDFromOptions(opts)

	affinityKind := "turn"
	affinityValue := turnID
	for _, candidate := range []struct {
		kind  string
		value string
	}{
		{kind: "execution_session_id", value: executionSessionID},
		{kind: "thread_id", value: threadID},
		{kind: "prompt_cache_key", value: promptCacheKey},
		{kind: "session_id", value: sessionID},
	} {
		if strings.TrimSpace(candidate.value) == "" {
			continue
		}
		affinityKind = candidate.kind
		affinityValue = candidate.value
		break
	}

	tenantDigest := "anonymous"
	if apiKey := strings.TrimSpace(helps.APIKeyFromContext(ctx)); apiKey != "" {
		tenantDigest = codexFingerprintDigest(apiKey)
	}
	clientThreadID := ""
	if parsed, err := uuid.Parse(strings.TrimSpace(threadID)); err == nil && parsed != uuid.Nil {
		clientThreadID = parsed.String()
	}
	spoofThreadID := ""
	for _, candidate := range []string{executionSessionID, threadID, promptCacheKey, sessionID} {
		parsed, err := uuid.Parse(strings.TrimSpace(candidate))
		if err == nil && parsed != uuid.Nil {
			spoofThreadID = parsed.String()
			break
		}
	}
	return affinityKind, codexFingerprintDigest(affinityValue), tenantDigest, clientThreadID, spoofThreadID
}

func firstCodexPreparedIdentityValue(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func codexSessionRequestKind(opts cliproxyexecutor.Options, payload []byte) string {
	if strings.EqualFold(strings.Trim(strings.TrimSpace(opts.Alt), "/"), "responses/compact") {
		return "compaction"
	}
	if util.JSONMayContainAnyField(payload, "generate") {
		if generate := gjson.GetBytes(payload, "generate"); generate.Exists() && !generate.Bool() {
			return "prewarm"
		}
	}
	return helps.SnapshotCodexRequestScope(payload, opts.Headers).Kind
}

func codexSpoofSessionIdentityEnabled(cfg *config.Config) bool {
	return cfg != nil && cfg.Codex.SpoofSessionIdentity
}

// ShouldPrepareRequestAuth reports whether persistent Codex identity metadata
// must be completed before the credential is used.
func (e *CodexExecutor) ShouldPrepareRequestAuth(auth *cliproxyauth.Auth) bool {
	return codexInstallationIDNeedsPreparation(auth) || codexSessionIdentityPoolNeedsPreparation(auth, e.cfg) || codexAgentIdentityNeedsTask(auth)
}

// PrepareRequestAuth prepares a stable installation ID and registers a missing
// Agent Identity task for the manager to persist in one credential update.
func (e *CodexExecutor) PrepareRequestAuth(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if auth == nil {
		return nil, errors.New("codex executor: auth is nil")
	}
	updated, _ := prepareCodexInstallationID(auth)
	var err error
	updated, _, err = prepareCodexSessionIdentityPool(updated, e.cfg)
	if err != nil {
		return nil, err
	}
	if codexAgentIdentityNeedsTask(updated) {
		return e.registerAgentIdentityTask(ctx, updated)
	}
	return updated, nil
}

func codexAgentIdentityNeedsTask(auth *cliproxyauth.Auth) bool {
	if auth == nil || !codexauth.IsAgentIdentityMetadata(auth.Metadata) {
		return false
	}
	credential, err := codexauth.ParseAgentIdentityCredential(auth.Metadata)
	return err == nil && strings.TrimSpace(credential.TaskID) == ""
}

// ShouldRecoverUnauthorized limits task recovery to explicit upstream task errors.
func (e *CodexExecutor) ShouldRecoverUnauthorized(auth *cliproxyauth.Auth, err error) bool {
	return auth != nil && codexauth.IsAgentIdentityMetadata(auth.Metadata) && codexauth.IsAgentTaskUnauthorizedError(err)
}

// RecoverUnauthorized registers a replacement task for an invalid Agent Identity task.
func (e *CodexExecutor) RecoverUnauthorized(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	return e.registerAgentIdentityTask(ctx, auth)
}

func (e *CodexExecutor) registerAgentIdentityTask(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if auth == nil {
		return nil, errors.New("codex executor: auth is nil")
	}
	credential, err := codexauth.ParseAgentIdentityCredential(auth.Metadata)
	if err != nil {
		return nil, fmt.Errorf("codex executor: parse Agent Identity: %w", err)
	}
	client := e.newCodexHTTPClient(ctx, auth, false)
	if e != nil && e.agentIdentityHTTPClient != nil {
		client = e.agentIdentityHTTPClient(ctx, auth)
	}
	baseURL := ""
	if e != nil {
		baseURL = e.agentIdentityBaseURL
	}
	var taskID string
	err = codexauth.RetryAgentIdentityRegistration(ctx, 3, func(registerCtx context.Context) error {
		var errRegister error
		taskID, errRegister = codexauth.RegisterAgentTask(registerCtx, client, baseURL, credential)
		return errRegister
	})
	if err != nil {
		return nil, fmt.Errorf("codex executor: register Agent Identity task: %w", err)
	}
	updated := auth.Clone()
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	updated.Metadata["task_id"] = taskID
	return updated, nil
}

func (e *CodexExecutor) translateCodexRequestBodies(ctx context.Context, from, to sdktranslator.Format, baseModel string, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, stream bool, translated ...bool) ([]byte, []byte, []byte) {
	if from == sdktranslator.FormatCodex || from == sdktranslator.FormatOpenAIResponse {
		enabled := e.codexPreparedSessionIdentity(ctx, req, opts).OrphanDelegationCompatibility
		if enabled {
			payload := helps.RewriteCodexOrphanDelegationInput(req.Payload, true)
			if len(opts.OriginalRequest) > 0 {
				if bytes.Equal(opts.OriginalRequest, req.Payload) {
					opts.OriginalRequest = payload
				} else {
					opts.OriginalRequest = helps.RewriteCodexOrphanDelegationInput(opts.OriginalRequest, true)
				}
			}
			req.Payload = payload
		}
	}
	return translateCodexRequestBodies(from, to, baseModel, req, opts, stream, translated...)
}

func translateCodexRequestBodies(from, to sdktranslator.Format, baseModel string, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, stream bool, translated ...bool) ([]byte, []byte, []byte) {
	originalPayload := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayload = opts.OriginalRequest
	}
	isCompat := helps.APIKeyModelIsCompat(req)
	body := req.Payload
	if len(translated) == 0 || !translated[0] || from == sdktranslator.FormatCodex || from == sdktranslator.FormatOpenAIResponse {
		body = helps.TranslateRequestWithAPIKeyModelCompatibility(from, to, baseModel, req.Payload, stream, isCompat)
	}
	originalTranslated := body
	if len(originalPayload) > 0 && !bytes.Equal(originalPayload, req.Payload) {
		originalTranslated = helps.TranslateRequestWithAPIKeyModelCompatibility(from, to, baseModel, originalPayload, stream, isCompat)
	}
	return originalPayload, originalTranslated, body
}

func dropCodexRawRequestCopies(req *cliproxyexecutor.Request, opts *cliproxyexecutor.Options) {
	if req != nil {
		req.Payload = nil
	}
	if opts != nil {
		opts.OriginalRequest = nil
	}
}

func codexStreamBodyRefs(ctx context.Context, opts cliproxyexecutor.Options, originalPayload, body, releasedOriginalPayload, releasedBody []byte) (*cliproxyexecutor.ReleasableBytes, *cliproxyexecutor.ReleasableBytes, func()) {
	originalRef := cliproxyexecutor.NewReleasableBytes(originalPayload)
	bodyRef := cliproxyexecutor.NewReleasableBytes(body)
	ctrl := cliproxyexecutor.RequestBodyReleaseControllerFromOptions(opts)
	if ctrl == nil {
		ctrl = cliproxyexecutor.RequestBodyReleaseControllerFromContext(ctx)
	}
	if ctrl == nil || ctrl.LogOnly() {
		return originalRef, bodyRef, func() {}
	}
	unregister := ctrl.RegisterReleaseCallback(func([]byte) {
		originalRef.Replace(releasedOriginalPayload)
		bodyRef.Replace(releasedBody)
	})
	return originalRef, bodyRef, unregister
}

func slimCodexOriginalPayloadForTranslation(from sdktranslator.Format, original []byte) []byte {
	if len(original) == 0 {
		return nil
	}
	tools := gjson.GetBytes(original, "tools")
	if !tools.IsArray() {
		return nil
	}
	switch from {
	case sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse:
		return slimCodexOpenAITools(tools.Array())
	case sdktranslator.FormatClaude:
		return slimCodexClaudeTools(tools.Array())
	case sdktranslator.FormatGemini:
		return slimCodexGeminiTools(tools.Array())
	default:
		return nil
	}
}

func slimCodexOpenAITools(tools []gjson.Result) []byte {
	out := []byte(`{"tools":[]}`)
	index := 0
	for _, tool := range tools {
		if tool.Get("type").String() == "custom" {
			namePath := "name"
			if tool.Get("custom").IsObject() {
				namePath = "custom.name"
			}
			if name := tool.Get(namePath).String(); name != "" {
				entry, _ := sjson.SetBytes([]byte(`{"type":"custom"}`), namePath, name)
				out, _ = sjson.SetRawBytes(out, fmt.Sprintf("tools.%d", index), entry)
				index++
			}
			continue
		}
		name := strings.TrimSpace(tool.Get("function.name").String())
		if name == "" {
			continue
		}
		entry := []byte(`{"type":"function","function":{}}`)
		entry, _ = sjson.SetBytes(entry, "function.name", name)
		out, _ = sjson.SetRawBytes(out, fmt.Sprintf("tools.%d", index), entry)
		index++
	}
	if index == 0 {
		return nil
	}
	return out
}

func slimCodexClaudeTools(tools []gjson.Result) []byte {
	out := []byte(`{"tools":[]}`)
	index := 0
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Get("name").String())
		if name == "" {
			continue
		}
		entry := []byte(`{}`)
		entry, _ = sjson.SetBytes(entry, "name", name)
		out, _ = sjson.SetRawBytes(out, fmt.Sprintf("tools.%d", index), entry)
		index++
	}
	if index == 0 {
		return nil
	}
	return out
}

func slimCodexGeminiTools(tools []gjson.Result) []byte {
	out := []byte(`{"tools":[]}`)
	toolIndex := 0
	for _, tool := range tools {
		declarations := tool.Get("functionDeclarations")
		if !declarations.IsArray() {
			continue
		}
		entry := []byte(`{"functionDeclarations":[]}`)
		declarationIndex := 0
		for _, declaration := range declarations.Array() {
			name := strings.TrimSpace(declaration.Get("name").String())
			if name == "" {
				continue
			}
			item := []byte(`{}`)
			item, _ = sjson.SetBytes(item, "name", name)
			entry, _ = sjson.SetRawBytes(entry, fmt.Sprintf("functionDeclarations.%d", declarationIndex), item)
			declarationIndex++
		}
		if declarationIndex == 0 {
			continue
		}
		out, _ = sjson.SetRawBytes(out, fmt.Sprintf("tools.%d", toolIndex), entry)
		toolIndex++
	}
	if toolIndex == 0 {
		return nil
	}
	return out
}

func slimCodexBodyForStreamUsage(body []byte) []byte {
	if len(body) == 0 {
		return nil
	}
	tools := gjson.GetBytes(body, "tools")
	if !tools.IsArray() {
		return nil
	}
	out := []byte(`{"tools":[]}`)
	index := 0
	for _, tool := range tools.Array() {
		if tool.Get("type").String() != "image_generation" {
			continue
		}
		entry := []byte(`{"type":"image_generation"}`)
		if model := strings.TrimSpace(tool.Get("model").String()); model != "" {
			entry, _ = sjson.SetBytes(entry, "model", model)
		}
		out, _ = sjson.SetRawBytes(out, fmt.Sprintf("tools.%d", index), entry)
		index++
	}
	if index == 0 {
		return nil
	}
	return out
}

// PrepareRequest injects Codex credentials into the outgoing HTTP request.
func (e *CodexExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if req == nil {
		return nil
	}
	apiKey, _ := codexCreds(auth)
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	clientBetaFeatures := headerValueCaseInsensitive(req.Header, "X-Codex-Beta-Features")
	deleteHeaderCaseInsensitive(req.Header, "Authorization")
	util.ApplyCustomHeadersFromAttrs(req, attrs)
	applyCodexSoftwareIdentity(req.Header, auth, e.cfg)
	applyCodexBetaFeatures(req.Header, auth, e.cfg, clientBetaFeatures)
	authorization, err := codexauth.AuthorizationHeader(authMetadata(auth), apiKey, time.Now())
	if err != nil {
		return fmt.Errorf("codex executor: build authorization: %w", err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	return nil
}

// HttpRequest injects Codex credentials into the request and executes it.
func (e *CodexExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("codex executor: request is nil")
	}
	if ctx == nil {
		ctx = req.Context()
	}
	ctx = contextWithCodexFingerprintPersona(ctx, e.cfg, auth)
	httpReq := req.WithContext(ctx)
	if err := e.PrepareRequest(httpReq, auth); err != nil {
		return nil, err
	}
	httpClient := e.newCodexHTTPClient(ctx, auth, false)
	return helps.DoUpstreamHTTPRequest(httpClient, httpReq)
}

func (e *CodexExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	ctx = cliproxyexecutor.WithCodexStateSnapshot(ctx)
	if opts.SourceFormat == sdktranslator.FormatCodexLive {
		return resp, helps.CodexLiveNativeRouteError{}
	}
	if opts.SourceFormat == sdktranslator.FormatCodexAlphaSearch {
		return e.executeAlphaSearch(ctx, auth, req, opts)
	}
	if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
		if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent != nil {
			return resp, errCurrent
		}
		return resp, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
	}
	ctx = contextWithCodexFingerprintPersona(ctx, e.cfg, auth)
	opts, err = e.ensureCodexPreparedSessionIdentity(ctx, req, opts, cliproxyexecutor.RequestOperationExecute)
	if err != nil {
		return resp, err
	}
	ctx = helps.WithCodexPromptCacheLogRedaction(ctx, e.codexPreparedSessionIdentity(ctx, req, opts).PromptCacheLog)
	if isCodexOpenAIImageRequest(opts) {
		return e.executeOpenAIImage(ctx, auth, req, opts)
	}
	if opts.Alt == "responses/compact" {
		return e.executeCompact(ctx, auth, req, opts)
	}
	baseModel := thinking.ParseSuffix(req.Model).ModelName

	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth, false)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	to := sdktranslator.FromString("codex")
	originalPayload, originalTranslated, body := e.translateCodexRequestBodies(ctx, from, to, baseModel, req, opts, false)

	body, err = helps.ApplyRequestThinking(body, req, opts, from.String(), to.String(), e.Identifier())
	if err != nil {
		return resp, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, originalTranslated, requestedModel)
	originalTranslated = nil
	body, err = helps.RewriteCodexRequestEnvelope(body, helps.CodexRequestRewriteOptions{
		Model:              baseModel,
		Stream:             helps.CodexStreamForceEnabled,
		StripResponseState: true,
		EnsureInstructions: true,
	})
	if err != nil {
		return resp, err
	}
	body, err = e.applyDisabledImageGenerationToolPolicy(auth, body)
	if err != nil {
		return resp, err
	}
	body = helps.SanitizeCodexInputItemIDs(body)
	body = helps.SanitizeCodexReasoningEncryptedContent(ctx, "codex executor", body, helps.APIKeyModelIsCompat(req))
	body = helps.NormalizeCodexToolSelection(body)
	body = e.codexPreparedSessionIdentity(ctx, req, opts).ResponsesLite.ApplyBody(body, false)
	body, multiAgentResponse := helps.OptimizeCodexMultiAgentV2Request(body, e.codexPreparedSessionIdentity(ctx, req, opts).MultiAgentV2, helps.APIKeyModelIsCompat(req))
	replayAuthID := ""
	if auth != nil {
		replayAuthID = auth.ID
	}
	replayNamespace := helps.ReasoningReplayNamespace(ctx, e.Identifier(), replayAuthID, auth.RuntimeInstanceID())
	body, replayScope := helps.ApplyCodexReasoningReplay(ctx, from.String(), replayNamespace, baseModel, originalPayload, body, req.Metadata, opts.Metadata, opts.Headers)
	reporter.SetRequestServiceTierFromPayload(body)
	imageRequest := cliproxyauth.PayloadHasImageGenerationTool(body)

	url := strings.TrimSuffix(baseURL, "/") + "/responses"
	httpReq, upstreamBody, identityState, err := e.cacheHelper(ctx, from, url, auth, req, opts, originalPayload, body, true)
	if err != nil {
		return resp, err
	}
	if err = applyCodexHeaders(httpReq, auth, apiKey, true, e.cfg); err != nil {
		return resp, err
	}
	applyCodexIdentityConfuseHeaders(httpReq.Header, &identityState)
	upstreamBody, err = e.applyCodexHTTPSessionIdentity(ctx, auth, req, opts, httpReq, upstreamBody, &identityState)
	if err != nil {
		return resp, err
	}
	releasedOriginalPayload := slimCodexOriginalPayloadForTranslation(from, originalPayload)
	releasedBody := slimCodexBodyForStreamUsage(body)
	originalRef, bodyRef, unregisterBodies := codexStreamBodyRefs(ctx, opts, originalPayload, body, releasedOriginalPayload, releasedBody)
	defer unregisterBodies()
	defer originalRef.Release()
	defer bodyRef.Release()
	originalPayload = nil
	body = nil
	dropCodexRawRequestCopies(&req, &opts)
	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       url,
		Method:    http.MethodPost,
		Headers:   httpReq.Header.Clone(),
		Body:      upstreamBody,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})
	guardModel := gjson.GetBytes(upstreamBody, "model").String()
	if guardModel == "" {
		guardModel = baseModel
	}
	upstreamBody = nil
	httpClient := reporter.TrackHTTPClient(e.newCodexHTTPClient(ctx, auth, imageRequest))
	httpResp, err := helps.DoUpstreamHTTPRequest(httpClient, httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex executor: close response body error: %v", errClose)
		}
	}()
	helps.ObserveCodexHTTPQuota(ctx, auth, httpResp.Header)
	reporter.ObserveHTTPResponse(httpResp.StatusCode, httpResp.Header)
	helps.CaptureStateHeaders(ctx, httpResp)
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	managedStateUse := helps.ManagedStateUse(ctx, auth, req.Model, httpReq.Header)
	guard := helps.NewCodexResponseGuard(ctx, e.cfg, auth, guardModel, opts, false, "http", httpResp.StatusCode, httpResp.Header, func(evidence config.CodexResponseEvidence) {
		managedStateUse.RejectGuardEvidence(httpResp.Header, evidence)
	})
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		guard.UpstreamFailure(httpResp.StatusCode)
		b, _ := io.ReadAll(httpResp.Body)
		upstreamBody := codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), b)
		helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamBody)
		helps.DebugCodexResponseError(ctx, httpResp.StatusCode, httpResp.Header.Get("Content-Type"), upstreamBody)
		clientBody := applyCodexIdentityExposeResponsePayload(upstreamBody, identityState)
		helps.ClearCodexReasoningReplayOnInvalidSignature(replayScope, httpResp.StatusCode, clientBody)
		err = newCodexStatusErr(httpResp.StatusCode, clientBody)
		return resp, err
	}
	managedStateUse.Observe(httpResp.Header, nil)

	defer func() { guard.Finish(err) }()
	var guardErr error
	if !guard.Enforces() {
		helps.ReleaseRequestBodyAfterStreamEstablished(ctx, opts)
	}
	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte
	var completedEvent []byte
	var terminalErr *statusErr
	var terminalEvent []byte
	processEvent := func(rawLine []byte) bool {
		line := rawLine
		if terminalErr != nil || len(completedEvent) > 0 {
			helps.AppendAPIResponseChunk(ctx, e.cfg, line)
			return true
		}
		trimmedLine := bytes.TrimSpace(line)
		if !bytes.HasPrefix(trimmedLine, dataTag) {
			helps.AppendAPIResponseChunk(ctx, e.cfg, line)
			return true
		}

		eventData := bytes.TrimSpace(trimmedLine[len(dataTag):])
		if guardErr = guard.Observe(eventData, false); guardErr != nil {
			return false
		}
		helps.ObserveResponsesTokenEvent(reporter, eventData)
		eventType := gjson.GetBytes(eventData, "type").String()
		if eventType == "response.output_item.done" {
			helps.AppendAPIResponseChunk(ctx, e.cfg, line)
			collectCodexOutputItemDone(eventData, outputItemsByIndex, &outputItemsFallback)
			return true
		}

		clientEventData := applyCodexIdentityExposeResponsePayload(eventData, identityState)
		if originalEventErr, ok := codexTerminalStreamError(clientEventData); ok {
			line = codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), line)
			helps.AppendAPIResponseChunk(ctx, e.cfg, line)
			trimmedLine = bytes.TrimSpace(line)
			eventData = bytes.TrimSpace(trimmedLine[len(dataTag):])
			clientEventData = applyCodexIdentityExposeResponsePayload(eventData, identityState)
			eventErr := originalEventErr
			if sanitizedEventErr, parsed := codexTerminalStreamError(clientEventData); parsed {
				eventErr = sanitizedEventErr
			} else {
				eventErr.msg = string(bytes.TrimSpace(line))
				eventErr.responseBody = string(clientEventData)
			}
			terminalErr = &eventErr
			terminalEvent = bytes.Clone(clientEventData)
			return true
		}
		helps.AppendAPIResponseChunk(ctx, e.cfg, line)
		eventData = normalizeCodexCompletion(eventData)
		if !isCodexSuccessfulCompletion(eventData) && !helps.IsCodexPartialResponse(eventData) {
			return true
		}
		if isCodexSuccessfulCompletion(eventData) {
			rawData := bytes.TrimSpace(bytes.TrimSpace(rawLine)[len(dataTag):])
			managedStateUse.Observe(nil, rawData)
		}
		completedEvent = bytes.Clone(eventData)
		return true
	}
	scanner := bufio.NewScanner(httpResp.Body)
	scanner.Buffer(make([]byte, 64*1024), codexSSEMaxFrameBytes)
	scanner.Split(helps.SplitSSEDataEvents)
	for scanner.Scan() {
		if !processEvent(scanner.Bytes()) {
			break
		}
	}
	if guardErr != nil {
		return resp, guardErr
	}
	if errRead := scanner.Err(); errRead != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, errRead)
		return resp, errRead
	}
	if terminalErr != nil {
		helps.ClearCodexReasoningReplayOnInvalidSignature(replayScope, terminalErr.code, terminalEvent)
		err = *terminalErr
		return resp, err
	}
	if len(completedEvent) > 0 {
		if detail, ok := helps.ParseCodexUsage(completedEvent); ok {
			reporter.Publish(ctx, detail)
		}
		publishCodexImageToolUsage(ctx, reporter, bodyRef.Bytes(), completedEvent)

		completedData := patchCodexCompletedOutput(completedEvent, outputItemsByIndex, outputItemsFallback)
		helps.CaptureStateCompletion(ctx, httpResp.Header, completedData)
		completedEvent = nil
		outputItemsByIndex = nil
		outputItemsFallback = nil
		var param any
		clientCompletedData := applyCodexIdentityExposeResponsePayload(completedData, identityState)
		clientCompletedData = multiAgentResponse.Rewrite(clientCompletedData)
		out := sdktranslator.TranslateNonStream(ctx, to, from, req.Model, originalRef.Bytes(), bodyRef.Bytes(), clientCompletedData, &param)
		if from == sdktranslator.FormatOpenAIResponse {
			out = helps.EnsureResponsesUsageDetails(out)
		}
		if isCodexSuccessfulCompletion(completedData) {
			helps.ObserveManagedStateCompletion(ctx, auth, baseModel, httpReq.Header)
			helps.CacheCodexReasoningReplayFromCompleted(replayScope, completedData, ctx)
		}
		resp = cliproxyexecutor.Response{Payload: out, Headers: codexSuccessfulResponseHeaders(auth, httpResp.Header)}
		return resp, nil
	}
	err = statusErr{code: 408, msg: "stream error: stream disconnected before completion: stream closed before response.completed"}
	return resp, err
}

func (e *CodexExecutor) executeCompact(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	ctx = cliproxyexecutor.WithCodexStateSnapshot(ctx)
	if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
		if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent != nil {
			return resp, errCurrent
		}
		return resp, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
	}
	ctx = contextWithCodexFingerprintPersona(ctx, e.cfg, auth)
	baseModel := thinking.ParseSuffix(req.Model).ModelName

	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth, false)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	to := sdktranslator.FromString("openai-response")
	originalPayload, originalTranslated, body := e.translateCodexRequestBodies(ctx, from, to, baseModel, req, opts, false)

	body, err = helps.ApplyRequestThinking(body, req, opts, from.String(), to.String(), e.Identifier())
	if err != nil {
		return resp, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, originalTranslated, requestedModel)
	originalTranslated = nil
	body, _ = helps.SetStringIfDifferent(body, "model", baseModel)
	body, _ = sjson.DeleteBytes(body, "stream")
	body = normalizeCodexInstructions(body)
	body, err = e.applyDisabledImageGenerationToolPolicy(auth, body)
	if err != nil {
		return resp, err
	}
	body = helps.SanitizeCodexInputItemIDs(body)
	body = helps.SanitizeCodexReasoningEncryptedContent(ctx, "codex executor", body, helps.APIKeyModelIsCompat(req))
	body = helps.NormalizeCodexToolSelection(body)
	body = e.codexPreparedSessionIdentity(ctx, req, opts).ResponsesLite.ApplyBody(body, false)
	body, multiAgentResponse := helps.OptimizeCodexMultiAgentV2Request(body, e.codexPreparedSessionIdentity(ctx, req, opts).MultiAgentV2, helps.APIKeyModelIsCompat(req))
	reporter.SetRequestServiceTierFromPayload(body)
	imageRequest := cliproxyauth.PayloadHasImageGenerationTool(body)

	url := strings.TrimSuffix(baseURL, "/") + "/responses/compact"
	httpReq, upstreamBody, identityState, err := e.cacheHelper(ctx, from, url, auth, req, opts, originalPayload, body, true)
	if err != nil {
		return resp, err
	}
	if err = applyCodexHeaders(httpReq, auth, apiKey, false, e.cfg); err != nil {
		return resp, err
	}
	applyCodexIdentityConfuseHeaders(httpReq.Header, &identityState)
	upstreamBody, err = e.applyCodexHTTPSessionIdentity(ctx, auth, req, opts, httpReq, upstreamBody, &identityState)
	if err != nil {
		return resp, err
	}
	releasedOriginalPayload := slimCodexOriginalPayloadForTranslation(from, originalPayload)
	releasedBody := slimCodexBodyForStreamUsage(body)
	originalRef, bodyRef, unregisterBodies := codexStreamBodyRefs(ctx, opts, originalPayload, body, releasedOriginalPayload, releasedBody)
	defer unregisterBodies()
	defer originalRef.Release()
	defer bodyRef.Release()
	originalPayload = nil
	body = nil
	dropCodexRawRequestCopies(&req, &opts)
	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       url,
		Method:    http.MethodPost,
		Headers:   httpReq.Header.Clone(),
		Body:      upstreamBody,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})
	upstreamBody = nil
	httpClient := reporter.TrackHTTPClient(e.newCodexHTTPClient(ctx, auth, imageRequest))
	httpResp, err := helps.DoUpstreamHTTPRequest(httpClient, httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex executor: close response body error: %v", errClose)
		}
	}()
	helps.ObserveCodexHTTPQuota(ctx, auth, httpResp.Header)
	reporter.ObserveHTTPResponse(httpResp.StatusCode, httpResp.Header)
	helps.CaptureStateHeaders(ctx, httpResp)
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	managedStateUse := helps.ManagedStateUse(ctx, auth, req.Model, httpReq.Header)
	managedStateUse.Observe(httpResp.Header, nil)
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		b, _ := io.ReadAll(httpResp.Body)
		upstreamBody := codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), b)
		helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamBody)
		helps.DebugCodexResponseError(ctx, httpResp.StatusCode, httpResp.Header.Get("Content-Type"), upstreamBody)
		clientBody := applyCodexIdentityExposeResponsePayload(upstreamBody, identityState)
		err = newCodexStatusErr(httpResp.StatusCode, clientBody)
		return resp, err
	}
	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	managedStateUse.Observe(nil, data)
	upstreamData := data
	helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamData)
	reporter.Publish(ctx, helps.ParseOpenAIUsage(upstreamData))
	reporter.EnsurePublished(ctx)
	var param any
	clientData := applyCodexIdentityExposeResponsePayload(upstreamData, identityState)
	clientData = multiAgentResponse.Rewrite(clientData)
	out := sdktranslator.TranslateNonStream(ctx, to, from, req.Model, originalRef.Bytes(), bodyRef.Bytes(), clientData, &param)
	if from == sdktranslator.FormatOpenAIResponse {
		out = helps.EnsureResponsesUsageDetails(out)
	}
	resp = cliproxyexecutor.Response{Payload: out, Headers: codexSuccessfulResponseHeaders(auth, httpResp.Header)}
	return resp, nil
}

func (e *CodexExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	return e.executeStream(ctx, auth, req, opts, false)
}

func (e *CodexExecutor) executeStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, translated bool) (_ *cliproxyexecutor.StreamResult, err error) {
	ctx = cliproxyexecutor.WithCodexStateSnapshot(ctx)
	isGrokClient := grokbuild.IsGrokClientContext(ctx, opts.Headers)
	if opts.SourceFormat == sdktranslator.FormatCodexLive {
		return nil, helps.CodexLiveNativeRouteError{}
	}
	if opts.SourceFormat == sdktranslator.FormatCodexAlphaSearch {
		return nil, statusErr{code: http.StatusNotImplemented, msg: "Codex search does not support streaming", skipAuthResult: true}
	}
	if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) {
		if errCurrent := codexWebsocketExecutionStateError(ctx, auth); errCurrent != nil {
			return nil, errCurrent
		}
		return nil, cliproxyexecutor.NewUpstreamWebsocketReplayRequiredError()
	}
	ctx = contextWithCodexFingerprintPersona(ctx, e.cfg, auth)
	opts, err = e.ensureCodexPreparedSessionIdentity(ctx, req, opts, cliproxyexecutor.RequestOperationStream)
	if err != nil {
		return nil, err
	}
	ctx = helps.WithCodexPromptCacheLogRedaction(ctx, e.codexPreparedSessionIdentity(ctx, req, opts).PromptCacheLog)
	if isCodexOpenAIImageRequest(opts) {
		return e.executeOpenAIImageStream(ctx, auth, req, opts)
	}
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "streaming not supported for /responses/compact"}
	}
	baseModel := thinking.ParseSuffix(req.Model).ModelName

	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth, true)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	to := sdktranslator.FromString("codex")
	originalPayload, originalTranslated, body := e.translateCodexRequestBodies(ctx, from, to, baseModel, req, opts, true, translated)

	body, err = helps.ApplyRequestThinking(body, req, opts, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, originalTranslated, requestedModel)
	originalTranslated = nil
	body, err = helps.RewriteCodexRequestEnvelope(body, helps.CodexRequestRewriteOptions{
		Model:              baseModel,
		Stream:             helps.CodexStreamPreserve,
		StripResponseState: true,
		EnsureInstructions: true,
	})
	if err != nil {
		return nil, err
	}
	body, err = e.applyDisabledImageGenerationToolPolicy(auth, body)
	if err != nil {
		return nil, err
	}
	body = helps.SanitizeCodexInputItemIDs(body)
	body = helps.SanitizeCodexReasoningEncryptedContent(ctx, "codex executor", body, helps.APIKeyModelIsCompat(req))
	body = helps.NormalizeCodexToolSelection(body)
	body = e.codexPreparedSessionIdentity(ctx, req, opts).ResponsesLite.ApplyBody(body, false)
	body, multiAgentResponse := helps.OptimizeCodexMultiAgentV2Request(body, e.codexPreparedSessionIdentity(ctx, req, opts).MultiAgentV2, helps.APIKeyModelIsCompat(req))
	replayAuthID := ""
	if auth != nil {
		replayAuthID = auth.ID
	}
	replayNamespace := helps.ReasoningReplayNamespace(ctx, e.Identifier(), replayAuthID, auth.RuntimeInstanceID())
	body, replayScope := helps.ApplyCodexReasoningReplay(ctx, from.String(), replayNamespace, baseModel, originalPayload, body, req.Metadata, opts.Metadata, opts.Headers)
	reporter.SetRequestServiceTierFromPayload(body)
	imageRequest := cliproxyauth.PayloadHasImageGenerationTool(body)
	imageStreamPassthrough := metadataBool(opts.Metadata, cliproxyexecutor.ImageGenerationStreamPassthroughMetadataKey) && from == sdktranslator.FormatOpenAIResponse && imageRequest
	trustUpstreamSSE := metadataBool(opts.Metadata, cliproxyexecutor.TrustUpstreamSSEMetadataKey) && from == sdktranslator.FormatOpenAIResponse

	url := strings.TrimSuffix(baseURL, "/") + "/responses"
	httpReq, upstreamBody, identityState, err := e.cacheHelper(ctx, from, url, auth, req, opts, originalPayload, body, true)
	if err != nil {
		return nil, err
	}
	if err = applyCodexHeaders(httpReq, auth, apiKey, true, e.cfg); err != nil {
		return nil, err
	}
	applyCodexIdentityConfuseHeaders(httpReq.Header, &identityState)
	upstreamBody, err = e.applyCodexHTTPSessionIdentity(ctx, auth, req, opts, httpReq, upstreamBody, &identityState)
	if err != nil {
		return nil, err
	}
	releasedOriginalPayload := slimCodexOriginalPayloadForTranslation(from, originalPayload)
	releasedBody := slimCodexBodyForStreamUsage(body)
	streamOriginalPayload, streamBody, unregisterStreamBodies := codexStreamBodyRefs(ctx, opts, originalPayload, body, releasedOriginalPayload, releasedBody)
	cleanupBodies := func() {
		unregisterStreamBodies()
		streamOriginalPayload.Release()
		streamBody.Release()
	}
	originalPayload = nil
	body = nil
	dropCodexRawRequestCopies(&req, &opts)
	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       url,
		Method:    http.MethodPost,
		Headers:   httpReq.Header.Clone(),
		Body:      upstreamBody,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})
	guardModel := gjson.GetBytes(upstreamBody, "model").String()
	if guardModel == "" {
		guardModel = baseModel
	}
	upstreamBody = nil

	httpClient := reporter.TrackHTTPClient(e.newCodexHTTPClient(ctx, auth, imageRequest))
	httpResp, err := helps.DoUpstreamHTTPRequest(httpClient, httpReq)
	if err != nil {
		cleanupBodies()
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	helps.ObserveCodexHTTPQuota(ctx, auth, httpResp.Header)
	reporter.ObserveHTTPResponse(httpResp.StatusCode, httpResp.Header)
	helps.CaptureStateHeaders(ctx, httpResp)
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	managedStateUse := helps.ManagedStateUse(ctx, auth, req.Model, httpReq.Header)
	guard := helps.NewCodexResponseGuard(ctx, e.cfg, auth, guardModel, opts, true, "sse", httpResp.StatusCode, httpResp.Header, func(evidence config.CodexResponseEvidence) {
		managedStateUse.RejectGuardEvidence(httpResp.Header, evidence)
	})
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		guard.UpstreamFailure(httpResp.StatusCode)
		defer cleanupBodies()
		data, readErr := io.ReadAll(httpResp.Body)
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex executor: close response body error: %v", errClose)
		}
		if readErr != nil {
			helps.RecordAPIResponseError(ctx, e.cfg, readErr)
			return nil, readErr
		}
		upstreamBody := codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), data)
		helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamBody)
		helps.DebugCodexResponseError(ctx, httpResp.StatusCode, httpResp.Header.Get("Content-Type"), upstreamBody)
		clientBody := applyCodexIdentityExposeResponsePayload(upstreamBody, identityState)
		helps.ClearCodexReasoningReplayOnInvalidSignature(replayScope, httpResp.StatusCode, clientBody)
		err = newCodexStatusErr(httpResp.StatusCode, clientBody)
		return nil, err
	}
	managedStateUse.Observe(httpResp.Header, nil)

	if state, ok := opts.Metadata[cliproxyexecutor.ImageGenerationStreamPassthroughStateMetadataKey].(*cliproxyexecutor.ImageGenerationStreamPassthroughState); ok {
		state.SetEnabled(imageStreamPassthrough)
	}
	bootstrapEnabled := e.codexPreparedSessionIdentity(ctx, req, opts).StreamBootstrapBuffering
	if bootstrapEnabled && helps.RequestBodyReplayable(ctx, opts) || guard.Enforces() {
		bodyReplay, failure, errProbe := helps.ProbeCodexSSEBootstrap(ctx, httpResp.Body, func() bool {
			return bootstrapEnabled && helps.RequestBodyReplayable(ctx, opts)
		}, guard)
		if errProbe != nil || failure != nil {
			cleanupBodies()
			if errClose := httpResp.Body.Close(); errClose != nil {
				log.Errorf("codex executor: close bootstrap response body error: %v", errClose)
			}
			if errProbe != nil {
				guard.Finish(errProbe)
				helps.RecordAPIResponseError(ctx, e.cfg, errProbe)
				return nil, errProbe
			}
			_ = guard.Observe(failure.Payload, false)
			upstreamError := codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), failure.Payload)
			helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamError)
			clientError := applyCodexIdentityExposeResponsePayload(upstreamError, identityState)
			return nil, statusErrWithHeaders{statusErr: newCodexStatusErr(failure.Status, helps.CodexBootstrapErrorBody(clientError)), headers: httpResp.Header.Clone()}
		}
		httpResp.Body = bodyReplay
	}
	helps.ReleaseRequestBodyAfterStreamEstablished(ctx, opts)
	out := make(chan cliproxyexecutor.StreamChunk, cliproxyexecutor.StreamBufferSize)
	go func() {
		defer close(out)
		defer func() { guard.Finish(ctx.Err()) }()
		defer cleanupBodies()
		defer func() {
			if errClose := httpResp.Body.Close(); errClose != nil {
				log.Errorf("codex executor: close response body error: %v", errClose)
			}
		}()
		scanner := bufio.NewScanner(httpResp.Body)
		scanner.Buffer(make([]byte, 64*1024), codexSSEMaxFrameBytes)
		if trustUpstreamSSE {
			scanner.Split(splitCodexSSELinesPreserveEndings)
		} else {
			scanner.Split(helps.SplitSSEDataEvents)
		}
		claudeInputTokens := helps.ClaudeInputTokenState{Estimate: e.codexPreparedSessionIdentity(ctx, req, opts).ClaudeInputTokensEstimate}
		var param any
		outputItemsByIndex := make(map[int64][]byte)
		var outputItemsFallback [][]byte
		var trustedFrame []byte
		var pendingImageCompletionEvent []byte
		var pendingTranslatedCompletionEvent []byte
		emit := func(chunk cliproxyexecutor.StreamChunk) bool {
			guard.Finish(chunk.Err)
			if ctx == nil {
				out <- chunk
				if chunk.Err == nil && len(chunk.Payload) > 0 {
					guard.Commit()
				}
				return true
			}
			select {
			case <-ctx.Done():
				return false
			case out <- chunk:
				if chunk.Err == nil && len(chunk.Payload) > 0 {
					guard.Commit()
				}
				return true
			}
		}
		emitSuccessfulTerminal := func() bool {
			reporter.EnsurePublished(ctx)
			if metadataBool(opts.Metadata, cliproxyexecutor.StreamTerminalMarkerMetadataKey) {
				return emit(cliproxyexecutor.SuccessfulStreamTerminalChunk())
			}
			return true
		}
		flushTrustedFrame := func() (bool, bool) {
			if len(bytes.TrimSpace(trustedFrame)) == 0 {
				trustedFrame = nil
				return true, false
			}
			frame := trustedFrame
			trustedFrame = nil
			if data, ok := codexSSEFrameDataPayload(frame); ok {
				if errGuard := guard.Observe(data, false); errGuard != nil {
					reporter.PublishFailure(ctx, errGuard)
					_ = emit(cliproxyexecutor.StreamChunk{Err: errGuard})
					return false, false
				}
			}
			if managedStateUse.Version > 0 {
				if data, ok := codexSSEFrameDataPayload(frame); ok && isCodexSuccessfulCompletion(normalizeCodexCompletion(data)) {
					managedStateUse.Observe(nil, data)
				}
			}
			if transformed, ok := grokbuild.TransformKeepaliveSSEFrame(frame, isGrokClient); ok {
				return emit(cliproxyexecutor.StreamChunk{Payload: transformed}), false
			}
			frame = applyCodexIdentityExposeResponsePayload(frame, identityState)
			frame = multiAgentResponse.RewriteSSEFrame(frame)
			hasData := false
			terminal := false
			if !reporter.IsTTFTSet() || codexTrustedSSEFrameNeedsInspection(frame) {
				data, frameHasData := codexSSEFrameDataPayload(frame)
				hasData = frameHasData
				if hasData {
					helps.ObserveResponsesTokenEvent(reporter, data)
					if terminalErr, ok := codexTerminalStreamError(data); ok {
						helps.ClearCodexReasoningReplayOnInvalidSignature(replayScope, terminalErr.code, data)
						reporter.PublishFailure(ctx, terminalErr)
						_ = emit(cliproxyexecutor.StreamChunk{Err: terminalErr})
						return false, false
					}
					publishCodexStreamUsage(reporter, streamBody.Bytes(), data)
					terminal = isCodexSuccessfulCompletion(data) || helps.IsCodexPartialResponse(data)
				}
			}
			return emit(cliproxyexecutor.StreamChunk{Payload: frame}), terminal
		}
		for scanner.Scan() {
			line := scanner.Bytes()
			if !trustUpstreamSSE {
				if bytes.HasPrefix(line, dataTag) {
					if errGuard := guard.Observe(bytes.TrimSpace(line[len(dataTag):]), false); errGuard != nil {
						reporter.PublishFailure(ctx, errGuard)
						_ = emit(cliproxyexecutor.StreamChunk{Err: errGuard})
						return
					}
				}
				if managedStateUse.Version > 0 && bytes.HasPrefix(line, dataTag) {
					data := bytes.TrimSpace(line[len(dataTag):])
					if isCodexSuccessfulCompletion(normalizeCodexCompletion(data)) {
						managedStateUse.Observe(nil, data)
					}
				}
				line = multiAgentResponse.RewriteSSEFrame(line)
			}
			clientLine := line
			if !trustUpstreamSSE {
				clientLine = applyCodexIdentityExposeResponsePayload(line, identityState)
			}
			if bytes.HasPrefix(clientLine, dataTag) {
				clientData := bytes.TrimSpace(clientLine[len(dataTag):])
				if !trustUpstreamSSE {
					helps.ObserveResponsesTokenEvent(reporter, clientData)
				}
				// Multi-line trusted frames are classified after their data fields are joined.
				if originalTerminalErr, ok := codexTerminalStreamError(clientData); ok && (!trustUpstreamSSE || gjson.ValidBytes(clientData)) {
					helps.ObserveResponsesTokenEvent(reporter, clientData)
					line = codexauth.SanitizeAgentIdentityErrorBody(authMetadata(auth), line)
					clientLine = applyCodexIdentityExposeResponsePayload(line, identityState)
					clientData = bytes.TrimSpace(clientLine[len(dataTag):])
					terminalErr := originalTerminalErr
					if sanitizedTerminalErr, parsed := codexTerminalStreamError(clientData); parsed {
						terminalErr = sanitizedTerminalErr
					} else {
						terminalErr.msg = string(bytes.TrimSpace(line))
						terminalErr.responseBody = string(clientData)
					}
					helps.AppendAPIResponseChunk(ctx, e.cfg, line)
					helps.ClearCodexReasoningReplayOnInvalidSignature(replayScope, terminalErr.code, clientData)
					reporter.PublishFailure(ctx, terminalErr)
					_ = emit(cliproxyexecutor.StreamChunk{Err: terminalErr})
					return
				}
			}
			helps.AppendAPIResponseChunk(ctx, e.cfg, line)
			if !trustUpstreamSSE {
				if transformed, ok := grokbuild.TransformKeepaliveSSELine(clientLine, isGrokClient); ok {
					if !emit(cliproxyexecutor.StreamChunk{Payload: transformed}) {
						return
					}
					continue
				}
			}
			if trustUpstreamSSE {
				nextTrustedFrame, errFrame := appendBoundedCodexTrustedSSEFrame(
					trustedFrame,
					clientLine,
					codexSSEMaxFrameBytes,
				)
				if errFrame != nil {
					frameErr := codexStreamStatusErr(
						http.StatusBadGateway,
						errFrame.Error(),
						"stream_frame_too_large",
						"server_error",
						nil,
					)
					frameErr.skipAuthResult = true
					helps.RecordAPIResponseError(ctx, e.cfg, frameErr)
					reporter.PublishFailure(ctx, frameErr)
					_ = emit(cliproxyexecutor.StreamChunk{Err: frameErr})
					return
				}
				trustedFrame = nextTrustedFrame
				if len(bytes.TrimSpace(line)) == 0 {
					emitted, terminal := flushTrustedFrame()
					if !emitted {
						return
					}
					if terminal && scanner.Err() == nil {
						emitSuccessfulTerminal()
						return
					}
					continue
				}
				continue
			}
			if imageStreamPassthrough {
				if codexSSEEventCompletion(clientLine) {
					pendingImageCompletionEvent = append(pendingImageCompletionEvent[:0], normalizeCodexSSEEventLine(clientLine)...)
					continue
				}
				clientLine = normalizeCodexSSEPassThroughLine(clientLine)
				if from == sdktranslator.FormatOpenAIResponse {
					clientLine = helps.EnsureResponsesUsageDetails(clientLine)
				}
				terminal := false
				trimmedClientLine := bytes.TrimSpace(clientLine)
				if bytes.HasPrefix(trimmedClientLine, dataTag) {
					data := bytes.TrimSpace(trimmedClientLine[len(dataTag):])
					terminal = isCodexSuccessfulCompletion(data) || helps.IsCodexPartialResponse(data)
					switch gjson.GetBytes(data, "type").String() {
					case "response.output_item.done":
						collectCodexOutputItemDone(data, outputItemsByIndex, &outputItemsFallback)
					case "response.completed", "response.incomplete":
						publishCodexStreamUsage(reporter, streamBody.Bytes(), data)
						data = patchCodexCompletedOutput(data, outputItemsByIndex, outputItemsFallback)
						clientLine = append([]byte("data: "), data...)
					}
					if len(pendingImageCompletionEvent) > 0 {
						if terminal {
							pendingEvent := []byte("event: " + gjson.GetBytes(data, "type").String())
							pendingEvent = append(pendingEvent, '\n')
							if !emit(cliproxyexecutor.StreamChunk{Payload: pendingEvent}) {
								return
							}
						}
						pendingImageCompletionEvent = pendingImageCompletionEvent[:0]
					}
				} else if len(trimmedClientLine) == 0 || bytes.HasPrefix(trimmedClientLine, []byte("event:")) {
					pendingImageCompletionEvent = pendingImageCompletionEvent[:0]
				}
				payload := append([]byte(nil), clientLine...)
				payload = append(payload, '\n')
				if terminal {
					payload = append(payload, '\n')
				}
				if !emit(cliproxyexecutor.StreamChunk{Payload: payload}) {
					return
				}
				if terminal && scanner.Err() == nil {
					emitSuccessfulTerminal()
					return
				}
				continue
			}
			if codexSSEEventCompletion(clientLine) {
				pendingTranslatedCompletionEvent = append(pendingTranslatedCompletionEvent[:0], normalizeCodexSSEEventLine(clientLine)...)
				continue
			}
			translatedLine := bytes.Clone(line)
			terminal := false
			var completedReplay []byte

			if bytes.HasPrefix(line, dataTag) {
				data := bytes.TrimSpace(line[5:])
				terminal = isCodexSuccessfulCompletion(data) || helps.IsCodexPartialResponse(data)
				if terminal {
					data = normalizeCodexCompletion(data)
				}
				switch gjson.GetBytes(data, "type").String() {
				case "response.output_item.done":
					collectCodexOutputItemDone(data, outputItemsByIndex, &outputItemsFallback)
				case "response.completed", "response.incomplete":
					if detail, ok := helps.ParseCodexUsage(data); ok {
						reporter.Observe(detail)
					}
					observeCodexImageToolUsage(reporter, streamBody.Bytes(), data)
					data = patchCodexCompletedOutput(data, outputItemsByIndex, outputItemsFallback)
					if isCodexSuccessfulCompletion(data) {
						helps.ObserveManagedStateCompletion(ctx, auth, baseModel, httpReq.Header)
						completedReplay = data
					}
					translatedLine = append([]byte("data: "), data...)
				}
				if len(pendingTranslatedCompletionEvent) > 0 {
					if terminal {
						pendingTranslatedCompletionEvent = []byte("event: " + gjson.GetBytes(data, "type").String())
						eventChunks := sdktranslator.TranslateStream(ctx, to, from, req.Model, streamOriginalPayload.Bytes(), streamBody.Bytes(), pendingTranslatedCompletionEvent, &param)
						eventChunks = claudeInputTokens.Apply(eventChunks)
						for i := range eventChunks {
							if !emit(cliproxyexecutor.StreamChunk{Payload: eventChunks[i]}) {
								return
							}
						}
					}
					pendingTranslatedCompletionEvent = pendingTranslatedCompletionEvent[:0]
				}
			} else {
				trimmedClientLine := bytes.TrimSpace(clientLine)
				if len(trimmedClientLine) == 0 || bytes.HasPrefix(trimmedClientLine, []byte("event:")) {
					pendingTranslatedCompletionEvent = pendingTranslatedCompletionEvent[:0]
				}
			}

			translatedLine = applyCodexIdentityExposeResponsePayload(translatedLine, identityState)
			chunks := sdktranslator.TranslateStream(ctx, to, from, req.Model, streamOriginalPayload.Bytes(), streamBody.Bytes(), translatedLine, &param)
			chunks = claudeInputTokens.Apply(chunks)
			for i := range chunks {
				chunkPayload := chunks[i]
				if from == sdktranslator.FormatOpenAIResponse {
					chunkPayload = helps.EnsureResponsesUsageDetails(chunkPayload)
				}
				if !emit(cliproxyexecutor.StreamChunk{Payload: chunkPayload}) {
					return
				}
			}
			if terminal && scanner.Err() == nil {
				if emitSuccessfulTerminal() && len(completedReplay) > 0 {
					helps.CacheCodexReasoningReplayFromCompleted(replayScope, completedReplay, ctx)
				}
				return
			}
		}
		if trustUpstreamSSE && scanner.Err() == nil {
			emitted, terminal := flushTrustedFrame()
			if !emitted {
				return
			}
			if terminal {
				emitSuccessfulTerminal()
				return
			}
		}
		if errScan := scanner.Err(); errScan != nil {
			helps.RecordAPIResponseError(ctx, e.cfg, errScan)
			reporter.PublishFailure(ctx, errScan)
			_ = emit(cliproxyexecutor.StreamChunk{Err: errScan})
		} else {
			errIncomplete := helps.IncompleteStreamError("codex")
			helps.RecordAPIResponseError(ctx, e.cfg, errIncomplete)
			reporter.PublishFailure(ctx, errIncomplete)
			_ = emit(cliproxyexecutor.StreamChunk{Err: errIncomplete})
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: codexSuccessfulResponseHeaders(auth, httpResp.Header), Chunks: out}, nil
}

func splitCodexSSELinesPreserveEndings(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for index, value := range data {
		switch value {
		case '\n':
			return index + 1, data[:index+1], nil
		case '\r':
			if index+1 == len(data) && !atEOF {
				return 0, nil, nil
			}
			lineEnd := index + 1
			if lineEnd < len(data) && data[lineEnd] == '\n' {
				lineEnd++
			}
			return lineEnd, data[:lineEnd], nil
		}
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

func appendBoundedCodexTrustedSSEFrame(frame, line []byte, maxBytes int) ([]byte, error) {
	if maxBytes < 1 || len(frame) > maxBytes || len(line) > maxBytes-len(frame) {
		return frame, fmt.Errorf("codex trusted SSE frame exceeds %d bytes", maxBytes)
	}
	return append(frame, line...), nil
}

func normalizeCodexSSEEventLine(line []byte) []byte {
	trimmed := bytes.TrimSpace(line)
	if !bytes.HasPrefix(trimmed, []byte("event:")) || strings.TrimSpace(string(trimmed[len("event:"):])) != "response.done" {
		return line
	}
	return []byte("event: response.completed")
}

func normalizeCodexSSEPassThroughLine(line []byte) []byte {
	trimmed := bytes.TrimSpace(line)
	if !bytes.HasPrefix(trimmed, dataTag) {
		return line
	}
	data := bytes.TrimSpace(trimmed[len(dataTag):])
	if !isCodexSuccessfulCompletion(data) && !helps.IsCodexPartialResponse(data) {
		return line
	}
	return append([]byte("data: "), normalizeCodexCompletion(data)...)
}

func (e *CodexExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if opts.SourceFormat == sdktranslator.FormatCodexLive {
		return cliproxyexecutor.Response{}, helps.CodexLiveNativeRouteError{}
	}
	if opts.SourceFormat == sdktranslator.FormatCodexAlphaSearch {
		return cliproxyexecutor.Response{}, statusErr{code: http.StatusNotImplemented, msg: "Codex search does not support token counting", skipAuthResult: true}
	}
	baseModel := thinking.ParseSuffix(req.Model).ModelName

	from := opts.SourceFormat
	to := sdktranslator.FromString("codex")
	body := helps.TranslateRequestWithAPIKeyModelCompatibility(from, to, baseModel, req.Payload, false, helps.APIKeyModelIsCompat(req))

	body, err := helps.ApplyRequestThinking(body, req, opts, from.String(), to.String(), e.Identifier())
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}

	body, _ = helps.SetStringIfDifferent(body, "model", baseModel)
	body, _ = sjson.DeleteBytes(body, "previous_response_id")
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	body, _ = sjson.DeleteBytes(body, "stream_options")
	body, _ = helps.SetBoolIfDifferent(body, "stream", false)
	body = normalizeCodexInstructions(body)

	if helps.APIKeyModelIsCompat(req) {
		body, _ = helps.OptimizeCodexMultiAgentV2Request(body, e.codexPreparedSessionIdentity(ctx, req, opts).MultiAgentV2, true)
	}
	enc, err := tokenizerForCodexModel(baseModel)
	if err != nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("codex executor: tokenizer init failed: %w", err)
	}

	count, err := countCodexInputTokens(enc, body)
	if err != nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("codex executor: token counting failed: %w", err)
	}

	usageJSON := fmt.Sprintf(`{"response":{"usage":{"input_tokens":%d,"output_tokens":0,"total_tokens":%d}}}`, count, count)
	translated := sdktranslator.TranslateTokenCount(ctx, to, from, count, []byte(usageJSON))
	return cliproxyexecutor.Response{Payload: translated}, nil
}

func tokenizerForCodexModel(model string) (tokenizer.Codec, error) {
	sanitized := strings.ToLower(strings.TrimSpace(model))
	switch {
	case sanitized == "":
		return tokenizer.Get(tokenizer.Cl100kBase)
	case strings.HasPrefix(sanitized, "gpt-5"):
		return tokenizer.ForModel(tokenizer.GPT5)
	case strings.HasPrefix(sanitized, "gpt-4.1"):
		return tokenizer.ForModel(tokenizer.GPT41)
	case strings.HasPrefix(sanitized, "gpt-4o"):
		return tokenizer.ForModel(tokenizer.GPT4o)
	case strings.HasPrefix(sanitized, "gpt-4"):
		return tokenizer.ForModel(tokenizer.GPT4)
	case strings.HasPrefix(sanitized, "gpt-3.5"), strings.HasPrefix(sanitized, "gpt-3"):
		return tokenizer.ForModel(tokenizer.GPT35Turbo)
	default:
		return tokenizer.Get(tokenizer.Cl100kBase)
	}
}

func countCodexInputTokens(enc tokenizer.Codec, body []byte) (int64, error) {
	if enc == nil {
		return 0, fmt.Errorf("encoder is nil")
	}
	if len(body) == 0 {
		return 0, nil
	}

	root := gjson.ParseBytes(body)
	var segments []string

	if inst := strings.TrimSpace(root.Get("instructions").String()); inst != "" {
		segments = append(segments, inst)
	}

	inputItems := root.Get("input")
	if inputItems.IsArray() {
		arr := inputItems.Array()
		for i := range arr {
			item := arr[i]
			switch item.Get("type").String() {
			case "message":
				content := item.Get("content")
				if content.IsArray() {
					parts := content.Array()
					for j := range parts {
						part := parts[j]
						if text := strings.TrimSpace(part.Get("text").String()); text != "" {
							segments = append(segments, text)
						}
					}
				}
			case "function_call":
				if name := strings.TrimSpace(item.Get("name").String()); name != "" {
					segments = append(segments, name)
				}
				if args := strings.TrimSpace(item.Get("arguments").String()); args != "" {
					segments = append(segments, args)
				}
			case "function_call_output":
				if out := strings.TrimSpace(item.Get("output").String()); out != "" {
					segments = append(segments, out)
				}
			default:
				if text := strings.TrimSpace(item.Get("text").String()); text != "" {
					segments = append(segments, text)
				}
			}
		}
	}

	tools := root.Get("tools")
	if tools.IsArray() {
		tarr := tools.Array()
		for i := range tarr {
			tool := tarr[i]
			if name := strings.TrimSpace(tool.Get("name").String()); name != "" {
				segments = append(segments, name)
			}
			if desc := strings.TrimSpace(tool.Get("description").String()); desc != "" {
				segments = append(segments, desc)
			}
			if params := tool.Get("parameters"); params.Exists() {
				val := params.Raw
				if params.Type == gjson.String {
					val = params.String()
				}
				if trimmed := strings.TrimSpace(val); trimmed != "" {
					segments = append(segments, trimmed)
				}
			}
		}
	}

	textFormat := root.Get("text.format")
	if textFormat.Exists() {
		if name := strings.TrimSpace(textFormat.Get("name").String()); name != "" {
			segments = append(segments, name)
		}
		if schema := textFormat.Get("schema"); schema.Exists() {
			val := schema.Raw
			if schema.Type == gjson.String {
				val = schema.String()
			}
			if trimmed := strings.TrimSpace(val); trimmed != "" {
				segments = append(segments, trimmed)
			}
		}
	}

	text := strings.Join(segments, "\n")
	if text == "" {
		return 0, nil
	}

	count, err := enc.Count(text)
	if err != nil {
		return 0, err
	}
	return int64(count), nil
}

func (e *CodexExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	log.Debugf("codex executor: refresh called")
	if auth == nil {
		return nil, statusErr{code: 500, msg: "codex executor: auth is nil"}
	}
	var refreshToken string
	if auth.Metadata != nil {
		if v, ok := auth.Metadata["refresh_token"].(string); ok && v != "" {
			refreshToken = v
		}
	}
	if refreshToken == "" {
		return auth, nil
	}
	svc := codexauth.NewCodexAuthWithProxyURL(e.cfg, auth.EffectiveProxyURL())
	if codexEnforceSoftwareIdentity(e.cfg) {
		if userAgent := codexCustomHeaderValue(auth, "User-Agent"); userAgent != "" {
			svc.SetSoftwareIdentityUserAgent(userAgent)
		}
	}
	td, err := svc.RefreshTokensWithRetry(ctx, refreshToken, 3)
	if err != nil {
		return nil, err
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["id_token"] = td.IDToken
	auth.Metadata["access_token"] = td.AccessToken
	if td.RefreshToken != "" {
		auth.Metadata["refresh_token"] = td.RefreshToken
	}
	if td.AccountID != "" {
		auth.Metadata["account_id"] = td.AccountID
	}
	auth.Metadata["email"] = td.Email
	// Use unified key in files
	auth.Metadata["expired"] = td.Expire
	auth.Metadata["type"] = "codex"
	now := time.Now().Format(time.RFC3339)
	auth.Metadata["last_refresh"] = now
	return auth, nil
}

type codexIdentityConfuseState struct {
	protectedPromptCacheKey string
	protectedSessionID      string
	enabled                 bool
	authID                  string
	originalPromptCacheKey  string
	promptCacheKey          string
	turnIDBase              string
	turnIDs                 []codexIdentityReplacement
	contextWindows          []helps.CodexResponseIdentityReplacement
}

type codexIdentityReplacement struct {
	original string
	confused string
}

type codexSessionIdentityState struct {
	enabled        bool
	projectSession bool
	converged      bool
	identity       helps.CodexSessionIdentity
	turnMetadata   string
}

func (e *CodexExecutor) applyCodexHTTPSessionIdentity(
	ctx context.Context,
	auth *cliproxyauth.Auth,
	req cliproxyexecutor.Request,
	opts cliproxyexecutor.Options,
	httpReq *http.Request,
	rawJSON []byte,
	identityConfuse *codexIdentityConfuseState,
) ([]byte, error) {
	prepared := e.codexPreparedSessionIdentity(ctx, req, opts)
	prepared.ResponsesLite.ApplyHeaders(httpReq.Header)
	applyCodexSoftwareIdentity(httpReq.Header, auth, e.cfg, req.Model)
	projected, state, err := e.projectCodexSessionIdentity(ctx, auth, req, opts, rawJSON, identityConfuse)
	if err != nil {
		closeCodexRequestBody(httpReq)
		return nil, err
	}
	ensureCodexTurnStateHeader(httpReq.Header, opts.Headers)
	applyCodexSessionIdentityHeaders(httpReq.Header, state, false)
	helps.RestoreCodexParentThreadHeader(httpReq.Header, projected)
	projected, err = prepared.PromptCacheKey.ApplyFinal(projected, httpReq.Header)
	if err != nil {
		closeCodexRequestBody(httpReq)
		invalid := codexStreamStatusErr(http.StatusInternalServerError, err.Error(), "invalid_prompt_cache_key", "invalid_request_error", nil)
		invalid.skipAuthResult = true
		return nil, invalid
	}
	if state.enabled || !bytes.Equal(projected, rawJSON) {
		closeCodexRequestBody(httpReq)
		bodyReader := cliproxyexecutor.NewReleasableReadCloser(projected, nil)
		httpReq.Body = bodyReader
		httpReq.ContentLength = int64(bodyReader.Len())
	}
	guardCodexTurnStateHeader(e.cfg, auth, httpReq.Header)
	if errState := helps.ApplyManagedState(ctx, e.cfg, auth, req.Model, httpReq.Header, httpReq.URL.String()); errState != nil {
		return nil, errState
	}
	return projected, nil
}

func (e *CodexExecutor) projectCodexSessionIdentity(
	ctx context.Context,
	auth *cliproxyauth.Auth,
	req cliproxyexecutor.Request,
	opts cliproxyexecutor.Options,
	rawJSON []byte,
	identityConfuse *codexIdentityConfuseState,
) ([]byte, codexSessionIdentityState, error) {
	if auth == nil || codexAuthUsesAPIKey(auth) {
		return rawJSON, codexSessionIdentityState{}, nil
	}
	prepared := e.codexPreparedSessionIdentity(ctx, req, opts)
	if identityConfuse != nil && strings.TrimSpace(identityConfuse.turnIDBase) == "" {
		identityConfuse.turnIDBase = prepared.TurnID
	}
	client := codexClientSessionIdentitySource(ctx, opts)
	originalBodySessionID := gjson.GetBytes(rawJSON, "client_metadata.session_id").String()
	currentPromptCacheKey := gjson.GetBytes(rawJSON, "prompt_cache_key").String()
	originalPromptCacheKey := currentPromptCacheKey
	if identityConfuse != nil && identityConfuse.originalPromptCacheKey != "" {
		originalPromptCacheKey = identityConfuse.originalPromptCacheKey
	}
	clientThreadID := strings.TrimSpace(prepared.ClientThreadID)
	if clientThreadID == "" {
		clientThreadID = originalCodexClientThreadID(client.ThreadID, rawJSON)
	}
	fingerprint, err := resolveCodexConvergedFingerprint(auth, prepared, clientThreadID)
	if err != nil {
		status := codexStreamStatusErr(
			http.StatusInternalServerError,
			"prepare Codex session identity: "+err.Error(),
			"codex_session_identity_preparation_failed",
			"server_error",
			nil,
		)
		status.skipAuthResult = true
		return nil, codexSessionIdentityState{}, status
	}
	converged := fingerprint.mode != codexauth.FingerprintModeOff
	projectSession := prepared.Enabled || fingerprint.mode == codexauth.FingerprintModeSession || fingerprint.mode == codexauth.FingerprintModeFull
	if prepared.RequestKind == "memory" {
		projectSession = false
	}
	mapContext := fingerprint.mode == codexauth.FingerprintModeSession || fingerprint.mode == codexauth.FingerprintModeFull || (identityConfuse != nil && identityConfuse.enabled)
	if !prepared.Enabled && !converged && !mapContext {
		return rawJSON, codexSessionIdentityState{}, nil
	}
	defaults := helps.CodexSessionIdentity{
		SessionID: prepared.SessionID, ThreadID: prepared.ThreadID, TurnID: prepared.TurnID,
		WindowID: prepared.WindowID, RequestKind: prepared.RequestKind,
	}
	admin := codexCredentialSessionIdentitySource(auth)
	if identityConfuse != nil && identityConfuse.enabled {
		rawJSON = applyCodexIdentityConfuseFlatTurnID(rawJSON, identityConfuse)
		admin = applyCodexIdentityConfuseSessionSource(admin, identityConfuse)
		client = applyCodexIdentityConfuseSessionSource(client, identityConfuse)
	}
	projection := helps.CodexSessionIdentityProjection{
		ProtectedPromptCacheKey: prepared.PromptCacheKey.Key,
		InstallationID:          fingerprint.installationID,
		ProjectSession:          projectSession,
	}
	if mapContext {
		projection.ContextWindowScope = codexFingerprintAccountScope(auth)
	}
	if identityConfuse != nil {
		projection.ContextReplacements = &identityConfuse.contextWindows
		for _, replacement := range identityConfuse.turnIDs {
			projection.KnownReplacements = append(projection.KnownReplacements, helps.CodexResponseIdentityReplacement{From: replacement.original, To: replacement.confused, Role: helps.CodexResponseTurnIdentity})
		}
	}
	if fingerprint.mode == codexauth.FingerprintModeSession || fingerprint.mode == codexauth.FingerprintModeFull {
		projection.ForcedIdentity = helps.CodexSessionIdentity{
			SessionID: fingerprint.sessionID,
			ThreadID:  fingerprint.threadID,
			TurnID:    fingerprint.turnID,
			WindowID:  fingerprint.windowID,
		}
		if originalPromptCacheKey != "" && originalPromptCacheKey == originalBodySessionID {
			projection.PromptCacheKeyAlias = currentPromptCacheKey
		}
	}
	projected, identity, turnMetadata, err := helps.ProjectCodexSessionIdentityWithProjection(
		rawJSON,
		admin,
		helps.CodexSessionIdentityHeaderSource{},
		client,
		defaults,
		projection,
	)
	if err != nil {
		status := codexStreamStatusErr(
			http.StatusBadRequest,
			"invalid Codex session identity metadata: "+err.Error(),
			"invalid_session_identity_metadata",
			"invalid_request_error",
			nil,
		)
		status.skipAuthResult = true
		return nil, codexSessionIdentityState{}, status
	}
	if projection.PromptCacheKeyAlias != "" && identityConfuse != nil && identityConfuse.enabled {
		identityConfuse.promptCacheKey = identity.SessionID
	}
	if identityConfuse != nil && identity.TurnID != "" {
		oldNestedTurn := gjson.Get(gjson.GetBytes(rawJSON, "client_metadata.x-codex-turn-metadata").String(), "turn_id").String()
		oldFlatTurn := gjson.GetBytes(rawJSON, "client_metadata.turn_id").String()
		for index := range identityConfuse.turnIDs {
			if identityConfuse.turnIDs[index].confused == oldNestedTurn || identityConfuse.turnIDs[index].confused == oldFlatTurn || identityConfuse.turnIDs[index].confused == gjson.Get(admin.TurnMetadata, "turn_id").String() {
				identityConfuse.turnIDs[index].confused = identity.TurnID
			}
		}
	}
	return projected, codexSessionIdentityState{
		enabled: true, projectSession: projectSession, converged: converged,
		identity: identity, turnMetadata: turnMetadata,
	}, nil
}

func applyCodexIdentityConfuseFlatTurnID(rawJSON []byte, state *codexIdentityConfuseState) []byte {
	if state == nil || !state.enabled {
		return rawJSON
	}
	turnID := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.turn_id").String())
	if turnID == "" {
		return rawJSON
	}
	confusedTurnID := state.confuseTurnID(turnID)
	if confusedTurnID == turnID {
		return rawJSON
	}
	updated, err := sjson.SetBytes(rawJSON, "client_metadata.turn_id", confusedTurnID)
	if err != nil {
		return rawJSON
	}
	return updated
}

func applyCodexIdentityConfuseSessionSource(source helps.CodexSessionIdentityHeaderSource, state *codexIdentityConfuseState) helps.CodexSessionIdentityHeaderSource {
	if state == nil || !state.enabled {
		return source
	}
	if source.TurnMetadata != "" {
		source.TurnMetadata = applyCodexTurnMetadataIdentityConfuse(source.TurnMetadata, state)
	}
	return source
}

func (e *CodexExecutor) codexPreparedSessionIdentity(ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) codexPreparedSessionIdentity {
	if opaque, ok := cliproxyexecutor.ProviderPreparedRequest(opts, e.Identifier()); ok {
		switch prepared := opaque.(type) {
		case codexPreparedSessionIdentity:
			return prepared
		case *codexPreparedSessionIdentity:
			if prepared != nil {
				return *prepared
			}
		}
	}
	operation := cliproxyexecutor.RequestOperationExecute
	if opts.Stream {
		operation = cliproxyexecutor.RequestOperationStream
	}
	opaque, _ := e.PrepareProviderRequest(ctx, req, opts, operation)
	prepared, _ := opaque.(codexPreparedSessionIdentity)
	return prepared
}

func (e *CodexExecutor) ensureCodexPreparedSessionIdentity(ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, operation cliproxyexecutor.RequestOperation) (cliproxyexecutor.Options, error) {
	if _, ok := cliproxyexecutor.ProviderPreparedRequest(opts, e.Identifier()); ok {
		return opts, nil
	}
	prepared, err := e.PrepareProviderRequest(ctx, req, opts, operation)
	if err != nil {
		return opts, err
	}
	return cliproxyexecutor.WithProviderPreparedRequest(opts, e.Identifier(), prepared), nil
}

func codexCredentialSessionIdentitySource(auth *cliproxyauth.Auth) helps.CodexSessionIdentityHeaderSource {
	if auth == nil || len(auth.Attributes) == 0 {
		return helps.CodexSessionIdentityHeaderSource{}
	}
	headers := make(http.Header)
	util.ApplyCustomHeadersFromAttrs(&http.Request{Header: headers}, auth.Attributes)
	return codexSessionIdentitySourceFromHeaders(headers)
}

func codexClientSessionIdentitySource(ctx context.Context, opts cliproxyexecutor.Options) helps.CodexSessionIdentityHeaderSource {
	headers := make(http.Header)
	if ctx != nil {
		if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
			copyCodexHeaders(headers, ginCtx.Request.Header)
		}
	}
	copyCodexHeaders(headers, opts.Headers)
	return codexSessionIdentitySourceFromHeaders(headers)
}

func codexSessionIdentitySourceFromHeaders(headers http.Header) helps.CodexSessionIdentityHeaderSource {
	sessionID := headerValueCaseInsensitive(headers, "Session-Id")
	if sessionID == "" {
		sessionID = headerValueCaseInsensitive(headers, "Session_id")
	}
	return helps.CodexSessionIdentityHeaderSource{
		ParentThreadID: strings.TrimSpace(headerValueCaseInsensitive(headers, "X-Codex-Parent-Thread-Id")),
		InstallationID: strings.TrimSpace(headerValueCaseInsensitive(headers, "X-Codex-Installation-Id")),
		SessionID:      strings.TrimSpace(sessionID),
		ThreadID:       strings.TrimSpace(headerValueCaseInsensitive(headers, "Thread-Id")),
		WindowID:       strings.TrimSpace(headerValueCaseInsensitive(headers, "X-Codex-Window-Id")),
		TurnMetadata:   strings.TrimSpace(headerValueCaseInsensitive(headers, "X-Codex-Turn-Metadata")),
	}
}

func copyCodexHeaders(target, source http.Header) {
	for key, values := range source {
		deleteHeaderCaseInsensitive(target, key)
		for _, value := range values {
			target.Add(key, value)
		}
	}
}

func applyCodexSessionIdentityHeaders(headers http.Header, state codexSessionIdentityState, websocket bool) {
	if headers == nil || !state.enabled {
		return
	}
	deleteHeaderCaseInsensitive(headers, "X-Codex-Installation-Id")
	if state.identity.InstallationID != "" {
		setHeaderCasePreserved(headers, "X-Codex-Installation-Id", state.identity.InstallationID)
	}
	if state.turnMetadata != "" {
		deleteHeaderCaseInsensitive(headers, "X-Codex-Turn-Metadata")
		setHeaderCasePreserved(headers, "X-Codex-Turn-Metadata", state.turnMetadata)
	}
	if state.identity.ParentThreadID != "" {
		deleteHeaderCaseInsensitive(headers, "X-Codex-Parent-Thread-Id")
		setHeaderCasePreserved(headers, "X-Codex-Parent-Thread-Id", state.identity.ParentThreadID)
	}
	if !state.projectSession {
		return
	}
	for _, name := range []string{"Session-Id", "Session_id", "Thread-Id", "X-Codex-Window-Id"} {
		deleteHeaderCaseInsensitive(headers, name)
	}
	setHeaderCasePreserved(headers, "Session-Id", state.identity.SessionID)
	setHeaderCasePreserved(headers, "Thread-Id", state.identity.ThreadID)
	setHeaderCasePreserved(headers, "X-Codex-Window-Id", state.identity.WindowID)
	if state.converged {
		deleteHeaderCaseInsensitive(headers, "X-Client-Request-Id")
		setHeaderCasePreserved(headers, "X-Client-Request-Id", state.identity.ThreadID)
	}
	if websocket {
		deleteHeaderCaseInsensitive(headers, "session_id")
		setHeaderCasePreserved(headers, "session_id", state.identity.SessionID)
	}
}

func closeCodexRequestBody(req *http.Request) {
	if req == nil || req.Body == nil {
		return
	}
	if err := req.Body.Close(); err != nil {
		log.Debugf("codex executor: close replaced request body: %v", err)
	}
}

func (e *CodexExecutor) cacheHelper(ctx context.Context, from sdktranslator.Format, url string, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, userPayload []byte, rawJSON []byte, allowIdentityConfuse bool) (*http.Request, []byte, codexIdentityConfuseState, error) {
	var cache helps.CodexCache
	if from == "claude" {
		userIDResult := gjson.GetBytes(req.Payload, "metadata.user_id")
		if userIDResult.Exists() {
			key := fmt.Sprintf("%s-%s", req.Model, userIDResult.String())
			var errCache error
			cache, errCache = codexPromptCacheIdentity(key)
			if errCache != nil {
				return nil, nil, codexIdentityConfuseState{}, errCache
			}
		}
	} else if from == "openai-response" {
		promptCacheKey := gjson.GetBytes(req.Payload, "prompt_cache_key")
		if promptCacheKey.Exists() {
			cache.ID = promptCacheKey.String()
		}
	} else if from == "openai" {
		if apiKey := strings.TrimSpace(helps.APIKeyFromContext(ctx)); apiKey != "" {
			var errCache error
			cache, errCache = codexPromptCacheIdentity(codexPromptCacheLookupKey("openai", apiKey))
			if errCache != nil {
				return nil, nil, codexIdentityConfuseState{}, errCache
			}
		}
	}

	if cache.ID != "" {
		rawJSON, _ = helps.SetStringIfDifferent(rawJSON, "prompt_cache_key", cache.ID)
	}
	sessionHeaderID := ""
	if _, errParse := uuid.Parse(strings.TrimSpace(cache.ID)); errParse == nil {
		sessionHeaderID = cache.ID
	}
	var identityState codexIdentityConfuseState
	prepared := e.codexPreparedSessionIdentity(ctx, req, opts)
	if allowIdentityConfuse {
		rawJSON, identityState = applyCodexPreparedIdentityConfuseBody(e.cfg, auth, userPayload, rawJSON, prepared)
	} else {
		rawJSON = prepared.PromptCacheKey.Apply(rawJSON)
	}
	if identityState.promptCacheKey != "" {
		cache.ID = identityState.promptCacheKey
	}
	bodyReader := cliproxyexecutor.NewReleasableReadCloser(rawJSON, nil)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bodyReader)
	if err != nil {
		return nil, nil, codexIdentityConfuseState{}, err
	}
	httpReq.ContentLength = int64(bodyReader.Len())
	if sessionHeaderID != "" {
		httpReq.Header.Set("Session-Id", sessionHeaderID)
	}
	return httpReq, rawJSON, identityState, nil
}

func codexPromptCacheIdentity(key string) (helps.CodexCache, error) {
	if cached, ok := helps.GetCodexCache(key); ok {
		if parsed, err := uuid.Parse(strings.TrimSpace(cached.ID)); err == nil && parsed.Version() == 7 {
			return cached, nil
		}
	}
	id, err := helps.NewCodexUUIDv7()
	if err != nil {
		return helps.CodexCache{}, err
	}
	cache := helps.CodexCache{ID: id, Expire: time.Now().Add(time.Hour)}
	helps.SetCodexCache(key, cache)
	return cache, nil
}

func codexPromptCacheLookupKey(namespace, value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return fmt.Sprintf("%s:%x", strings.TrimSpace(namespace), sum)
}

func applyCodexIdentityConfuseBody(cfg *config.Config, auth *cliproxyauth.Auth, userPayload []byte, rawJSON []byte, turnIDBase ...string) ([]byte, codexIdentityConfuseState) {
	return applyCodexIdentityConfuseBodyWithCacheKey(cfg, auth, userPayload, rawJSON, "", turnIDBase...)
}

func applyCodexPreparedIdentityConfuseBody(cfg *config.Config, auth *cliproxyauth.Auth, userPayload, rawJSON []byte, prepared codexPreparedSessionIdentity) ([]byte, codexIdentityConfuseState) {
	enabled := prepared.IdentityConfuseEnabled
	if !prepared.IdentityPolicyFrozen {
		enabled = codexIdentityConfuseEnabled(cfg)
	}
	rawJSON, state := applyCodexIdentityConfuseBodyWithPolicy(enabled, auth, userPayload, rawJSON, prepared.PromptCacheKey.Key, prepared.TurnID)
	state.protectedSessionID = prepared.PromptCacheKey.SessionID
	return prepared.PromptCacheKey.Apply(rawJSON), state
}

func applyCodexIdentityConfuseBodyWithCacheKey(cfg *config.Config, auth *cliproxyauth.Auth, userPayload []byte, rawJSON []byte, protectedKey string, turnIDBase ...string) ([]byte, codexIdentityConfuseState) {
	return applyCodexIdentityConfuseBodyWithPolicy(codexIdentityConfuseEnabled(cfg), auth, userPayload, rawJSON, protectedKey, turnIDBase...)
}

func applyCodexIdentityConfuseBodyWithPolicy(enabled bool, auth *cliproxyauth.Auth, userPayload []byte, rawJSON []byte, protectedKey string, turnIDBase ...string) ([]byte, codexIdentityConfuseState) {
	if !enabled || auth == nil || strings.TrimSpace(auth.ID) == "" || len(rawJSON) == 0 {
		return rawJSON, codexIdentityConfuseState{}
	}

	state := codexIdentityConfuseState{enabled: true, authID: strings.TrimSpace(auth.ID), protectedPromptCacheKey: protectedKey}
	if len(turnIDBase) > 0 {
		state.turnIDBase = strings.TrimSpace(turnIDBase[0])
	}
	promptCacheKey := gjson.GetBytes(userPayload, "prompt_cache_key").String()
	if protectedKey != "" {
		promptCacheKey = protectedKey
	}
	if strings.TrimSpace(promptCacheKey) != "" {
		state.originalPromptCacheKey = promptCacheKey
		state.promptCacheKey = codexIdentityConfuseUUID(auth.ID, "prompt-cache", promptCacheKey)
		if protectedKey == "" {
			rawJSON, _ = helps.SetStringIfDifferent(rawJSON, "prompt_cache_key", state.promptCacheKey)
		}
	}
	if installationID := strings.TrimSpace(gjson.GetBytes(userPayload, "client_metadata.x-codex-installation-id").String()); installationID != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.x-codex-installation-id", codexIdentityConfuseUUID(auth.ID, "installation", installationID))
	}
	if turnMetadata := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-turn-metadata").String()); turnMetadata != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.x-codex-turn-metadata", applyCodexTurnMetadataIdentityConfuse(turnMetadata, &state))
	}

	return rawJSON, state
}

func applyCodexIdentityConfuseHeaders(headers http.Header, state *codexIdentityConfuseState) {
	if headers == nil {
		return
	}
	if state == nil || !state.enabled {
		return
	}

	if rawTurnMetadata := strings.TrimSpace(headers.Get("X-Codex-Turn-Metadata")); rawTurnMetadata != "" {
		headers.Set("X-Codex-Turn-Metadata", applyCodexTurnMetadataIdentityConfuse(rawTurnMetadata, state))
	}
	if state.promptCacheKey == "" {
		return
	}

	if headerValueCaseInsensitive(headers, "Conversation_id") != "" {
		setHeaderCasePreserved(headers, "Conversation_id", state.promptCacheKey)
	}
}

func applyCodexTurnMetadataIdentityConfuse(rawTurnMetadata string, state *codexIdentityConfuseState) string {
	updatedTurnMetadata := rawTurnMetadata
	if state == nil || !state.enabled {
		return updatedTurnMetadata
	}
	if state.promptCacheKey != "" && gjson.Get(rawTurnMetadata, "prompt_cache_key").Exists() {
		if state.protectedPromptCacheKey == "" {
			updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "prompt_cache_key", state.promptCacheKey)
		}
	}
	if turnID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "turn_id").String()); turnID != "" {
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "turn_id", state.confuseTurnID(turnID))
	}
	return updatedTurnMetadata
}

func applyCodexIdentityExposeResponsePayload(payload []byte, state codexIdentityConfuseState) []byte {
	// Upstream identities are already mapped. Restore directly from that snapshot;
	// mapping them forward again can mistake a mapped ID for another original ID.
	return helps.ReplaceCodexResponseIdentities(payload, state.responseIdentityReplacements(true))
}

func (state codexIdentityConfuseState) responseIdentityReplacements(reverse bool) []helps.CodexResponseIdentityReplacement {
	role := helps.CodexResponseCacheAndSessionIdentity
	if state.protectedSessionID != "" {
		role = helps.CodexResponseThreadAndWindowIdentity
	} else if state.protectedPromptCacheKey != "" {
		role = helps.CodexResponseSessionIdentity
	}
	replacements := make([]helps.CodexResponseIdentityReplacement, 0, 1+len(state.turnIDs))
	add := func(from, to string, role helps.CodexResponseIdentityRole) {
		if from == "" || to == "" || from == to {
			return
		}
		if reverse {
			from, to = to, from
		}
		replacements = append(replacements, helps.CodexResponseIdentityReplacement{From: from, To: to, Role: role})
	}
	add(state.originalPromptCacheKey, state.promptCacheKey, role)
	for _, turn := range state.turnIDs {
		add(turn.original, turn.confused, helps.CodexResponseTurnIdentity)
	}
	for _, window := range state.contextWindows {
		add(window.From, window.To, window.Role)
	}
	return replacements
}

func (state *codexIdentityConfuseState) confuseTurnID(turnID string) string {
	turnID = strings.TrimSpace(turnID)
	if state == nil || !state.enabled || strings.TrimSpace(state.authID) == "" || turnID == "" {
		return turnID
	}
	for _, replacement := range state.turnIDs {
		if replacement.original == turnID || replacement.confused == turnID {
			return replacement.confused
		}
	}
	confusedTurnID := codexIdentityConfuseTurnUUID(state.authID, turnID, state.turnIDBase)
	state.turnIDs = append(state.turnIDs, codexIdentityReplacement{original: turnID, confused: confusedTurnID})
	return confusedTurnID
}

func codexIdentityConfuseEnabled(cfg *config.Config) bool {
	if cfg == nil || !cfg.Codex.IdentityConfuse {
		return false
	}
	strategy := strings.ToLower(strings.TrimSpace(cfg.Routing.Strategy))
	return cfg.Routing.SessionAffinity || strategy == "fill-first" || strategy == "fillfirst" || strategy == "ff"
}

func codexIdentityConfuseUUID(authID string, kind string, value string) string {
	name := strings.Join([]string{"cli-proxy-api", "codex", "identity-confuse", kind, strings.TrimSpace(authID), strings.TrimSpace(value)}, ":")
	switch kind {
	case "installation":
		return deriveStableCodexFingerprintUUID("identity-confuse-installation", name)
	case "turn":
		return codexIdentityConfuseTurnUUID(authID, value, value)
	}
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String()
}

func codexIdentityConfuseTurnUUID(authID, value, timestampSource string) string {
	name := strings.Join([]string{"cli-proxy-api", "codex", "identity-confuse", "turn", strings.TrimSpace(authID), strings.TrimSpace(value)}, ":")
	sum := sha256.Sum256([]byte(name))
	var id uuid.UUID
	copy(id[:], sum[:16])
	if source, err := uuid.Parse(strings.TrimSpace(timestampSource)); err == nil && source.Version() == 7 {
		copy(id[:6], source[:6])
	}
	id[6] = (id[6] & 0x0f) | 0x70
	id[8] = (id[8] & 0x3f) | 0x80
	return id.String()
}

func applyCodexHeaders(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, cfg *config.Config) error {
	var ginHeaders http.Header
	if ginCtx, ok := r.Context().Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
		ginHeaders = ginCtx.Request.Header
	}
	return applyCodexHeadersFromSources(r, auth, token, stream, cfg, ginHeaders)
}

func applyCodexDirectImageHeaders(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, cfg *config.Config) error {
	var ginHeaders http.Header
	if ginCtx, ok := r.Context().Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
		ginHeaders = ginCtx.Request.Header.Clone()
		ginHeaders.Del("User-Agent")
	}
	return applyCodexHeadersFromSources(r, auth, token, stream, cfg, ginHeaders)
}

func applyCodexHeadersFromSources(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, cfg *config.Config, ginHeaders http.Header) error {
	r.Header.Set("Content-Type", "application/json")
	clientBetaFeatures := firstCodexHeaderValue("X-Codex-Beta-Features", r.Header, ginHeaders)

	if clientBetaFeatures != "" {
		r.Header.Set("X-Codex-Beta-Features", clientBetaFeatures)
	}
	misc.EnsureHeader(r.Header, ginHeaders, "Version", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Codex-Turn-Metadata", "")
	misc.EnsureHeader(r.Header, ginHeaders, codexTurnStateHeader, "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Client-Request-Id", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Codex-Installation-Id", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Codex-Window-Id", "")
	misc.EnsureHeader(r.Header, ginHeaders, "Thread-Id", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Codex-Parent-Thread-Id", "")
	ensureCodexHTTPSessionHeader(r.Header, ginHeaders)
	misc.EnsureHeader(r.Header, ginHeaders, "X-OpenAI-Internal-Codex-Responses-Lite", "")
	cfgUserAgent, cfgOriginator, _ := codexHeaderDefaults(cfg, auth)
	ensureHeaderWithConfigPrecedence(r.Header, ginHeaders, "User-Agent", cfgUserAgent, codexUserAgent)

	if stream {
		r.Header.Set("Accept", "text/event-stream")
	} else {
		r.Header.Set("Accept", "application/json")
	}
	r.Header.Set("Connection", "Keep-Alive")

	isAPIKey := false
	if auth != nil && auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes["api_key"]); v != "" {
			isAPIKey = true
		}
	}
	if originator := strings.TrimSpace(ginHeaders.Get("Originator")); originator != "" {
		r.Header.Set("Originator", originator)
	} else if cfgOriginator != "" {
		r.Header.Set("Originator", cfgOriginator)
	} else if !isAPIKey {
		r.Header.Set("Originator", codexOriginator)
	}
	if !isAPIKey {
		metadata := authMetadata(auth)
		if accountID := codexauth.EffectiveRequestAccountID(metadata); accountID != "" {
			r.Header.Set("Chatgpt-Account-Id", accountID)
		}
		if codexauth.ChatGPTAccountIsFedRAMP(metadata) {
			r.Header.Set("X-OpenAI-Fedramp", "true")
		}
	}
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	deleteHeaderCaseInsensitive(r.Header, "Authorization")
	util.ApplyCustomHeadersFromAttrs(r, attrs)
	normalizeCodexHTTPSessionHeader(r.Header, codexCustomHTTPSessionHeader(attrs))
	applyCodexSoftwareIdentity(r.Header, auth, cfg)
	applyCodexBetaFeatures(r.Header, auth, cfg, clientBetaFeatures)
	authorization, err := codexauth.AuthorizationHeader(authMetadata(auth), token, time.Now())
	if err != nil {
		return fmt.Errorf("codex executor: build authorization: %w", err)
	}
	if authorization != "" {
		r.Header.Set("Authorization", authorization)
	}
	return nil
}

func applyCodexSoftwareIdentity(headers http.Header, auth *cliproxyauth.Auth, cfg *config.Config, models ...string) {
	if headers == nil {
		return
	}
	if codexAuthUsesAPIKey(auth) || !codexEnforceSoftwareIdentity(cfg) {
		if value := headerValueCaseInsensitive(headers, "User-Agent"); !httpguts.ValidHeaderFieldValue(value) {
			candidate := ""
			if cfg != nil {
				candidate = cfg.CodexHeaderDefaults.UserAgent
			}
			deleteHeaderCaseInsensitive(headers, "User-Agent")
			setHeaderCasePreserved(headers, "User-Agent", codexauth.ResolveSoftwareIdentity(candidate).UserAgent)
		}
		return
	}
	candidate := codexCustomHeaderValue(auth, "User-Agent")
	if !codexauth.ValidSoftwareIdentityUserAgent(candidate) && cfg != nil {
		candidate = cfg.CodexHeaderDefaults.UserAgent
	}
	identity := codexauth.ResolveSoftwareIdentity(candidate)
	if len(models) > 0 {
		identity = codexauth.ResolveSoftwareIdentityForModel(candidate, thinking.ParseSuffix(models[0]).ModelName)
	}
	deleteHeaderCaseInsensitive(headers, "User-Agent")
	deleteHeaderCaseInsensitive(headers, "Originator")
	deleteHeaderCaseInsensitive(headers, "Version")
	setHeaderCasePreserved(headers, "User-Agent", identity.UserAgent)
	setHeaderCasePreserved(headers, "Originator", identity.Originator)
	setHeaderCasePreserved(headers, "Version", identity.Version)
}

func applyCodexBetaFeatures(headers http.Header, auth *cliproxyauth.Auth, cfg *config.Config, clientValue string) {
	if headers == nil || codexAuthUsesAPIKey(auth) {
		return
	}
	value := codexCustomHeaderValue(auth, "X-Codex-Beta-Features")
	if value == "" {
		value = strings.TrimSpace(clientValue)
	}
	if value == "" && cfg != nil {
		value = strings.TrimSpace(cfg.CodexHeaderDefaults.BetaFeatures)
	}
	deleteHeaderCaseInsensitive(headers, "X-Codex-Beta-Features")
	if value != "" {
		setHeaderCasePreserved(headers, "X-Codex-Beta-Features", value)
	}
}

func codexEnforceSoftwareIdentity(cfg *config.Config) bool {
	return cfg == nil || cfg.Codex.ResolvedEnforceSoftwareIdentity()
}

func codexCustomHeaderValue(auth *cliproxyauth.Auth, name string) string {
	if auth == nil {
		return ""
	}
	for key, value := range auth.Attributes {
		if !strings.HasPrefix(key, "header:") || !strings.EqualFold(strings.TrimSpace(strings.TrimPrefix(key, "header:")), name) {
			continue
		}
		if strings.EqualFold(name, "User-Agent") {
			return value
		}
		return strings.TrimSpace(value)
	}
	return ""
}

func firstCodexHeaderValue(name string, sources ...http.Header) string {
	for _, source := range sources {
		if value := headerValueCaseInsensitive(source, name); value != "" {
			return value
		}
	}
	return ""
}

func ensureCodexHTTPSessionHeader(target, source http.Header) {
	value := headerValueCaseInsensitive(source, "Session-Id")
	if value == "" {
		value = headerValueCaseInsensitive(source, "Session_id")
	}
	if value == "" {
		value = headerValueCaseInsensitive(target, "Session-Id")
	}
	if value == "" {
		value = headerValueCaseInsensitive(target, "Session_id")
	}
	normalizeCodexHTTPSessionHeader(target, value)
}

func normalizeCodexHTTPSessionHeader(headers http.Header, preferredValue string) {
	if headers == nil {
		return
	}
	value := strings.TrimSpace(preferredValue)
	if value == "" {
		value = headerValueCaseInsensitive(headers, "Session-Id")
	}
	if value == "" {
		value = headerValueCaseInsensitive(headers, "Session_id")
	}
	deleteHeaderCaseInsensitive(headers, "Session-Id")
	deleteHeaderCaseInsensitive(headers, "Session_id")
	if value != "" {
		setHeaderCasePreserved(headers, "Session-Id", value)
	}
}

func codexCustomHTTPSessionHeader(attrs map[string]string) string {
	var legacyValue string
	for key, value := range attrs {
		if !strings.HasPrefix(key, "header:") {
			continue
		}
		name := strings.TrimSpace(strings.TrimPrefix(key, "header:"))
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if strings.EqualFold(name, "Session-Id") {
			return value
		}
		if strings.EqualFold(name, "Session_id") {
			legacyValue = value
		}
	}
	return legacyValue
}

func authMetadata(auth *cliproxyauth.Auth) map[string]any {
	if auth == nil {
		return nil
	}
	return auth.Metadata
}

func (e *CodexExecutor) newCodexHTTPClient(ctx context.Context, auth *cliproxyauth.Auth, imageRequest bool) *http.Client {
	var cfg *config.Config
	if e != nil {
		cfg = e.cfg
	}
	if codexFingerprintJA3Enabled(cfg) {
		return helps.StateProbeHTTPClient(ctx, helps.AutoCookieHTTPClient(ctx, cfg, auth, helps.NewCodexNativeTLSHTTP1Client(ctx, cfg, auth, 0)))
	}
	forceHTTP1 := codexFingerprintShouldForceHTTP1(cfg, imageRequest)
	if forceHTTP1 {
		return helps.StateProbeHTTPClient(ctx, helps.AutoCookieHTTPClient(ctx, cfg, auth, helps.NewProxyAwareHTTP1Client(ctx, cfg, auth, 0)))
	}
	return helps.StateProbeHTTPClient(ctx, helps.AutoCookieHTTPClient(ctx, cfg, auth, helps.NewProxyAwareHTTPClient(ctx, cfg, auth, 0)))
}

func newCodexStatusErr(statusCode int, body []byte) statusErr {
	errCode := statusCode
	isUsageLimit := helps.IsCodexUsageLimitError(body)
	if isUsageLimit || isCodexModelCapacityError(body) {
		errCode = http.StatusTooManyRequests
	}
	requestScopedContextError := !isUsageLimit && isCodexContextTooLargeRequestError(errCode, body)
	originalBody := body
	if !isUsageLimit {
		body = classifyCodexStatusError(errCode, body)
	}
	err := statusErr{code: errCode, msg: string(body)}
	if !bytes.Equal(originalBody, body) {
		err.responseBody = string(originalBody)
	}
	if requestScopedContextError {
		err.skipAuthResult = true
	}
	if retryAfter := parseCodexRetryAfter(errCode, originalBody, time.Now()); retryAfter != nil {
		err.retryAfter = retryAfter
	}
	return err
}

func classifyCodexStatusError(statusCode int, body []byte) []byte {
	code, errType, ok := codexStatusErrorClassification(statusCode, body)
	if !ok {
		return body
	}
	message := gjson.GetBytes(body, "error.message").String()
	if message == "" {
		message = gjson.GetBytes(body, "message").String()
	}
	if message == "" {
		message = strings.TrimSpace(string(body))
	}
	if message == "" {
		message = http.StatusText(statusCode)
	}
	out := []byte(`{"error":{}}`)
	out, _ = sjson.SetBytes(out, "error.message", message)
	out, _ = sjson.SetBytes(out, "error.type", errType)
	out, _ = sjson.SetBytes(out, "error.code", code)
	return out
}

func codexStatusErrorClassification(statusCode int, body []byte) (code string, errType string, ok bool) {
	if cliproxyauth.IsPolicyRefusalError(statusErr{code: statusCode, msg: string(body)}) {
		return "", "", false
	}
	lower := strings.ToLower(strings.TrimSpace(string(body)))
	upstreamCode := ""
	for _, path := range []string{"error.code", "response.error.code", "code"} {
		if upstreamCode = strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, path).String())); upstreamCode != "" {
			break
		}
	}
	upstreamType := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "error.type").String()))

	switch {
	case isCodexContextTooLargeRequestError(statusCode, body):
		return "context_too_large", "invalid_request_error", true
	case strings.Contains(lower, "invalid signature in thinking block") || strings.Contains(lower, "invalid_encrypted_content"):
		return "thinking_signature_invalid", "invalid_request_error", true
	case upstreamCode == "previous_response_not_found" || strings.Contains(lower, "previous_response_not_found") || strings.Contains(lower, "previous_response_id") && strings.Contains(lower, "not found"):
		return "previous_response_not_found", "invalid_request_error", true
	case statusCode == http.StatusUnauthorized && isAgentTaskErrorCode(upstreamCode):
		return upstreamCode, "authentication_error", true
	case statusCode == http.StatusUnauthorized || upstreamType == "authentication_error" || upstreamCode == "invalid_api_key" || strings.Contains(lower, "invalid or expired token") || strings.Contains(lower, "refresh_token_reused"):
		return "auth_unavailable", "authentication_error", true
	default:
		return "", "", false
	}
}

func isAgentTaskErrorCode(code string) bool {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "invalid_task_id", "task_not_found", "task_expired":
		return true
	default:
		return false
	}
}

func isCodexContextTooLargeRequestError(statusCode int, body []byte) bool {
	if statusCode == http.StatusRequestEntityTooLarge {
		return true
	}
	upstreamCode := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "error.code").String()))
	if upstreamCode == "context_length_exceeded" || upstreamCode == "context_too_large" {
		return true
	}
	upstreamType := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "error.type").String()))
	if upstreamType != "" && upstreamType != "invalid_request_error" {
		return false
	}
	errorMessage := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "error.message").String()))
	if errorMessage == "" {
		errorMessage = strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "message").String()))
	}
	return strings.Contains(errorMessage, "context length") ||
		strings.Contains(errorMessage, "context_length") ||
		strings.Contains(errorMessage, "maximum context") ||
		statusCode != http.StatusTooManyRequests && strings.Contains(errorMessage, "too many tokens")
}

func normalizeCodexInstructions(body []byte) []byte {
	instructions := gjson.GetBytes(body, "instructions")
	if !instructions.Exists() || instructions.Type == gjson.Null {
		body, _ = sjson.SetBytes(body, "instructions", "")
	}
	return body
}

func publishCodexImageToolUsage(ctx context.Context, reporter *helps.UsageReporter, body []byte, completedData []byte) {
	detail, ok := helps.ParseCodexImageToolUsage(completedData)
	if !ok {
		return
	}
	reporter.EnsurePublished(ctx)
	reporter.PublishAdditionalModel(ctx, codexImageGenerationToolModel(body), detail)
}

func observeCodexImageToolUsage(reporter *helps.UsageReporter, body []byte, completedData []byte) {
	detail, ok := helps.ParseCodexImageToolUsage(completedData)
	if !ok {
		return
	}
	reporter.ObserveAdditionalModel(codexImageGenerationToolModel(body), detail)
}

func publishCodexStreamUsage(reporter *helps.UsageReporter, body []byte, data []byte) {
	if !isCodexSuccessfulCompletion(data) && !helps.IsCodexPartialResponse(data) {
		return
	}
	if detail, ok := helps.ParseCodexUsage(data); ok {
		reporter.Observe(detail)
	}
	observeCodexImageToolUsage(reporter, body, data)
}

func codexSSEFrameDataPayload(frame []byte) ([]byte, bool) {
	var payload []byte
	found := false
	forEachCodexSSELine(frame, func(line []byte) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, dataTag) {
			return
		}
		if found {
			payload = append(payload, '\n')
		}
		payload = append(payload, bytes.TrimSpace(line[len(dataTag):])...)
		found = true
	})
	return payload, found
}

func forEachCodexSSELine(data []byte, visit func([]byte)) {
	if visit == nil {
		return
	}
	lineStart := 0
	for index := 0; index < len(data); {
		if data[index] != '\r' && data[index] != '\n' {
			index++
			continue
		}
		visit(data[lineStart:index])
		index++
		if data[index-1] == '\r' && index < len(data) && data[index] == '\n' {
			index++
		}
		lineStart = index
	}
	visit(data[lineStart:])
}

func codexTrustedSSEFrameNeedsInspection(frame []byte) bool {
	for _, marker := range [][]byte{
		[]byte("response.completed"),
		[]byte("response.done"),
		[]byte("response.failed"),
		[]byte("response.incomplete"),
		[]byte(`"error"`),
	} {
		if bytes.Contains(frame, marker) {
			return true
		}
	}
	return false
}

func codexStreamDataPayload(line []byte) ([]byte, bool) {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, dataTag) {
		return nil, false
	}
	return bytes.TrimSpace(line[len(dataTag):]), true
}

func metadataBool(meta map[string]any, key string) bool {
	if len(meta) == 0 {
		return false
	}
	switch v := meta[key].(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

func (e *CodexExecutor) applyDisabledImageGenerationToolPolicy(auth *cliproxyauth.Auth, body []byte) ([]byte, error) {
	if !cliproxyauth.AuthDisablesImageGeneration(e.cfg, auth, e.Identifier()) || !cliproxyauth.PayloadHasImageGenerationTool(body) {
		return body, nil
	}
	action, ok := config.NormalizeDisabledImageGenerationToolAction(disabledImageGenerationToolAction(e.cfg))
	if !ok || action == config.DisabledImageGenerationToolActionRemove {
		return removeCodexImageGenerationTool(body), nil
	}
	return nil, disabledImageGenerationToolError(e.cfg)
}

func disabledImageGenerationToolAction(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	return cfg.DisabledImageGenerationToolAction
}

func disabledImageGenerationToolError(cfg *config.Config) statusErr {
	var errCfg config.DisabledImageGenerationToolErrorConfig
	if cfg != nil {
		errCfg = cfg.DisabledImageGenerationToolError
	}
	errCfg = config.NormalizeDisabledImageGenerationToolError(errCfg)
	body := []byte(`{"error":{}}`)
	body, _ = sjson.SetBytes(body, "error.message", errCfg.Message)
	body, _ = sjson.SetBytes(body, "error.type", errCfg.Type)
	body, _ = sjson.SetBytes(body, "error.code", errCfg.Code)
	return statusErr{code: errCfg.StatusCode, msg: string(body), skipAuthResult: true, localPolicy: "disabled_image_generation_tool"}
}

func removeCodexImageGenerationTool(body []byte) []byte {
	paths := []string{"tools"}
	if input := gjson.GetBytes(body, "input"); input.IsArray() {
		for index, item := range input.Array() {
			if item.Get("type").String() == "additional_tools" {
				paths = append(paths, fmt.Sprintf("input.%d.tools", index))
			}
		}
	}
	for _, path := range paths {
		tools := gjson.GetBytes(body, path)
		if !tools.IsArray() {
			continue
		}
		var retained bytes.Buffer
		retained.WriteByte('[')
		retainedCount := 0
		for _, tool := range tools.Array() {
			toolRaw, keep := removeCodexImageGenerationFromTool(tool)
			if !keep {
				continue
			}
			if retainedCount > 0 {
				retained.WriteByte(',')
			}
			retained.Write(toolRaw)
			retainedCount++
		}
		retained.WriteByte(']')
		if retainedCount == 0 && path == "tools" {
			body, _ = sjson.DeleteBytes(body, path)
		} else {
			body, _ = sjson.SetRawBytes(body, path, retained.Bytes())
		}
	}
	if !helps.HasCodexToolDeclarations(body) {
		body, _ = sjson.DeleteBytes(body, "tool_choice")
		body, _ = sjson.DeleteBytes(body, "parallel_tool_calls")
	}
	if codexToolChoiceSelectsImageGeneration(gjson.GetBytes(body, "tool_choice")) {
		body, _ = sjson.DeleteBytes(body, "tool_choice")
	}
	return helps.PruneCodexAllowedImageTools(body)
}

func removeCodexImageGenerationFromTool(tool gjson.Result) ([]byte, bool) {
	if tool.Get("type").String() != "namespace" {
		if cliproxyauth.ToolHasImageGeneration(tool) {
			return nil, false
		}
		return []byte(tool.Raw), true
	}
	if strings.TrimSpace(tool.Get("name").String()) != "image_gen" || !tool.Get("tools").IsArray() {
		return []byte(tool.Raw), true
	}

	nested := []byte(`[]`)
	nestedCount := 0
	for _, candidate := range tool.Get("tools").Array() {
		if cliproxyauth.IsImageGenerationNamespaceMember(candidate) {
			continue
		}
		nested, _ = sjson.SetRawBytes(nested, fmt.Sprintf("%d", nestedCount), []byte(candidate.Raw))
		nestedCount++
	}
	if nestedCount == 0 {
		return nil, false
	}
	updated, errSet := sjson.SetRawBytes([]byte(tool.Raw), "tools", nested)
	if errSet != nil {
		return []byte(tool.Raw), true
	}
	return updated, true
}

func codexToolChoiceSelectsImageGeneration(choice gjson.Result) bool {
	if !choice.Exists() || !choice.IsObject() {
		return false
	}
	choiceType := strings.TrimSpace(choice.Get("type").String())
	switch choiceType {
	case "image_generation":
		return true
	case "function":
		name := strings.TrimSpace(choice.Get("name").String())
		if name == "" {
			name = strings.TrimSpace(choice.Get("function.name").String())
		}
		return name == "image_gen.imagegen"
	case "namespace":
		return strings.TrimSpace(choice.Get("name").String()) == "image_gen"
	default:
		return false
	}
}

func codexImageGenerationToolModel(body []byte) string {
	declarations := []gjson.Result{gjson.GetBytes(body, "tools")}
	if input := gjson.GetBytes(body, "input"); input.IsArray() {
		for _, item := range input.Array() {
			if item.Get("type").String() == "additional_tools" {
				declarations = append(declarations, item.Get("tools"))
			}
		}
	}
	for _, tools := range declarations {
		if !tools.IsArray() {
			continue
		}
		for _, tool := range tools.Array() {
			if tool.Get("type").String() != "image_generation" {
				continue
			}
			if model := strings.TrimSpace(tool.Get("model").String()); model != "" {
				return model
			}
			return codexDefaultImageToolModel
		}
	}
	return codexDefaultImageToolModel
}

func isCodexModelCapacityError(errorBody []byte) bool {
	if len(errorBody) == 0 {
		return false
	}
	candidates := []string{
		gjson.GetBytes(errorBody, "error.message").String(),
		gjson.GetBytes(errorBody, "message").String(),
		string(errorBody),
	}
	for _, candidate := range candidates {
		lower := strings.ToLower(strings.TrimSpace(candidate))
		if lower == "" {
			continue
		}
		if strings.Contains(lower, "selected model is at capacity") ||
			strings.Contains(lower, "model is at capacity. please try a different model") {
			return true
		}
	}
	return false
}

func parseCodexRetryAfter(statusCode int, errorBody []byte, now time.Time) *time.Duration {
	if statusCode != http.StatusTooManyRequests {
		return nil
	}
	return helps.CodexUsageLimitRetryAfter(errorBody, now)
}

func codexCreds(a *cliproxyauth.Auth) (apiKey, baseURL string) {
	if a == nil {
		return "", ""
	}
	if a.Attributes != nil {
		apiKey = a.Attributes["api_key"]
		baseURL = a.Attributes["base_url"]
	}
	if apiKey == "" && a.Metadata != nil {
		if v, ok := a.Metadata["access_token"].(string); ok {
			apiKey = v
		}
	}
	return
}

func (e *CodexExecutor) resolveCodexConfig(auth *cliproxyauth.Auth) *config.CodexKey {
	if auth == nil || e.cfg == nil {
		return nil
	}
	var attrKey, attrBase string
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range e.cfg.CodexKey {
		entry := &e.cfg.CodexKey[i]
		cfgKey := strings.TrimSpace(entry.APIKey)
		cfgBase := strings.TrimSpace(entry.BaseURL)
		if attrKey != "" && attrBase != "" {
			if strings.EqualFold(cfgKey, attrKey) && strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
			continue
		}
		if attrKey != "" && strings.EqualFold(cfgKey, attrKey) {
			if cfgBase == "" || strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
		}
		if attrKey == "" && attrBase != "" && strings.EqualFold(cfgBase, attrBase) {
			return entry
		}
	}
	if attrKey != "" {
		for i := range e.cfg.CodexKey {
			entry := &e.cfg.CodexKey[i]
			if strings.EqualFold(strings.TrimSpace(entry.APIKey), attrKey) {
				return entry
			}
		}
	}
	return nil
}
