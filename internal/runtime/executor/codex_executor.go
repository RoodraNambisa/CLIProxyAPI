package executor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
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

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	codexUserAgent             = "codex-tui/0.135.0 (Mac OS 26.5.0; arm64) iTerm.app/3.6.10 (codex-tui; 0.135.0)"
	codexOriginator            = "codex-tui"
	codexDefaultImageToolModel = "gpt-image-2"
	codexSSEMaxFrameBytes      = 52_428_800
)

var dataTag = []byte("data:")

// Streamed Codex responses may emit response.output_item.done events while leaving
// response.completed.response.output empty. Keep the stream path aligned with the
// already-patched non-stream path by reconstructing response.output from those items.
func collectCodexOutputItemDone(eventData []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback *[][]byte) {
	itemResult := gjson.GetBytes(eventData, "item")
	if !itemResult.Exists() || itemResult.Type != gjson.JSON {
		return
	}
	outputIndexResult := gjson.GetBytes(eventData, "output_index")
	if outputIndexResult.Exists() {
		outputItemsByIndex[outputIndexResult.Int()] = []byte(itemResult.Raw)
		return
	}
	*outputItemsFallback = append(*outputItemsFallback, []byte(itemResult.Raw))
}

func patchCodexCompletedOutput(eventData []byte, outputItemsByIndex map[int64][]byte, outputItemsFallback [][]byte) []byte {
	outputResult := gjson.GetBytes(eventData, "response.output")
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

func codexTerminalStreamError(eventData []byte) (statusErr, bool) {
	eventType := gjson.GetBytes(eventData, "type").String()
	switch eventType {
	case "response.failed":
		return codexResponseFailedError(eventData), true
	case "response.incomplete":
		return codexResponseIncompleteError(eventData), true
	case "response.completed", "response.done":
		switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(eventData, "response.status").String())) {
		case "failed":
			return codexResponseFailedError(eventData), true
		case "incomplete":
			return codexResponseIncompleteError(eventData), true
		case "cancelled", "canceled":
			return codexResponseCancelledError(), true
		}
		if responseError := gjson.GetBytes(eventData, "response.error"); responseError.Exists() && strings.TrimSpace(responseError.Raw) != "null" {
			return codexResponseFailedError(eventData), true
		}
	case "error":
		return codexStreamErrorEventError(eventData), true
	default:
		return statusErr{}, false
	}
	return statusErr{}, false
}

func normalizeCodexCompletion(payload []byte) []byte {
	if strings.TrimSpace(gjson.GetBytes(payload, "type").String()) == "response.done" && isCodexSuccessfulCompletion(payload) {
		updated, err := sjson.SetBytes(payload, "type", "response.completed")
		if err == nil && len(updated) > 0 {
			return updated
		}
	}
	return payload
}

func isCodexSuccessfulCompletion(payload []byte) bool {
	if !isCodexCompletionType(gjson.GetBytes(payload, "type").String()) {
		return false
	}
	status := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "response.status").String()))
	if status != "" && status != "completed" {
		return false
	}
	responseError := gjson.GetBytes(payload, "response.error")
	return !responseError.Exists() || strings.TrimSpace(responseError.Raw) == "null"
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
	return isCodexSuccessfulCompletion(bytes.TrimSpace(line[len(dataTag):]))
}

func codexSSEEventCompletion(line []byte) bool {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, []byte("event:")) {
		return false
	}
	return isCodexCompletionType(string(bytes.TrimSpace(line[len("event:"):])))
}

func codexResponseFailedError(eventData []byte) statusErr {
	code := strings.TrimSpace(gjson.GetBytes(eventData, "response.error.code").String())
	message := strings.TrimSpace(gjson.GetBytes(eventData, "response.error.message").String())
	if message == "" {
		message = "response failed"
	}
	if code == "" {
		code = "server_error"
	}
	return codexStreamStatusErr(codexStreamErrorStatus(code, http.StatusInternalServerError), message, code, codexStreamErrorType(code), nil)
}

func codexResponseIncompleteError(eventData []byte) statusErr {
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
	var param *gjson.Result
	if paramResult := gjson.GetBytes(eventData, "error.param"); paramResult.Exists() {
		param = &paramResult
	} else if paramResult := gjson.GetBytes(eventData, "param"); paramResult.Exists() {
		param = &paramResult
	}
	return codexStreamStatusErr(codexStreamErrorStatus(code, http.StatusInternalServerError), message, code, errType, param)
}

func codexStreamErrorStatus(code string, fallback int) int {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "invalid_api_key", "authentication_error", "unauthorized":
		return http.StatusUnauthorized
	case "permission_error", "permission_denied", "forbidden":
		return http.StatusForbidden
	case "rate_limit_exceeded", "quota_exceeded", "insufficient_quota":
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
	case "invalid_api_key", "authentication_error", "unauthorized":
		return "authentication_error"
	case "permission_error", "permission_denied", "forbidden":
		return "permission_error"
	case "rate_limit_exceeded", "quota_exceeded", "insufficient_quota":
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
	cfg *config.Config
}

func NewCodexExecutor(cfg *config.Config) *CodexExecutor { return &CodexExecutor{cfg: cfg} }

func (e *CodexExecutor) Identifier() string { return "codex" }

func translateCodexRequestBodies(from, to sdktranslator.Format, baseModel string, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, stream bool) ([]byte, []byte, []byte) {
	originalPayload := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayload = opts.OriginalRequest
	}
	body := sdktranslator.TranslateRequest(from, to, baseModel, req.Payload, stream)
	originalTranslated := body
	if len(originalPayload) > 0 && !bytes.Equal(originalPayload, req.Payload) {
		originalTranslated = sdktranslator.TranslateRequest(from, to, baseModel, originalPayload, stream)
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
	if strings.TrimSpace(apiKey) != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(req, attrs)
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
	return httpClient.Do(httpReq)
}

func (e *CodexExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	ctx = contextWithCodexFingerprintPersona(ctx, e.cfg, auth)
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

	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	to := sdktranslator.FromString("codex")
	originalPayload, originalTranslated, body := translateCodexRequestBodies(from, to, baseModel, req, opts, false)

	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return resp, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, originalTranslated, requestedModel)
	originalTranslated = nil
	body, _ = sjson.SetBytes(body, "model", baseModel)
	body, _ = sjson.SetBytes(body, "stream", true)
	body, _ = sjson.DeleteBytes(body, "previous_response_id")
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	body, _ = sjson.DeleteBytes(body, "stream_options")
	body = normalizeCodexInstructions(body)
	body, err = e.applyDisabledImageGenerationToolPolicy(auth, body)
	if err != nil {
		return resp, err
	}
	body = helps.SanitizeCodexReasoningEncryptedContent(ctx, "codex executor", body)
	body = helps.NormalizeCodexToolSelection(body)
	replayAuthID := ""
	if auth != nil {
		replayAuthID = auth.ID
	}
	replayNamespace := helps.ReasoningReplayNamespace(ctx, e.Identifier(), replayAuthID)
	body, replayScope := helps.ApplyCodexReasoningReplay(ctx, from.String(), replayNamespace, baseModel, originalPayload, body, req.Metadata, opts.Metadata, opts.Headers)
	reporter.SetRequestServiceTierFromPayload(body)
	imageRequest := cliproxyauth.PayloadHasImageGenerationTool(body)

	url := strings.TrimSuffix(baseURL, "/") + "/responses"
	httpReq, upstreamBody, identityState, err := e.cacheHelper(ctx, from, url, auth, req, originalPayload, body, true)
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
	applyCodexHeaders(httpReq, auth, apiKey, true, e.cfg)
	applyCodexIdentityConfuseHeaders(httpReq.Header, &identityState)
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
	httpClient := e.newCodexHTTPClient(ctx, auth, imageRequest)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex executor: close response body error: %v", errClose)
		}
	}()
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		b, _ := io.ReadAll(httpResp.Body)
		upstreamBody := applyCodexIdentityConfuseResponsePayload(b, identityState)
		helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamBody)
		helps.LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", httpResp.StatusCode, helps.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), upstreamBody))
		clientBody := applyCodexIdentityExposeResponsePayload(upstreamBody, identityState)
		helps.ClearCodexReasoningReplayOnInvalidSignature(replayScope, httpResp.StatusCode, clientBody)
		err = newCodexStatusErr(httpResp.StatusCode, clientBody)
		return resp, err
	}
	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	upstreamData := applyCodexIdentityConfuseResponsePayload(data, identityState)
	helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamData)

	lines := bytes.Split(upstreamData, []byte("\n"))
	outputItemsByIndex := make(map[int64][]byte)
	var outputItemsFallback [][]byte
	for _, line := range lines {
		if !bytes.HasPrefix(line, dataTag) {
			continue
		}

		eventData := bytes.TrimSpace(line[5:])
		eventType := gjson.GetBytes(eventData, "type").String()

		if eventType == "response.output_item.done" {
			collectCodexOutputItemDone(eventData, outputItemsByIndex, &outputItemsFallback)
			continue
		}

		clientEventData := applyCodexIdentityExposeResponsePayload(eventData, identityState)
		if terminalErr, ok := codexTerminalStreamError(clientEventData); ok {
			helps.ClearCodexReasoningReplayOnInvalidSignature(replayScope, terminalErr.code, clientEventData)
			err = terminalErr
			return resp, err
		}
		eventData = normalizeCodexCompletion(eventData)
		eventType = gjson.GetBytes(eventData, "type").String()

		if eventType != "response.completed" {
			continue
		}

		if detail, ok := helps.ParseCodexUsage(eventData); ok {
			reporter.Publish(ctx, detail)
		}
		publishCodexImageToolUsage(ctx, reporter, bodyRef.Bytes(), eventData)

		completedData := patchCodexCompletedOutput(eventData, outputItemsByIndex, outputItemsFallback)
		helps.CacheCodexReasoningReplayFromCompleted(replayScope, completedData)

		var param any
		clientCompletedData := applyCodexIdentityExposeResponsePayload(completedData, identityState)
		out := sdktranslator.TranslateNonStream(ctx, to, from, req.Model, originalRef.Bytes(), bodyRef.Bytes(), clientCompletedData, &param)
		resp = cliproxyexecutor.Response{Payload: out, Headers: httpResp.Header.Clone()}
		return resp, nil
	}
	err = statusErr{code: 408, msg: "stream error: stream disconnected before completion: stream closed before response.completed"}
	return resp, err
}

func (e *CodexExecutor) executeCompact(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	ctx = contextWithCodexFingerprintPersona(ctx, e.cfg, auth)
	baseModel := thinking.ParseSuffix(req.Model).ModelName

	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}

	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	to := sdktranslator.FromString("openai-response")
	originalPayload, originalTranslated, body := translateCodexRequestBodies(from, to, baseModel, req, opts, false)

	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return resp, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, originalTranslated, requestedModel)
	originalTranslated = nil
	body, _ = sjson.SetBytes(body, "model", baseModel)
	body, _ = sjson.DeleteBytes(body, "stream")
	body = normalizeCodexInstructions(body)
	body, err = e.applyDisabledImageGenerationToolPolicy(auth, body)
	if err != nil {
		return resp, err
	}
	body = helps.SanitizeCodexReasoningEncryptedContent(ctx, "codex executor", body)
	body = helps.NormalizeCodexToolSelection(body)
	reporter.SetRequestServiceTierFromPayload(body)
	imageRequest := cliproxyauth.PayloadHasImageGenerationTool(body)

	url := strings.TrimSuffix(baseURL, "/") + "/responses/compact"
	httpReq, upstreamBody, identityState, err := e.cacheHelper(ctx, from, url, auth, req, originalPayload, body, true)
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
	applyCodexHeaders(httpReq, auth, apiKey, false, e.cfg)
	applyCodexIdentityConfuseHeaders(httpReq.Header, &identityState)
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
	httpClient := e.newCodexHTTPClient(ctx, auth, imageRequest)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex executor: close response body error: %v", errClose)
		}
	}()
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		b, _ := io.ReadAll(httpResp.Body)
		upstreamBody := applyCodexIdentityConfuseResponsePayload(b, identityState)
		helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamBody)
		helps.LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", httpResp.StatusCode, helps.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), upstreamBody))
		clientBody := applyCodexIdentityExposeResponsePayload(upstreamBody, identityState)
		err = newCodexStatusErr(httpResp.StatusCode, clientBody)
		return resp, err
	}
	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	upstreamData := applyCodexIdentityConfuseResponsePayload(data, identityState)
	helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamData)
	reporter.Publish(ctx, helps.ParseOpenAIUsage(upstreamData))
	reporter.EnsurePublished(ctx)
	var param any
	clientData := applyCodexIdentityExposeResponsePayload(upstreamData, identityState)
	out := sdktranslator.TranslateNonStream(ctx, to, from, req.Model, originalRef.Bytes(), bodyRef.Bytes(), clientData, &param)
	resp = cliproxyexecutor.Response{Payload: out, Headers: httpResp.Header.Clone()}
	return resp, nil
}

func (e *CodexExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	ctx = contextWithCodexFingerprintPersona(ctx, e.cfg, auth)
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

	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	from := opts.SourceFormat
	to := sdktranslator.FromString("codex")
	originalPayload, originalTranslated, body := translateCodexRequestBodies(from, to, baseModel, req, opts, true)

	body, err = thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, err
	}

	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	body = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", body, originalTranslated, requestedModel)
	originalTranslated = nil
	body, _ = sjson.DeleteBytes(body, "previous_response_id")
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	body, _ = sjson.DeleteBytes(body, "stream_options")
	body, _ = sjson.SetBytes(body, "model", baseModel)
	body = normalizeCodexInstructions(body)
	body, err = e.applyDisabledImageGenerationToolPolicy(auth, body)
	if err != nil {
		return nil, err
	}
	body = helps.SanitizeCodexReasoningEncryptedContent(ctx, "codex executor", body)
	body = helps.NormalizeCodexToolSelection(body)
	replayAuthID := ""
	if auth != nil {
		replayAuthID = auth.ID
	}
	replayNamespace := helps.ReasoningReplayNamespace(ctx, e.Identifier(), replayAuthID)
	body, replayScope := helps.ApplyCodexReasoningReplay(ctx, from.String(), replayNamespace, baseModel, originalPayload, body, req.Metadata, opts.Metadata, opts.Headers)
	reporter.SetRequestServiceTierFromPayload(body)
	imageRequest := cliproxyauth.PayloadHasImageGenerationTool(body)
	imageStreamPassthrough := metadataBool(opts.Metadata, cliproxyexecutor.ImageGenerationStreamPassthroughMetadataKey) && from == sdktranslator.FormatOpenAIResponse && imageRequest
	trustUpstreamSSE := metadataBool(opts.Metadata, cliproxyexecutor.TrustUpstreamSSEMetadataKey) && from == sdktranslator.FormatOpenAIResponse

	url := strings.TrimSuffix(baseURL, "/") + "/responses"
	httpReq, upstreamBody, identityState, err := e.cacheHelper(ctx, from, url, auth, req, originalPayload, body, true)
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
	applyCodexHeaders(httpReq, auth, apiKey, true, e.cfg)
	applyCodexIdentityConfuseHeaders(httpReq.Header, &identityState)
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

	httpClient := e.newCodexHTTPClient(ctx, auth, imageRequest)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		cleanupBodies()
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		defer cleanupBodies()
		data, readErr := io.ReadAll(httpResp.Body)
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex executor: close response body error: %v", errClose)
		}
		if readErr != nil {
			helps.RecordAPIResponseError(ctx, e.cfg, readErr)
			return nil, readErr
		}
		upstreamBody := applyCodexIdentityConfuseResponsePayload(data, identityState)
		helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamBody)
		helps.LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", httpResp.StatusCode, helps.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), upstreamBody))
		clientBody := applyCodexIdentityExposeResponsePayload(upstreamBody, identityState)
		helps.ClearCodexReasoningReplayOnInvalidSignature(replayScope, httpResp.StatusCode, clientBody)
		err = newCodexStatusErr(httpResp.StatusCode, clientBody)
		return nil, err
	}
	if state, ok := opts.Metadata[cliproxyexecutor.ImageGenerationStreamPassthroughStateMetadataKey].(*cliproxyexecutor.ImageGenerationStreamPassthroughState); ok {
		state.SetEnabled(imageStreamPassthrough)
	}
	helps.ReleaseRequestBodyAfterStreamEstablished(ctx, opts)
	out := make(chan cliproxyexecutor.StreamChunk, cliproxyexecutor.StreamBufferSize)
	go func() {
		defer close(out)
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
		}
		var param any
		outputItemsByIndex := make(map[int64][]byte)
		var outputItemsFallback [][]byte
		var trustedFrame []byte
		var pendingImageCompletionEvent []byte
		var pendingTranslatedCompletionEvent []byte
		emit := func(chunk cliproxyexecutor.StreamChunk) bool {
			if ctx == nil {
				out <- chunk
				return true
			}
			select {
			case <-ctx.Done():
				return false
			case out <- chunk:
				return true
			}
		}
		emitSuccessfulTerminal := func() {
			reporter.EnsurePublished(ctx)
			if metadataBool(opts.Metadata, cliproxyexecutor.StreamTerminalMarkerMetadataKey) {
				_ = emit(cliproxyexecutor.SuccessfulStreamTerminalChunk())
			}
		}
		flushTrustedFrame := func() (bool, bool) {
			if len(bytes.TrimSpace(trustedFrame)) == 0 {
				trustedFrame = nil
				return true, false
			}
			frame := trustedFrame
			trustedFrame = nil
			hasData := false
			terminal := false
			if codexTrustedSSEFrameNeedsInspection(frame) {
				data, frameHasData := codexSSEFrameDataPayload(frame)
				hasData = frameHasData
				if hasData {
					if terminalErr, ok := codexTerminalStreamError(data); ok {
						helps.ClearCodexReasoningReplayOnInvalidSignature(replayScope, terminalErr.code, data)
						reporter.PublishFailure(ctx, terminalErr)
						_ = emit(cliproxyexecutor.StreamChunk{Err: terminalErr})
						return false, false
					}
					publishCodexStreamUsage(reporter, streamBody.Bytes(), data)
					terminal = isCodexSuccessfulCompletion(data)
				}
			}
			return emit(cliproxyexecutor.StreamChunk{Payload: frame}), terminal
		}
		for scanner.Scan() {
			line := scanner.Bytes()
			if !trustUpstreamSSE {
				line = applyCodexIdentityConfuseResponsePayload(line, identityState)
			}
			helps.AppendAPIResponseChunk(ctx, e.cfg, line)
			clientLine := applyCodexIdentityExposeResponsePayload(line, identityState)
			if bytes.HasPrefix(clientLine, dataTag) {
				clientData := bytes.TrimSpace(clientLine[len(dataTag):])
				if terminalErr, ok := codexTerminalStreamError(clientData); ok {
					helps.ClearCodexReasoningReplayOnInvalidSignature(replayScope, terminalErr.code, clientData)
					reporter.PublishFailure(ctx, terminalErr)
					_ = emit(cliproxyexecutor.StreamChunk{Err: terminalErr})
					return
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
				terminal := false
				trimmedClientLine := bytes.TrimSpace(clientLine)
				if bytes.HasPrefix(trimmedClientLine, dataTag) {
					data := bytes.TrimSpace(trimmedClientLine[len(dataTag):])
					terminal = isCodexSuccessfulCompletion(data)
					switch gjson.GetBytes(data, "type").String() {
					case "response.output_item.done":
						collectCodexOutputItemDone(data, outputItemsByIndex, &outputItemsFallback)
					case "response.completed":
						data = patchCodexCompletedOutput(data, outputItemsByIndex, outputItemsFallback)
						clientLine = append([]byte("data: "), data...)
					}
					if len(pendingImageCompletionEvent) > 0 {
						if terminal {
							pendingEvent := append([]byte(nil), pendingImageCompletionEvent...)
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

			if bytes.HasPrefix(line, dataTag) {
				data := bytes.TrimSpace(line[5:])
				terminal = isCodexSuccessfulCompletion(data)
				if terminal {
					data = normalizeCodexCompletion(data)
				}
				switch gjson.GetBytes(data, "type").String() {
				case "response.output_item.done":
					collectCodexOutputItemDone(data, outputItemsByIndex, &outputItemsFallback)
				case "response.completed":
					if detail, ok := helps.ParseCodexUsage(data); ok {
						reporter.Observe(detail)
					}
					observeCodexImageToolUsage(reporter, streamBody.Bytes(), data)
					data = patchCodexCompletedOutput(data, outputItemsByIndex, outputItemsFallback)
					helps.CacheCodexReasoningReplayFromCompleted(replayScope, data)
					translatedLine = append([]byte("data: "), data...)
				}
				if len(pendingTranslatedCompletionEvent) > 0 {
					if terminal {
						eventChunks := sdktranslator.TranslateStream(ctx, to, from, req.Model, streamOriginalPayload.Bytes(), streamBody.Bytes(), pendingTranslatedCompletionEvent, &param)
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
			for i := range chunks {
				if !emit(cliproxyexecutor.StreamChunk{Payload: chunks[i]}) {
					return
				}
			}
			if terminal && scanner.Err() == nil {
				emitSuccessfulTerminal()
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
			reporter.PublishFailure(ctx)
			_ = emit(cliproxyexecutor.StreamChunk{Err: errScan})
		} else {
			errIncomplete := helps.IncompleteStreamError("codex")
			helps.RecordAPIResponseError(ctx, e.cfg, errIncomplete)
			reporter.PublishFailure(ctx, errIncomplete)
			_ = emit(cliproxyexecutor.StreamChunk{Err: errIncomplete})
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: httpResp.Header.Clone(), Chunks: out}, nil
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
	if !isCodexSuccessfulCompletion(data) {
		return line
	}
	return append([]byte("data: "), normalizeCodexCompletion(data)...)
}

func (e *CodexExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName

	from := opts.SourceFormat
	to := sdktranslator.FromString("codex")
	body := sdktranslator.TranslateRequest(from, to, baseModel, req.Payload, false)

	body, err := thinking.ApplyThinking(body, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}

	body, _ = sjson.SetBytes(body, "model", baseModel)
	body, _ = sjson.DeleteBytes(body, "previous_response_id")
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	body, _ = sjson.DeleteBytes(body, "stream_options")
	body, _ = sjson.SetBytes(body, "stream", false)
	body = normalizeCodexInstructions(body)

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
	enabled                bool
	authID                 string
	originalPromptCacheKey string
	promptCacheKey         string
	turnIDs                []codexIdentityReplacement
}

type codexIdentityReplacement struct {
	original string
	confused string
}

func (e *CodexExecutor) cacheHelper(ctx context.Context, from sdktranslator.Format, url string, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, userPayload []byte, rawJSON []byte, allowIdentityConfuse bool) (*http.Request, []byte, codexIdentityConfuseState, error) {
	var cache helps.CodexCache
	if from == "claude" {
		userIDResult := gjson.GetBytes(req.Payload, "metadata.user_id")
		if userIDResult.Exists() {
			key := fmt.Sprintf("%s-%s", req.Model, userIDResult.String())
			var ok bool
			if cache, ok = helps.GetCodexCache(key); !ok {
				cache = helps.CodexCache{
					ID:     uuid.New().String(),
					Expire: time.Now().Add(1 * time.Hour),
				}
				helps.SetCodexCache(key, cache)
			}
		}
	} else if from == "openai-response" {
		promptCacheKey := gjson.GetBytes(req.Payload, "prompt_cache_key")
		if promptCacheKey.Exists() {
			cache.ID = promptCacheKey.String()
		}
	} else if from == "openai" {
		if apiKey := strings.TrimSpace(helps.APIKeyFromContext(ctx)); apiKey != "" {
			cache.ID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-proxy-api:codex:prompt-cache:"+apiKey)).String()
		}
	}

	if cache.ID != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "prompt_cache_key", cache.ID)
	}
	var identityState codexIdentityConfuseState
	if allowIdentityConfuse {
		rawJSON, identityState = applyCodexIdentityConfuseBody(e.cfg, auth, userPayload, rawJSON)
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
	if cache.ID != "" {
		httpReq.Header.Set("Session_id", cache.ID)
	}
	return httpReq, rawJSON, identityState, nil
}

func applyCodexIdentityConfuseBody(cfg *config.Config, auth *cliproxyauth.Auth, userPayload []byte, rawJSON []byte) ([]byte, codexIdentityConfuseState) {
	if !codexIdentityConfuseEnabled(cfg) || auth == nil || strings.TrimSpace(auth.ID) == "" || len(rawJSON) == 0 {
		return rawJSON, codexIdentityConfuseState{}
	}

	state := codexIdentityConfuseState{enabled: true, authID: strings.TrimSpace(auth.ID)}
	if promptCacheKey := strings.TrimSpace(gjson.GetBytes(userPayload, "prompt_cache_key").String()); promptCacheKey != "" {
		state.originalPromptCacheKey = promptCacheKey
		state.promptCacheKey = codexIdentityConfuseUUID(auth.ID, "prompt-cache", promptCacheKey)
		rawJSON, _ = sjson.SetBytes(rawJSON, "prompt_cache_key", state.promptCacheKey)
	}
	if installationID := strings.TrimSpace(gjson.GetBytes(userPayload, "client_metadata.x-codex-installation-id").String()); installationID != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.x-codex-installation-id", codexIdentityConfuseUUID(auth.ID, "installation", installationID))
	}
	if turnMetadata := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-turn-metadata").String()); turnMetadata != "" {
		rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.x-codex-turn-metadata", applyCodexTurnMetadataIdentityConfuse(turnMetadata, &state))
	}
	if state.promptCacheKey != "" {
		if windowID := strings.TrimSpace(gjson.GetBytes(rawJSON, "client_metadata.x-codex-window-id").String()); windowID != "" {
			rawJSON, _ = sjson.SetBytes(rawJSON, "client_metadata.x-codex-window-id", state.promptCacheKey+":0")
		}
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

	setHeaderCasePreserved(headers, "Session-Id", state.promptCacheKey)
	if headerValueCaseInsensitive(headers, "session_id") != "" {
		sessionHeaderKey := "session_id"
		if _, ok := headers["Session_id"]; ok {
			sessionHeaderKey = "Session_id"
		}
		setHeaderCasePreserved(headers, sessionHeaderKey, state.promptCacheKey)
	}
	if headerValueCaseInsensitive(headers, "Conversation_id") != "" {
		setHeaderCasePreserved(headers, "Conversation_id", state.promptCacheKey)
	}
	headers.Set("X-Client-Request-Id", state.promptCacheKey)
	headers.Set("Thread-Id", state.promptCacheKey)
	headers.Set("X-Codex-Window-Id", state.promptCacheKey+":0")
}

func applyCodexTurnMetadataIdentityConfuse(rawTurnMetadata string, state *codexIdentityConfuseState) string {
	updatedTurnMetadata := rawTurnMetadata
	if state == nil || !state.enabled {
		return updatedTurnMetadata
	}
	if state.promptCacheKey != "" && gjson.Get(rawTurnMetadata, "prompt_cache_key").Exists() {
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "prompt_cache_key", state.promptCacheKey)
	} else if state.promptCacheKey != "" && state.originalPromptCacheKey != "" {
		updatedTurnMetadata = strings.ReplaceAll(updatedTurnMetadata, state.originalPromptCacheKey, state.promptCacheKey)
	}
	if turnID := strings.TrimSpace(gjson.Get(rawTurnMetadata, "turn_id").String()); turnID != "" {
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "turn_id", state.confuseTurnID(turnID))
	}
	if state.promptCacheKey != "" && gjson.Get(rawTurnMetadata, "window_id").Exists() {
		updatedTurnMetadata, _ = sjson.Set(updatedTurnMetadata, "window_id", state.promptCacheKey+":0")
	}
	return updatedTurnMetadata
}

func applyCodexIdentityConfuseResponsePayload(payload []byte, state codexIdentityConfuseState) []byte {
	payload = replaceCodexIdentityResponsePayload(payload, state.originalPromptCacheKey, state.promptCacheKey)
	for _, turnID := range state.turnIDs {
		payload = replaceCodexIdentityResponsePayload(payload, turnID.original, turnID.confused)
	}
	return payload
}

func applyCodexIdentityExposeResponsePayload(payload []byte, state codexIdentityConfuseState) []byte {
	payload = replaceCodexIdentityResponsePayload(payload, state.promptCacheKey, state.originalPromptCacheKey)
	for _, turnID := range state.turnIDs {
		payload = replaceCodexIdentityResponsePayload(payload, turnID.confused, turnID.original)
	}
	return payload
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
	confusedTurnID := codexIdentityConfuseUUID(state.authID, "turn", turnID)
	state.turnIDs = append(state.turnIDs, codexIdentityReplacement{original: turnID, confused: confusedTurnID})
	return confusedTurnID
}

func replaceCodexIdentityResponsePayload(payload []byte, from string, to string) []byte {
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	if len(payload) == 0 || from == "" || to == "" || from == to || !bytes.Contains(payload, []byte(from)) {
		return payload
	}
	return bytes.ReplaceAll(payload, []byte(from), []byte(to))
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
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String()
}

func applyCodexHeaders(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, cfg *config.Config) {
	var ginHeaders http.Header
	if ginCtx, ok := r.Context().Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
		ginHeaders = ginCtx.Request.Header
	}
	applyCodexHeadersFromSources(r, auth, token, stream, cfg, ginHeaders)
}

func applyCodexDirectImageHeaders(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, cfg *config.Config) {
	var ginHeaders http.Header
	if ginCtx, ok := r.Context().Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
		ginHeaders = ginCtx.Request.Header.Clone()
		ginHeaders.Del("User-Agent")
	}
	applyCodexHeadersFromSources(r, auth, token, stream, cfg, ginHeaders)
}

func applyCodexHeadersFromSources(r *http.Request, auth *cliproxyauth.Auth, token string, stream bool, cfg *config.Config, ginHeaders http.Header) {
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+token)

	if ginHeaders.Get("X-Codex-Beta-Features") != "" {
		r.Header.Set("X-Codex-Beta-Features", ginHeaders.Get("X-Codex-Beta-Features"))
	}
	misc.EnsureHeader(r.Header, ginHeaders, "Version", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Codex-Turn-Metadata", "")
	misc.EnsureHeader(r.Header, ginHeaders, "X-Client-Request-Id", "")
	cfgUserAgent, cfgOriginator, _ := codexHeaderDefaults(cfg, auth)
	ensureHeaderWithConfigPrecedence(r.Header, ginHeaders, "User-Agent", cfgUserAgent, codexUserAgent)

	if strings.Contains(r.Header.Get("User-Agent"), "Mac OS") {
		misc.EnsureHeader(r.Header, ginHeaders, "Session_id", uuid.NewString())
	}

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
		if auth != nil && auth.Metadata != nil {
			if accountID, ok := auth.Metadata["account_id"].(string); ok {
				r.Header.Set("Chatgpt-Account-Id", accountID)
			}
		}
	}
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(r, attrs)
}

func (e *CodexExecutor) newCodexHTTPClient(ctx context.Context, auth *cliproxyauth.Auth, imageRequest bool) *http.Client {
	var cfg *config.Config
	if e != nil {
		cfg = e.cfg
	}
	if codexFingerprintJA3Enabled(cfg) {
		return helps.NewCodexNativeTLSHTTP1Client(ctx, cfg, auth, 0)
	}
	forceHTTP1 := codexFingerprintShouldForceHTTP1(cfg, imageRequest)
	if forceHTTP1 {
		return helps.NewProxyAwareHTTP1Client(ctx, cfg, auth, 0)
	}
	return helps.NewProxyAwareHTTPClient(ctx, cfg, auth, 0)
}

func newCodexStatusErr(statusCode int, body []byte) statusErr {
	errCode := statusCode
	if isCodexModelCapacityError(body) {
		errCode = http.StatusTooManyRequests
	}
	requestScopedContextError := isCodexContextTooLargeRequestError(errCode, body)
	body = classifyCodexStatusError(errCode, body)
	err := statusErr{code: errCode, msg: string(body)}
	if requestScopedContextError {
		err.skipAuthResult = true
	}
	if retryAfter := parseCodexRetryAfter(errCode, body, time.Now()); retryAfter != nil {
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
	errorMessage := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "error.message").String()))
	if errorMessage == "" {
		errorMessage = strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "message").String()))
	}
	lower := strings.ToLower(strings.TrimSpace(string(body)))
	upstreamCode := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "error.code").String()))
	upstreamType := strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "error.type").String()))
	isInvalidRequest := upstreamType == "" || upstreamType == "invalid_request_error"

	switch {
	case isCodexContextTooLargeRequestError(statusCode, body) || isInvalidRequest && statusCode != http.StatusTooManyRequests && strings.Contains(errorMessage, "too many tokens"):
		return "context_too_large", "invalid_request_error", true
	case strings.Contains(lower, "invalid signature in thinking block") || strings.Contains(lower, "invalid_encrypted_content"):
		return "thinking_signature_invalid", "invalid_request_error", true
	case upstreamCode == "previous_response_not_found" || strings.Contains(lower, "previous_response_not_found") || strings.Contains(lower, "previous_response_id") && strings.Contains(lower, "not found"):
		return "previous_response_not_found", "invalid_request_error", true
	case statusCode == http.StatusUnauthorized || upstreamType == "authentication_error" || upstreamCode == "invalid_api_key" || strings.Contains(lower, "invalid or expired token") || strings.Contains(lower, "refresh_token_reused"):
		return "auth_unavailable", "authentication_error", true
	default:
		return "", "", false
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
		strings.Contains(errorMessage, "maximum context")
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
	if !isCodexCompletionType(gjson.GetBytes(data, "type").String()) {
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
	return statusErr{code: errCfg.StatusCode, msg: string(body), skipAuthResult: true}
}

func removeCodexImageGenerationTool(body []byte) []byte {
	tools := gjson.GetBytes(body, "tools")
	if !tools.IsArray() {
		return body
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
	if retainedCount == 0 {
		body, _ = sjson.DeleteBytes(body, "tools")
		body, _ = sjson.DeleteBytes(body, "tool_choice")
		body, _ = sjson.DeleteBytes(body, "parallel_tool_calls")
	} else {
		body, _ = sjson.SetRawBytes(body, "tools", retained.Bytes())
	}
	if codexToolChoiceSelectsImageGeneration(gjson.GetBytes(body, "tool_choice")) {
		body, _ = sjson.DeleteBytes(body, "tool_choice")
	}
	return body
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
	tools := gjson.GetBytes(body, "tools")
	if tools.IsArray() {
		for _, tool := range tools.Array() {
			if tool.Get("type").String() != "image_generation" {
				continue
			}
			if model := strings.TrimSpace(tool.Get("model").String()); model != "" {
				return model
			}
			break
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
	if statusCode != http.StatusTooManyRequests || len(errorBody) == 0 {
		return nil
	}
	if strings.TrimSpace(gjson.GetBytes(errorBody, "error.type").String()) != "usage_limit_reached" {
		return nil
	}
	if resetsAt := gjson.GetBytes(errorBody, "error.resets_at").Int(); resetsAt > 0 {
		resetAtTime := time.Unix(resetsAt, 0)
		if resetAtTime.After(now) {
			retryAfter := resetAtTime.Sub(now)
			return &retryAfter
		}
	}
	if resetsInSeconds := gjson.GetBytes(errorBody, "error.resets_in_seconds").Int(); resetsInSeconds > 0 {
		retryAfter := time.Duration(resetsInSeconds) * time.Second
		return &retryAfter
	}
	return nil
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
