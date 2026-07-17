package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	fhttp "github.com/bogdanfinn/fhttp"
	fhttptrace "github.com/bogdanfinn/fhttp/httptrace"
	"github.com/google/uuid"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	chatGPTWebClientVersion         = "prod-a194cd50d4416d3c0b47c740f206b12ce60f5887"
	chatGPTWebClientBuildNumber     = "6708908"
	chatGPTWebSearchModel           = "gpt-5-5"
	chatGPTWebSSEMaxFrameBytes      = 50 << 20
	chatGPTWebMaxErrorBodyBytes     = 1 << 20
	chatGPTWebMaxJSONBodyBytes      = 32 << 20
	chatGPTWebMaxHTMLBodyBytes      = 16 << 20
	chatGPTWebMaxBootstrapRedirects = 5
)

type chatGPTWebPreparedRequest struct {
	baseModel        string
	routeModel       string
	responseFormat   sdktranslator.Format
	originalPayload  []byte
	canonicalBody    []byte
	request          helps.ChatGPTWebRequest
	terminalMarker   bool
	trustUpstreamSSE bool
	maxImageResults  int
}

type chatGPTWebRequirements struct {
	Token          string
	ProofToken     string
	TurnstileToken string
	SOToken        string
}

type chatGPTWebTextResult struct {
	Text    string
	Query   string
	Sources []chatGPTWebSearchSource
	Search  bool
	Usage   map[string]any
}

type chatGPTWebSearchSource struct {
	Title string `json:"title,omitempty"`
	URL   string `json:"url"`
}

func (e *ChatGPTWebExecutor) executeRuntime(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	if selectedAuthInstanceRetired(opts) {
		return resp, errXAIWebsocketSessionTerminated
	}
	prepared, err := e.prepareRuntimeRequest(ctx, auth, req, opts, false)
	if err != nil {
		return resp, err
	}
	reporter := helps.NewExecutorUsageReporter(ctx, e, prepared.routeModel, auth)
	defer reporter.TrackFailure(ctx, &err)
	reporter.SetTranslatedReasoningEffort(prepared.canonicalBody, e.Identifier())

	client, credential, err := e.newRuntimeClient(auth)
	if err != nil {
		return resp, err
	}
	defer e.finishChatGPTWebRuntimeClient(ctx, auth, credential, client)

	if prepared.request.Image != nil {
		completed, headers, errImage := e.executeChatGPTWebImage(ctx, client, credential, prepared)
		if errImage != nil {
			return resp, errImage
		}
		reporter.EnsurePublished(ctx)
		var param any
		out := sdktranslator.TranslateNonStream(ctx, sdktranslator.FormatCodex, prepared.responseFormat, prepared.routeModel,
			prepared.originalPayload, prepared.canonicalBody, completed, &param)
		return cliproxyexecutor.Response{Payload: out, Headers: headers}, nil
	}

	result, headers, errText := e.executeChatGPTWebText(ctx, client, credential, prepared)
	if errText != nil {
		return resp, errText
	}
	result.Usage = estimateChatGPTWebUsage(prepared.routeModel, prepared.request, result.Text)
	completed := buildChatGPTWebCompletedEvent(prepared.routeModel, result)
	if detail, ok := helps.ParseCodexUsage(completed); ok {
		reporter.Publish(ctx, detail)
	} else {
		reporter.EnsurePublished(ctx)
	}
	var param any
	out := sdktranslator.TranslateNonStream(ctx, sdktranslator.FormatCodex, prepared.responseFormat, prepared.routeModel,
		prepared.originalPayload, prepared.canonicalBody, completed, &param)
	return cliproxyexecutor.Response{Payload: out, Headers: headers}, nil
}

func (e *ChatGPTWebExecutor) executeRuntimeStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	if selectedAuthInstanceRetired(opts) {
		return nil, errXAIWebsocketSessionTerminated
	}
	prepared, err := e.prepareRuntimeRequest(ctx, auth, req, opts, true)
	if err != nil {
		return nil, err
	}
	passthroughState, _ := opts.Metadata[cliproxyexecutor.ImageGenerationStreamPassthroughStateMetadataKey].(*cliproxyexecutor.ImageGenerationStreamPassthroughState)
	if passthroughState != nil {
		passthroughState.SetEnabled(false)
	}
	client, credential, err := e.newRuntimeClient(auth)
	if err != nil {
		return nil, err
	}

	if prepared.request.Image != nil {
		imageStreamPassthrough := metadataBool(opts.Metadata, cliproxyexecutor.ImageGenerationStreamPassthroughMetadataKey)
		execution, errImage := e.beginChatGPTWebImage(ctx, client, credential, prepared)
		if errImage != nil {
			e.finishChatGPTWebRuntimeClient(ctx, auth, credential, client)
			return nil, errImage
		}
		return e.streamDeferredChatGPTWebResponse(ctx, auth, credential, prepared, client, execution.headers, passthroughState, imageStreamPassthrough, func() ([]byte, error) {
			completed, errFinish := e.finishChatGPTWebImage(ctx, client, credential, prepared, execution)
			return completed, chatGPTWebCommittedRequestError(ctx, errFinish)
		}), nil
	}

	if chatGPTWebRequestUsesSearch(prepared) {
		execution, errSearch := e.beginChatGPTWebSearch(ctx, client, credential, prepared)
		if errSearch != nil {
			e.finishChatGPTWebRuntimeClient(ctx, auth, credential, client)
			return nil, errSearch
		}
		return e.streamDeferredChatGPTWebResponse(ctx, auth, credential, prepared, client, execution.headers, nil, false, func() ([]byte, error) {
			result, errFinish := e.finishChatGPTWebSearch(ctx, client, credential, execution)
			if errFinish != nil {
				return nil, chatGPTWebCommittedRequestError(ctx, errFinish)
			}
			result.Usage = estimateChatGPTWebUsage(prepared.routeModel, prepared.request, result.Text)
			return buildChatGPTWebCompletedEvent(prepared.routeModel, result), nil
		}), nil
	}

	response, accumulator, errOpen := e.openChatGPTWebConversation(ctx, client, credential, prepared)
	if errOpen != nil {
		e.finishChatGPTWebRuntimeClient(ctx, auth, credential, client)
		return nil, errOpen
	}
	headers := cloneChatGPTWebHeaders(response.Header)
	reporter := helps.NewExecutorUsageReporter(ctx, e, prepared.routeModel, auth)
	reporter.SetTranslatedReasoningEffort(prepared.canonicalBody, e.Identifier())
	out := make(chan cliproxyexecutor.StreamChunk, cliproxyexecutor.StreamBufferSize)
	go func() {
		defer close(out)
		defer e.finishChatGPTWebRuntimeClient(ctx, auth, credential, client)
		defer func() {
			if errClose := response.Body.Close(); errClose != nil {
				log.Errorf("chatgpt web executor: close response body: %v", errClose)
			}
		}()
		if !sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.BootstrapCommitStreamChunk()) {
			return
		}
		responseID := "resp_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		messageID := "msg_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		sequencer := &chatGPTWebEventSequencer{}
		var param any
		emit := func(event []byte) bool {
			event = sequencer.Next(event)
			chunks := sdktranslator.TranslateStream(ctx, sdktranslator.FormatCodex, prepared.responseFormat, prepared.routeModel,
				prepared.originalPayload, prepared.canonicalBody, append([]byte("data: "), event...), &param)
			for _, chunk := range chunks {
				chunk = chatGPTWebTrustedSSEFrame(chunk, prepared.trustUpstreamSSE)
				if !sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Payload: chunk}) {
					return false
				}
			}
			return true
		}
		started := false
		emitStart := func() bool {
			if started {
				return true
			}
			if !emit(buildChatGPTWebCreatedEvent(responseID, prepared.routeModel)) {
				return false
			}
			if !emit(buildChatGPTWebInProgressEvent(responseID, prepared.routeModel)) {
				return false
			}
			for _, event := range buildChatGPTWebMessageAddedEvents(responseID, messageID, 0) {
				if !emit(event) {
					return false
				}
			}
			started = true
			return true
		}

		deltaCh := make(chan string)
		resultCh := make(chan error, 1)
		go func() {
			errConsume := consumeChatGPTWebConversation(ctx, response.Body, accumulator, func(delta string) bool {
				if ctx == nil {
					deltaCh <- delta
					return true
				}
				select {
				case deltaCh <- delta:
					return true
				case <-ctx.Done():
					return false
				}
			})
			resultCh <- errConsume
		}()

		initialWait := e.streamInitialWait
		if initialWait < 0 {
			initialWait = 0
		}
		initialTimer := time.NewTimer(initialWait)
		defer initialTimer.Stop()
		var heartbeatTicker *time.Ticker
		var heartbeat <-chan time.Time
		var contextDone <-chan struct{}
		if ctx != nil {
			contextDone = ctx.Done()
		}
		defer func() {
			if heartbeatTicker != nil {
				heartbeatTicker.Stop()
			}
		}()
		sendHeartbeat := func() bool {
			return sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Payload: chatGPTWebDeferredHeartbeat(prepared, "", 0)})
		}

		for {
			select {
			case delta := <-deltaCh:
				if !emitStart() || !emit(buildChatGPTWebTextDeltaEvent(responseID, messageID, 0, delta)) {
					return
				}
			case errConsume := <-resultCh:
				if errConsume != nil {
					errConsume = chatGPTWebCommittedRequestError(ctx, chatGPTWebUpstreamProtocolError(ctx, errConsume))
					reporter.PublishFailure(ctx, errConsume)
					_ = sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: errConsume})
					return
				}
				if !emitStart() {
					return
				}
				text := accumulator.Text()
				usage := estimateChatGPTWebUsage(prepared.routeModel, prepared.request, text)
				terminalEvents := buildChatGPTWebTerminalEvents(responseID, messageID, prepared.routeModel, text, nil, "", false, usage)
				for _, event := range terminalEvents {
					if !emit(event) {
						return
					}
				}
				if detail, ok := helps.ParseCodexUsage(terminalEvents[len(terminalEvents)-1]); ok {
					reporter.Publish(ctx, detail)
				} else {
					reporter.EnsurePublished(ctx)
				}
				if metadataBool(opts.Metadata, cliproxyexecutor.StreamTerminalMarkerMetadataKey) {
					_ = sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.SuccessfulStreamTerminalChunk())
				}
				return
			case <-initialTimer.C:
				if !sendHeartbeat() {
					return
				}
				if e.streamHeartbeat > 0 {
					heartbeatTicker = time.NewTicker(e.streamHeartbeat)
					heartbeat = heartbeatTicker.C
				}
			case <-heartbeat:
				if !sendHeartbeat() {
					return
				}
			case <-contextDone:
				return
			}
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: headers, Chunks: out}, nil
}

func (e *ChatGPTWebExecutor) prepareRuntimeRequest(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, stream bool) (*chatGPTWebPreparedRequest, error) {
	baseModel := strings.TrimSpace(thinking.ParseSuffix(req.Model).ModelName)
	routeModel := strings.TrimSpace(helps.PayloadRequestedModel(opts, req.Model))
	if routeModel == "" {
		routeModel = baseModel
	}
	originalSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalSource = opts.OriginalRequest
	}
	rawRequestBytes := max(len(req.Payload), len(originalSource))
	if rawRequestBytes > helps.ChatGPTWebMaxRequestBytes {
		return nil, statusErr{
			code:           http.StatusRequestEntityTooLarge,
			msg:            fmt.Sprintf("chatgpt web request exceeds %d bytes", helps.ChatGPTWebMaxRequestBytes),
			skipAuthResult: true,
		}
	}
	if rawRequestBytes > helps.ChatGPTWebMaxTextRequestBytes &&
		!chatGPTWebRawRequestHasImageInputs(req.Payload, opts.SourceFormat) &&
		!chatGPTWebRawRequestHasImageInputs(originalSource, opts.SourceFormat) {
		return nil, statusErr{
			code:           http.StatusRequestEntityTooLarge,
			msg:            fmt.Sprintf("chatgpt web text request exceeds %d bytes", helps.ChatGPTWebMaxTextRequestBytes),
			skipAuthResult: true,
		}
	}
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	canonicalBody, err := sdktranslator.TranslateRequestChecked(opts.SourceFormat, sdktranslator.FormatCodex, baseModel, req.Payload, stream)
	if err != nil {
		return nil, err
	}
	canonicalBody, err = thinking.ApplyThinking(canonicalBody, req.Model, opts.SourceFormat.String(), e.Identifier(), e.Identifier())
	if err != nil {
		return nil, err
	}
	canonicalBody = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, sdktranslator.FormatCodex.String(), opts.SourceFormat.String(), "",
		canonicalBody, originalSource, routeModel, helps.PayloadRequestPath(opts), opts.Headers)
	forcedTool := ""
	if chatGPTWebSearchAlias(routeModel) || chatGPTWebOriginalRequestUsesSearch(opts.OriginalRequest) {
		forcedTool = "search"
	}
	parsed, err := helps.ParseChatGPTWebRequestWithForcedTool(canonicalBody, forcedTool)
	if err != nil {
		return nil, statusErr{
			code:           http.StatusBadRequest,
			msg:            err.Error(),
			skipAuthResult: true,
			retryOtherAuth: helps.IsChatGPTWebProviderUnsupported(err),
		}
	}
	if parsed.Model == "" {
		parsed.Model = baseModel
	}
	if rawRequestBytes > helps.ChatGPTWebMaxTextRequestBytes && !chatGPTWebRequestHasImageInputs(parsed) {
		return nil, statusErr{
			code:           http.StatusRequestEntityTooLarge,
			msg:            fmt.Sprintf("chatgpt web text request exceeds %d bytes", helps.ChatGPTWebMaxTextRequestBytes),
			skipAuthResult: true,
		}
	}
	if parsed.WebSearch && parsed.Image != nil {
		return nil, statusErr{
			code:           http.StatusBadRequest,
			msg:            "chatgpt web cannot combine web search and image generation in one request",
			skipAuthResult: true,
			retryOtherAuth: true,
		}
	}
	if err = validateChatGPTWebImageRequest(parsed.Image); err != nil {
		return nil, err
	}
	if parsed.Image == nil {
		if err = validateChatGPTWebMessageImageInputs(parsed.Messages); err != nil {
			return nil, err
		}
	}
	var originalPayload []byte
	if parsed.Image != nil {
		originalPayload = helps.SlimRequestBodyForTranslation(originalSource)
		canonicalBody = helps.SlimRequestBodyForTranslation(canonicalBody)
	} else {
		originalPayload = bytes.Clone(originalSource)
	}
	return &chatGPTWebPreparedRequest{
		baseModel:       baseModel,
		routeModel:      routeModel,
		responseFormat:  responseFormat,
		originalPayload: originalPayload,
		canonicalBody:   canonicalBody,
		request:         parsed,
		terminalMarker:  metadataBool(opts.Metadata, cliproxyexecutor.StreamTerminalMarkerMetadataKey),
		trustUpstreamSSE: metadataBool(opts.Metadata, cliproxyexecutor.TrustUpstreamSSEMetadataKey) &&
			responseFormat == sdktranslator.FormatOpenAIResponse,
		maxImageResults: chatGPTWebMaxImageResults(opts.Metadata),
	}, nil
}

func chatGPTWebRequestHasImageInputs(request helps.ChatGPTWebRequest) bool {
	if request.Image != nil && (len(request.Image.Images) > 0 || strings.TrimSpace(request.Image.MaskURL) != "") {
		return true
	}
	for _, message := range request.Messages {
		for _, part := range message.Parts {
			if strings.TrimSpace(part.ImageURL) != "" {
				return true
			}
		}
	}
	return false
}

func chatGPTWebRawRequestHasImageInputs(payload []byte, format sdktranslator.Format) bool {
	if len(payload) == 0 {
		return false
	}
	root := gjson.ParseBytes(payload)
	if !root.IsObject() {
		return false
	}
	if chatGPTWebJSONValuePresent(root.Get("image")) ||
		chatGPTWebJSONValuePresent(root.Get("images")) ||
		chatGPTWebJSONValuePresent(root.Get("mask")) {
		return true
	}
	switch format {
	case sdktranslator.FormatOpenAI:
		return chatGPTWebJSONArrayAny(root.Get("messages"), func(message gjson.Result) bool {
			return chatGPTWebOpenAIContentHasImage(message.Get("content"))
		})
	case sdktranslator.FormatOpenAIResponse, sdktranslator.FormatCodex, sdktranslator.FormatInteractions:
		return chatGPTWebOpenAIResponseInputHasImage(root.Get("input"))
	case sdktranslator.FormatClaude:
		return chatGPTWebJSONArrayAny(root.Get("messages"), func(message gjson.Result) bool {
			return chatGPTWebClaudeContentHasImage(message.Get("content"))
		})
	case sdktranslator.FormatGemini, sdktranslator.FormatAntigravity:
		return chatGPTWebJSONArrayAny(root.Get("contents"), func(content gjson.Result) bool {
			return chatGPTWebJSONArrayAny(content.Get("parts"), func(part gjson.Result) bool {
				return chatGPTWebJSONValuePresent(part.Get("inline_data")) ||
					chatGPTWebJSONValuePresent(part.Get("inlineData")) ||
					chatGPTWebJSONValuePresent(part.Get("file_data")) ||
					chatGPTWebJSONValuePresent(part.Get("fileData"))
			})
		})
	default:
		return false
	}
}

func chatGPTWebJSONValuePresent(result gjson.Result) bool {
	if !result.Exists() || result.Type == gjson.Null {
		return false
	}
	if result.IsArray() {
		present := false
		result.ForEach(func(_, value gjson.Result) bool {
			present = chatGPTWebJSONValuePresent(value)
			return !present
		})
		return present
	}
	raw := strings.TrimSpace(result.Raw)
	return raw != "" && raw != `""` && raw != "{}"
}

func chatGPTWebJSONArrayAny(result gjson.Result, predicate func(gjson.Result) bool) bool {
	if predicate == nil {
		return false
	}
	if !result.IsArray() {
		return predicate(result)
	}
	matched := false
	result.ForEach(func(_, value gjson.Result) bool {
		matched = predicate(value)
		return !matched
	})
	return matched
}

func chatGPTWebOpenAIResponseInputHasImage(input gjson.Result) bool {
	return chatGPTWebJSONArrayAny(input, func(item gjson.Result) bool {
		switch strings.ToLower(strings.TrimSpace(item.Get("type").String())) {
		case "image", "image_url", "input_image":
			return true
		}
		return chatGPTWebOpenAIContentHasImage(item.Get("content"))
	})
}

func chatGPTWebOpenAIContentHasImage(content gjson.Result) bool {
	return chatGPTWebJSONArrayAny(content, func(part gjson.Result) bool {
		switch strings.ToLower(strings.TrimSpace(part.Get("type").String())) {
		case "image", "image_url", "input_image":
			return true
		}
		return chatGPTWebJSONValuePresent(part.Get("image_url")) ||
			chatGPTWebJSONValuePresent(part.Get("image"))
	})
}

func chatGPTWebClaudeContentHasImage(content gjson.Result) bool {
	return chatGPTWebJSONArrayAny(content, func(part gjson.Result) bool {
		return strings.EqualFold(strings.TrimSpace(part.Get("type").String()), "image") ||
			chatGPTWebJSONValuePresent(part.Get("source.data"))
	})
}

func validateChatGPTWebMessageImageInputs(messages []helps.ChatGPTWebMessage) error {
	var references []string
	for _, message := range messages {
		for _, part := range message.Parts {
			if imageURL := strings.TrimSpace(part.ImageURL); imageURL != "" {
				references = append(references, imageURL)
			}
		}
	}
	if len(references) == 0 {
		return nil
	}
	for _, reference := range references {
		reference = strings.TrimSpace(reference)
		if strings.Contains(reference, "://") && !strings.HasPrefix(strings.ToLower(reference), "data:") {
			return statusErr{
				code:           http.StatusBadRequest,
				msg:            "chatgpt web only supports base64 image inputs",
				skipAuthResult: true,
				retryOtherAuth: true,
			}
		}
	}
	if err := helps.ValidateChatGPTWebImageReferences(
		references,
		chatGPTWebMaxImageBytes,
		chatGPTWebMaxImageRequestBytes,
	); err != nil {
		return statusErr{
			code:           http.StatusRequestEntityTooLarge,
			msg:            err.Error(),
			skipAuthResult: true,
		}
	}
	return nil
}

func chatGPTWebMaxImageResults(metadata map[string]any) int {
	if metadata == nil {
		return 0
	}
	maxResults, _ := metadata[cliproxyexecutor.ImageGenerationMaxResultsMetadataKey].(int)
	if maxResults < 0 {
		return 0
	}
	return maxResults
}

func (e *ChatGPTWebExecutor) newRuntimeClient(auth *cliproxyauth.Auth) (*chatgptwebauth.Client, *chatgptwebauth.Credential, error) {
	if auth == nil {
		return nil, nil, errors.New("chatgpt web credential is nil")
	}
	credential, err := chatgptwebauth.ParseCredential(auth.Metadata)
	if err != nil {
		return nil, nil, fmt.Errorf("parse chatgpt web credential: %w", err)
	}
	if strings.TrimSpace(credential.AccessToken) == "" {
		return nil, nil, statusErr{code: http.StatusUnauthorized, msg: "chatgpt web access token is empty"}
	}
	if err = chatgptwebauth.EnsureCredentialRuntimeIDsForURL(credential, chatgptwebauth.CredentialRuntimeIdentityReader(auth.ID, credential), e.chatGPTWebBaseURL()); err != nil {
		return nil, nil, fmt.Errorf("initialize chatgpt web browser identity: %w", err)
	}
	client, err := chatgptwebauth.NewClient(credential.Persona, e.proxyURL(auth), credential.Cookies)
	if err != nil {
		return nil, nil, fmt.Errorf("create chatgpt web browser client: %w", err)
	}
	baseURL := e.chatGPTWebBaseURL()
	deviceID := strings.TrimSpace(credential.DeviceID)
	if err = client.SetCookie(baseURL, "oai-did", deviceID); err != nil {
		client.CloseIdleConnections()
		return nil, nil, err
	}
	return client, credential, nil
}

func (e *ChatGPTWebExecutor) finishChatGPTWebRuntimeClient(ctx context.Context, auth *cliproxyauth.Auth, credential *chatgptwebauth.Credential, client *chatgptwebauth.Client) {
	if client == nil {
		return
	}
	defer client.CloseIdleConnections()
	if e == nil || e.manager == nil || auth == nil || credential == nil {
		return
	}
	cookies := client.ExportCookies()
	persona := client.Persona()
	if reflect.DeepEqual(credential.Cookies, cookies) &&
		reflect.DeepEqual(credential.Persona, persona) &&
		chatGPTWebMetadataString(auth.Metadata, "device_id") == credential.DeviceID &&
		chatGPTWebMetadataString(auth.Metadata, "session_id") == credential.SessionID {
		return
	}
	persistCtx := context.Background()
	if ctx != nil {
		persistCtx = context.WithoutCancel(ctx)
	}
	baselineCookies := append([]chatgptwebauth.Cookie(nil), credential.Cookies...)
	_, _, errUpdate := e.manager.MutateRuntimeMetadataIfCurrent(persistCtx, auth, func(current *cliproxyauth.Auth) {
		currentCookies := baselineCookies
		if currentCredential, errParse := chatgptwebauth.ParseCredential(current.Metadata); errParse == nil {
			currentCookies = currentCredential.Cookies
		}
		if current.Metadata == nil {
			current.Metadata = make(map[string]any)
		}
		current.Metadata["cookies"] = mergeChatGPTWebCookieDelta(currentCookies, baselineCookies, cookies)
		current.Metadata["persona"] = persona
		current.Metadata["device_id"] = credential.DeviceID
		current.Metadata["session_id"] = credential.SessionID
	})
	if errUpdate != nil {
		log.WithField("auth_id", auth.ID).Warnf("chatgpt web executor: persist runtime session: %v", errUpdate)
	}
}

type chatGPTWebCookieKey struct {
	name   string
	path   string
	domain string
	host   string
}

func mergeChatGPTWebCookieDelta(current, baseline, next []chatgptwebauth.Cookie) []chatgptwebauth.Cookie {
	currentByKey := chatGPTWebCookiesByKey(current)
	baselineByKey := chatGPTWebCookiesByKey(baseline)
	nextByKey := chatGPTWebCookiesByKey(next)
	for key := range baselineByKey {
		if _, exists := nextByKey[key]; !exists {
			delete(currentByKey, key)
		}
	}
	for key, cookie := range nextByKey {
		if previous, exists := baselineByKey[key]; exists && reflect.DeepEqual(previous, cookie) {
			continue
		}
		currentByKey[key] = cookie
	}
	merged := make([]chatgptwebauth.Cookie, 0, len(currentByKey))
	for _, cookie := range currentByKey {
		merged = append(merged, cookie)
	}
	sort.SliceStable(merged, func(left, right int) bool {
		leftKey := chatGPTWebCookieIdentity(merged[left])
		rightKey := chatGPTWebCookieIdentity(merged[right])
		if leftKey.host != rightKey.host {
			return leftKey.host < rightKey.host
		}
		if leftKey.domain != rightKey.domain {
			return leftKey.domain < rightKey.domain
		}
		if leftKey.path != rightKey.path {
			return leftKey.path < rightKey.path
		}
		return leftKey.name < rightKey.name
	})
	return merged
}

func chatGPTWebCookiesByKey(cookies []chatgptwebauth.Cookie) map[chatGPTWebCookieKey]chatgptwebauth.Cookie {
	byKey := make(map[chatGPTWebCookieKey]chatgptwebauth.Cookie, len(cookies))
	for _, cookie := range cookies {
		if strings.TrimSpace(cookie.Name) == "" {
			continue
		}
		byKey[chatGPTWebCookieIdentity(cookie)] = cookie
	}
	return byKey
}

func chatGPTWebCookieIdentity(cookie chatgptwebauth.Cookie) chatGPTWebCookieKey {
	return chatGPTWebCookieKey{
		name:   cookie.Name,
		path:   cookie.Path,
		domain: strings.ToLower(strings.TrimSpace(cookie.Domain)),
		host:   strings.ToLower(strings.TrimSpace(cookie.Host)),
	}
}

func chatGPTWebMetadataString(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	value, _ := metadata[key].(string)
	return strings.TrimSpace(value)
}

// FetchModels refreshes the authenticated ChatGPT Web model catalog.
func (e *ChatGPTWebExecutor) FetchModels(ctx context.Context, auth *cliproxyauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
	client, credential, err := e.newRuntimeClient(auth)
	if err != nil {
		return nil, err
	}
	defer e.finishChatGPTWebRuntimeClient(ctx, auth, credential, client)
	bootstrapPath := "/"
	bootstrapHeaders := e.chatGPTWebHeaders(credential, bootstrapPath, map[string]string{
		"accept":         "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"sec-fetch-dest": "document",
		"sec-fetch-mode": "navigate",
		"sec-fetch-site": "none",
	})
	response, err := e.doChatGPTWebBootstrapRequest(
		ctx,
		client,
		credential,
		e.chatGPTWebBaseURL()+bootstrapPath,
		bootstrapHeaders,
	)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	bootstrap, err := readChatGPTWebResponseBody(response, chatGPTWebMaxHTMLBodyBytes)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, newChatGPTWebStatusError(response.StatusCode, bootstrapPath, bootstrap, response.Header)
	}
	path := "/backend-api/models?history_and_training_disabled=false"
	headers := e.chatGPTWebHeaders(credential, path, map[string]string{
		"x-openai-target-path":  "/backend-api/models",
		"x-openai-target-route": "/backend-api/models",
	})
	response, err = e.doChatGPTWebBootstrapRequest(
		ctx,
		client,
		credential,
		e.chatGPTWebBaseURL()+path,
		headers,
	)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	payload, err := readChatGPTWebResponseBody(response, chatGPTWebMaxJSONBodyBytes)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	sanitizedPayload := chatGPTWebResponseLogBody(path, payload)
	helps.AppendAPIResponseChunk(ctx, e.cfg, sanitizedPayload)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, newChatGPTWebStatusError(response.StatusCode, path, sanitizedPayload, response.Header)
	}
	return chatgptwebauth.DecodeCatalog(payload)
}

func (e *ChatGPTWebExecutor) executeChatGPTWebText(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, prepared *chatGPTWebPreparedRequest) (chatGPTWebTextResult, http.Header, error) {
	if chatGPTWebRequestUsesSearch(prepared) {
		return e.executeChatGPTWebSearch(ctx, client, credential, prepared)
	}
	response, accumulator, err := e.openChatGPTWebConversation(ctx, client, credential, prepared)
	if err != nil {
		return chatGPTWebTextResult{}, nil, err
	}
	defer func() {
		if errClose := response.Body.Close(); errClose != nil {
			log.Errorf("chatgpt web executor: close response body: %v", errClose)
		}
	}()
	if err := consumeChatGPTWebConversation(ctx, response.Body, accumulator, nil); err != nil {
		return chatGPTWebTextResult{}, nil, chatGPTWebCommittedRequestError(ctx, chatGPTWebUpstreamProtocolError(ctx, err))
	}
	return chatGPTWebTextResult{Text: accumulator.Text()}, cloneChatGPTWebHeaders(response.Header), nil
}

func (e *ChatGPTWebExecutor) openChatGPTWebConversation(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, prepared *chatGPTWebPreparedRequest) (*fhttp.Response, *helps.ChatGPTWebConversationAccumulator, error) {
	requirements, err := e.chatGPTWebRequirements(ctx, client, credential)
	if err != nil {
		return nil, nil, err
	}
	messages, err := e.buildChatGPTWebConversationMessages(ctx, client, credential, prepared.request.Messages)
	if err != nil {
		return nil, nil, err
	}
	path := "/backend-api/conversation"
	body := map[string]any{
		"action":                        "next",
		"messages":                      messages,
		"model":                         prepared.request.Model,
		"parent_message_id":             uuid.NewString(),
		"conversation_mode":             map[string]any{"kind": "primary_assistant"},
		"conversation_origin":           nil,
		"force_paragen":                 false,
		"force_paragen_model_slug":      "",
		"force_rate_limit":              false,
		"force_use_sse":                 true,
		"history_and_training_disabled": true,
		"reset_rate_limits":             false,
		"suggestions":                   []any{},
		"supported_encodings":           []any{},
		"system_hints":                  []any{},
		"timezone":                      "Asia/Shanghai",
		"timezone_offset_min":           -480,
		"variant_purpose":               "comparison_implicit",
		"websocket_request_id":          uuid.NewString(),
		"client_contextual_info":        chatGPTWebClientContext(),
	}
	if effort := normalizeChatGPTWebThinkingEffort(prepared.request.ReasoningEffort); effort != "" {
		body["thinking_effort"] = effort
	}
	headers := e.chatGPTWebHeaders(credential, path, map[string]string{
		"accept":       "text/event-stream",
		"content-type": "application/json",
		"openai-sentinel-chat-requirements-token": requirements.Token,
		"openai-sentinel-proof-token":             requirements.ProofToken,
		"openai-sentinel-turnstile-token":         requirements.TurnstileToken,
		"openai-sentinel-so-token":                requirements.SOToken,
	})
	response, err := e.doChatGPTWebJSONStream(ctx, client, credential, path, headers, body)
	if err != nil {
		return nil, nil, err
	}
	return response, helps.NewChatGPTWebConversationAccumulator(prepared.request.Messages), nil
}

func (e *ChatGPTWebExecutor) chatGPTWebRequirements(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential) (chatGPTWebRequirements, error) {
	baseURL := e.chatGPTWebBaseURL()
	bootstrapPath := "/"
	bootstrapHeaders := e.chatGPTWebHeaders(credential, bootstrapPath, map[string]string{
		"accept":         "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
		"sec-fetch-dest": "document",
		"sec-fetch-mode": "navigate",
		"sec-fetch-site": "none",
	})
	response, err := e.doChatGPTWebBootstrapRequest(ctx, client, credential, baseURL+bootstrapPath, bootstrapHeaders)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return chatGPTWebRequirements{}, err
	}
	bootstrap, err := readChatGPTWebResponseBody(response, chatGPTWebMaxHTMLBodyBytes)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return chatGPTWebRequirements{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return chatGPTWebRequirements{}, newChatGPTWebStatusError(response.StatusCode, bootstrapPath, bootstrap, response.Header)
	}
	sources, dataBuild := chatgptwebauth.ParseConversationPoWResources(bootstrap)
	pToken, err := chatgptwebauth.BuildConversationRequirementsToken(credential.Persona, sources, dataBuild, e.runtimeRand, e.now)
	if err != nil {
		return chatGPTWebRequirements{}, chatGPTWebLocalProtocolError(
			http.StatusBadGateway,
			"build chatgpt web requirements token: "+err.Error(),
		)
	}
	preparePath := "/backend-api/sentinel/chat-requirements/prepare"
	_, prepareData, err := e.doChatGPTWebJSON(ctx, client, credential, preparePath, map[string]any{"p": pToken})
	if err != nil {
		return chatGPTWebRequirements{}, err
	}
	var prepare map[string]any
	if err := json.Unmarshal(prepareData, &prepare); err != nil {
		return chatGPTWebRequirements{}, chatGPTWebLocalProtocolError(
			http.StatusBadGateway,
			"decode chatgpt web requirements prepare: "+err.Error(),
		)
	}
	if requiredJSONFlag(prepare, "arkose", "required") {
		return chatGPTWebRequirements{}, chatGPTWebLocalProtocolError(
			http.StatusForbidden,
			"chatgpt web requires an unsupported Arkose challenge",
		)
	}
	if requiredJSONFlag(prepare, "turnstile", "required") {
		return chatGPTWebRequirements{}, chatGPTWebLocalProtocolError(
			http.StatusForbidden,
			"chatgpt web requires an unsupported Turnstile challenge",
		)
	}
	proofToken := ""
	if requiredJSONFlag(prepare, "proofofwork", "required") {
		proof, _ := prepare["proofofwork"].(map[string]any)
		proofToken, err = chatgptwebauth.BuildConversationProofToken(
			ctx,
			chatGPTWebAnyString(proof["seed"]),
			chatGPTWebAnyString(proof["difficulty"]),
			credential.Persona, sources, dataBuild, e.runtimeRand, e.now,
		)
		if err != nil {
			if ctx != nil && ctx.Err() != nil {
				return chatGPTWebRequirements{}, ctx.Err()
			}
			return chatGPTWebRequirements{}, chatGPTWebLocalProtocolError(
				http.StatusBadGateway,
				"build chatgpt web proof token: "+err.Error(),
			)
		}
	}
	finalizePath := "/backend-api/sentinel/chat-requirements/finalize"
	_, finalizeData, err := e.doChatGPTWebJSON(ctx, client, credential, finalizePath, map[string]any{
		"prepare_token":   chatGPTWebAnyString(prepare["prepare_token"]),
		"proof_token":     proofToken,
		"turnstile_token": "",
	})
	if err != nil {
		return chatGPTWebRequirements{}, err
	}
	var finalize map[string]any
	if err := json.Unmarshal(finalizeData, &finalize); err != nil {
		return chatGPTWebRequirements{}, chatGPTWebLocalProtocolError(
			http.StatusBadGateway,
			"decode chatgpt web requirements finalize: "+err.Error(),
		)
	}
	token := chatGPTWebAnyString(finalize["token"])
	if token == "" {
		return chatGPTWebRequirements{}, chatGPTWebLocalProtocolError(
			http.StatusBadGateway,
			"chatgpt web requirements response is missing token",
		)
	}
	return chatGPTWebRequirements{
		Token:          token,
		ProofToken:     proofToken,
		TurnstileToken: chatGPTWebAnyString(finalize["turnstile_token"]),
		SOToken:        chatGPTWebAnyString(finalize["so_token"]),
	}, nil
}

func (e *ChatGPTWebExecutor) doChatGPTWebBootstrapRequest(
	ctx context.Context,
	client *chatgptwebauth.Client,
	credential *chatgptwebauth.Credential,
	targetURL string,
	headers map[string]string,
) (*fhttp.Response, error) {
	if client == nil {
		return nil, errors.New("chatgpt web bootstrap client is nil")
	}
	originalURL, err := url.Parse(strings.TrimSpace(targetURL))
	if err != nil || originalURL == nil || originalURL.Scheme == "" || originalURL.Host == "" {
		return nil, errors.New("chatgpt web bootstrap URL is invalid")
	}
	currentURL := originalURL
	for redirects := 0; ; redirects++ {
		e.recordChatGPTWebRequest(ctx, credential, http.MethodGet, currentURL.String(), headers, nil)
		response, errRequest := client.DoNoRedirectStream(ctx, http.MethodGet, currentURL.String(), headers, nil)
		if errRequest != nil {
			return nil, errRequest
		}
		helps.RecordAPIResponseMetadata(ctx, e.cfg, response.StatusCode, chatGPTWebResponseLogHeaders(response.Header))
		if !chatGPTWebBootstrapRedirectStatus(response.StatusCode) {
			return response, nil
		}
		location := strings.TrimSpace(response.Header.Get("Location"))
		if location == "" {
			return response, nil
		}
		nextURL, errLocation := currentURL.Parse(location)
		if errLocation != nil || !sameChatGPTWebAssetOrigin(originalURL, nextURL) {
			return response, nil
		}
		if redirects >= chatGPTWebMaxBootstrapRedirects {
			_ = response.Body.Close()
			return nil, fmt.Errorf("chatgpt web redirect chain exceeds %d hops", chatGPTWebMaxBootstrapRedirects)
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, chatGPTWebMaxErrorBodyBytes))
		if errClose := response.Body.Close(); errClose != nil {
			return nil, fmt.Errorf("close chatgpt web bootstrap redirect response: %w", errClose)
		}
		currentURL = nextURL
	}
}

func chatGPTWebBootstrapRedirectStatus(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

func (e *ChatGPTWebExecutor) doChatGPTWebJSON(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, path string, body any) (*fhttp.Response, []byte, error) {
	headers := e.chatGPTWebHeaders(credential, path, map[string]string{
		"accept":       "application/json",
		"content-type": "application/json",
	})
	return e.doChatGPTWebJSONWithHeaders(ctx, client, credential, path, headers, body)
}

func (e *ChatGPTWebExecutor) doChatGPTWebJSONWithHeaders(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, path string, headers map[string]string, body any) (*fhttp.Response, []byte, error) {
	payload, _ := json.Marshal(body)
	e.recordChatGPTWebRequest(ctx, credential, http.MethodPost, path, headers, payload)
	response, err := client.DoJSONStream(ctx, http.MethodPost, e.chatGPTWebBaseURL()+path, headers, body)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, nil, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, response.StatusCode, chatGPTWebResponseLogHeaders(response.Header))
	data, err := readChatGPTWebResponseBody(response, chatGPTWebMaxJSONBodyBytes)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return response, nil, err
	}
	sanitizedData := chatGPTWebResponseLogBody(path, data)
	helps.AppendAPIResponseChunk(ctx, e.cfg, sanitizedData)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response, nil, newChatGPTWebStatusError(response.StatusCode, path, sanitizedData, response.Header)
	}
	return response, data, nil
}

func (e *ChatGPTWebExecutor) doChatGPTWebJSONStream(ctx context.Context, client *chatgptwebauth.Client, credential *chatgptwebauth.Credential, path string, headers map[string]string, body any) (*fhttp.Response, error) {
	payload, _ := json.Marshal(body)
	e.recordChatGPTWebRequest(ctx, credential, http.MethodPost, path, headers, payload)
	traceCtx := ctx
	if traceCtx == nil {
		traceCtx = context.Background()
	}
	var requestWritten atomic.Bool
	traceCtx = fhttptrace.WithClientTrace(traceCtx, &fhttptrace.ClientTrace{
		WroteRequest: func(fhttptrace.WroteRequestInfo) {
			requestWritten.Store(true)
		},
	})
	response, err := client.DoJSONStream(traceCtx, http.MethodPost, e.chatGPTWebBaseURL()+path, headers, body)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		if requestWritten.Load() {
			err = chatGPTWebCommittedRequestError(ctx, err)
		}
		return nil, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, response.StatusCode, chatGPTWebResponseLogHeaders(response.Header))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data := readAndCloseChatGPTWebErrorBody(response.Body)
		sanitizedData := chatGPTWebResponseLogBody(path, data)
		helps.AppendAPIResponseChunk(ctx, e.cfg, sanitizedData)
		return nil, newChatGPTWebStatusError(response.StatusCode, path, sanitizedData, response.Header)
	}
	return response, nil
}

func readChatGPTWebResponseBody(response *fhttp.Response, maxSuccessBytes int) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, errors.New("chatgpt web response body is nil")
	}
	var (
		payload []byte
		errRead error
	)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return readAndCloseChatGPTWebErrorBody(response.Body), nil
	} else {
		payload, errRead = readChatGPTWebSuccessBody(response.Body, maxSuccessBytes)
	}
	errClose := response.Body.Close()
	if errRead != nil {
		return nil, errRead
	}
	if errClose != nil {
		return nil, fmt.Errorf("close chatgpt web response body: %w", errClose)
	}
	return payload, nil
}

func readAndCloseChatGPTWebErrorBody(body io.ReadCloser) []byte {
	if body == nil {
		return []byte("<upstream-error-body-unavailable>")
	}
	payload, errRead := readChatGPTWebErrorBody(body)
	_ = body.Close()
	if errRead != nil {
		return []byte("<upstream-error-body-unavailable>")
	}
	return payload
}

func readChatGPTWebSuccessBody(body io.Reader, maxBytes int) ([]byte, error) {
	if body == nil {
		return nil, errors.New("chatgpt web response body is nil")
	}
	if maxBytes < 1 {
		return nil, errors.New("chatgpt web response body limit is invalid")
	}
	payload, err := io.ReadAll(io.LimitReader(body, int64(maxBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("read chatgpt web response body: %w", err)
	}
	if len(payload) > maxBytes {
		return nil, statusErr{
			code:           http.StatusBadGateway,
			msg:            fmt.Sprintf("chatgpt web response body exceeds %d bytes", maxBytes),
			skipAuthResult: true,
		}
	}
	return payload, nil
}

func readChatGPTWebErrorBody(body io.Reader) ([]byte, error) {
	if body == nil {
		return nil, errors.New("chatgpt web response body is nil")
	}
	payload, err := io.ReadAll(io.LimitReader(body, chatGPTWebMaxErrorBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read chatgpt web error body: %w", err)
	}
	if len(payload) > chatGPTWebMaxErrorBodyBytes {
		return []byte("<upstream-error-body-truncated>"), nil
	}
	return payload, nil
}

func consumeChatGPTWebConversation(ctx context.Context, body io.Reader, accumulator *helps.ChatGPTWebConversationAccumulator, onDelta func(string) bool) error {
	decoder := helps.NewChatGPTWebSSEDecoder(chatGPTWebSSEMaxFrameBytes)
	buffer := make([]byte, 32<<10)
	applyPayload := func(payload []byte) (bool, error) {
		delta, done, err := accumulator.Apply(payload)
		if err != nil {
			return false, err
		}
		if delta != "" && onDelta != nil && !onDelta(delta) {
			if ctx != nil && ctx.Err() != nil {
				return false, ctx.Err()
			}
			return false, context.Canceled
		}
		return done, nil
	}
	for {
		count, errRead := body.Read(buffer)
		if count > 0 {
			payloads, err := decoder.Feed(buffer[:count], false)
			if err != nil {
				return err
			}
			for _, payload := range payloads {
				done, err := applyPayload(payload)
				if err != nil {
					return err
				}
				if done {
					return nil
				}
			}
		}
		if errRead != nil {
			if !errors.Is(errRead, io.EOF) {
				return errRead
			}
			payloads, err := decoder.Feed(nil, true)
			if err != nil {
				return err
			}
			for _, payload := range payloads {
				done, errApply := applyPayload(payload)
				if errApply != nil {
					return errApply
				}
				if done {
					return nil
				}
			}
			return helps.IncompleteStreamError("chatgpt web")
		}
	}
}

func chatGPTWebUpstreamProtocolError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	var status interface{ StatusCode() int }
	if errors.As(err, &status) {
		return err
	}
	return statusErr{
		code:           http.StatusBadGateway,
		msg:            err.Error(),
		skipAuthResult: true,
		retryOtherAuth: true,
	}
}

func (e *ChatGPTWebExecutor) chatGPTWebHeaders(credential *chatgptwebauth.Credential, path string, extra map[string]string) map[string]string {
	headers := map[string]string{
		"authorization":           "Bearer " + strings.TrimSpace(credential.AccessToken),
		"origin":                  e.chatGPTWebBaseURL(),
		"referer":                 e.chatGPTWebBaseURL() + "/",
		"oai-device-id":           strings.TrimSpace(credential.DeviceID),
		"oai-session-id":          strings.TrimSpace(credential.SessionID),
		"oai-language":            credential.Persona.Language,
		"oai-client-version":      chatGPTWebClientVersion,
		"oai-client-build-number": chatGPTWebClientBuildNumber,
		"x-openai-target-path":    path,
		"x-openai-target-route":   path,
		"priority":                "u=1, i",
		"pragma":                  "no-cache",
		"sec-fetch-dest":          "empty",
		"sec-fetch-mode":          "cors",
		"sec-fetch-site":          "same-origin",
	}
	for key, value := range extra {
		if strings.TrimSpace(value) != "" {
			headers[key] = value
		}
	}
	return headers
}

func (e *ChatGPTWebExecutor) chatGPTWebBaseURL() string {
	if e != nil && strings.TrimSpace(e.runtimeBaseURL) != "" {
		return strings.TrimRight(strings.TrimSpace(e.runtimeBaseURL), "/")
	}
	return "https://chatgpt.com"
}

func (e *ChatGPTWebExecutor) recordChatGPTWebRequest(ctx context.Context, credential *chatgptwebauth.Credential, method, path string, headers map[string]string, body []byte) {
	httpHeaders := chatGPTWebRequestLogHeaders(headers)
	authValue := ""
	if credential != nil {
		authValue = chatGPTWebLogEmail(credential.Email)
	}
	requestURL := path
	if !strings.HasPrefix(requestURL, "http://") && !strings.HasPrefix(requestURL, "https://") {
		requestURL = e.chatGPTWebBaseURL() + path
	}
	requestURL = chatGPTWebRequestLogURL(requestURL)
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       requestURL,
		Method:    method,
		Headers:   httpHeaders,
		Body:      chatGPTWebRequestLogBody(path, body),
		Provider:  e.Identifier(),
		AuthType:  "email",
		AuthValue: authValue,
	})
}

func chatGPTWebRequestLogHeaders(headers map[string]string) http.Header {
	httpHeaders := make(http.Header, len(headers))
	for key, value := range headers {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "authorization",
			"oai-device-id",
			"oai-session-id",
			"openai-sentinel-chat-requirements-token",
			"openai-sentinel-proof-token",
			"openai-sentinel-turnstile-token",
			"openai-sentinel-so-token",
			"x-conduit-token":
			value = "<redacted>"
		}
		httpHeaders.Set(key, value)
	}
	return httpHeaders
}

func chatGPTWebLogEmail(value string) string {
	value = strings.TrimSpace(value)
	local, domain, found := strings.Cut(value, "@")
	if !found || local == "" || domain == "" {
		return "<redacted-email>"
	}
	prefix := local[:1]
	if len(local) > 1 {
		prefix = local[:2]
	}
	return prefix + "***@" + domain
}

func chatGPTWebRequestLogBody(path string, body []byte) []byte {
	if len(body) == 0 {
		return body
	}
	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return []byte("<redacted-non-json-body>")
	}
	redactPreparePayload := strings.Contains(strings.ToLower(path), "/chat-requirements/prepare")
	var redact func(any)
	redact = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, item := range typed {
				normalized := strings.ToLower(strings.TrimSpace(key))
				switch normalized {
				case "access_token", "refresh_token", "id_token", "session_token",
					"prepare_token", "proof_token", "turnstile_token", "so_token",
					"conduit_token", "token", "password", "totp_secret", "cookie", "cookies":
					typed[key] = "<redacted>"
					continue
				case "email", "login_hint":
					typed[key] = chatGPTWebLogEmail(chatGPTWebAnyString(item))
					continue
				case "p":
					if redactPreparePayload {
						typed[key] = "<redacted>"
						continue
					}
				}
				if redacted, ok := chatGPTWebRedactSignedURL(item); ok {
					typed[key] = redacted
					continue
				}
				redact(item)
			}
		case []any:
			for index, item := range typed {
				if redacted, ok := chatGPTWebRedactSignedURL(item); ok {
					typed[index] = redacted
					continue
				}
				redact(item)
			}
		}
	}
	if redacted, ok := chatGPTWebRedactSignedURL(root); ok {
		root = redacted
	} else {
		redact(root)
	}
	sanitized, err := json.Marshal(root)
	if err != nil {
		return []byte("<redacted-json-body>")
	}
	return sanitized
}

func chatGPTWebRequestLogURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "<redacted-invalid-url>"
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	return parsed.String()
}

func chatGPTWebResponseLogBody(path string, body []byte) []byte {
	if len(body) == 0 {
		return body
	}
	if bytes.Equal(body, []byte("<upstream-error-body-truncated>")) {
		return body
	}
	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return []byte("<redacted-non-json-response-body>")
	}
	redactGenericURL := strings.Contains(strings.ToLower(path), "/download")
	var redact func(any)
	redact = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, item := range typed {
				normalized := strings.ToLower(strings.TrimSpace(key))
				switch normalized {
				case "access_token", "refresh_token", "id_token", "session_token",
					"prepare_token", "proof_token", "turnstile_token", "so_token",
					"conduit_token", "token", "password", "totp_secret", "cookie", "cookies":
					typed[key] = "<redacted>"
					continue
				}
				if normalized == "upload_url" || normalized == "download_url" || (redactGenericURL && normalized == "url") {
					typed[key] = "<redacted-signed-url>"
					continue
				}
				if redacted, ok := chatGPTWebRedactSignedURL(item); ok {
					typed[key] = redacted
					continue
				}
				redact(item)
			}
		case []any:
			for index, item := range typed {
				if redacted, ok := chatGPTWebRedactSignedURL(item); ok {
					typed[index] = redacted
					continue
				}
				redact(item)
			}
		}
	}
	if redacted, ok := chatGPTWebRedactSignedURL(root); ok {
		root = redacted
	} else {
		redact(root)
	}
	sanitized, err := json.Marshal(root)
	if err != nil {
		return []byte("<redacted-json-response-body>")
	}
	return sanitized
}

func chatGPTWebRedactSignedURL(value any) (string, bool) {
	rawURL, ok := value.(string)
	if !ok {
		return "", false
	}
	trimmed := strings.TrimSpace(rawURL)
	if (strings.Contains(trimmed, "https://") || strings.Contains(trimmed, "http://")) &&
		strings.Contains(trimmed, "?") {
		return "<redacted-signed-url>", true
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", false
	}
	if parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == "" {
		return "", false
	}
	return "<redacted-signed-url>", true
}

func chatGPTWebStatusErrorBody(path string, body []byte) []byte {
	if bytes.Equal(body, []byte("<upstream-error-body-truncated>")) {
		return body
	}
	if len(body) == 0 || json.Valid(body) {
		return chatGPTWebResponseLogBody(path, body)
	}
	return []byte("<redacted-non-json-response-body>")
}

type chatGPTWebHTTPError struct {
	statusErr
	headers http.Header
}

func (e chatGPTWebHTTPError) Headers() http.Header {
	if len(e.headers) == 0 {
		return nil
	}
	return e.headers.Clone()
}

func newChatGPTWebStatusError(code int, path string, body []byte, headers fhttp.Header) chatGPTWebHTTPError {
	sanitizedBody := chatGPTWebStatusErrorBody(path, body)
	err := chatGPTWebHTTPError{statusErr: statusErr{code: code, msg: strings.TrimSpace(string(sanitizedBody))}}
	switch code {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusMethodNotAllowed,
		http.StatusConflict, http.StatusRequestEntityTooLarge,
		http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity:
		err.statusErr.skipAuthResult = true
	}
	if code == http.StatusNotFound {
		err.statusErr.retryOtherAuth = true
	}
	retryAfter := ""
	if headers != nil {
		retryAfter = headers.Get("Retry-After")
	}
	if strings.TrimSpace(retryAfter) != "" {
		err.statusErr.retryAfter = parseXAIRetryAfterHeader(retryAfter, time.Now())
		err.headers = http.Header{"Retry-After": []string{retryAfter}}
	}
	return err
}

func chatGPTWebLocalProtocolError(code int, message string) statusErr {
	return statusErr{
		code:           code,
		msg:            strings.TrimSpace(message),
		skipAuthResult: true,
	}
}

func cloneChatGPTWebHeaders(headers fhttp.Header) http.Header {
	if headers == nil {
		return nil
	}
	cloned := make(http.Header, len(headers))
	for key, values := range headers {
		if strings.HasPrefix(key, ":") ||
			strings.EqualFold(key, "Set-Cookie") ||
			strings.EqualFold(key, "Set-Cookie2") {
			continue
		}
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}

func chatGPTWebResponseLogHeaders(headers fhttp.Header) http.Header {
	cloned := cloneChatGPTWebHeaders(headers)
	if cloned == nil {
		return nil
	}
	cloned.Del("Cookie")
	cloned.Del("Set-Cookie")
	cloned.Del("Set-Cookie2")
	if cloned.Get("Location") != "" {
		cloned.Set("Location", "<redacted-location>")
	}
	return cloned
}

func requiredJSONFlag(root map[string]any, key, field string) bool {
	value, _ := root[key].(map[string]any)
	required, _ := value[field].(bool)
	return required
}

func normalizeChatGPTWebThinkingEffort(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "low", "medium", "high":
		return strings.ToLower(strings.TrimSpace(value))
	case "xhigh", "extended":
		return "extended"
	default:
		return ""
	}
}

func chatGPTWebClientContext() map[string]any {
	return map[string]any{
		"is_dark_mode":      false,
		"time_since_loaded": 120,
		"page_height":       900,
		"page_width":        1400,
		"pixel_ratio":       2,
		"screen_height":     1440,
		"screen_width":      2560,
		"app_name":          "chatgpt.com",
	}
}

func chatGPTWebRequestUsesSearch(prepared *chatGPTWebPreparedRequest) bool {
	return prepared != nil && (prepared.request.WebSearch || chatGPTWebSearchAlias(prepared.routeModel))
}

func chatGPTWebSearchAlias(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	return strings.HasPrefix(model, "gpt-4o-search-preview") ||
		strings.HasPrefix(model, "gpt-4o-mini-search-preview") ||
		strings.HasPrefix(model, "gpt-5-search-api")
}

func chatGPTWebOriginalRequestUsesSearch(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	return gjson.GetBytes(payload, "web_search_options").Exists()
}

func sendChatGPTWebStreamChunk(ctx context.Context, out chan<- cliproxyexecutor.StreamChunk, chunk cliproxyexecutor.StreamChunk) bool {
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

func (e *ChatGPTWebExecutor) streamDeferredChatGPTWebResponse(
	ctx context.Context,
	auth *cliproxyauth.Auth,
	credential *chatgptwebauth.Credential,
	prepared *chatGPTWebPreparedRequest,
	client *chatgptwebauth.Client,
	headers http.Header,
	passthroughState *cliproxyexecutor.ImageGenerationStreamPassthroughState,
	enablePassthrough bool,
	work func() ([]byte, error),
) *cliproxyexecutor.StreamResult {
	out := make(chan cliproxyexecutor.StreamChunk)
	reporter := helps.NewExecutorUsageReporter(ctx, e, prepared.routeModel, auth)
	go func() {
		defer close(out)
		if !sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.BootstrapCommitStreamChunk()) {
			e.finishChatGPTWebRuntimeClient(ctx, auth, credential, client)
			return
		}
		type deferredResult struct {
			completed []byte
			err       error
		}
		resultCh := make(chan deferredResult, 1)
		go func() {
			defer e.finishChatGPTWebRuntimeClient(ctx, auth, credential, client)
			completed, err := work()
			resultCh <- deferredResult{completed: completed, err: err}
		}()

		initialWait := e.streamInitialWait
		if initialWait < 0 {
			initialWait = 0
		}
		initialTimer := time.NewTimer(initialWait)
		defer initialTimer.Stop()
		var heartbeatTicker *time.Ticker
		var heartbeat <-chan time.Time
		var contextDone <-chan struct{}
		if ctx != nil {
			contextDone = ctx.Done()
		}
		defer func() {
			if heartbeatTicker != nil {
				heartbeatTicker.Stop()
			}
		}()

		var completed []byte
		sendPayload := func(payload []byte) bool {
			if enablePassthrough && passthroughState != nil && chatGPTWebStreamHasSemanticPayload(payload) {
				passthroughState.SetEnabled(true)
			}
			payload = chatGPTWebTrustedSSEFrame(payload, prepared.trustUpstreamSSE)
			return sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Payload: payload})
		}
		sendHeartbeat := func() bool {
			payload := chatGPTWebDeferredHeartbeat(prepared, "", 0)
			return sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Payload: payload})
		}
		for completed == nil {
			select {
			case result := <-resultCh:
				if result.err != nil {
					reporter.PublishFailure(ctx, result.err)
					_ = sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: result.err})
					return
				}
				if len(result.completed) == 0 {
					errEmpty := errors.New("chatgpt web deferred stream completed without a response")
					reporter.PublishFailure(ctx, errEmpty)
					_ = sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: errEmpty})
					return
				}
				completed = result.completed
			case <-initialTimer.C:
				if !sendHeartbeat() {
					return
				}
				if e.streamHeartbeat > 0 {
					heartbeatTicker = time.NewTicker(e.streamHeartbeat)
					heartbeat = heartbeatTicker.C
				}
			case <-heartbeat:
				if !sendHeartbeat() {
					return
				}
			case <-contextDone:
				return
			}
		}

		var param any
		response := gjson.GetBytes(completed, "response")
		responseID := response.Get("id").String()
		model := response.Get("model").String()
		if model == "" {
			model = prepared.routeModel
		}
		emitEvent := func(event []byte) bool {
			chunks := sdktranslator.TranslateStream(ctx, sdktranslator.FormatCodex, prepared.responseFormat, prepared.routeModel,
				prepared.originalPayload, prepared.canonicalBody, append([]byte("data: "), event...), &param)
			for _, chunk := range chunks {
				if !sendPayload(chunk) {
					return false
				}
			}
			return true
		}
		if !emitChatGPTWebEventsFromCompleted(responseID, model, response, emitEvent) {
			if ctx != nil {
				reporter.PublishFailure(ctx, ctx.Err())
			}
			return
		}
		if detail, ok := helps.ParseCodexUsage(completed); ok {
			reporter.Publish(ctx, detail)
		} else {
			reporter.EnsurePublished(ctx)
		}
		if prepared.terminalMarker {
			_ = sendChatGPTWebStreamChunk(ctx, out, cliproxyexecutor.SuccessfulStreamTerminalChunk())
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: headers, Chunks: out}
}

func chatGPTWebTrustedSSEFrame(payload []byte, enabled bool) []byte {
	if !enabled || len(payload) == 0 || bytes.HasSuffix(payload, []byte("\n\n")) || bytes.HasSuffix(payload, []byte("\r\n\r\n")) {
		return payload
	}
	framed := make([]byte, 0, len(payload)+2)
	framed = append(framed, payload...)
	if bytes.HasSuffix(payload, []byte{'\n'}) {
		return append(framed, '\n')
	}
	return append(framed, '\n', '\n')
}

func chatGPTWebStreamHasSemanticPayload(payload []byte) bool {
	for _, line := range bytes.Split(payload, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == ':' {
			continue
		}
		return true
	}
	return false
}

func chatGPTWebDeferredHeartbeat(prepared *chatGPTWebPreparedRequest, responseID string, createdAt int64) []byte {
	_ = prepared
	_ = responseID
	_ = createdAt
	return []byte(": chatgpt-web upstream pending\n\n")
}

func buildChatGPTWebCompletedEvent(model string, result chatGPTWebTextResult) []byte {
	responseID := "resp_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	messageID := "msg_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	output := make([]any, 0, 2)
	if result.Search {
		searchItem := map[string]any{
			"type":   "web_search_call",
			"id":     "ws_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
			"status": "completed",
			"action": chatGPTWebSearchAction(result.Query, result.Sources),
		}
		output = append(output, searchItem)
	}
	output = append(output, map[string]any{
		"type":   "message",
		"id":     messageID,
		"role":   "assistant",
		"status": "completed",
		"content": []any{map[string]any{
			"type":        "output_text",
			"text":        result.Text,
			"annotations": chatGPTWebSourceAnnotations(result.Text, result.Sources),
		}},
	})
	return marshalChatGPTWebEvent(map[string]any{
		"type": "response.completed",
		"response": map[string]any{
			"id":         responseID,
			"object":     "response",
			"created_at": time.Now().Unix(),
			"status":     "completed",
			"model":      model,
			"output":     output,
			"usage":      chatGPTWebUsageOrZero(result.Usage),
		},
	})
}

func buildChatGPTWebCreatedEvent(responseID, model string) []byte {
	return marshalChatGPTWebEvent(map[string]any{
		"type": "response.created",
		"response": map[string]any{
			"id":         responseID,
			"object":     "response",
			"created_at": time.Now().Unix(),
			"status":     "in_progress",
			"model":      model,
			"output":     []any{},
		},
	})
}

func buildChatGPTWebInProgressEvent(responseID, model string) []byte {
	return marshalChatGPTWebEvent(map[string]any{
		"type": "response.in_progress",
		"response": map[string]any{
			"id":         responseID,
			"object":     "response",
			"created_at": time.Now().Unix(),
			"status":     "in_progress",
			"model":      model,
			"output":     []any{},
		},
	})
}

func buildChatGPTWebMessageAddedEvents(responseID, messageID string, outputIndex int) [][]byte {
	return [][]byte{
		marshalChatGPTWebEvent(map[string]any{
			"type":         "response.output_item.added",
			"response_id":  responseID,
			"output_index": outputIndex,
			"item": map[string]any{
				"type": "message", "id": messageID, "role": "assistant", "status": "in_progress", "content": []any{},
			},
		}),
		marshalChatGPTWebEvent(map[string]any{
			"type":          "response.content_part.added",
			"response_id":   responseID,
			"item_id":       messageID,
			"output_index":  outputIndex,
			"content_index": 0,
			"part": map[string]any{
				"type": "output_text", "text": "", "annotations": []any{},
			},
		}),
	}
}

func buildChatGPTWebTextDeltaEvent(responseID, messageID string, outputIndex int, delta string) []byte {
	return marshalChatGPTWebEvent(map[string]any{
		"type":          "response.output_text.delta",
		"response_id":   responseID,
		"item_id":       messageID,
		"output_index":  outputIndex,
		"content_index": 0,
		"delta":         delta,
	})
}

func buildChatGPTWebMessageTerminalEvents(responseID, messageID string, outputIndex int, text string, annotations []any) [][]byte {
	message := map[string]any{
		"type":   "message",
		"id":     messageID,
		"role":   "assistant",
		"status": "completed",
		"content": []any{map[string]any{
			"type":        "output_text",
			"text":        text,
			"annotations": annotations,
		}},
	}
	return [][]byte{
		marshalChatGPTWebEvent(map[string]any{
			"type": "response.output_text.done", "response_id": responseID, "item_id": messageID,
			"output_index": outputIndex, "content_index": 0, "text": text,
		}),
		marshalChatGPTWebEvent(map[string]any{
			"type": "response.content_part.done", "response_id": responseID, "item_id": messageID,
			"output_index": outputIndex, "content_index": 0,
			"part": map[string]any{"type": "output_text", "text": text, "annotations": annotations},
		}),
		marshalChatGPTWebEvent(map[string]any{
			"type": "response.output_item.done", "response_id": responseID, "output_index": outputIndex, "item": message,
		}),
	}
}

func buildChatGPTWebTerminalEvents(responseID, messageID, model, text string, sources []chatGPTWebSearchSource, query string, search bool, usage map[string]any) [][]byte {
	annotations := chatGPTWebSourceAnnotations(text, sources)
	events := buildChatGPTWebMessageTerminalEvents(responseID, messageID, 0, text, annotations)
	message := map[string]any{
		"type":   "message",
		"id":     messageID,
		"role":   "assistant",
		"status": "completed",
		"content": []any{map[string]any{
			"type":        "output_text",
			"text":        text,
			"annotations": annotations,
		}},
	}
	output := []any{message}
	if search {
		output = append([]any{map[string]any{
			"type": "web_search_call", "id": "ws_" + strings.ReplaceAll(uuid.NewString(), "-", ""), "status": "completed",
			"action": chatGPTWebSearchAction(query, sources),
		}}, output...)
	}
	events = append(events, marshalChatGPTWebEvent(map[string]any{
		"type": "response.completed",
		"response": map[string]any{
			"id": responseID, "object": "response", "created_at": time.Now().Unix(), "status": "completed",
			"model": model, "output": output, "usage": chatGPTWebUsageOrZero(usage),
		},
	}))
	return events
}

func estimateChatGPTWebUsage(model string, request helps.ChatGPTWebRequest, outputText string) map[string]any {
	var inputTokens, outputTokens int64
	if encoder, err := helps.TokenizerForModel(model); err == nil {
		if count, errCount := encoder.Count(chatGPTWebUsageInput(request)); errCount == nil {
			inputTokens = int64(count)
		}
		if count, errCount := encoder.Count(outputText); errCount == nil {
			outputTokens = int64(count)
		}
	}
	return map[string]any{
		"input_tokens":  inputTokens,
		"output_tokens": outputTokens,
		"total_tokens":  inputTokens + outputTokens,
		"input_tokens_details": map[string]any{
			"cached_tokens": 0,
		},
		"output_tokens_details": map[string]any{
			"reasoning_tokens": 0,
		},
	}
}

func chatGPTWebUsageInput(request helps.ChatGPTWebRequest) string {
	segments := make([]string, 0, len(request.Messages)*2+2)
	for _, message := range request.Messages {
		if role := strings.TrimSpace(message.Role); role != "" {
			segments = append(segments, role)
		}
		for _, part := range message.Parts {
			if text := strings.TrimSpace(part.Text); text != "" {
				segments = append(segments, text)
			}
			if strings.TrimSpace(part.ImageURL) != "" {
				segments = append(segments, "[image]")
			}
		}
	}
	if effort := strings.TrimSpace(request.ReasoningEffort); effort != "" {
		segments = append(segments, effort)
	}
	if request.WebSearch {
		segments = append(segments, "web_search")
	}
	return strings.Join(segments, "\n")
}

func chatGPTWebUsageOrZero(usage map[string]any) map[string]any {
	if usage != nil {
		return usage
	}
	return estimateChatGPTWebUsage("", helps.ChatGPTWebRequest{}, "")
}

func chatGPTWebSearchAction(query string, sources []chatGPTWebSearchSource) map[string]any {
	actionSources := make([]any, 0, len(sources))
	for _, source := range sources {
		if sourceURL := strings.TrimSpace(source.URL); sourceURL != "" {
			actionSources = append(actionSources, map[string]any{"type": "url", "url": sourceURL})
		}
	}
	action := map[string]any{
		"type":    "search",
		"sources": actionSources,
	}
	if query = strings.TrimSpace(query); query != "" {
		action["query"] = query
		action["queries"] = []string{query}
	}
	return action
}

func buildChatGPTWebGenericOutputAddedEvent(responseID string, outputIndex int, item gjson.Result) []byte {
	var added map[string]any
	if item.Get("type").String() == "image_generation_call" {
		added = map[string]any{
			"type": item.Get("type").String(),
			"id":   item.Get("id").String(),
		}
	} else if err := json.Unmarshal([]byte(item.Raw), &added); err != nil {
		added = map[string]any{"type": item.Get("type").String()}
	}
	added["status"] = "in_progress"
	return marshalChatGPTWebEvent(map[string]any{
		"type":         "response.output_item.added",
		"response_id":  responseID,
		"output_index": outputIndex,
		"item":         added,
	})
}

func emitChatGPTWebEventsFromCompleted(responseID, model string, response gjson.Result, emit func([]byte) bool) bool {
	if emit == nil {
		return false
	}
	sequencer := &chatGPTWebEventSequencer{}
	emitNext := func(event []byte) bool {
		return emit(sequencer.Next(event))
	}
	if !emitNext(buildChatGPTWebCreatedEvent(responseID, model)) ||
		!emitNext(buildChatGPTWebInProgressEvent(responseID, model)) {
		return false
	}
	for outputIndex, item := range response.Get("output").Array() {
		itemID := item.Get("id").String()
		if item.Get("type").String() == "message" {
			for _, event := range buildChatGPTWebMessageAddedEvents(responseID, itemID, outputIndex) {
				if !emitNext(event) {
					return false
				}
			}
			text := item.Get("content.0.text").String()
			if text != "" && !emitNext(buildChatGPTWebTextDeltaEvent(responseID, itemID, outputIndex, text)) {
				return false
			}
			annotations := make([]any, 0)
			for _, annotation := range item.Get("content.0.annotations").Array() {
				annotations = append(annotations, json.RawMessage(annotation.Raw))
			}
			for _, event := range buildChatGPTWebMessageTerminalEvents(responseID, itemID, outputIndex, text, annotations) {
				if !emitNext(event) {
					return false
				}
			}
			continue
		}
		if !emitNext(buildChatGPTWebGenericOutputAddedEvent(responseID, outputIndex, item)) ||
			!emitNext(marshalChatGPTWebEvent(map[string]any{
				"type": "response.output_item.done", "response_id": responseID, "output_index": outputIndex,
				"item": json.RawMessage(item.Raw),
			})) {
			return false
		}
	}
	return emitNext(marshalChatGPTWebEvent(map[string]any{
		"type": "response.completed", "response": json.RawMessage(response.Raw),
	}))
}

type chatGPTWebEventSequencer struct {
	next int64
}

func (sequencer *chatGPTWebEventSequencer) Next(event []byte) []byte {
	if sequencer == nil {
		return event
	}
	sequence := sequencer.next
	sequencer.next++
	withSequence, err := sjson.SetBytes(event, "sequence_number", sequence)
	if err != nil {
		return event
	}
	return withSequence
}

func chatGPTWebSourceAnnotations(text string, sources []chatGPTWebSearchSource) []any {
	annotations := make([]any, 0, len(sources))
	sourceSection := strings.LastIndex(text, "\n\nSources:")
	searchOffset := 0
	if sourceSection >= 0 {
		searchOffset = sourceSection
	}
	for _, source := range sources {
		sourceURL := strings.TrimSpace(source.URL)
		if sourceURL == "" || searchOffset >= len(text) {
			continue
		}
		relativeIndex := strings.Index(text[searchOffset:], sourceURL)
		if relativeIndex < 0 {
			continue
		}
		startByte := searchOffset + relativeIndex
		endByte := startByte + len(sourceURL)
		annotations = append(annotations, map[string]any{
			"type":        "url_citation",
			"url":         sourceURL,
			"title":       source.Title,
			"start_index": utf8.RuneCountInString(text[:startByte]),
			"end_index":   utf8.RuneCountInString(text[:endByte]),
		})
		searchOffset = endByte
	}
	return annotations
}

func marshalChatGPTWebEvent(value any) []byte {
	payload, _ := json.Marshal(value)
	return payload
}

func chatGPTWebAnyString(value any) string {
	if value == nil {
		return ""
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "<nil>" {
		return ""
	}
	return text
}
