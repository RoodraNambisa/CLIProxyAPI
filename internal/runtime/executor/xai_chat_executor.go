package executor

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func (e *XAIExecutor) usesDirectChat(opts core.Options) bool {
	cfg := e.cfg
	if plan := helps.XAIPlanFromOptions(opts); plan != nil {
		cfg = plan.Config
	}
	return cfg != nil && cfg.XAI.ChatCompletionsMode == "direct" && opts.SourceFormat == sdktranslator.FormatOpenAI
}

// prepareChatRequest keeps Chat messages and tool structures intact. It shares
// routing and the request identity snapshot with the Responses executor.
func (e *XAIExecutor) prepareChatRequest(ctx context.Context, auth *coreauth.Auth, req core.Request, opts core.Options, stream bool) (*xaiPreparedRequest, error) {
	original := req.Payload
	if len(opts.OriginalRequest) > 0 {
		original = opts.OriginalRequest
	}
	body := bytes.Clone(req.Payload)
	if !gjson.ValidBytes(body) || !gjson.GetBytes(body, "messages").IsArray() {
		return nil, statusErr{code: http.StatusBadRequest, msg: "xai direct Chat requires a JSON messages array"}
	}
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	if e.cfg != nil {
		body = helps.ApplyXAIChatDefaults(body, e.cfg.XAI.RequestDefaults)
	}
	var err error
	body, err = helps.ApplyRequestThinking(body, req, opts, "openai", "openai", "xai")
	if err != nil {
		return nil, err
	}
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, "openai", opts.SourceFormat.String(), "", body, original,
		helps.PayloadRequestedModel(opts, req.Model), helps.PayloadRequestPath(opts), opts.Headers)
	body, _ = sjson.SetBytes(body, "model", baseModel)
	body, _ = sjson.SetBytes(body, "stream", stream)
	tools := gjson.GetBytes(body, "tools")
	if !tools.Exists() || tools.Type == gjson.Null || (tools.IsArray() && len(tools.Array()) == 0) {
		choice := gjson.GetBytes(body, "tool_choice")
		if choice.Exists() && choice.Type != gjson.Null {
			if choice.Type != gjson.String || (choice.Str != "none" && choice.Str != "auto") {
				return nil, statusErr{code: http.StatusBadRequest, skipAuthResult: true, msg: "xai tool_choice requires tool declarations"}
			}
			body, _ = sjson.DeleteBytes(body, "tool_choice")
		}
	}
	if stream && !gjson.GetBytes(body, "stream_options.include_usage").Exists() {
		body, _ = sjson.SetBytes(body, "stream_options.include_usage", true)
	}
	sessionID, err := xaiResolveComposerSessionID(ctx, req, opts, baseModel)
	if err != nil {
		return nil, err
	}
	if cacheKey := gjson.GetBytes(body, "prompt_cache_key"); cacheKey.Type == gjson.String && cacheKey.Str != "" {
		sessionID = cacheKey.Str
	}
	var identity helps.XAIIdentityProjection
	if plan := helps.XAIPlanFromOptions(opts); plan != nil {
		headers := make(http.Header)
		helps.ApplyXAIResourceHeaders(&http.Request{Header: headers}, auth, e.cfg)
		body, identity, err = plan.Project(auth, body, headers, sessionID)
		if err != nil {
			return nil, err
		}
		// Chat uses the conversation header for caching. Explicit conversation
		// passthrough still wins when it differs from an explicit cache key.
		if identity.CacheKey != "" && !(plan.Config.XAI.PassthroughClientIdentity && plan.Conversation != "") {
			identity.Conversation = identity.CacheKey
		}
	}
	return &xaiPreparedRequest{
		baseModel: baseModel, from: opts.SourceFormat, to: sdktranslator.FormatOpenAI,
		responseFormat: core.ResponseFormatOrSource(opts), originalPayload: original,
		body: body, sessionID: sessionID, identity: identity,
	}, nil
}

func (e *XAIExecutor) sendChatRequest(ctx context.Context, auth *coreauth.Auth, req core.Request, opts core.Options, prepared *xaiPreparedRequest, reporter *helps.UsageReporter, stream bool) (*http.Response, error) {
	baseURL, err := xaiModelBaseURL(auth, e.cfg, req.Model)
	if err != nil {
		return nil, err
	}
	requestURL := strings.TrimRight(baseURL, "/") + "/chat/completions"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(prepared.body))
	if err != nil {
		return nil, err
	}
	token, _ := xaiCreds(auth)
	applyXAIChatHeaders(httpReq, auth, token, stream, prepared.sessionID, e.cfg)
	httpReq.Header.Set("x-grok-model-override", prepared.baseModel)
	e.applyPreparedXAIHeaders(ctx, auth, httpReq.Header, prepared, opts)
	e.recordXAIRequest(ctx, auth, requestURL, http.MethodPost, httpReq.Header.Clone(), prepared.body)
	client := reporter.TrackHTTPClient(helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0))
	resp, err := executeXAIHTTPRequest(client, httpReq, auth, opts)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, resp.StatusCode, resp.Header.Clone())
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer e.closeChatResponse(resp)
		data, errRead := io.ReadAll(resp.Body)
		if errRead != nil {
			return nil, errRead
		}
		helps.AppendAPIResponseChunk(ctx, e.cfg, data)
		return nil, xaiStatusErrFromResponse(resp.StatusCode, data, resp.Header, time.Now())
	}
	return resp, nil
}

func (e *XAIExecutor) closeChatResponse(resp *http.Response) {
	if errClose := resp.Body.Close(); errClose != nil {
		log.Errorf("xai Chat executor: close response body error: %v", errClose)
	}
}

func (e *XAIExecutor) executeChat(ctx context.Context, auth *coreauth.Auth, req core.Request, opts core.Options) (_ core.Response, err error) {
	prepared, err := e.prepareChatRequest(ctx, auth, req, opts, false)
	if err != nil {
		return core.Response{}, err
	}
	reporter := helps.NewExecutorUsageReporter(ctx, e, prepared.baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)
	reporter.SetTranslatedReasoningEffort(prepared.body, "openai")
	reporter.SetRequestServiceTierFromPayload(prepared.body)
	resp, err := e.sendChatRequest(ctx, auth, req, opts, prepared, reporter, false)
	if err != nil {
		return core.Response{}, err
	}
	defer e.closeChatResponse(resp)
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return core.Response{}, err
	}
	helps.AppendAPIResponseChunk(ctx, e.cfg, data)
	if helps.IsJSONStreamProtocolError(data) {
		return core.Response{}, xaiStatusErrFromResponse(http.StatusBadGateway, data, resp.Header, time.Now())
	}
	if !gjson.ValidBytes(data) || !gjson.GetBytes(data, "choices").IsArray() || len(gjson.GetBytes(data, "choices").Array()) == 0 {
		return core.Response{}, statusErr{code: http.StatusBadGateway, msg: "xai Chat returned no completion choices"}
	}
	reporter.Publish(ctx, helps.ParseOpenAIUsage(data))
	reporter.EnsurePublished(ctx)
	var param any
	output := sdktranslator.TranslateNonStream(ctx, prepared.to, prepared.responseFormat, req.Model, prepared.originalPayload, prepared.body, data, &param)
	return core.Response{Payload: output, Headers: resp.Header.Clone()}, nil
}

func (e *XAIExecutor) executeChatStream(ctx context.Context, auth *coreauth.Auth, req core.Request, opts core.Options) (_ *core.StreamResult, err error) {
	prepared, err := e.prepareChatRequest(ctx, auth, req, opts, true)
	if err != nil {
		return nil, err
	}
	reporter := helps.NewExecutorUsageReporter(ctx, e, prepared.baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)
	reporter.SetTranslatedReasoningEffort(prepared.body, "openai")
	reporter.SetRequestServiceTierFromPayload(prepared.body)
	resp, err := e.sendChatRequest(ctx, auth, req, opts, prepared, reporter, true)
	if err != nil {
		return nil, err
	}
	out := make(chan core.StreamChunk)
	go func() {
		defer close(out)
		defer e.closeChatResponse(resp)
		send := func(chunk core.StreamChunk) bool {
			if ctx.Err() != nil {
				return false
			}
			select {
			case out <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}
		fail := func(streamErr error) {
			helps.RecordAPIResponseError(ctx, e.cfg, streamErr)
			reporter.PublishFailure(ctx, streamErr)
			send(core.StreamChunk{Err: streamErr})
		}
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(nil, 52_428_800)
		scanner.Split(helps.ScanSSEFrames)
		var usage helps.StreamUsageBuffer
		var param any
		for scanner.Scan() {
			frame := scanner.Bytes()
			helps.AppendAPIResponseChunk(ctx, e.cfg, frame)
			for _, event := range helps.ParseOpenAIStreamFrame(frame) {
				if event.Err != nil {
					fail(event.Err)
					return
				}
				terminal := helps.IsOpenAIStreamTerminal(event.Data)
				if terminal && scanner.Err() != nil {
					fail(scanner.Err())
					return
				}
				line := append([]byte("data: "), event.Data...)
				usage.ObserveOpenAIStream(line)
				for _, chunk := range sdktranslator.TranslateStream(ctx, prepared.to, prepared.responseFormat, req.Model, prepared.originalPayload, prepared.body, line, &param) {
					if !send(core.StreamChunk{Payload: chunk}) {
						return
					}
				}
				if terminal {
					usage.Publish(ctx, reporter)
					reporter.EnsurePublished(ctx)
					if metadataBool(opts.Metadata, core.StreamTerminalMarkerMetadataKey) {
						send(core.SuccessfulStreamTerminalChunk())
					}
					return
				}
			}
		}
		if errScan := scanner.Err(); errScan != nil {
			fail(errScan)
		} else {
			fail(helps.IncompleteStreamError("xai Chat"))
		}
	}()
	return &core.StreamResult{Headers: resp.Header.Clone(), Chunks: out}, nil
}
