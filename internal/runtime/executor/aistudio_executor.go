// Package executor provides runtime execution capabilities for various AI service providers.
// This file implements the AI Studio executor that routes requests through a websocket-backed
// transport for the AI Studio provider.
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
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/wsrelay"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// AIStudioExecutor routes AI Studio requests through a websocket-backed transport.
type AIStudioExecutor struct {
	provider string
	relay    *wsrelay.Manager
	cfg      *config.Config
}

// NewAIStudioExecutor creates a new AI Studio executor instance.
//
// Parameters:
//   - cfg: The application configuration
//   - provider: The provider name
//   - relay: The websocket relay manager
//
// Returns:
//   - *AIStudioExecutor: A new AI Studio executor instance
func NewAIStudioExecutor(cfg *config.Config, provider string, relay *wsrelay.Manager) *AIStudioExecutor {
	return &AIStudioExecutor{provider: strings.ToLower(provider), relay: relay, cfg: cfg}
}

// Identifier returns the executor identifier.
func (e *AIStudioExecutor) Identifier() string { return "aistudio" }

// PrepareRequest prepares the HTTP request for execution.
func (e *AIStudioExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if req == nil {
		return nil
	}
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(req, attrs)
	return nil
}

// HttpRequest forwards an arbitrary HTTP request through the websocket relay.
func (e *AIStudioExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("aistudio executor: request is nil")
	}
	if ctx == nil {
		ctx = req.Context()
	}
	if e.relay == nil {
		return nil, fmt.Errorf("aistudio executor: ws relay is nil")
	}
	if auth == nil || auth.ID == "" {
		return nil, fmt.Errorf("aistudio executor: missing auth")
	}
	httpReq := req.WithContext(ctx)
	if err := e.PrepareRequest(httpReq, auth); err != nil {
		return nil, err
	}
	if httpReq.URL == nil || strings.TrimSpace(httpReq.URL.String()) == "" {
		return nil, fmt.Errorf("aistudio executor: request URL is empty")
	}

	var body []byte
	if httpReq.Body != nil {
		b, errRead := io.ReadAll(httpReq.Body)
		if errRead != nil {
			return nil, errRead
		}
		body = b
		httpReq.Body = io.NopCloser(bytes.NewReader(b))
	}

	wsReq := &wsrelay.HTTPRequest{
		Method:  httpReq.Method,
		URL:     httpReq.URL.String(),
		Headers: httpReq.Header.Clone(),
		Body:    body,
	}
	wsResp, errRelay := e.relay.NonStream(ctx, auth.ID, wsReq)
	if errRelay != nil {
		return nil, errRelay
	}
	if wsResp == nil {
		return nil, fmt.Errorf("aistudio executor: ws response is nil")
	}

	statusText := http.StatusText(wsResp.Status)
	if statusText == "" {
		statusText = "Unknown"
	}
	resp := &http.Response{
		StatusCode:    wsResp.Status,
		Status:        fmt.Sprintf("%d %s", wsResp.Status, statusText),
		Header:        wsResp.Headers.Clone(),
		Body:          io.NopCloser(bytes.NewReader(wsResp.Body)),
		ContentLength: int64(len(wsResp.Body)),
		Request:       httpReq,
	}
	return resp, nil
}

// Execute performs a non-streaming request to the AI Studio API.
func (e *AIStudioExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	if opts.Alt == "responses/compact" {
		return resp, statusErr{code: http.StatusNotImplemented, msg: "/responses/compact not supported"}
	}
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	_, body, err := e.translateRequest(req, opts, false)
	if err != nil {
		return resp, err
	}
	payload := body.payload
	originalRef, payloadRef, unregisterBodies := helps.RequestBodyRefs(ctx, opts, opts.OriginalRequest, payload)
	defer unregisterBodies()
	defer originalRef.Release()
	defer payloadRef.Release()
	toFormat := body.toFormat

	endpoint := e.buildEndpoint(baseModel, body.action, opts.Alt)
	wsReq := &wsrelay.HTTPRequest{
		Method:  http.MethodPost,
		URL:     endpoint,
		Headers: http.Header{"Content-Type": []string{"application/json"}},
		Body:    payload,
	}
	body.payload = nil
	payload = nil
	req.Payload = nil
	opts.OriginalRequest = nil
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(&http.Request{Header: wsReq.Headers}, attrs)

	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       endpoint,
		Method:    http.MethodPost,
		Headers:   wsReq.Headers.Clone(),
		Body:      payloadRef.Bytes(),
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})

	wsResp, err := e.relay.NonStream(ctx, authID, wsReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, wsResp.Status, wsResp.Headers.Clone())
	if len(wsResp.Body) > 0 {
		helps.AppendAPIResponseChunk(ctx, e.cfg, wsResp.Body)
	}
	if wsResp.Status < 200 || wsResp.Status >= 300 {
		return resp, statusErr{code: wsResp.Status, msg: string(wsResp.Body)}
	}
	reporter.Publish(ctx, helps.ParseGeminiUsage(wsResp.Body))
	var param any
	out := sdktranslator.TranslateNonStream(ctx, toFormat, opts.SourceFormat, req.Model, originalRef.Bytes(), payloadRef.Bytes(), wsResp.Body, &param)
	resp = cliproxyexecutor.Response{Payload: ensureColonSpacedJSON(out), Headers: wsResp.Headers.Clone()}
	return resp, nil
}

// ExecuteStream performs a streaming request to the AI Studio API.
func (e *AIStudioExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	if opts.Alt == "responses/compact" {
		return nil, statusErr{code: http.StatusNotImplemented, msg: "/responses/compact not supported"}
	}
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	_, body, err := e.translateRequest(req, opts, true)
	if err != nil {
		return nil, err
	}
	payload := body.payload
	originalRef, payloadRef, unregisterBodies := helps.RequestBodyRefs(ctx, opts, opts.OriginalRequest, payload)
	cleanupBodies := func() {
		unregisterBodies()
		originalRef.Release()
		payloadRef.Release()
	}
	toFormat := body.toFormat

	endpoint := e.buildEndpoint(baseModel, body.action, opts.Alt)
	wsReq := &wsrelay.HTTPRequest{
		Method:  http.MethodPost,
		URL:     endpoint,
		Headers: http.Header{"Content-Type": []string{"application/json"}},
		Body:    payload,
	}
	body.payload = nil
	payload = nil
	req.Payload = nil
	opts.OriginalRequest = nil
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(&http.Request{Header: wsReq.Headers}, attrs)
	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       endpoint,
		Method:    http.MethodPost,
		Headers:   wsReq.Headers.Clone(),
		Body:      payloadRef.Bytes(),
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})
	streamParent := ctx
	if streamParent == nil {
		streamParent = context.Background()
	}
	streamCtx, cancelStream := context.WithCancel(streamParent)
	wsStream, err := e.relay.Stream(streamCtx, authID, wsReq)
	if err != nil {
		cancelStream()
		cleanupBodies()
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	firstEvent, ok := <-wsStream
	if !ok {
		cancelStream()
		cleanupBodies()
		err = fmt.Errorf("wsrelay: stream closed before start")
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	if firstEvent.Status > 0 && firstEvent.Status != http.StatusOK {
		defer cancelStream()
		defer cleanupBodies()
		metadataLogged := false
		if firstEvent.Status > 0 {
			helps.RecordAPIResponseMetadata(ctx, e.cfg, firstEvent.Status, firstEvent.Headers.Clone())
			metadataLogged = true
		}
		var body bytes.Buffer
		if len(firstEvent.Payload) > 0 {
			helps.AppendAPIResponseChunk(ctx, e.cfg, firstEvent.Payload)
			body.Write(firstEvent.Payload)
		}
		if firstEvent.Type == wsrelay.MessageTypeStreamEnd {
			return nil, statusErr{code: firstEvent.Status, msg: body.String()}
		}
		for event := range wsStream {
			if event.Err != nil {
				helps.RecordAPIResponseError(ctx, e.cfg, event.Err)
				if body.Len() == 0 {
					body.WriteString(event.Err.Error())
				}
				break
			}
			if !metadataLogged && event.Status > 0 {
				helps.RecordAPIResponseMetadata(ctx, e.cfg, event.Status, event.Headers.Clone())
				metadataLogged = true
			}
			if len(event.Payload) > 0 {
				helps.AppendAPIResponseChunk(ctx, e.cfg, event.Payload)
				body.Write(event.Payload)
			}
			if event.Type == wsrelay.MessageTypeStreamEnd {
				break
			}
		}
		return nil, statusErr{code: firstEvent.Status, msg: body.String()}
	}
	if firstEvent.Err == nil && firstEvent.Type != wsrelay.MessageTypeError && firstEvent.Type != wsrelay.MessageTypeStreamEnd {
		helps.ReleaseRequestBodyAfterStreamEstablished(ctx, opts)
	}
	out := make(chan cliproxyexecutor.StreamChunk)
	go func(first wsrelay.StreamEvent) {
		defer close(out)
		defer cancelStream()
		defer cleanupBodies()
		var param any
		metadataLogged := false
		markerRequested := metadataBool(opts.Metadata, cliproxyexecutor.StreamTerminalMarkerMetadataKey)
		protocolFailed := false
		var protocolErr error
		emittedPayload := false
		terminalSeen := false
		terminalReady := false
		var streamUsage helps.StreamUsageBuffer
		var observationBuffer []byte
		emitStreamError := func(streamErr error) {
			if streamErr == nil {
				streamErr = helps.IncompleteStreamError(e.Identifier())
			}
			helps.RecordAPIResponseError(ctx, e.cfg, streamErr)
			reporter.PublishFailure(ctx, streamErr)
			select {
			case out <- cliproxyexecutor.StreamChunk{Err: streamErr}:
			case <-ctx.Done():
			}
		}
		observeLine := func(line []byte) {
			payload := helps.JSONPayload(line)
			if len(payload) == 0 {
				return
			}
			if helps.IsJSONStreamProtocolError(payload) {
				protocolFailed = true
				if protocolErr == nil {
					protocolErr = helps.JSONStreamProtocolError(e.Identifier(), payload)
				}
				return
			}
			terminal := helps.IsGeminiStreamTerminal(payload)
			detail, usagePresent := helps.ParseGeminiStreamUsage(line)
			streamUsage.Observe(detail, usagePresent)
			terminalSeen = terminalSeen || terminal
			terminalReady = terminalReady || terminal && !helps.GeminiTerminalAwaitsUsage(payload) || terminalSeen && usagePresent
		}
		publishStreamUsage := func(failed bool) {
			if failed {
				reporter.PublishFailure(ctx)
				return
			}
			streamUsage.Publish(ctx, reporter)
			reporter.EnsurePublished(ctx)
		}
		processEvent := func(event wsrelay.StreamEvent) bool {
			if event.Err != nil {
				emitStreamError(fmt.Errorf("wsrelay: %w", event.Err))
				return false
			}
			switch event.Type {
			case wsrelay.MessageTypeStreamStart:
				if !metadataLogged && event.Status > 0 {
					helps.RecordAPIResponseMetadata(ctx, e.cfg, event.Status, event.Headers.Clone())
					metadataLogged = true
				}
			case wsrelay.MessageTypeStreamChunk:
				if len(event.Payload) > 0 {
					helps.AppendAPIResponseChunk(ctx, e.cfg, event.Payload)
					if errObserve := helps.ObserveSSELines(&observationBuffer, event.Payload, false, streamScannerBuffer, observeLine); errObserve != nil {
						emitStreamError(fmt.Errorf("wsrelay: %w", errObserve))
						return false
					}
					if protocolFailed {
						emitStreamError(protocolErr)
						return false
					}
					filtered := helps.FilterSSEUsageMetadata(event.Payload)
					lines := sdktranslator.TranslateStream(ctx, toFormat, opts.SourceFormat, req.Model, originalRef.Bytes(), payloadRef.Bytes(), filtered, &param)
					for i := range lines {
						emittedPayload = emittedPayload || len(lines[i]) > 0
						out <- cliproxyexecutor.StreamChunk{Payload: ensureColonSpacedJSON(lines[i])}
					}
					if markerRequested && terminalReady && !protocolFailed {
						publishStreamUsage(false)
						if emittedPayload {
							out <- cliproxyexecutor.SuccessfulStreamTerminalChunk()
						}
						return false
					}
					break
				}
			case wsrelay.MessageTypeStreamEnd:
				if errObserve := helps.ObserveSSELines(&observationBuffer, nil, true, streamScannerBuffer, observeLine); errObserve != nil {
					emitStreamError(fmt.Errorf("wsrelay: %w", errObserve))
					return false
				}
				if protocolFailed || !terminalSeen {
					if protocolFailed {
						emitStreamError(protocolErr)
					} else {
						emitStreamError(helps.IncompleteStreamError(e.Identifier()))
					}
					return false
				}
				if markerRequested && emittedPayload {
					out <- cliproxyexecutor.SuccessfulStreamTerminalChunk()
				}
				publishStreamUsage(false)
				return false
			case wsrelay.MessageTypeHTTPResp:
				if errObserve := helps.ObserveSSELines(&observationBuffer, nil, true, streamScannerBuffer, observeLine); errObserve != nil {
					emitStreamError(fmt.Errorf("wsrelay: %w", errObserve))
					return false
				}
				if !metadataLogged && event.Status > 0 {
					helps.RecordAPIResponseMetadata(ctx, e.cfg, event.Status, event.Headers.Clone())
					metadataLogged = true
				}
				if len(event.Payload) > 0 {
					helps.AppendAPIResponseChunk(ctx, e.cfg, event.Payload)
				}
				responseFailed := protocolFailed || event.Status < http.StatusOK || event.Status >= http.StatusMultipleChoices ||
					!gjson.ValidBytes(event.Payload) || helps.IsJSONStreamProtocolError(event.Payload)
				if responseFailed {
					streamErr := protocolErr
					if event.Status < http.StatusOK || event.Status >= http.StatusMultipleChoices {
						streamErr = statusErr{code: event.Status, msg: string(event.Payload)}
					} else if streamErr == nil && helps.IsJSONStreamProtocolError(event.Payload) {
						streamErr = helps.JSONStreamProtocolError(e.Identifier(), event.Payload)
					} else if streamErr == nil {
						streamErr = helps.IncompleteStreamError(e.Identifier())
					}
					emitStreamError(streamErr)
					return false
				}
				lines := sdktranslator.TranslateStream(ctx, toFormat, opts.SourceFormat, req.Model, originalRef.Bytes(), payloadRef.Bytes(), event.Payload, &param)
				for i := range lines {
					emittedPayload = emittedPayload || len(lines[i]) > 0
					out <- cliproxyexecutor.StreamChunk{Payload: ensureColonSpacedJSON(lines[i])}
				}
				streamUsage.Observe(helps.ParseGeminiStreamUsage(event.Payload))
				publishStreamUsage(false)
				if markerRequested && emittedPayload {
					out <- cliproxyexecutor.SuccessfulStreamTerminalChunk()
				}
				return false
			case wsrelay.MessageTypeError:
				streamErr := event.Err
				if streamErr == nil {
					streamErr = errors.New("upstream relay error")
				}
				emitStreamError(fmt.Errorf("wsrelay: %w", streamErr))
				return false
			}
			return true
		}
		if !processEvent(first) {
			return
		}
		for event := range wsStream {
			if !processEvent(event) {
				return
			}
		}
		emitStreamError(helps.IncompleteStreamError(e.Identifier()))
	}(firstEvent)
	return &cliproxyexecutor.StreamResult{Headers: firstEvent.Headers.Clone(), Chunks: out}, nil
}

// CountTokens counts tokens for the given request using the AI Studio API.
func (e *AIStudioExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	_, body, err := e.translateRequest(req, opts, false)
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}

	body.payload, _ = sjson.DeleteBytes(body.payload, "generationConfig")
	body.payload, _ = sjson.DeleteBytes(body.payload, "tools")
	body.payload, _ = sjson.DeleteBytes(body.payload, "safetySettings")

	endpoint := e.buildEndpoint(baseModel, "countTokens", "")
	wsReq := &wsrelay.HTTPRequest{
		Method:  http.MethodPost,
		URL:     endpoint,
		Headers: http.Header{"Content-Type": []string{"application/json"}},
		Body:    body.payload,
	}
	req.Payload = nil
	opts.OriginalRequest = nil
	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       endpoint,
		Method:    http.MethodPost,
		Headers:   wsReq.Headers.Clone(),
		Body:      body.payload,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})
	body.payload = nil
	resp, err := e.relay.NonStream(ctx, authID, wsReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return cliproxyexecutor.Response{}, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, resp.Status, resp.Headers.Clone())
	if len(resp.Body) > 0 {
		helps.AppendAPIResponseChunk(ctx, e.cfg, resp.Body)
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return cliproxyexecutor.Response{}, statusErr{code: resp.Status, msg: string(resp.Body)}
	}
	totalTokens := gjson.GetBytes(resp.Body, "totalTokens").Int()
	if totalTokens <= 0 {
		return cliproxyexecutor.Response{}, fmt.Errorf("wsrelay: totalTokens missing in response")
	}
	translated := sdktranslator.TranslateTokenCount(ctx, body.toFormat, opts.SourceFormat, totalTokens, resp.Body)
	return cliproxyexecutor.Response{Payload: translated}, nil
}

// Refresh refreshes the authentication credentials (no-op for AI Studio).
func (e *AIStudioExecutor) Refresh(_ context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	return auth, nil
}

type translatedPayload struct {
	payload  []byte
	action   string
	toFormat sdktranslator.Format
}

func (e *AIStudioExecutor) translateRequest(req cliproxyexecutor.Request, opts cliproxyexecutor.Options, stream bool) ([]byte, translatedPayload, error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName

	from := opts.SourceFormat
	to := sdktranslator.FromString("gemini")
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalPayload := originalPayloadSource
	originalTranslated := sdktranslator.TranslateRequest(from, to, baseModel, originalPayload, stream)
	payload := sdktranslator.TranslateRequest(from, to, baseModel, req.Payload, stream)
	payload, err := thinking.ApplyThinking(payload, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, translatedPayload{}, err
	}
	payload = fixGeminiImageAspectRatio(baseModel, payload)
	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	payload = helps.ApplyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", payload, originalTranslated, requestedModel)
	payload, _ = sjson.DeleteBytes(payload, "generationConfig.maxOutputTokens")
	payload, _ = sjson.DeleteBytes(payload, "generationConfig.responseMimeType")
	payload, _ = sjson.DeleteBytes(payload, "generationConfig.responseJsonSchema")
	metadataAction := "generateContent"
	if req.Metadata != nil {
		if action, _ := req.Metadata["action"].(string); action == "countTokens" {
			metadataAction = action
		}
	}
	action := metadataAction
	if stream && action != "countTokens" {
		action = "streamGenerateContent"
	}
	payload, _ = sjson.DeleteBytes(payload, "session_id")
	return payload, translatedPayload{payload: payload, action: action, toFormat: to}, nil
}

func (e *AIStudioExecutor) buildEndpoint(model, action, alt string) string {
	base := fmt.Sprintf("%s/%s/models/%s:%s", glEndpoint, glAPIVersion, model, action)
	if action == "streamGenerateContent" {
		if alt == "" {
			return base + "?alt=sse"
		}
		return base + "?$alt=" + url.QueryEscape(alt)
	}
	if alt != "" && action != "countTokens" {
		return base + "?$alt=" + url.QueryEscape(alt)
	}
	return base
}

// ensureColonSpacedJSON normalizes JSON objects so that colons are followed by a single space while
// keeping the payload otherwise compact. Non-JSON inputs are returned unchanged.
func ensureColonSpacedJSON(payload []byte) []byte {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return payload
	}

	var decoded any
	if err := json.Unmarshal(trimmed, &decoded); err != nil {
		return payload
	}

	indented, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		return payload
	}

	compacted := make([]byte, 0, len(indented))
	inString := false
	skipSpace := false

	for i := 0; i < len(indented); i++ {
		ch := indented[i]
		if ch == '"' {
			// A quote is escaped only when preceded by an odd number of consecutive backslashes.
			// For example: "\\\"" keeps the quote inside the string, but "\\\\" closes the string.
			backslashes := 0
			for j := i - 1; j >= 0 && indented[j] == '\\'; j-- {
				backslashes++
			}
			if backslashes%2 == 0 {
				inString = !inString
			}
		}

		if !inString {
			if ch == '\n' || ch == '\r' {
				skipSpace = true
				continue
			}
			if skipSpace {
				if ch == ' ' || ch == '\t' {
					continue
				}
				skipSpace = false
			}
		}

		compacted = append(compacted, ch)
	}

	return compacted
}
