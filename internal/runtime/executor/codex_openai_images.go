package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const (
	codexOpenAIImageSourceFormat     = "openai-image"
	codexOpenAIImageGenerations      = "images/generations"
	codexOpenAIImageEdits            = "images/edits"
	codexOpenAIImageMaxResponseBytes = 128 << 20
	codexOpenAIImageMaxErrorBytes    = 1 << 20
	codexOpenAIImageMaxSSEFrameBytes = 50 << 20
)

func isCodexOpenAIImageRequest(opts cliproxyexecutor.Options) bool {
	return strings.EqualFold(strings.TrimSpace(opts.SourceFormat.String()), codexOpenAIImageSourceFormat)
}

func codexOpenAIImageEndpoint(opts cliproxyexecutor.Options) (string, error) {
	switch strings.Trim(strings.ToLower(strings.TrimSpace(opts.Alt)), "/") {
	case codexOpenAIImageGenerations:
		return "/images/generations", nil
	case codexOpenAIImageEdits:
		return "/images/edits", nil
	default:
		return "", statusErr{code: http.StatusBadRequest, msg: "unsupported native image endpoint"}
	}
}

func readCodexOpenAIImageResponseBody(body io.Reader, maxBytes int64) ([]byte, error) {
	if body == nil {
		return nil, statusErr{code: http.StatusBadGateway, msg: "codex image response body is nil", skipAuthResult: true}
	}
	if maxBytes < 1 {
		return nil, statusErr{code: http.StatusBadGateway, msg: "codex image response limit is invalid", skipAuthResult: true}
	}
	payload, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > maxBytes {
		return nil, statusErr{
			code:           http.StatusBadGateway,
			msg:            fmt.Sprintf("codex image response exceeds %d bytes", maxBytes),
			skipAuthResult: true,
		}
	}
	return payload, nil
}

func codexPrepareOpenAIImageBody(req cliproxyexecutor.Request, stream bool) ([]byte, string, error) {
	if !json.Valid(req.Payload) {
		return nil, "", statusErr{code: http.StatusBadRequest, msg: "invalid image request JSON"}
	}
	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = strings.TrimSpace(gjson.GetBytes(req.Payload, "model").String())
	}
	if model == "" {
		return nil, "", statusErr{code: http.StatusBadRequest, msg: "model is required"}
	}
	streamMode := helps.CodexStreamRemove
	if stream {
		streamMode = helps.CodexStreamForceEnabled
	}
	body, err := helps.RewriteCodexRequestEnvelope(req.Payload, helps.CodexRequestRewriteOptions{
		Model:  model,
		Stream: streamMode,
	})
	if err != nil {
		return nil, "", statusErr{code: http.StatusBadRequest, msg: err.Error()}
	}
	return body, model, nil
}

func (e *CodexExecutor) executeOpenAIImage(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	endpoint, err := codexOpenAIImageEndpoint(opts)
	if err != nil {
		return resp, err
	}
	body, model, err := codexPrepareOpenAIImageBody(req, false)
	if err != nil {
		return resp, err
	}
	if cliproxyauth.AuthDisablesImageGeneration(e.cfg, auth, e.Identifier()) {
		return resp, disabledImageGenerationToolError(e.cfg)
	}

	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}
	reporter := helps.NewUsageReporter(ctx, e.Identifier(), model, auth)
	defer reporter.TrackFailure(ctx, &err)

	originalPayload := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayload = opts.OriginalRequest
	}
	url := strings.TrimSuffix(baseURL, "/") + endpoint
	httpReq, upstreamBody, identityState, err := e.cacheHelper(ctx, sdktranslator.FromString(codexOpenAIImageSourceFormat), url, auth, req, originalPayload, body, true)
	if err != nil {
		return resp, err
	}
	body = nil
	originalPayload = nil
	dropCodexRawRequestCopies(&req, &opts)
	applyCodexDirectImageHeaders(httpReq, auth, apiKey, false, e.cfg)
	applyCodexIdentityConfuseHeaders(httpReq.Header, &identityState)
	recordCodexOpenAIImageRequest(ctx, e, auth, url, httpReq.Header.Clone(), upstreamBody)
	upstreamBody = nil

	httpResp, err := e.newCodexHTTPClient(ctx, auth, true).Do(httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex executor: close native image response body error: %v", errClose)
		}
	}()
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	maxResponseBytes := int64(codexOpenAIImageMaxResponseBytes)
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		maxResponseBytes = codexOpenAIImageMaxErrorBytes
	}
	data, readErr := readCodexOpenAIImageResponseBody(httpResp.Body, maxResponseBytes)
	if readErr != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, readErr)
		return resp, readErr
	}
	upstreamData := applyCodexIdentityConfuseResponsePayload(data, identityState)
	helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamData)
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		helps.LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", httpResp.StatusCode, helps.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), upstreamData))
		clientBody := applyCodexIdentityExposeResponsePayload(upstreamData, identityState)
		err = newCodexStatusErr(httpResp.StatusCode, clientBody)
		return resp, err
	}
	reporter.Publish(ctx, helps.ParseOpenAIUsage(upstreamData))
	reporter.EnsurePublished(ctx)
	clientData := applyCodexIdentityExposeResponsePayload(upstreamData, identityState)
	return cliproxyexecutor.Response{Payload: clientData, Headers: httpResp.Header.Clone()}, nil
}

func (e *CodexExecutor) executeOpenAIImageStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	endpoint, err := codexOpenAIImageEndpoint(opts)
	if err != nil {
		return nil, err
	}
	body, model, err := codexPrepareOpenAIImageBody(req, true)
	if err != nil {
		return nil, err
	}
	if cliproxyauth.AuthDisablesImageGeneration(e.cfg, auth, e.Identifier()) {
		return nil, disabledImageGenerationToolError(e.cfg)
	}

	apiKey, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}
	reporter := helps.NewUsageReporter(ctx, e.Identifier(), model, auth)
	defer reporter.TrackFailure(ctx, &err)

	originalPayload := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayload = opts.OriginalRequest
	}
	url := strings.TrimSuffix(baseURL, "/") + endpoint
	httpReq, upstreamBody, identityState, err := e.cacheHelper(ctx, sdktranslator.FromString(codexOpenAIImageSourceFormat), url, auth, req, originalPayload, body, true)
	if err != nil {
		return nil, err
	}
	body = nil
	originalPayload = nil
	dropCodexRawRequestCopies(&req, &opts)
	applyCodexDirectImageHeaders(httpReq, auth, apiKey, true, e.cfg)
	applyCodexIdentityConfuseHeaders(httpReq.Header, &identityState)
	recordCodexOpenAIImageRequest(ctx, e, auth, url, httpReq.Header.Clone(), upstreamBody)
	upstreamBody = nil

	httpResp, err := e.newCodexHTTPClient(ctx, auth, true).Do(httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		data, readErr := readCodexOpenAIImageResponseBody(httpResp.Body, codexOpenAIImageMaxErrorBytes)
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex executor: close native image response body error: %v", errClose)
		}
		if readErr != nil {
			helps.RecordAPIResponseError(ctx, e.cfg, readErr)
			return nil, readErr
		}
		upstreamBody := applyCodexIdentityConfuseResponsePayload(data, identityState)
		helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamBody)
		helps.LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", httpResp.StatusCode, helps.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), upstreamBody))
		clientBody := applyCodexIdentityExposeResponsePayload(upstreamBody, identityState)
		err = newCodexStatusErr(httpResp.StatusCode, clientBody)
		return nil, err
	}

	helps.ReleaseRequestBodyAfterStreamEstablished(ctx, opts)
	out := make(chan cliproxyexecutor.StreamChunk, cliproxyexecutor.StreamBufferSize)
	go func() {
		defer close(out)
		defer func() {
			if errClose := httpResp.Body.Close(); errClose != nil {
				log.Errorf("codex executor: close native image response body error: %v", errClose)
			}
		}()
		frameBuffer := helps.NewSSEFrameBuffer(codexOpenAIImageMaxSSEFrameBytes)
		readBuffer := make([]byte, 64*1024)
		var streamUsage helps.StreamUsageBuffer
		markerRequested := metadataBool(opts.Metadata, cliproxyexecutor.StreamTerminalMarkerMetadataKey)
		protocolFailed := false
		var protocolErr error
		terminalSeen := false
		for {
			bytesRead, readErr := httpResp.Body.Read(readBuffer)
			frames, frameErr := frameBuffer.Feed(readBuffer[:bytesRead], errors.Is(readErr, io.EOF))
			if frameErr != nil {
				helps.RecordAPIResponseError(ctx, e.cfg, frameErr)
				reporter.PublishFailure(ctx, frameErr)
				_ = emitCodexOpenAIImageStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: frameErr})
				return
			}
			for _, frame := range frames {
				upstreamFrame := applyCodexIdentityConfuseResponsePayload(frame, identityState)
				helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamFrame)
				clientFrame := applyCodexIdentityExposeResponsePayload(upstreamFrame, identityState)
				terminal := false
				for _, line := range helps.SplitSSEFrameLines(clientFrame) {
					streamUsage.ObserveOpenAIStream(line)
					if payload := helps.JSONPayload(line); len(payload) > 0 && helps.IsJSONStreamProtocolError(payload) {
						protocolFailed = true
						if protocolErr == nil {
							protocolErr = helps.JSONStreamProtocolError("codex image", payload)
						}
					}
					terminal = terminal || helps.IsOpenAIStreamTerminal(line) || codexSSEDataCompletion(line)
				}
				terminalSeen = terminalSeen || terminal
				if !emitCodexOpenAIImageStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Payload: clientFrame}) {
					return
				}
				if markerRequested && terminal && !protocolFailed && (readErr == nil || errors.Is(readErr, io.EOF)) {
					streamUsage.Publish(ctx, reporter)
					reporter.EnsurePublished(ctx)
					_ = emitCodexOpenAIImageStreamChunk(ctx, out, cliproxyexecutor.SuccessfulStreamTerminalChunk())
					return
				}
			}
			if readErr == nil {
				continue
			}
			if errors.Is(readErr, io.EOF) {
				if terminalSeen && !protocolFailed {
					streamUsage.Publish(ctx, reporter)
					reporter.EnsurePublished(ctx)
					if markerRequested {
						_ = emitCodexOpenAIImageStreamChunk(ctx, out, cliproxyexecutor.SuccessfulStreamTerminalChunk())
					}
					return
				}
				streamErr := protocolErr
				if streamErr == nil {
					streamErr = helps.IncompleteStreamError("codex image")
				}
				helps.RecordAPIResponseError(ctx, e.cfg, streamErr)
				reporter.PublishFailure(ctx, streamErr)
				_ = emitCodexOpenAIImageStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: streamErr})
				return
			}
			helps.RecordAPIResponseError(ctx, e.cfg, readErr)
			reporter.PublishFailure(ctx)
			_ = emitCodexOpenAIImageStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: readErr})
			return
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: httpResp.Header.Clone(), Chunks: out}, nil
}

func emitCodexOpenAIImageStreamChunk(ctx context.Context, out chan<- cliproxyexecutor.StreamChunk, chunk cliproxyexecutor.StreamChunk) bool {
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

func recordCodexOpenAIImageRequest(ctx context.Context, e *CodexExecutor, auth *cliproxyauth.Auth, url string, headers http.Header, body []byte) {
	if e == nil {
		return
	}
	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       url,
		Method:    http.MethodPost,
		Headers:   headers,
		Body:      body,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})
}
