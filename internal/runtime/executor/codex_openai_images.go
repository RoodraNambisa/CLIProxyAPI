package executor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	codexOpenAIImageSourceFormat = "openai-image"
	codexOpenAIImageGenerations  = "images/generations"
	codexOpenAIImageEdits        = "images/edits"
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

func codexPrepareOpenAIImageBody(req cliproxyexecutor.Request, stream bool) ([]byte, string, error) {
	if !json.Valid(req.Payload) {
		return nil, "", statusErr{code: http.StatusBadRequest, msg: "invalid image request JSON"}
	}
	body := bytes.Clone(req.Payload)
	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = strings.TrimSpace(gjson.GetBytes(body, "model").String())
	}
	if model == "" {
		return nil, "", statusErr{code: http.StatusBadRequest, msg: "model is required"}
	}
	body, _ = sjson.SetBytes(body, "model", model)
	if stream {
		body, _ = sjson.SetBytes(body, "stream", true)
	} else {
		body, _ = sjson.DeleteBytes(body, "stream")
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
	httpReq, upstreamBody, identityState, err := e.cacheHelper(ctx, sdktranslator.FromString(codexOpenAIImageSourceFormat), url, auth, req, originalPayload, body)
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
	data, readErr := io.ReadAll(httpResp.Body)
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
	httpReq, upstreamBody, identityState, err := e.cacheHelper(ctx, sdktranslator.FromString(codexOpenAIImageSourceFormat), url, auth, req, originalPayload, body)
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
		data, readErr := io.ReadAll(httpResp.Body)
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
		reader := bufio.NewReaderSize(httpResp.Body, 64*1024)
		for {
			line, readErr := reader.ReadBytes('\n')
			if len(line) > 0 {
				upstreamLine := applyCodexIdentityConfuseResponsePayload(line, identityState)
				helps.AppendAPIResponseChunk(ctx, e.cfg, upstreamLine)
				if detail, ok := helps.ParseOpenAIStreamUsage(upstreamLine); ok {
					reporter.Publish(ctx, detail)
				}
				clientLine := applyCodexIdentityExposeResponsePayload(upstreamLine, identityState)
				if !emitCodexOpenAIImageStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Payload: clientLine}) {
					return
				}
			}
			if readErr == nil {
				continue
			}
			if errors.Is(readErr, io.EOF) {
				reporter.EnsurePublished(ctx)
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
