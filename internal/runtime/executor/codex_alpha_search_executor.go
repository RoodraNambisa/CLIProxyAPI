package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// executeAlphaSearch forwards the native search protocol without Responses
// translation, session replay, cache-key generation or tool normalization.
func (e *CodexExecutor) executeAlphaSearch(ctx context.Context, auth *cliproxyauth.Auth, req core.Request, opts core.Options) (resp core.Response, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err = ctx.Err(); err != nil {
		return resp, err
	}
	if opts.Stream || core.RequiredUpstreamWebsocket(ctx) {
		return resp, statusErr{code: http.StatusNotImplemented, msg: "Codex search supports non-streaming HTTP requests only", skipAuthResult: true}
	}
	if !cliproxyauth.SupportsCodexAlphaSearch(auth) {
		return resp, statusErr{code: http.StatusNotImplemented, msg: "Selected credential does not support Codex search", skipAuthResult: true}
	}
	token, baseURL := codexCreds(auth)
	if strings.TrimSpace(token) == "" {
		return resp, statusErr{code: http.StatusServiceUnavailable, msg: "Codex search access token is unavailable", skipAuthResult: true, retryOtherAuth: true}
	}
	endpoint, err := helps.CodexAlphaSearchURL(baseURL, auth.Attributes["api_key"] != "")
	if err != nil {
		return resp, statusErr{code: http.StatusBadGateway, msg: err.Error(), skipAuthResult: true}
	}
	body, err := helps.RewriteCodexAlphaSearchBody(req.Payload, req.Model)
	if err != nil {
		code := http.StatusBadRequest
		if len(req.Payload) > helps.CodexAlphaSearchMaxRequestBytes {
			code = http.StatusRequestEntityTooLarge
		}
		return resp, statusErr{code: code, msg: err.Error(), skipAuthResult: true}
	}
	ctx = contextWithCodexFingerprintPersona(ctx, e.cfg, auth)
	ctx = helps.WithCodexPromptCacheLogRedaction(ctx, helps.SnapshotCodexPromptCacheLog(ctx, req.Payload))
	reporter := helps.NewUsageReporter(ctx, e.Identifier(), req.Model, auth)
	defer reporter.TrackFailure(ctx, &err)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return resp, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	var incoming http.Header
	if c, ok := ctx.Value("gin").(*gin.Context); ok && c != nil && c.Request != nil {
		incoming = c.Request.Header
	}
	for _, name := range []string{"Version", "User-Agent", "Session_id", "X-Client-Request-Id"} {
		if value := firstCodexHeaderValue(name, opts.Headers, incoming); value != "" {
			httpReq.Header.Set(name, value)
		}
	}
	userAgent, originator, _ := codexHeaderDefaults(e.cfg, auth)
	ensureHeaderWithConfigPrecedence(httpReq.Header, incoming, "User-Agent", userAgent, codexUserAgent)
	if originator == "" {
		originator = codexOriginator
	}
	httpReq.Header.Set("Originator", originator)
	if auth.Attributes["api_key"] == "" {
		if accountID := codexauth.EffectiveRequestAccountID(auth.Metadata); accountID != "" {
			httpReq.Header.Set("Chatgpt-Account-Id", accountID)
		}
		if codexauth.ChatGPTAccountIsFedRAMP(auth.Metadata) {
			httpReq.Header.Set("X-OpenAI-Fedramp", "true")
		}
	}
	if err = e.PrepareRequest(httpReq, auth); err != nil {
		return resp, err
	}
	applyCodexSoftwareIdentity(httpReq.Header, auth, e.cfg, req.Model)
	authType, authValue := auth.AccountInfo()
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{URL: endpoint, Method: http.MethodPost, Headers: httpReq.Header.Clone(), Body: body, Provider: e.Identifier(), AuthID: auth.ID, AuthLabel: auth.Label, AuthType: authType, AuthValue: authValue})
	dropCodexRawRequestCopies(&req, &opts)
	body = nil
	httpResp, err := helps.DoUpstreamHTTPRequest(e.newCodexHTTPClient(ctx, auth, false), httpReq)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codex search: close response body: %v", errClose)
		}
	}()
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	data, err := io.ReadAll(io.LimitReader(httpResp.Body, helps.CodexAlphaSearchMaxResponseBytes+1))
	if errContext := ctx.Err(); errContext != nil {
		return resp, errContext
	}
	if err != nil || len(data) > helps.CodexAlphaSearchMaxResponseBytes {
		message := "Failed to read Codex search response"
		tooLarge := len(data) > helps.CodexAlphaSearchMaxResponseBytes
		if tooLarge {
			message = "Codex search response exceeds 32 MiB"
		}
		code := httpResp.StatusCode
		success := code >= 200 && code < 300
		if success {
			code = http.StatusBadGateway
		}
		responseErr := statusErr{code: code, msg: message, skipAuthResult: tooLarge && success, retryAfter: parseXAIRetryAfterHeader(httpResp.Header.Get("Retry-After"), time.Now())}
		err = helps.WithCodexAlphaSearchReadError(statusErrWithHeaders{statusErr: responseErr, headers: httpResp.Header.Clone()}, err)
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	helps.AppendAPIResponseChunk(ctx, e.cfg, data)
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		upstreamErr := statusErr{code: httpResp.StatusCode, msg: string(data), skipAuthResult: isCodexContextTooLargeRequestError(httpResp.StatusCode, data)}
		upstreamErr.retryAfter = parseCodexRetryAfter(httpResp.StatusCode, data, time.Now())
		if upstreamErr.retryAfter == nil {
			upstreamErr.retryAfter = parseXAIRetryAfterHeader(httpResp.Header.Get("Retry-After"), time.Now())
		}
		return resp, statusErrWithHeaders{statusErr: upstreamErr, headers: httpResp.Header.Clone()}
	}
	helps.ReleaseRequestBodyAfterStreamEstablished(ctx, opts)
	if gjson.GetBytes(data, "usage").IsObject() {
		reporter.Publish(ctx, helps.ParseOpenAIUsage(data))
	} else {
		reporter.EnsurePublished(ctx)
	}
	return core.Response{StatusCode: httpResp.StatusCode, Payload: data, Headers: httpResp.Header.Clone()}, nil
}
