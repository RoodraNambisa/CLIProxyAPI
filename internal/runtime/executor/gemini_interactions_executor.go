package executor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const geminiInteractionsAPIRevision = "2026-05-20"

// NewGeminiInteractionsExecutor creates a Gemini executor bound to native Interactions credentials.
func NewGeminiInteractionsExecutor(cfg *config.Config) *GeminiExecutor {
	return &GeminiExecutor{cfg: cfg, identifier: "gemini-interactions"}
}

// RequestToFormat reports the upstream request format used after auth selection.
func (e *GeminiExecutor) RequestToFormat(_ cliproxyexecutor.Request, opts cliproxyexecutor.Options) sdktranslator.Format {
	if strings.EqualFold(strings.TrimSpace(e.Identifier()), "gemini-interactions") && nativeInteractionsSourceFormat(opts.SourceFormat) {
		return sdktranslator.FormatInteractions
	}
	return sdktranslator.FormatGemini
}

func shouldExecuteNativeInteractions(auth *cliproxyauth.Auth, opts cliproxyexecutor.Options) bool {
	return isNativeInteractionsAuth(auth) && nativeInteractionsSourceFormat(opts.SourceFormat)
}

func nativeInteractionsSourceFormat(format sdktranslator.Format) bool {
	switch format {
	case sdktranslator.FormatInteractions, sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse, sdktranslator.FormatClaude, sdktranslator.FormatGemini:
		return true
	default:
		return false
	}
}

func isNativeInteractionsAuth(auth *cliproxyauth.Auth) bool {
	return auth != nil && strings.EqualFold(strings.TrimSpace(auth.Provider), "gemini-interactions")
}

func geminiInteractionsAPIVersion(opts cliproxyexecutor.Options) string {
	if opts.Metadata != nil {
		if version, ok := opts.Metadata[cliproxyexecutor.InteractionsAPIVersionMetadataKey].(string); ok {
			switch strings.ToLower(strings.TrimSpace(version)) {
			case "v1":
				return "v1"
			case "v1beta":
				return "v1beta"
			}
		}
	}
	return "v1beta"
}

func applyGeminiInteractionsHeaders(req *http.Request, auth *cliproxyauth.Auth, opts cliproxyexecutor.Options, version string) {
	if req == nil {
		return
	}
	applyGeminiHeaders(req, auth)
	if req.Header.Get("Api-Revision") == "" && opts.Headers != nil {
		if revision := strings.TrimSpace(opts.Headers.Get("Api-Revision")); revision != "" {
			req.Header.Set("Api-Revision", revision)
		}
	}
	if req.Header.Get("Api-Revision") == "" && opts.Metadata != nil {
		if revision, ok := opts.Metadata[cliproxyexecutor.InteractionsAPIRevisionMetadataKey].(string); ok {
			req.Header.Set("Api-Revision", strings.TrimSpace(revision))
		}
	}
	if req.Header.Get("Api-Revision") == "" && version == "v1beta" {
		req.Header.Set("Api-Revision", geminiInteractionsAPIRevision)
	}
}

func translateGeminiInteractionsRequestBody(model string, payload []byte, opts cliproxyexecutor.Options, stream bool) []byte {
	if opts.SourceFormat == "" || opts.SourceFormat == sdktranslator.FormatInteractions {
		return bytes.Clone(payload)
	}
	return sdktranslator.TranslateRequest(opts.SourceFormat, sdktranslator.FormatInteractions, model, payload, stream)
}

func geminiInteractionsPayloadConfigSource(model string, payload []byte, opts cliproxyexecutor.Options, stream bool) []byte {
	source := opts.OriginalRequest
	if len(source) == 0 {
		source = payload
	}
	return translateGeminiInteractionsRequestBody(model, source, opts, stream)
}

func applyGeminiInteractionsThinking(body []byte, model string) ([]byte, error) {
	return thinking.ApplyThinking(body, model, sdktranslator.FormatInteractions.String(), sdktranslator.FormatInteractions.String(), "gemini")
}

func (e *GeminiExecutor) buildInteractionsBody(req cliproxyexecutor.Request, opts cliproxyexecutor.Options, stream bool) ([]byte, error) {
	targetName := thinking.ParseSuffix(req.Model).ModelName
	body := translateGeminiInteractionsRequestBody(targetName, req.Payload, opts, stream)
	if gjson.GetBytes(body, "model").Exists() && targetName != "" {
		body, _ = sjson.SetBytes(body, "model", targetName)
	}

	var err error
	body, err = applyGeminiInteractionsThinking(body, req.Model)
	if err != nil {
		return nil, err
	}

	originalTranslated := geminiInteractionsPayloadConfigSource(targetName, req.Payload, opts, stream)
	body = helps.ApplyPayloadConfigWithRequest(
		e.cfg,
		targetName,
		sdktranslator.FormatInteractions.String(),
		opts.SourceFormat.String(),
		"",
		body,
		originalTranslated,
		helps.PayloadRequestedModel(opts, req.Model),
		helps.PayloadRequestPath(opts),
		opts.Headers,
	)
	if stream {
		body, _ = sjson.SetBytes(body, "stream", true)
	}
	return body, nil
}

func geminiAuthLogFields(auth *cliproxyauth.Auth) (string, string, string, string) {
	if auth == nil {
		return "", "", "", ""
	}
	authType, authValue := auth.AccountInfo()
	return auth.ID, auth.Label, authType, authValue
}

func geminiInteractionsSSEPayload(frame []byte) []byte {
	trimmed := bytes.TrimSpace(frame)
	if len(trimmed) == 0 {
		return nil
	}
	if bytes.HasPrefix(trimmed, []byte("{")) {
		return trimmed
	}
	var payload []byte
	for _, line := range bytes.Split(frame, []byte{'\n'}) {
		line = bytes.TrimSpace(bytes.TrimRight(line, "\r"))
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(line[len("data:"):])
		if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
			continue
		}
		if len(payload) > 0 {
			payload = append(payload, '\n')
		}
		payload = append(payload, data...)
	}
	return payload
}

func geminiInteractionsSSEDone(frame []byte) bool {
	trimmed := bytes.TrimSpace(frame)
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		return true
	}
	for _, line := range bytes.Split(frame, []byte{'\n'}) {
		line = bytes.TrimSpace(bytes.TrimRight(line, "\r"))
		if bytes.EqualFold(line, []byte("event: done")) {
			return true
		}
		if bytes.HasPrefix(line, []byte("data:")) && bytes.Equal(bytes.TrimSpace(line[len("data:"):]), []byte("[DONE]")) {
			return true
		}
	}
	return false
}

func geminiInteractionsStreamTerminal(frame, payload []byte) bool {
	done := geminiInteractionsSSEDone(frame)
	if done || !gjson.ValidBytes(payload) {
		return done
	}
	for _, path := range []string{"event_type", "type"} {
		switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, path).String())) {
		case "interaction.completed", "finish":
			return true
		}
	}
	return false
}

func geminiInteractionsStreamProtocolError(payload []byte) error {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("[DONE]")) {
		return nil
	}
	if helps.IsJSONStreamProtocolError(payload) {
		return helps.JSONStreamProtocolError("gemini-interactions", payload)
	}
	for _, path := range []string{"event_type", "type"} {
		eventType := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, path).String()))
		switch eventType {
		case "error", "interaction.failed":
			return helps.JSONStreamProtocolError("gemini-interactions", payload)
		case "interaction.completed":
			switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "interaction.status").String())) {
			case "failed", "cancelled", "canceled", "incomplete":
				return helps.JSONStreamProtocolError("gemini-interactions", payload)
			}
		}
	}
	return nil
}

func appendGeminiInteractionsFrameLine(frame, line []byte, maxBytes int) ([]byte, error) {
	nextSize := len(frame) + len(line)
	if len(frame) > 0 {
		nextSize++
	}
	if nextSize < len(frame) || nextSize > maxBytes {
		return frame, fmt.Errorf("gemini-interactions: SSE frame exceeds %d bytes", maxBytes)
	}
	if len(frame) > 0 {
		frame = append(frame, '\n')
	}
	return append(frame, line...), nil
}

func (e *GeminiExecutor) executeInteractions(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	targetName := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewExecutorUsageReporter(ctx, e, targetName, auth)
	defer reporter.TrackFailure(ctx, &err)

	body, errBuild := e.buildInteractionsBody(req, opts, false)
	if errBuild != nil {
		return resp, errBuild
	}
	reporter.SetRequestServiceTierFromPayload(body)

	version := geminiInteractionsAPIVersion(opts)
	url := fmt.Sprintf("%s/%s/interactions", resolveGeminiBaseURL(auth), version)
	bodyReader := cliproxyexecutor.NewReleasableReadCloser(body, nil)
	httpReq, errRequest := http.NewRequestWithContext(ctx, http.MethodPost, url, bodyReader)
	if errRequest != nil {
		return resp, errRequest
	}
	httpReq.ContentLength = int64(bodyReader.Len())
	original := opts.OriginalRequest
	if len(original) == 0 {
		original = req.Payload
	}
	originalRef, bodyRef, unregisterBodies := helps.RequestBodyRefs(ctx, opts, original, body)
	defer unregisterBodies()
	defer originalRef.Release()
	defer bodyRef.Release()
	original = nil
	body = nil
	req.Payload = nil
	opts.OriginalRequest = nil

	httpReq.Header.Set("Content-Type", "application/json")
	if apiKey := geminiAPIKey(auth); apiKey != "" {
		httpReq.Header.Set("x-goog-api-key", apiKey)
	}
	applyGeminiInteractionsHeaders(httpReq, auth, opts, version)
	authID, authLabel, authType, authValue := geminiAuthLogFields(auth)
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       url,
		Method:    http.MethodPost,
		Headers:   httpReq.Header.Clone(),
		Body:      bodyRef.Bytes(),
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})

	httpClient := reporter.TrackHTTPClient(helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0))
	httpResp, errDo := httpClient.Do(httpReq)
	if errDo != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, errDo)
		return resp, errDo
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("gemini executor: close interactions response body error: %v", errClose)
		}
	}()
	responseHeaders := httpResp.Header.Clone()
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, responseHeaders)
	if httpResp.StatusCode < http.StatusOK || httpResp.StatusCode >= http.StatusMultipleChoices {
		data, errRead := io.ReadAll(httpResp.Body)
		if errRead != nil {
			helps.RecordAPIResponseError(ctx, e.cfg, errRead)
		}
		helps.AppendAPIResponseChunk(ctx, e.cfg, data)
		helps.LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", httpResp.StatusCode, helps.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), data))
		message := string(data)
		if message == "" && errRead != nil {
			message = errRead.Error()
		}
		err = statusErr{code: httpResp.StatusCode, msg: message}
		return resp, err
	}
	data, errRead := io.ReadAll(httpResp.Body)
	if errRead != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, errRead)
		return resp, errRead
	}
	helps.AppendAPIResponseChunk(ctx, e.cfg, data)

	reporter.Publish(ctx, helps.ParseInteractionsUsage(data))
	var param any
	out := sdktranslator.TranslateNonStream(ctx, sdktranslator.FormatInteractions, cliproxyexecutor.ResponseFormatOrSource(opts), req.Model, originalRef.Bytes(), bodyRef.Bytes(), data, &param)
	return cliproxyexecutor.Response{Payload: out, Headers: responseHeaders}, nil
}

func (e *GeminiExecutor) executeInteractionsStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	targetName := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewExecutorUsageReporter(ctx, e, targetName, auth)
	defer reporter.TrackFailure(ctx, &err)

	body, errBuild := e.buildInteractionsBody(req, opts, true)
	if errBuild != nil {
		return nil, errBuild
	}
	reporter.SetRequestServiceTierFromPayload(body)

	version := geminiInteractionsAPIVersion(opts)
	url := fmt.Sprintf("%s/%s/interactions", resolveGeminiBaseURL(auth), version)
	bodyReader := cliproxyexecutor.NewReleasableReadCloser(body, nil)
	httpReq, errRequest := http.NewRequestWithContext(ctx, http.MethodPost, url, bodyReader)
	if errRequest != nil {
		return nil, errRequest
	}
	httpReq.ContentLength = int64(bodyReader.Len())
	original := opts.OriginalRequest
	if len(original) == 0 {
		original = req.Payload
	}
	originalRef, bodyRef, unregisterBodies := helps.RequestBodyRefs(ctx, opts, original, body)
	cleanupBodies := func() {
		unregisterBodies()
		originalRef.Release()
		bodyRef.Release()
	}
	original = nil
	body = nil
	req.Payload = nil
	opts.OriginalRequest = nil

	httpReq.Header.Set("Content-Type", "application/json")
	if apiKey := geminiAPIKey(auth); apiKey != "" {
		httpReq.Header.Set("x-goog-api-key", apiKey)
	}
	applyGeminiInteractionsHeaders(httpReq, auth, opts, version)
	authID, authLabel, authType, authValue := geminiAuthLogFields(auth)
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       url,
		Method:    http.MethodPost,
		Headers:   httpReq.Header.Clone(),
		Body:      bodyRef.Bytes(),
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})

	httpClient := reporter.TrackHTTPClient(helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0))
	httpResp, errDo := httpClient.Do(httpReq)
	if errDo != nil {
		cleanupBodies()
		helps.RecordAPIResponseError(ctx, e.cfg, errDo)
		return nil, errDo
	}
	responseHeaders := httpResp.Header.Clone()
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, responseHeaders)
	if httpResp.StatusCode < http.StatusOK || httpResp.StatusCode >= http.StatusMultipleChoices {
		defer cleanupBodies()
		data, errRead := io.ReadAll(httpResp.Body)
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("gemini executor: close interactions error response body error: %v", errClose)
		}
		if errRead != nil {
			helps.RecordAPIResponseError(ctx, e.cfg, errRead)
		}
		helps.AppendAPIResponseChunk(ctx, e.cfg, data)
		helps.LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", httpResp.StatusCode, helps.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), data))
		message := string(data)
		if message == "" && errRead != nil {
			message = errRead.Error()
		}
		return nil, statusErr{code: httpResp.StatusCode, msg: message}
	}

	helps.ReleaseRequestBodyAfterStreamEstablished(ctx, opts)
	out := make(chan cliproxyexecutor.StreamChunk)
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	go e.consumeInteractionsStream(ctx, httpResp.Body, req.Model, responseFormat, opts, originalRef, bodyRef, reporter, cleanupBodies, out)
	return &cliproxyexecutor.StreamResult{Headers: responseHeaders, Chunks: out}, nil
}

func (e *GeminiExecutor) consumeInteractionsStream(
	ctx context.Context,
	responseBody io.ReadCloser,
	model string,
	responseFormat sdktranslator.Format,
	opts cliproxyexecutor.Options,
	originalRef, bodyRef *cliproxyexecutor.ReleasableBytes,
	reporter *helps.UsageReporter,
	cleanupBodies func(),
	out chan<- cliproxyexecutor.StreamChunk,
) {
	defer close(out)
	defer cleanupBodies()
	defer func() {
		if errClose := responseBody.Close(); errClose != nil {
			log.Errorf("gemini executor: close interactions stream body error: %v", errClose)
		}
	}()

	send := func(chunk cliproxyexecutor.StreamChunk) bool {
		select {
		case out <- chunk:
			return true
		case <-ctx.Done():
			return false
		}
	}
	scanner := bufio.NewScanner(responseBody)
	scanner.Buffer(nil, streamScannerBuffer)
	markerRequested := metadataBool(opts.Metadata, cliproxyexecutor.StreamTerminalMarkerMetadataKey)
	var param any
	var frame []byte
	var protocolErr error
	terminalSeen := false
	doneSeen := false

	emitFrame := func() bool {
		rawFrame := bytes.Clone(frame)
		frame = frame[:0]
		trimmed := bytes.TrimSpace(rawFrame)
		if len(trimmed) == 0 {
			return true
		}
		payload := geminiInteractionsSSEPayload(rawFrame)
		if len(payload) == 0 && geminiInteractionsSSEDone(rawFrame) {
			payload = []byte("[DONE]")
		}
		if protocolErr = geminiInteractionsStreamProtocolError(payload); protocolErr != nil {
			return false
		}
		if detail, ok := helps.ParseInteractionsStreamUsage(payload); ok {
			reporter.Observe(detail)
		}
		terminalSeen = terminalSeen || geminiInteractionsStreamTerminal(rawFrame, payload)
		doneSeen = doneSeen || geminiInteractionsSSEDone(rawFrame)

		if responseFormat == sdktranslator.FormatInteractions {
			visibleFrame := append(bytes.TrimRight(rawFrame, "\r\n"), '\n', '\n')
			return send(cliproxyexecutor.StreamChunk{Payload: visibleFrame})
		}
		if len(payload) == 0 {
			return true
		}
		lines := sdktranslator.TranslateStream(ctx, sdktranslator.FormatInteractions, responseFormat, model, originalRef.Bytes(), bodyRef.Bytes(), bytes.Clone(payload), &param)
		for i := range lines {
			if !send(cliproxyexecutor.StreamChunk{Payload: lines[i]}) {
				return false
			}
		}
		return true
	}

	aborted := false
	for scanner.Scan() {
		line := bytes.Clone(scanner.Bytes())
		helps.AppendAPIResponseChunk(ctx, e.cfg, line)
		if len(bytes.TrimSpace(line)) == 0 {
			if !emitFrame() {
				aborted = true
				break
			}
			if doneSeen {
				break
			}
			continue
		}
		var errFrame error
		frame, errFrame = appendGeminiInteractionsFrameLine(frame, line, streamScannerBuffer)
		if errFrame != nil {
			protocolErr = errFrame
			aborted = true
			break
		}
	}
	errScan := scanner.Err()
	if !aborted && !doneSeen && errScan == nil && len(bytes.TrimSpace(frame)) > 0 {
		aborted = !emitFrame()
	}

	if protocolErr != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, protocolErr)
		reporter.PublishFailure(ctx, protocolErr)
		_ = send(cliproxyexecutor.StreamChunk{Err: protocolErr})
		return
	}
	if errContext := ctx.Err(); aborted && errContext != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, errContext)
		reporter.PublishFailure(ctx, errContext)
		return
	}
	if errScan != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, errScan)
		reporter.PublishFailure(ctx, errScan)
		_ = send(cliproxyexecutor.StreamChunk{Err: errScan})
		return
	}
	if !terminalSeen {
		streamErr := helps.IncompleteStreamError("gemini-interactions")
		helps.RecordAPIResponseError(ctx, e.cfg, streamErr)
		reporter.PublishFailure(ctx, streamErr)
		_ = send(cliproxyexecutor.StreamChunk{Err: streamErr})
		return
	}

	reporter.EnsurePublished(ctx)
	if markerRequested {
		_ = send(cliproxyexecutor.SuccessfulStreamTerminalChunk())
	}
}
