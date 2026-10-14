package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

func (e *XAIExecutor) executeSpeech(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	model := strings.TrimSpace(gjson.GetBytes(req.Payload, "model").String())
	if model == "" {
		model = strings.TrimSpace(req.Model)
	}
	reporter := helps.NewExecutorUsageReporter(ctx, e, model, auth)
	defer reporter.TrackFailure(ctx, &err)

	token, _ := xaiCreds(auth)
	baseURL, errBase := helps.XAIModelAPIOnlyBaseURL(auth, e.cfg, req.Model)
	if errBase != nil {
		return resp, errBase
	}
	requestURL := strings.TrimSuffix(baseURL, "/") + "/tts"

	// Payload rules target the "openai" protocol, matching xAI image/video and config.example.yaml.
	payload := helps.ApplyPayloadConfigWithRequest(e.cfg, model, "openai", opts.SourceFormat.String(), "", req.Payload, req.Payload, helps.PayloadRequestedModel(opts, req.Model), helps.PayloadRequestPath(opts), opts.Headers)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(payload))
	if err != nil {
		return resp, err
	}
	applyXAIHeaders(httpReq, auth, token, false, "", e.cfg)
	// Official TTS returns raw audio. The media header helper asks for JSON.
	httpReq.Header.Set("Accept", "*/*")
	e.recordXAIRequest(ctx, auth, requestURL, httpReq.Method, httpReq.Header.Clone(), payload)

	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	httpResp, err := executeXAIHTTPRequest(httpClient, httpReq, auth, opts, e.cfg)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("xai executor: close response body error: %v", errClose)
		}
	}()
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())

	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	helps.AppendAPIResponseChunk(ctx, e.cfg, data)

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		helps.LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", httpResp.StatusCode, helps.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), data))
		errStatus := xaiStatusErr(httpResp.StatusCode, data)
		if httpResp.StatusCode == http.StatusNotFound && !helps.XAISpeechModelUnavailable(data) {
			errStatus.skipAuthResult = true
		}
		err = errStatus
		return resp, err
	}

	reporter.EnsurePublished(ctx)
	return cliproxyexecutor.Response{Payload: data, Headers: httpResp.Header.Clone()}, nil
}
