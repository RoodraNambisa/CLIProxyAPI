package live

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

// HandleCall creates a native OAuth WebRTC call. Admission is committed only
// when its selected credential instance and owner are registered locally.
func (h *Handler) HandleCall(c *gin.Context) {
	r := h.begin(c)
	if r == nil {
		return
	}
	defer r.finish()
	if r.mediaError != nil {
		r.fail(c, r.mediaError, nil, http.StatusServiceUnavailable, "Realtime media configuration is unavailable", "server_error", "realtime_media_unavailable", nil, nil)
		return
	}
	body, errRead := readCallBody(c.Request.Body)
	if errRead != nil {
		status := http.StatusBadRequest
		if errors.Is(errRead, errCallBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		r.fail(c, errRead, nil, status, "Failed to read realtime call request; maximum size is 16 MiB", "invalid_request_error", "invalid_realtime_request", nil, nil)
		return
	}
	payload, errPayload := prepareCallPayload(body, c.GetHeader("Content-Type"))
	if errPayload != nil {
		status := http.StatusBadRequest
		if errors.Is(errPayload, errCallBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		r.fail(c, errPayload, nil, status, errPayload.Error(), "invalid_request_error", "invalid_realtime_request", nil, nil)
		return
	}
	owner, _ := requestCallOwner(c)
	secretPrincipal := ""
	if grant, temporary := clientSecretAuthorization(c); temporary {
		payload, errPayload = payload.withClientSecret(grant)
		if errPayload != nil {
			status := http.StatusBadRequest
			if errors.Is(errPayload, errCallBodyTooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
			r.fail(c, errPayload, nil, status, "Failed to apply realtime credential session", "invalid_request_error", "invalid_realtime_request", nil, nil)
			return
		}
		secretPrincipal = grant.principal
	}
	clientOffer := ""
	if r.media != nil {
		clientOffer, errPayload = payload.mediaOffer()
		if errPayload != nil {
			r.fail(c, errPayload, nil, http.StatusBadRequest, errPayload.Error(), "invalid_request_error", "invalid_realtime_request", nil, nil)
			return
		}
	}
	// Copy only immutable accounting metadata. Never detach the entire request
	// with WithoutCancel, which would retain Gin and the request body for an hour.
	lifetime := h.root
	if metadata, ok := usage.RequestMetadataFromContext(r.ctx); ok {
		lifetime = usage.WithRequestMetadata(lifetime, metadata)
	}
	lease, errAcquire := h.manager.AcquireCodexLiveSession(r.ctx, lifetime, payload.model, core.Options{Headers: c.Request.Header.Clone()})
	if errAcquire != nil {
		r.fail(c, errAcquire, nil, realtimeErrorStatus(errAcquire, http.StatusServiceUnavailable), "No available Codex realtime credential for this model", "server_error", "codex_auth_unavailable", nil, nil)
		return
	}
	retained, allocatedID := false, ""
	var media *pionMediaSession
	defer func() {
		if !retained {
			_ = media.Close()
			if allocatedID != "" {
				h.rollbackCall(lease, allocatedID)
			} else {
				lease.Close()
			}
		}
	}()
	payload, errPayload = payload.withModel(lease.Model())
	if errPayload != nil {
		status := http.StatusBadRequest
		if errors.Is(errPayload, errCallBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		r.fail(c, errPayload, nil, status, "Failed to encode realtime call request", "invalid_request_error", "invalid_realtime_request", nil, nil)
		return
	}
	if r.media != nil {
		proxyURL := lease.CloneAuth().EffectiveProxyURL()
		if proxyURL == "" {
			proxyURL = r.mediaProxyURL
		}
		var offer string
		var errMedia error
		media, offer, errMedia = r.media.NewSession(r.ctx, lease.Context(), clientOffer, proxyURL)
		if errMedia != nil {
			status, kind, code := http.StatusServiceUnavailable, "server_error", "realtime_media_unavailable"
			if errors.Is(errMedia, errMediaInvalidOffer) {
				status, kind, code = http.StatusBadRequest, "invalid_request_error", "invalid_realtime_request"
			}
			if errors.Is(errMedia, errMediaCapacity) {
				code = "realtime_media_capacity"
			}
			r.fail(c, errMedia, nil, status, "Realtime media setup failed", kind, code, nil, nil)
			return
		}
		payload, errPayload = payload.withMediaOffer(offer)
		if errPayload != nil {
			status := http.StatusInternalServerError
			if errors.Is(errPayload, errCallBodyTooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
			r.fail(c, errPayload, nil, status, "Failed to encode realtime media offer", "server_error", "realtime_media_unavailable", nil, nil)
			return
		}
	}
	headers := realtimeCallHeaders(c.Request.Header, lease)
	headers.Set("Content-Type", payload.contentType)
	request, errRequest := http.NewRequestWithContext(r.ctx, http.MethodPost, h.callURL, bytes.NewReader(payload.body))
	if errRequest != nil {
		r.fail(c, errRequest, nil, http.StatusInternalServerError, "Realtime call target is unavailable", "server_error", "realtime_setup_unavailable", nil, nil)
		return
	}
	request.Header = headers
	if errActive := r.active(); errActive != nil {
		r.fail(c, errActive, nil, http.StatusServiceUnavailable, "Realtime setup is unavailable", "server_error", "realtime_setup_unavailable", nil, nil)
		return
	}
	if errCommit := lease.CommitUpstream(); errCommit != nil {
		r.fail(c, errCommit, lease, http.StatusServiceUnavailable, "Realtime credential is unavailable", "server_error", "codex_auth_unavailable", nil, nil)
		return
	}
	response, errRequest := lease.HttpRequest(request)
	if response != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
		allocatedID = callIDFromLocation(response.Header.Get("Location"))
	}
	if errRequest != nil || response == nil || response.Body == nil {
		closeRealtimeResponse(response)
		r.fail(c, errRequest, lease, realtimeErrorStatus(errRequest, http.StatusBadGateway), "Realtime call upstream is unavailable", "api_error", "realtime_upstream_unavailable", nil, nil)
		return
	}
	responseBody, errResponse := readCallBody(response.Body)
	closeRealtimeResponse(response)
	if errResponse != nil {
		r.fail(c, errResponse, lease, http.StatusBadGateway, "Failed to read realtime call response", "api_error", "realtime_upstream_unavailable", nil, nil)
		return
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		r.fail(c, nil, lease, response.StatusCode, "Realtime call was rejected by the upstream", "api_error", "realtime_upstream_unavailable", responseBody, response.Header)
		return
	}
	if allocatedID == "" || len(bytes.TrimSpace(responseBody)) == 0 {
		r.fail(c, nil, lease, http.StatusBadGateway, "Realtime response is missing an answer or valid call ID", "api_error", "invalid_realtime_response", nil, nil)
		return
	}
	if media != nil {
		answer, errMedia := media.AcceptUpstreamAnswer(r.ctx, string(responseBody))
		if errMedia != nil {
			r.fail(c, errMedia, lease, http.StatusBadGateway, "Realtime upstream media answer is unavailable", "api_error", "invalid_realtime_response", nil, nil)
			return
		}
		responseBody = []byte(answer)
	}
	call := &liveCall{id: allocatedID, owner: owner, lease: lease, secretPrincipal: secretPrincipal}
	publish := func() error { return h.calls.put(call) }
	if media != nil {
		call.onClose = func() { _ = media.Close() }
		publish = func() error { return media.Commit(func() error { return h.calls.put(call) }) }
	}
	if errCommit := r.commitWith(publish); errCommit != nil {
		if errors.Is(errCommit, errCallIDConflict) {
			// An upstream duplicate must not hang up a previously established call.
			allocatedID = ""
		}
		r.fail(c, errCommit, nil, http.StatusServiceUnavailable, "Realtime call could not be registered", "server_error", "realtime_setup_unavailable", nil, nil)
		return
	}
	retained = true
	if media != nil {
		// Installing after publication also delivers a failure that won the race
		// immediately after Commit. Cleanup owns only this exact stored call.
		media.SetCloseHandler(func(error) {
			if h.calls.detach(call) {
				h.rollbackCall(call.lease, call.id)
			}
		})
	}
	if handlers.PassthroughHeadersEnabled(r.base.Cfg) {
		headers := handlers.FilterUpstreamHeaders(response.Header)
		headers.Del("Location")
		handlers.WriteUpstreamHeaders(c.Writer.Header(), headers)
	}
	location := "/v1/realtime/calls/" + call.id
	if c.Request.URL.Path == "/v1/live" {
		location = "/v1/live/" + call.id
	}
	c.Header("Location", location)
	contentType := response.Header.Get("Content-Type")
	if contentType == "" || media != nil {
		contentType = "application/sdp"
	}
	c.Header("Content-Type", contentType)
	c.Status(response.StatusCode)
	if _, errWrite := c.Writer.Write(responseBody); errWrite != nil && h.calls.detach(call) {
		retained = false
	}
}

func realtimeCallHeaders(source http.Header, lease *auth.CodexLiveLease) http.Header {
	headers := realtimeProtocolHeaders(source)
	if alpha := source.Get("OpenAI-Alpha"); alpha != "" {
		headers.Set("OpenAI-Alpha", alpha)
	}
	if account, ok := lease.CloneAuth().Metadata["account_id"].(string); ok && strings.TrimSpace(account) != "" {
		headers.Set("Chatgpt-Account-Id", strings.TrimSpace(account))
	}
	return headers
}

// rollbackCall owns the lease until cleanup finishes. It keeps no Gin context,
// obeys process/credential cancellation, and never holds a configuration lock.
func (h *Handler) rollbackCall(lease *auth.CodexLiveLease, id string) {
	go func() {
		defer lease.Close()
		status, _, _, errHangup := h.hangupCall(h.root, lease, id, nil, nil)
		if errHangup != nil || status < 200 || status >= 300 {
			log.WithField("status", status).Debug("codex realtime: allocated call rollback did not complete")
		}
	}()
}

func (h *Handler) hangupCall(ctx context.Context, lease *auth.CodexLiveLease, id string, body []byte, headers http.Header) (int, []byte, http.Header, error) {
	base := strings.TrimRight(h.websocketBaseURL, "/")
	base = strings.Replace(base, "wss://", "https://", 1)
	base = strings.Replace(base, "ws://", "http://", 1)
	request, errRequest := http.NewRequestWithContext(ctx, http.MethodPost, base+"/realtime/calls/"+id+"/hangup", bytes.NewReader(body))
	if errRequest != nil {
		return 0, nil, nil, errRequest
	}
	request.Header = realtimeCallHeaders(headers, lease)
	if kind := headers.Get("Content-Type"); kind != "" {
		request.Header.Set("Content-Type", kind)
	}
	response, errRequest := lease.HttpRequest(request)
	if errRequest != nil {
		closeRealtimeResponse(response)
		return 0, nil, nil, errRequest
	}
	if response == nil || response.Body == nil {
		return 0, nil, nil, errors.New("realtime hangup returned no response")
	}
	defer closeRealtimeResponse(response)
	payload, errRead := readCallBody(response.Body)
	return response.StatusCode, payload, response.Header, errRead
}
