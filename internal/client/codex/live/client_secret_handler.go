package live

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

const clientSecretContextKey = "codexLiveClientSecretAuthorization"

func (h *Handler) CreateClientSecret(c *gin.Context) { h.createClientSecret(c, false) }

func (h *Handler) CreateLegacySession(c *gin.Context) { h.createClientSecret(c, true) }

func (h *Handler) createClientSecret(c *gin.Context, legacy bool) {
	// These are logging-only overrides understood by the existing response
	// wrapper. They never modify the actual request or credential response.
	c.Set("REQUEST_BODY_OVERRIDE", []byte(`{"redacted":"realtime credential request"}`))
	if _, temporary := c.Get(clientSecretContextKey); temporary {
		writeRealtimeError(c, http.StatusForbidden, "Creating realtime credentials requires a standard API key", "authentication_error", "realtime_standard_auth_required")
		return
	}
	r := h.begin(c)
	if r == nil {
		return
	}
	defer r.finish()
	body, errRead := readClientSecretBody(c.Request.Body)
	if errRead != nil {
		status := http.StatusBadRequest
		if errors.Is(errRead, errClientSecretBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		r.fail(c, errRead, nil, status, "Failed to read realtime credential request; maximum size is 64 KiB", "invalid_request_error", "invalid_realtime_request", nil, nil)
		return
	}
	request, errParse := parseClientSecretRequest(body, legacy)
	if errParse != nil {
		status, kind, code := http.StatusBadRequest, "invalid_request_error", "invalid_realtime_request"
		if errors.Is(errParse, errUnsupportedRealtimeSession) {
			status, kind, code = http.StatusNotImplemented, "not_supported_error", "realtime_capability_not_supported"
		}
		if errors.Is(errParse, errClientSecretBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		r.fail(c, errParse, nil, status, errParse.Error(), kind, code, nil, nil)
		return
	}
	owner, _ := requestCallOwner(c)
	for attempt := 0; attempt < 4; attempt++ {
		if errActive := r.active(); errActive != nil {
			r.fail(c, errActive, nil, http.StatusServiceUnavailable, "Realtime credential setup is unavailable", "server_error", "realtime_setup_unavailable", nil, nil)
			return
		}
		token, authorization, errPrepare := prepareClientSecret(h.secretRandom, h.clientSecrets.now(), request.session, request.model, owner, request.lifetime)
		if errPrepare != nil {
			r.fail(c, errPrepare, nil, http.StatusInternalServerError, "Failed to create realtime credential", "server_error", "realtime_client_secret_failed", nil, nil)
			return
		}
		session, errSession := clientSecretSessionResponse(authorization)
		if errSession != nil {
			r.fail(c, errSession, nil, http.StatusInternalServerError, "Failed to encode realtime session", "server_error", "realtime_session_failed", nil, nil)
			return
		}
		payload, errPayload := encodeClientSecretResponse(token, authorization, session, legacy)
		if errPayload != nil {
			r.fail(c, errPayload, nil, http.StatusInternalServerError, "Failed to encode realtime credential", "server_error", "realtime_session_failed", nil, nil)
			return
		}
		if errCommit := r.commitWith(func() error { return h.clientSecrets.put(token, authorization) }); errCommit != nil {
			if errors.Is(errCommit, errClientSecretCollision) {
				continue
			}
			status, kind, code := http.StatusServiceUnavailable, "server_error", "realtime_setup_unavailable"
			if errors.Is(errCommit, errClientSecretCapacity) {
				status, kind, code = http.StatusTooManyRequests, "rate_limit_error", "realtime_client_secret_capacity_exhausted"
				c.Header("Retry-After", "1")
			}
			r.fail(c, errCommit, nil, status, "Realtime credential could not be issued", kind, code, nil, nil)
			return
		}
		c.Set("RESPONSE_BODY_OVERRIDE", []byte(`{"redacted":"realtime credential response"}`))
		c.Header("Cache-Control", "no-store")
		c.Header("Content-Type", "application/json")
		c.Status(http.StatusOK)
		if _, errWrite := c.Writer.Write(payload); errWrite != nil {
			h.clientSecrets.remove(token, authorization.principal)
		}
		return
	}
	r.fail(c, errClientSecretCollision, nil, http.StatusInternalServerError, "Failed to create unique realtime credential", "server_error", "realtime_client_secret_failed", nil, nil)
}

func encodeClientSecretResponse(token string, a ClientSecretAuthorization, session json.RawMessage, legacy bool) ([]byte, error) {
	secret := struct {
		Value     string `json:"value"`
		ExpiresAt int64  `json:"expires_at"`
	}{token, a.expiresAt.Unix()}
	if legacy {
		object, errObject := callJSONObject(session, "session")
		if errObject != nil {
			return nil, errObject
		}
		object["client_secret"], _ = json.Marshal(secret)
		return json.Marshal(object)
	}
	return json.Marshal(struct {
		Value     string          `json:"value"`
		ExpiresAt int64           `json:"expires_at"`
		Session   json.RawMessage `json:"session"`
	}{token, a.expiresAt.Unix(), session})
}
