package live

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
)

// AuthenticateClientSecret checks only local credentials. Callers should try
// configured API-key authentication first so existing ek_-prefixed keys work.
func (h *Handler) AuthenticateClientSecret(request *http.Request) (ClientSecretAuthorization, bool, error) {
	token, handled, errToken := realtimeClientSecretToken(request)
	if !handled || errToken != nil {
		return ClientSecretAuthorization{}, handled, errToken
	}
	if h == nil || h.clientSecrets == nil {
		return ClientSecretAuthorization{}, true, errInvalidClientSecret
	}
	if errCtx := context.Cause(request.Context()); errCtx != nil {
		return ClientSecretAuthorization{}, true, errCtx
	}
	h.gate.mu.Lock()
	enabled := h.gate.enabled
	h.gate.mu.Unlock()
	if !enabled {
		return ClientSecretAuthorization{}, true, errLiveDisabled
	}
	authorization, errAuth := h.clientSecrets.authenticate(token)
	return authorization, true, errAuth
}

// ApplyClientSecretAuthorization installs only local scope, never the token.
// New requests and their final admission commit will validate the grant again.
func (h *Handler) ApplyClientSecretAuthorization(c *gin.Context, authorization ClientSecretAuthorization) bool {
	if h == nil || h.clientSecrets == nil || !h.clientSecrets.valid(authorization) {
		h.WriteClientSecretError(c, errInvalidClientSecret)
		return false
	}
	c.Set(clientSecretContextKey, authorization.clone())
	c.Set("apiKey", authorization.principal)
	c.Set("accessProvider", "codex-live-client-secret")
	c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: "codex"})
	return true
}

func clientSecretAuthorization(c *gin.Context) (ClientSecretAuthorization, bool) {
	value, exists := c.Get(clientSecretContextKey)
	if !exists {
		return ClientSecretAuthorization{}, false
	}
	a, ok := value.(ClientSecretAuthorization)
	return a, ok
}

func realtimeClientSecretToken(request *http.Request) (string, bool, error) {
	if request == nil {
		return "", false, nil
	}
	token := ""
	handled := false
	otherAuthorization := false
	for _, header := range request.Header.Values("Authorization") {
		fields := strings.Fields(header)
		if len(fields) == 2 && strings.EqualFold(fields[0], "Bearer") && strings.HasPrefix(fields[1], clientSecretPrefix) {
			handled = true
			if token != "" && token != fields[1] {
				return "", true, errInvalidClientSecret
			}
			token = fields[1]
		} else if strings.TrimSpace(header) != "" {
			otherAuthorization = true
		}
	}
	if request.Method == http.MethodGet && websocket.IsWebSocketUpgrade(request) {
		for _, protocol := range websocket.Subprotocols(request) {
			const prefix = "openai-insecure-api-key."
			if !strings.HasPrefix(protocol, prefix) {
				continue
			}
			handled = true
			candidate := strings.TrimPrefix(protocol, prefix)
			if !validClientSecretToken(candidate) {
				return "", true, errInvalidClientSecret
			}
			if token != "" && token != candidate {
				return "", true, errInvalidClientSecret
			}
			token = candidate
		}
	}
	if handled && (otherAuthorization || !validClientSecretToken(token)) {
		return "", true, errInvalidClientSecret
	}
	return token, handled, nil
}

// WriteClientSecretError terminates an authentication middleware chain without
// logging the presented token or interpreting failure as an upstream error.
func (*Handler) WriteClientSecretError(c *gin.Context, err error) {
	status, message, kind, code := http.StatusUnauthorized, errInvalidClientSecret.Error(), "authentication_error", "invalid_realtime_client_secret"
	if errors.Is(err, errLiveDisabled) {
		status, message, kind, code = http.StatusServiceUnavailable, errLiveDisabled.Error(), "server_error", liveDisabledCode
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		status, message, kind, code = 499, "Realtime request was cancelled", "invalid_request_error", "realtime_request_cancelled"
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusRequestTimeout
		}
	}
	c.Abort()
	writeRealtimeError(c, status, message, kind, code)
}
