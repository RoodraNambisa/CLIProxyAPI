package live

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type realtimePublicError struct {
	status int
	body   string
	cause  error
}

func (e *realtimePublicError) Error() string   { return e.body }
func (e *realtimePublicError) StatusCode() int { return e.status }
func (e *realtimePublicError) Unwrap() error   { return e.cause }

func (r *liveRequest) fail(c *gin.Context, cause error, lease *auth.CodexLiveLease, status int, message, kind, code string, body []byte, headers http.Header) {
	if errActive := r.active(); errActive != nil {
		cause = errActive
	}
	if errors.Is(cause, errLiveDisabled) {
		writeRealtimeError(c, http.StatusServiceUnavailable, errLiveDisabled.Error(), "server_error", liveDisabledCode)
		return
	}
	if errors.Is(cause, errInvalidClientSecret) {
		writeRealtimeError(c, http.StatusUnauthorized, errInvalidClientSecret.Error(), "authentication_error", "invalid_realtime_client_secret")
		return
	}
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		status, message, kind, code, body = 499, "Realtime request was cancelled", "invalid_request_error", "realtime_request_cancelled", nil
		if errors.Is(cause, context.DeadlineExceeded) {
			status = http.StatusRequestTimeout
		}
	}
	if len(body) == 0 {
		body, _ = json.Marshal(gin.H{"error": gin.H{"message": message, "type": kind, "code": code}})
	}
	var public error = &realtimePublicError{status: status, body: string(body), cause: cause}
	if lease != nil {
		public = lease.WithErrorSource(public)
	}
	errResponse := &interfaces.ErrorMessage{StatusCode: status, Error: public, Addon: handlers.FilterUpstreamHeaders(headers)}
	r.base.WriteErrorResponse(c, r.base.RewriteExecutionErrorResponseForContext(r.ctx, errResponse))
}

func realtimeErrorStatus(err error, fallback int) int {
	var status interface{ StatusCode() int }
	if errors.As(err, &status) && status.StatusCode() >= 400 && status.StatusCode() <= 599 {
		return status.StatusCode()
	}
	return fallback
}
