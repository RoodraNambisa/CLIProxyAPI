package auth

import (
	"errors"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func isResponsesCompactAvailabilityNeutralError(opts executor.Options, err error) bool {
	if opts.Alt != "responses/compact" || err == nil {
		return false
	}
	if isKnownRequestFault(err) {
		return true
	}
	if isInvalidGrantError(err) {
		return false
	}
	switch statusCodeFromError(err) {
	case http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusTooManyRequests:
		return false
	}
	body := err.Error()
	var source *Error
	if errors.As(err, &source) {
		body = source.Message
		if compactCredentialFaultCode(source.Code) {
			return false
		}
	}
	var headerError interface{ Headers() http.Header }
	if errors.As(err, &headerError) && strings.EqualFold(headerError.Headers().Get("Cf-Mitigated"), "challenge") {
		return false
	}
	for _, path := range []string{"error.type", "error.code", "type", "code", "response.error.type", "response.error.code"} {
		value := strings.ToLower(gjson.Get(body, path).String())
		if compactCredentialFaultCode(value) {
			return false
		}
	}
	return true
}

func compactCredentialFaultCode(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "authentication_error", "invalid_api_key", "invalid_access_token", "token_expired", "token_revoked", "account_deactivated", "cloudflare_challenge":
		return true
	}
	return false
}

func isResponsesCompactRequestFaultError(opts executor.Options, err error) bool {
	if !isResponsesCompactAvailabilityNeutralError(opts, err) {
		return false
	}
	if isKnownRequestFault(err) {
		return true
	}
	switch statusCodeFromError(err) {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusConflict, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity, http.StatusNotImplemented:
		return true
	}
	return false
}
