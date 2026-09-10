package auth

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

// SnapshotRequestErrorRetryPolicy reuses logical request rules for outer stream
// recovery, or captures current rules when no request context is provided.
func (m *Manager) SnapshotRequestErrorRetryPolicy(contexts ...context.Context) func(error) bool {
	rules := slices.Clone(m.requestNonRetryableErrorRules(contexts...))
	return func(err error) bool { return shouldRetryRequestRound(err, rules) }
}

// isKnownRequestFault recognizes structured request failures independently of
// transport status. Quota, payment, and authentication evidence takes priority.
// Image-specific configurable rules remain in non-retryable-errors.
func isKnownRequestFault(err error) bool {
	return matchesKnownRequestFault(err, false)
}

// IsRequestFaultError shares request-fault classification with transport error delivery.
// Authentication and quota evidence retain the same precedence as credential selection.
func IsRequestFaultError(err error) bool {
	return isKnownRequestFault(err)
}

// IsPolicyRefusalError allows executors to preserve structured policy failures
// before transport-level status normalization can erase their original meaning.
func IsPolicyRefusalError(err error) bool {
	return matchesKnownRequestFault(err, true)
}

func matchesKnownRequestFault(err error, policyOnly bool) bool {
	if err == nil || isInvalidGrantError(err) || isModelSupportError(err) {
		return false
	}
	status := statusCodeFromError(err)
	if status == http.StatusPaymentRequired || status == http.StatusTooManyRequests {
		return false
	}
	var codes, types []string
	var source *Error
	body := strings.TrimSpace(err.Error())
	if errors.As(err, &source) && source != nil {
		codes = append(codes, strings.ToLower(strings.TrimSpace(source.Code)))
		body = strings.TrimSpace(source.Message)
	}
	message := body
	// Error.Error and fmt.Errorf wrappers can prefix a plain message with a code.
	// Inspect only complete prefix tokens, never arbitrary words inside the message.
	for rest := body; rest != ""; {
		prefix, remaining, ok := strings.Cut(rest, ": ")
		if !ok || strings.ContainsAny(prefix, "{}\"\r\n") {
			break
		}
		if !strings.ContainsAny(prefix, " \t") {
			codes = append(codes, strings.ToLower(prefix))
		}
		rest = remaining
	}
	// Manager's error wrapper prepends a diagnostic code to the original JSON.
	if offset := strings.IndexByte(body, '{'); offset > 0 {
		body = body[offset:]
	}
	if gjson.Valid(body) {
		for _, prefix := range []string{"error.", "", "response.error.", "body.error."} {
			codes = append(codes, strings.ToLower(strings.TrimSpace(gjson.Get(body, prefix+"code").String())))
			types = append(types, strings.ToLower(strings.TrimSpace(gjson.Get(body, prefix+"type").String())))
		}
	}
	imageUserError := false
	for _, value := range types {
		if value == "authentication_error" {
			return false
		}
		imageUserError = imageUserError || value == "image_generation_user_error"
	}
	for _, code := range codes {
		switch code {
		case "authentication_error", "unauthorized", "invalid_grant", "invalid_api_key", "invalid_access_token", "token_expired", "token_revoked", "account_deactivated":
			return false
		}
	}
	for _, code := range codes {
		switch code {
		case "misalignment_policy_violation", "cyber_policy", "content_policy_violation":
			return true
		}
	}
	for _, value := range types {
		switch value {
		case "misalignment_policy_violation", "cyber_policy", "content_policy_violation":
			return true
		}
	}
	if policyOnly {
		return false
	}
	if isRequestScopedNotFoundMessage(message) {
		return true
	}
	if imageUserError {
		return false
	}
	for _, code := range codes {
		switch code {
		case "context_length_exceeded", "message_too_big", "string_above_max_length",
			"invalid_prompt", "invalid_value", "unsupported_value", "invalid_request_error", "previous_response_not_found":
			return true
		}
	}
	for _, value := range types {
		switch value {
		case "invalid_request", "invalid_request_error", "bad_request_error", "invalid_prompt":
			return true
		}
	}
	return false
}

// isConnectionLifecycleFailure never infers credential health from a response
// carrying an HTTP status. Disconnects remain distinct from non-retryable input.
func isConnectionLifecycleFailure(err error) bool {
	if err == nil || statusCodeFromError(err) != 0 {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) && closeErr != nil {
		return closeErr.Code == websocket.CloseNormalClosure || closeErr.Code == websocket.CloseGoingAway || closeErr.Code == websocket.CloseAbnormalClosure
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch message {
	case "context canceled", "context deadline exceeded", "eof", "unexpected eof":
		return true
	}
	for _, prefix := range []string{"websocket: close 1000 ", "websocket: close 1001 ", "websocket: close 1006 "} {
		if strings.Contains(message, prefix) {
			return true
		}
	}
	return strings.HasSuffix(message, ": unexpected eof") || strings.HasSuffix(message, ": eof") ||
		strings.HasSuffix(message, ": context canceled") || strings.HasSuffix(message, ": context deadline exceeded")
}

func isCredentialNeutralFailure(err *Error) bool {
	if err == nil || isInvalidGrantResultError(err) {
		return false
	}
	if requestScopedActionSuppressesCooldown(err) {
		return true
	}
	if err.HTTPStatus == http.StatusPaymentRequired || err.HTTPStatus == http.StatusTooManyRequests {
		return false
	}
	return isKnownRequestFault(err) || isConnectionLifecycleFailure(err) || isRequestScopedNotFoundResultError(err)
}
