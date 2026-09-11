package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

const notFoundCooldown = 10 * time.Minute

// IsModelNotFoundError recognizes model availability failures without treating
// caller input, policy refusals, authentication or quota failures as missing models.
func IsModelNotFoundError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch statusCodeFromError(err) {
	case http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden,
		http.StatusTooManyRequests, http.StatusRequestEntityTooLarge:
		return false
	}
	body := strings.TrimSpace(err.Error())
	var source *Error
	var identifiers, messages []string
	if errors.As(err, &source) && source != nil {
		identifiers = append(identifiers, source.Code)
		body = strings.TrimSpace(source.Message)
	}
	if offset := strings.IndexByte(body, '{'); offset > 0 {
		body = body[offset:]
	}
	if gjson.Valid(body) {
		for _, prefix := range []string{"error.", "", "response.error.", "body.error."} {
			identifiers = append(identifiers, gjson.Get(body, prefix+"code").String(), gjson.Get(body, prefix+"type").String())
			messages = append(messages, gjson.Get(body, prefix+"message").String())
		}
	} else {
		messages = append(messages, body)
	}
	matched := false
	for _, identifier := range identifiers {
		switch strings.ToLower(strings.TrimSpace(identifier)) {
		case "authentication_error", "invalid_api_key", "invalid_access_token", "invalid_grant",
			"token_expired", "token_revoked", "account_deactivated", "permission_error", "invalid_task_id", "task_expired", "task_not_found",
			"misalignment_policy_violation", "cyber_policy", "content_policy_violation",
			"usage_limit_reached", "rate_limit_error", "rate_limit_exceeded", "insufficient_quota",
			"context_length_exceeded", "context_too_large", "previous_response_not_found", "invalid_value",
			"invalid_prompt", "invalid_signature", "invalid_encrypted_content", "thinking_signature_invalid":
			return false
		case "model_not_found", "model_not_found_error", "unknown_model", "model_does_not_exist":
			matched = true
		}
	}
	if matched {
		return true
	}
	for _, message := range messages {
		message = strings.Trim(strings.ToLower(strings.TrimSpace(message)), " .!;\t\r\n")
		if strings.Contains(message, "in request") || strings.Contains(message, "in body") || strings.Contains(message, "request body") {
			continue
		}
		for _, phrase := range []string{"model not found", "unknown model", "no such model"} {
			if message == phrase || strings.HasPrefix(message, phrase+" ") || strings.HasPrefix(message, phrase+":") {
				return true
			}
		}
		if strings.HasPrefix(message, "the model ") || strings.HasPrefix(message, "model ") {
			for _, suffix := range []string{" does not exist", " does not exist or you do not have access to it", " not found"} {
				if strings.HasSuffix(message, suffix) {
					return true
				}
			}
		}
	}
	return false
}
