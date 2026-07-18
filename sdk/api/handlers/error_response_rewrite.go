package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

type projectedExecutionError struct {
	cause                   error
	originalStatusCode      int
	responseStatusRewritten bool
	responseBody            []byte
}

func (e *projectedExecutionError) Error() string {
	if e == nil {
		return ""
	}
	if e.responseBody != nil {
		return string(e.responseBody)
	}
	if e.cause != nil {
		return e.cause.Error()
	}
	return http.StatusText(e.originalStatusCode)
}

func (e *projectedExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *projectedExecutionError) OriginalStatusCode() int {
	if e == nil {
		return 0
	}
	return e.originalStatusCode
}

func (e *projectedExecutionError) OriginalErrorText() string {
	if e == nil || e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e *projectedExecutionError) ResponseStatusRewritten() bool {
	return e != nil && e.responseStatusRewritten
}

func (e *projectedExecutionError) ErrorResponseRewritten() bool {
	return e != nil
}

func (e *projectedExecutionError) RewrittenResponseBody() []byte {
	if e == nil || e.responseBody == nil {
		return nil
	}
	return bytes.Clone(e.responseBody)
}

// RewriteExecutionErrorResponse applies the first matching final-response rule without
// feeding the projected status or body back into auth retry, cooldown, or routing decisions.
func (h *BaseAPIHandler) RewriteExecutionErrorResponse(msg *interfaces.ErrorMessage) *interfaces.ErrorMessage {
	if msg == nil || h == nil || h.Cfg == nil || len(h.Cfg.ErrorResponseRewrites) == 0 {
		return msg
	}
	originalStatus := msg.StatusCode
	originalText := ""
	if msg.Error != nil {
		originalText = msg.Error.Error()
	}
	lowerText := strings.ToLower(originalText)

	for i := range h.Cfg.ErrorResponseRewrites {
		rule := &h.Cfg.ErrorResponseRewrites[i]
		messageContains := strings.TrimSpace(rule.MessageContains)
		if (rule.StatusCode == 0 && messageContains == "") ||
			(rule.StatusCode != 0 && (rule.StatusCode < 100 || rule.StatusCode > 599)) ||
			(rule.ResponseStatusCode != 0 && (rule.ResponseStatusCode < 400 || rule.ResponseStatusCode > 599)) ||
			(rule.ResponseStatusCode == 0 && rule.ResponseBody == nil) {
			continue
		}
		if rule.StatusCode != 0 && rule.StatusCode != originalStatus {
			continue
		}
		if messageContains != "" && !strings.Contains(lowerText, strings.ToLower(messageContains)) {
			continue
		}

		status := originalStatus
		if rule.ResponseStatusCode != 0 {
			status = rule.ResponseStatusCode
		}
		var body []byte
		if rule.ResponseBody != nil {
			if *rule.ResponseBody == nil {
				continue
			}
			encoded, errMarshal := json.Marshal(*rule.ResponseBody)
			if errMarshal != nil {
				continue
			}
			body = encoded
		}
		addon := msg.Addon.Clone()
		for key := range addon {
			if ShouldRemoveRewrittenErrorHeader(key, status, body != nil) {
				delete(addon, key)
			}
		}
		return &interfaces.ErrorMessage{
			StatusCode: status,
			Error: &projectedExecutionError{
				cause:                   msg.Error,
				originalStatusCode:      originalStatus,
				responseStatusRewritten: rule.ResponseStatusCode != 0,
				responseBody:            body,
			},
			Addon: addon,
		}
	}
	return msg
}

// ShouldRemoveRewrittenErrorHeader reports whether a projected response invalidates a header.
func ShouldRemoveRewrittenErrorHeader(key string, status int, bodyRewritten bool) bool {
	if bodyRewritten && rewrittenEntityHeader(key) {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(key), "Retry-After") &&
		status != http.StatusTooManyRequests && status != http.StatusServiceUnavailable
}

func rewrittenEntityHeader(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "content-length", "content-encoding", "content-range", "content-md5",
		"content-disposition", "digest", "etag", "last-modified", "trailer":
		return true
	default:
		return false
	}
}

// OriginalErrorText returns the pre-projection error text used by internal affinity decisions.
func OriginalErrorText(msg *interfaces.ErrorMessage) string {
	if msg == nil || msg.Error == nil {
		return ""
	}
	var projected interface{ OriginalErrorText() string }
	if errors.As(msg.Error, &projected) {
		return projected.OriginalErrorText()
	}
	return msg.Error.Error()
}

// RewrittenErrorResponseStatus reports whether a rule explicitly replaced the response status.
func RewrittenErrorResponseStatus(msg *interfaces.ErrorMessage) (int, bool) {
	if msg == nil || msg.Error == nil {
		return 0, false
	}
	var projected interface{ ResponseStatusRewritten() bool }
	if !errors.As(msg.Error, &projected) || !projected.ResponseStatusRewritten() {
		return 0, false
	}
	return msg.StatusCode, true
}

// IsErrorResponseRewritten reports whether the error carries a final client-response projection.
func IsErrorResponseRewritten(msg *interfaces.ErrorMessage) bool {
	if msg == nil || msg.Error == nil {
		return false
	}
	var projected interface{ ErrorResponseRewritten() bool }
	return errors.As(msg.Error, &projected) && projected.ErrorResponseRewritten()
}

// BuildErrorResponseBodyForMessage preserves the original body shape when only the status is rewritten.
func BuildErrorResponseBodyForMessage(status int, errText string, msg *interfaces.ErrorMessage) []byte {
	if body, rewritten := RewrittenErrorResponseBody(msg); rewritten {
		return body
	}
	if IsErrorResponseRewritten(msg) {
		status = OriginalErrorStatusCode(msg)
		errText = OriginalErrorText(msg)
	}
	return BuildErrorResponseBody(status, errText)
}

// OriginalErrorStatusCode returns the pre-projection status used by internal retry and affinity logic.
func OriginalErrorStatusCode(msg *interfaces.ErrorMessage) int {
	if msg == nil {
		return 0
	}
	if msg.Error != nil {
		var projected interface{ OriginalStatusCode() int }
		if errors.As(msg.Error, &projected) {
			if status := projected.OriginalStatusCode(); status > 0 {
				return status
			}
		}
	}
	return msg.StatusCode
}

// RewrittenErrorResponseBody returns the configured replacement object when a rule supplied one.
func RewrittenErrorResponseBody(msg *interfaces.ErrorMessage) ([]byte, bool) {
	if msg == nil || msg.Error == nil {
		return nil, false
	}
	var projected interface{ RewrittenResponseBody() []byte }
	if !errors.As(msg.Error, &projected) {
		return nil, false
	}
	body := projected.RewrittenResponseBody()
	if body == nil {
		return nil, false
	}
	return body, true
}
