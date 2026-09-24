package logging

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
)

const responseErrorContextKey = "logging.final-response-error"
const responseErrorCaptureLimit = 64 << 10

type responseErrorDiagnostic struct {
	status              int
	code, message, body string
	truncated           bool
	localPolicy         string
	original            *responseErrorDiagnostic
}

// errorResponseWriter observes only failed HTTP responses and preserves all transport methods.
type errorResponseWriter struct {
	gin.ResponseWriter
	body      []byte
	truncated bool
}

func (w *errorResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *errorResponseWriter) capture(data []byte) {
	if w.Status() < http.StatusBadRequest {
		return
	}
	remaining := responseErrorCaptureLimit - len(w.body)
	if len(data) > remaining {
		w.truncated = true
		data = data[:remaining]
	}
	w.body = append(w.body, data...)
}

func (w *errorResponseWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	w.capture(data[:n])
	return n, err
}

func (w *errorResponseWriter) WriteString(data string) (int, error) {
	n, err := w.ResponseWriter.WriteString(data)
	if w.Status() >= http.StatusBadRequest {
		remaining := responseErrorCaptureLimit - len(w.body)
		retained := n
		if retained > remaining {
			retained = remaining
			w.truncated = true
		}
		w.body = append(w.body, data[:retained]...)
	}
	return n, err
}

func safeResponseErrorText(c *gin.Context, value string, limit int) (string, bool) {
	if value == "" {
		return "", false
	}
	if redactor := util.PromptCacheLogForGin(c); redactor != nil {
		value = redactor.Redact(value)
	}
	secrets := []string{c.GetString("apiKey")}
	if c.Request != nil {
		for _, name := range []string{"Authorization", "X-API-Key", "X-Goog-API-Key"} {
			secret := c.Request.Header.Get(name)
			if name == "Authorization" {
				if parts := strings.Fields(secret); len(parts) == 2 {
					secret = parts[1]
				}
			}
			secrets = append(secrets, secret)
		}
		for _, cookie := range c.Request.Cookies() {
			secrets = append(secrets, cookie.Value)
		}
	}
	for _, secret := range secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "<redacted-key>")
		}
	}
	return managementdiag.ProcessText(value, managementdiag.DetailLevelSafe, limit)
}

func buildResponseErrorDiagnostic(c *gin.Context, status int, raw []byte) responseErrorDiagnostic {
	result := responseErrorDiagnostic{status: status}
	if len(raw) > responseErrorCaptureLimit {
		raw = raw[:responseErrorCaptureLimit]
		result.truncated = true
	}
	payload := gjson.ParseBytes(raw)
	for _, path := range []string{"error", "response.error", "detail"} {
		if nested := gjson.GetBytes(raw, path); nested.IsObject() || nested.Type == gjson.String {
			payload = nested
			break
		}
	}
	fields := map[string]any{}
	if payload.IsObject() {
		for _, name := range []string{"message", "code", "type", "param"} {
			v := payload.Get(name)
			if v.Type != gjson.String && !(name == "code" && v.Type == gjson.Number) {
				continue
			}
			limit := 128
			if name == "message" {
				limit = 1024
			}
			text, truncated := safeResponseErrorText(c, v.String(), limit)
			result.truncated = result.truncated || truncated
			fields[name] = text
			if name == "message" {
				result.message = text
			}
			if name == "code" {
				result.code = text
			}
		}
	} else {
		text := string(raw)
		if payload.Type == gjson.String {
			text = payload.String()
		}
		if !strings.HasPrefix(strings.TrimSpace(text), "{") && !strings.HasPrefix(strings.TrimSpace(text), "[") {
			var truncated bool
			result.message, truncated = safeResponseErrorText(c, text, 1024)
			result.truncated = result.truncated || truncated
		}
	}
	if result.message == "" {
		result.message = http.StatusText(status)
	}
	if result.code == "" {
		result.code = fmt.Sprintf("http_%d", status)
	}
	fields["message"] = result.message
	fields["code"] = result.code
	encoded, _ := json.Marshal(map[string]any{"error": fields})
	result.body = string(encoded)
	return result
}

// RecordResponseError retains a bounded public error summary for streams whose HTTP status is already committed.
func RecordResponseError(c *gin.Context, status int, body []byte, causes ...error) {
	if c == nil {
		return
	}
	if status <= 0 || status > 599 {
		status = http.StatusInternalServerError
	}
	diagnostic := buildResponseErrorDiagnostic(c, status, body)
	if len(causes) > 0 {
		diagnostic.localPolicy = LocalPolicyReason(causes[0])
		var rewritten interface {
			OriginalStatusCode() int
			OriginalErrorText() string
			ErrorResponseRewritten() bool
		}
		if errors.As(causes[0], &rewritten) && rewritten.ErrorResponseRewritten() {
			if originalStatus := rewritten.OriginalStatusCode(); originalStatus >= 100 && originalStatus <= 599 {
				text := rewritten.OriginalErrorText()
				truncated := len(text) > responseErrorCaptureLimit
				if truncated {
					text = text[:responseErrorCaptureLimit]
				}
				original := buildResponseErrorDiagnostic(c, originalStatus, []byte(text))
				var coded interface{ ExecutionResultErrorCode() string }
				if errors.As(causes[0], &coded) && coded.ExecutionResultErrorCode() != "" {
					original.code, _ = safeResponseErrorText(c, coded.ExecutionResultErrorCode(), 128)
				}
				original.body = ""
				original.truncated = original.truncated || truncated
				diagnostic.original = &original
			}
		}
	}
	c.Set(responseErrorContextKey, diagnostic)
}
