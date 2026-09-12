package helps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// Failure details are frozen with the attempt, before any retry reuses its diagnostics.
// Only public error fields are retained; echoed request payloads and credentials are omitted.
func populateUsageFailure(ctx context.Context, record *usage.Record, cause error, secrets ...string) {
	safe := func(value string, limit int) string {
		value = RedactCodexPromptCacheLog(ctx, value)
		if record.APIKey != "" {
			value = strings.ReplaceAll(value, record.APIKey, "<redacted-key>")
		}
		for _, secret := range secrets {
			if secret != "" {
				value = strings.ReplaceAll(value, secret, "<redacted-key>")
			}
		}
		text, _ := managementdiag.ProcessText(value, managementdiag.DetailLevelSafe, limit)
		return text
	}
	record.RequestID = safe(logging.GetRequestID(ctx), 128)
	if record.FailureStage == "" {
		record.FailureStage = "execution"
		if executor.IsUpstreamAttemptError(executor.ErrorFromUpstreamAttempt(ctx, cause)) || record.UpstreamCommitted {
			record.FailureStage = "upstream"
		}
	}
	code := ""
	var raw string
	var fallbackMessage string
	if cause != nil {
		raw = cause.Error()
		var status interface {
			error
			StatusCode() int
		}
		if errors.As(cause, &status) {
			if value := status.StatusCode(); value >= 100 && value <= 599 {
				record.StatusCode = value
			}
			raw = status.Error()
		}
		var coded interface{ ExecutionResultErrorCode() string }
		if errors.As(cause, &coded) {
			code = coded.ExecutionResultErrorCode()
		}
		var authError *cliproxyauth.Error
		if errors.As(cause, &authError) && authError != nil {
			if code == "" {
				code = authError.Code
			}
			raw = authError.Message
			fallbackMessage = authError.Message
			if diagnostic := authError.Diagnostic; diagnostic != nil {
				if record.FailureStage == "execution" && diagnostic.Stage != "" {
					record.FailureStage = safe(diagnostic.Stage, 128)
				}
				if diagnostic.ResponseBody != "" {
					raw = diagnostic.ResponseBody
				}
			}
		}
		var headers interface{ Headers() http.Header }
		if errors.As(cause, &headers) {
			for _, name := range []string{"X-Request-Id", "Request-Id", "Openai-Request-Id"} {
				if value := headers.Headers().Get(name); value != "" {
					record.UpstreamRequestID = safe(value, 128)
					break
				}
			}
		}
	}
	// Bound diagnostic work even when a gateway returned an oversized error page.
	if len(raw) > 64<<10 {
		raw = raw[:64<<10]
	}
	payload := gjson.Parse(raw)
	for _, path := range []string{"error", "response.error", "detail"} {
		if nested := gjson.Get(raw, path); nested.IsObject() || nested.Type == gjson.String {
			payload = nested
			break
		}
	}
	if payload.IsObject() {
		if message := payload.Get("message"); message.Type == gjson.String {
			record.ErrorMessage = safe(message.String(), 1024)
		}
		if kind := payload.Get("type"); kind.Type == gjson.String {
			record.ErrorType = safe(kind.String(), 128)
		}
		if value := payload.Get("code"); value.Type == gjson.String || value.Type == gjson.Number {
			code = value.String()
		}
		preview := map[string]any{}
		for _, name := range []string{"message", "type", "code", "param", "resets_at", "resets_in_seconds", "retry_after"} {
			value := payload.Get(name)
			if value.Type == gjson.String {
				preview[name] = safe(value.String(), 1024)
			} else if value.Type == gjson.Number {
				preview[name] = value.Value()
			}
		}
		if len(preview) > 0 {
			if data, err := json.Marshal(map[string]any{"error": preview}); err == nil {
				record.ErrorResponse, _ = managementdiag.ProcessResponseBody(string(data), managementdiag.DetailLevelSafe, 4096)
			}
		}
	} else if payload.Type == gjson.String {
		record.ErrorMessage = safe(payload.String(), 1024)
	} else if !strings.HasPrefix(strings.TrimSpace(raw), "{") && !strings.HasPrefix(strings.TrimSpace(raw), "[") {
		record.ErrorMessage = safe(raw, 1024)
	}
	if record.ErrorMessage == "" && fallbackMessage != "" && !gjson.Valid(fallbackMessage) {
		record.ErrorMessage = safe(fallbackMessage, 1024)
	}
	if errors.Is(cause, context.Canceled) {
		code = "request_canceled"
	} else if errors.Is(cause, context.DeadlineExceeded) {
		code = "request_deadline_exceeded"
	}
	if record.ErrorCode == "" || (strings.HasPrefix(record.ErrorCode, "http_") && code != "") {
		record.ErrorCode = safe(code, 128)
	}
	if record.ErrorCode == "" {
		if record.StatusCode != 0 {
			record.ErrorCode = fmt.Sprintf("http_%d", record.StatusCode)
		} else {
			record.ErrorCode = "request_failed"
		}
	}
}

func logUsageAttemptFailure(ctx context.Context, record usage.Record) {
	fields := log.Fields{
		"request_id": record.RequestID, "provider": record.Provider, "auth_index": record.AuthIndex,
		"stage": record.FailureStage, "code": record.ErrorCode, "status": record.StatusCode,
		"error_type": record.ErrorType,
	}
	if record.ErrorResponse != "" {
		fields["response_body"] = managementdiag.NewManagementOnlyValue(record.ErrorResponse)
	}
	if record.ErrorMessage != "" {
		fields["error_message"] = record.ErrorMessage
	}
	message := "provider request attempt failed"
	if record.ErrorMessage != "" {
		message += ": " + record.ErrorMessage
	}
	log.WithContext(ctx).WithFields(fields).Warn(message)
}
