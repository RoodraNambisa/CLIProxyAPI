package chatgptweb

import (
	"context"
	"errors"
	"strings"
)

// RecordLifecycleFailure retains only the classification, never response bodies
// or authentication material. This survives file persistence and reloads.
func (credential *Credential) RecordLifecycleFailure(err error) {
	if credential != nil {
		credential.LifecycleFailureCode, credential.LifecycleFailureStatus = lifecycleFailureDetails(err)
	}
}

func RecordLifecycleFailure(metadata map[string]any, err error) {
	if metadata == nil {
		return
	}
	code, status := lifecycleFailureDetails(err)
	if code == "" {
		delete(metadata, "lifecycle_failure_code")
		delete(metadata, "lifecycle_failure_status")
		return
	}
	metadata["lifecycle_failure_code"] = code
	metadata["lifecycle_failure_status"] = status
}

func lifecycleFailureDetails(err error) (string, int) {
	if err == nil {
		return "", 0
	}
	if authError, ok := AsAuthError(err); ok {
		code := SafeDiagnosticCode(authError.DiagnosticCode)
		if code == "" {
			code = SafeDiagnosticCode(authError.Code)
		}
		if code == "" {
			code = "authentication_failed"
		}
		status := authError.StatusCode
		if status == 0 {
			status = authError.Status
		}
		return code, status
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "acquisition_deadline_exceeded", 0
	}
	if errors.Is(err, context.Canceled) {
		return "acquisition_canceled", 0
	}
	return "authentication_failed", 0
}

// DeadLifecycleDeletionAllowed validates the provider-specific deletion reason
// in O(1), without parsing tokens or scanning credentials. Legacy session_expired
// records lack evidence distinguishing an expired session from an unknown 403.
// Keep them quarantined for review instead of automatically deleting them.
func DeadLifecycleDeletionAllowed(metadata map[string]any) bool {
	reason, _ := metadata["lifecycle_reason"].(string)
	reason = strings.ToLower(strings.TrimSpace(reason))
	failure, _ := metadata["lifecycle_failure_code"].(string)
	failure = strings.ToLower(strings.TrimSpace(failure))
	switch reason {
	case "account_deleted", "account_deactivated", "invalid_passkey_response":
		return failure == "" || failure == reason
	case "session_expired", "access_token_missing":
		return failure == reason
	default:
		return false
	}
}
