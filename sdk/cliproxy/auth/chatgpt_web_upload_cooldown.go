package auth

import (
	"context"
	"net/http"
	"strings"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

const (
	chatGPTWebUploadResetKey   = "upload_cooldown_until"
	chatGPTWebUploadBackoffKey = "upload_cooldown_backoff"
)

// ChatGPTWebUploadCooldownUntil is independent of image quota and model state.
func ChatGPTWebUploadCooldownUntil(auth *Auth) time.Time {
	if auth == nil || !strings.EqualFold(auth.Provider, "chatgpt-web") {
		return time.Time{}
	}
	return metadataTime(auth.Metadata[chatGPTWebUploadResetKey])
}

// ClearChatGPTWebUploadCooldown is used by an explicit full cooldown reset.
func ClearChatGPTWebUploadCooldown(auth *Auth) bool {
	if auth == nil || !strings.EqualFold(auth.Provider, "chatgpt-web") {
		return false
	}
	_, exists := auth.Metadata[chatGPTWebUploadResetKey]
	delete(auth.Metadata, chatGPTWebUploadResetKey)
	delete(auth.Metadata, chatGPTWebUploadBackoffKey)
	return exists
}

func isChatGPTWebUploadLimitResult(auth *Auth, result Result) bool {
	return auth != nil && strings.EqualFold(auth.Provider, "chatgpt-web") && !result.Success &&
		result.Error != nil && result.Error.Code == "chatgpt_web_upload_rate_limit" &&
		statusCodeFromResult(result.Error) == http.StatusTooManyRequests
}

func (m *Manager) applyChatGPTWebUploadCooldown(ctx context.Context, auth *Auth, result Result, now time.Time) {
	fixed, hasFixed := m.fixedErrorCooldownForResult(result.Error, ctx)
	if quotaCooldownDisabledForAuth(auth) || (!hasFixed && m.cooldownSkippedForStatus(http.StatusTooManyRequests, ctx)) {
		return
	}
	level := 0
	if previous := metadataInt(auth.Metadata[chatGPTWebUploadBackoffKey]); previous != nil {
		level = *previous
	}
	delay := time.Duration(0)
	switch {
	case hasFixed:
		delay = fixed.cooldown
	case result.RetryAfter != nil && *result.RetryAfter > 0:
		delay = *result.RetryAfter
	default:
		delay, level = nextQuotaCooldown(level, false)
	}
	if delay <= 0 {
		return
	}
	until := now.Add(delay)
	if previous := ChatGPTWebUploadCooldownUntil(auth); previous.After(until) {
		until = previous
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata[chatGPTWebUploadResetKey] = until.UTC().Format(time.RFC3339Nano)
	auth.Metadata[chatGPTWebUploadBackoffKey] = level
	auth.UpdatedAt = now
}

// Apply capability blocking after lifecycle/model checks so upload limits never
// hide a disabled or unrecoverable credential, or shorten another cooldown.
func withChatGPTWebUploadBlock(blocked bool, reason blockReason, next, until, now time.Time) (bool, blockReason, time.Time) {
	if !until.After(now) || (blocked && reason != blockReasonCooldown) {
		return blocked, reason, next
	}
	if !blocked || (!next.IsZero() && until.After(next)) {
		next = until
	}
	return true, blockReasonCooldown, next
}

func cloneAuthForRequestSelection(auth *Auth, opts core.Options) *Auth {
	clone := auth.Clone()
	clone.selectionUploadRequired = strings.EqualFold(auth.Provider, "chatgpt-web") && core.ChatGPTWebUploadRequired(opts)
	return clone
}

func cloneSelectedAuth(auth *Auth) *Auth {
	clone := auth.Clone()
	clone.selectionUploadRequired = false
	return clone
}
