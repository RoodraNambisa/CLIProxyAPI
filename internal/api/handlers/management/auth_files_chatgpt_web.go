package management

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type authFileRuntimeSummary struct {
	proxyBinding *proxypool.BindingStatus
}

func (h *Handler) authFileRuntimeSummaries() map[string]authFileRuntimeSummary {
	summaries := make(map[string]authFileRuntimeSummary)
	if h == nil {
		return summaries
	}
	h.mu.Lock()
	manager := h.proxyPoolManager
	h.mu.Unlock()
	if manager == nil {
		return summaries
	}
	for _, status := range manager.BindingStatuses() {
		statusCopy := status
		summaries[status.AuthID] = authFileRuntimeSummary{proxyBinding: &statusCopy}
	}
	return summaries
}

func (h *Handler) authFileRuntimeSummary(authID string) authFileRuntimeSummary {
	return h.authFileRuntimeSummaries()[strings.TrimSpace(authID)]
}

func applyChatGPTWebAuthFileSummary(entry gin.H, auth *coreauth.Auth, now time.Time) {
	if entry == nil || auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
		return
	}
	applyChatGPTWebMetadataSummary(entry, auth.Metadata, auth.LifecycleState(), now)
	if auth.LastError == nil {
		return
	}
	category := safeChatGPTWebErrorCategory(auth.LastError.Code)
	if auth.LastError.HTTPStatus == 429 {
		category = "rate_limited"
	} else if category == "authentication_failed" && summarizeAuthCooldown(auth, now).Active {
		category = "credential_cooldown"
	} else if category == "authentication_failed" {
		if reason, _ := entry["lifecycle_reason"].(string); reason != "" {
			category = reason
		}
	}
	entry["last_error"] = &coreauth.Error{
		Code:       category,
		Message:    safeChatGPTWebErrorMessage(category),
		Retryable:  auth.LastError.Retryable,
		HTTPStatus: auth.LastError.HTTPStatus,
	}
}

func applyChatGPTWebMetadataSummary(entry gin.H, metadata map[string]any, lifecycleState string, now time.Time) {
	if entry == nil {
		return
	}
	state := string(chatgptwebauth.SafeLifecycleState(lifecycleState))
	reason := chatgptwebauth.SafeLifecycleReason(stringValue(metadata, "lifecycle_reason"))
	entry["lifecycle_state"] = state
	entry["lifecycle_reason"] = reason
	entry["reason"] = reason
	entry["status_message"] = reason

	applyChatGPTWebSummaryTime(entry, metadata, "lifecycle_updated_at", "lifecycle_updated_at")
	applyChatGPTWebSummaryTime(entry, metadata, "last_login_at", "last_login_at")
	applyChatGPTWebSummaryTime(entry, metadata, "last_refresh_at", "last_refresh_at")
	applyChatGPTWebSummaryTime(entry, metadata, "last_relogin_at", "last_relogin_at")
	if expiresAt, ok := parseLastRefreshValue(metadata["expired"]); ok {
		entry["token_expires_at"] = expiresAt
		entry["token_expired"] = !now.Before(expiresAt)
	}
	entry["token_refreshable"] = strings.TrimSpace(stringValue(metadata, "refresh_token")) != ""
}

func applyChatGPTWebSummaryTime(entry gin.H, metadata map[string]any, responseKey, metadataKey string) {
	if timestamp, ok := parseLastRefreshValue(metadata[metadataKey]); ok {
		entry[responseKey] = timestamp
	}
}
