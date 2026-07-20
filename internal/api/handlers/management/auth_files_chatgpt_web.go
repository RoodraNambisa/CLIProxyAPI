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

func authFileRuntimeSummaryForAuth(auth *coreauth.Auth, graph *coreauth.ChatGPTWebDependencyGraph, summaries map[string]authFileRuntimeSummary) authFileRuntimeSummary {
	if auth == nil {
		return authFileRuntimeSummary{}
	}
	if sourceUID := coreauth.ChatGPTWebLinkedSourceUID(auth); sourceUID != "" {
		source, ambiguous := graph.SourceByUID(sourceUID)
		if ambiguous {
			return authFileRuntimeSummary{}
		}
		sourceID := coreauth.ChatGPTWebLinkedSourceID(auth)
		if source != nil && !coreauth.ChatGPTWebLinkedSourceMatches(auth, source) {
			return authFileRuntimeSummary{}
		}
		summary := summaries[sourceID]
		if summary.proxyBinding == nil || strings.TrimSpace(summary.proxyBinding.CredentialUID) != sourceUID {
			return authFileRuntimeSummary{}
		}
		return summary
	}
	return summaries[auth.ID]
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
	strategy := strings.TrimSpace(stringValue(metadata, "refresh_strategy"))
	mode := strings.TrimSpace(stringValue(metadata, "credential_mode"))
	if credential, errParse := chatgptwebauth.ParseCredential(metadata); errParse == nil {
		strategy = string(credential.RefreshStrategy)
		mode = credential.CredentialMode
	}
	entry["credential_mode"] = mode
	entry["refresh_strategy"] = strategy
	entry["token_only"] = strategy == string(chatgptwebauth.RefreshStrategyTokenOnly)
	entry["token_refreshable"] = strategy != "" && strategy != string(chatgptwebauth.RefreshStrategyTokenOnly)
	entry["source_auth_id"] = strings.TrimSpace(stringValue(metadata, "source_auth_id"))
	entry["source_credential_uid"] = strings.TrimSpace(stringValue(metadata, "source_credential_uid"))
	if uid := strings.TrimSpace(stringValue(metadata, "credential_uid")); uid != "" {
		entry["credential_uid"] = uid
	}
}

func applyChatGPTWebDependencySummary(entry gin.H, auth *coreauth.Auth, graph *coreauth.ChatGPTWebDependencyGraph) {
	if entry == nil || auth == nil {
		return
	}
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	switch provider {
	case "codex":
		uid := coreauth.ChatGPTWebCredentialUID(auth)
		entry["credential_uid"] = uid
		entry["deletion_state"] = strings.TrimSpace(stringValue(auth.Metadata, "deletion_state"))
		entry["retained_for_dependents"] = coreauth.ChatGPTWebAuthRetainedForDependents(auth)
		count, names, _ := retainedDependencySummary(auth, graph)
		entry["dependent_count"] = count
		entry["dependent_names"] = names
		if requestedAt := retainedDeletionRequestedAt(auth); requestedAt != "" {
			if parsed, errParse := time.Parse(time.RFC3339Nano, requestedAt); errParse == nil {
				entry["deletion_requested_at"] = parsed
			}
		}
	case "chatgpt-web":
		entry["source_missing"] = retainedSourceMissing(auth, graph)
	}
}

func applyChatGPTWebSummaryTime(entry gin.H, metadata map[string]any, responseKey, metadataKey string) {
	if timestamp, ok := parseLastRefreshValue(metadata[metadataKey]); ok {
		entry[responseKey] = timestamp
	}
}
