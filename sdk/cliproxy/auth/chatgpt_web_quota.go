package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

const chatGPTWebImageModel = chatgptwebauth.ImageModel

type chatGPTWebImageCapabilityState struct {
	blocked            bool
	confirmedExhausted bool
	rateLimited        bool
	modelCooldown      bool
	refreshDue         bool
	refreshInFlight    bool
	nextRetryAt        time.Time
}

type chatGPTWebImageCapabilityCandidate struct {
	authID     string
	capability chatGPTWebImageCapabilityState
}

type chatGPTWebAccountInfoRuntimeStateReader interface {
	AccountInfoAuthState(string) chatgptwebauth.AccountInfoAuthRuntimeState
}

type chatGPTWebAccountInfoAutomaticRefreshTrigger interface {
	TriggerAutomaticAccountInfoRefresh(string) bool
}

type chatGPTWebImageQuotaExhaustedError struct {
	resetAt        time.Time
	now            time.Time
	refreshPending bool
}

type chatGPTWebImageRateLimitError struct {
	cooldown   *modelCooldownError
	retryKnown bool
}

func newChatGPTWebImageRateLimitError(retryAt, now time.Time) *chatGPTWebImageRateLimitError {
	return &chatGPTWebImageRateLimitError{
		cooldown: newModelCooldownErrorUntil(
			chatGPTWebImageModel,
			chatgptwebauth.Provider,
			retryAt,
			now,
		),
		retryKnown: retryAt.After(now),
	}
}

func (e *chatGPTWebImageRateLimitError) Error() string {
	return e.cooldown.Error()
}

func (e *chatGPTWebImageRateLimitError) StatusCode() int {
	return e.cooldown.StatusCode()
}

func (e *chatGPTWebImageRateLimitError) Headers() http.Header {
	headers := e.cooldown.Headers()
	if !e.retryKnown {
		headers.Del("Retry-After")
	}
	return headers
}

func (e *chatGPTWebImageRateLimitError) RetryAfter() *time.Duration {
	if e == nil || e.cooldown == nil || !e.retryKnown {
		return nil
	}
	retryAfter := e.cooldown.resetIn
	return &retryAfter
}

func chatGPTWebImageQuotaRefreshPendingError(err error) bool {
	var rateLimitErr *chatGPTWebImageRateLimitError
	if errors.As(err, &rateLimitErr) &&
		rateLimitErr != nil &&
		!rateLimitErr.retryKnown {
		return true
	}
	var quotaErr *chatGPTWebImageQuotaExhaustedError
	return errors.As(err, &quotaErr) &&
		quotaErr != nil &&
		quotaErr.refreshPending
}

func newChatGPTWebImageQuotaExhaustedError(resetAt, now time.Time) *chatGPTWebImageQuotaExhaustedError {
	if now.IsZero() {
		now = time.Now()
	}
	return &chatGPTWebImageQuotaExhaustedError{resetAt: resetAt, now: now}
}

func (e *chatGPTWebImageQuotaExhaustedError) Error() string {
	errorBody := map[string]any{
		"code":    "image_quota_exhausted",
		"message": "All ChatGPT Web image-capable credentials have exhausted their image quota",
		"model":   chatGPTWebImageModel,
	}
	if resetSeconds := e.resetSeconds(); resetSeconds > 0 {
		errorBody["reset_seconds"] = resetSeconds
	}
	payload, errMarshal := json.Marshal(map[string]any{"error": errorBody})
	if errMarshal != nil {
		return `{"error":{"code":"image_quota_exhausted","message":"All ChatGPT Web image-capable credentials have exhausted their image quota"}}`
	}
	return string(payload)
}

func (e *chatGPTWebImageQuotaExhaustedError) StatusCode() int {
	return http.StatusTooManyRequests
}

func (e *chatGPTWebImageQuotaExhaustedError) Headers() http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	if resetSeconds := e.resetSeconds(); resetSeconds > 0 {
		headers.Set("Retry-After", strconv.Itoa(resetSeconds))
	}
	return headers
}

func (e *chatGPTWebImageQuotaExhaustedError) RetryAfter() *time.Duration {
	if e == nil || e.resetAt.IsZero() || !e.resetAt.After(e.now) {
		return nil
	}
	retryAfter := e.resetAt.Sub(e.now)
	return &retryAfter
}

func (e *chatGPTWebImageQuotaExhaustedError) resetSeconds() int {
	if e == nil || e.resetAt.IsZero() || !e.resetAt.After(e.now) {
		return 0
	}
	return int(math.Ceil(e.resetAt.Sub(e.now).Seconds()))
}

func chatGPTWebImageQuotaExhausted(auth *Auth) (bool, time.Time) {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") || auth.Metadata == nil {
		return false, time.Time{}
	}
	state := chatgptwebauth.NormalizeQuotaState(
		chatgptwebauth.QuotaState(strings.ToLower(strings.TrimSpace(metadataString(auth.Metadata["quota_state"])))),
		metadataInt(auth.Metadata["image_quota_remaining"]),
	)
	if state != chatgptwebauth.QuotaStateExhausted {
		return false, time.Time{}
	}
	return true, metadataTime(auth.Metadata["image_quota_reset_at"])
}

func chatGPTWebImageCapabilityUnavailable(auth *Auth, now time.Time) (bool, time.Time) {
	state := chatGPTWebImageCapabilityStateForAuth(auth, now)
	return state.blocked, state.nextRetryAt
}

// ChatGPTWebImageCapabilityUnavailable reports whether ChatGPT Web image
// generation is blocked independently of the credential's text capability.
func ChatGPTWebImageCapabilityUnavailable(auth *Auth, now time.Time) (bool, time.Time) {
	return chatGPTWebImageCapabilityUnavailable(auth, now)
}

func chatGPTWebImageModelState(auth *Auth) *ModelState {
	if auth == nil {
		return nil
	}
	if state := auth.ModelStates[chatGPTWebImageModel]; state != nil {
		return state
	}
	return auth.ModelStates[canonicalModelKey(chatGPTWebImageModel)]
}

func chatGPTWebImageModelProjection(auth *Auth, model string) bool {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		return false
	}
	modelKey := canonicalModelKey(model)
	if strings.EqualFold(modelKey, canonicalModelKey(chatGPTWebImageModel)) {
		return true
	}
	for _, info := range registry.GetGlobalRegistry().GetModelsForClient(auth.ID) {
		if info != nil &&
			info.Type == registry.OpenAIImageModelType &&
			strings.EqualFold(modelKey, canonicalModelKey(info.ID)) {
			return true
		}
	}
	return false
}

func chatGPTWebImageCapabilityStateForAuth(auth *Auth, now time.Time) chatGPTWebImageCapabilityState {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		return chatGPTWebImageCapabilityState{}
	}
	exhausted, resetAt := chatGPTWebImageQuotaExhausted(auth)
	capability := chatGPTWebImageCapabilityState{
		blocked:            exhausted,
		confirmedExhausted: exhausted,
		nextRetryAt:        futureTime(resetAt, now),
	}
	modelState := chatGPTWebImageModelState(auth)
	if modelState == nil {
		capability.refreshDue = capability.blocked && capability.nextRetryAt.IsZero()
		return capability
	}

	reason := strings.ToLower(strings.TrimSpace(modelState.Quota.Reason))
	quotaOwned := modelState.Quota.Exceeded && reason == "chatgpt_web_image_quota"
	quotaRetryAt := modelState.Quota.NextRecoverAt
	if quotaRetryAt.IsZero() {
		quotaRetryAt = modelState.NextRetryAfter
	}
	retryAt := laterTime(modelState.NextRetryAfter, quotaRetryAt)
	if quotaOwned {
		capability.blocked = true
		capability.confirmedExhausted = true
		capability.nextRetryAt = laterTime(capability.nextRetryAt, futureTime(retryAt, now))
		capability.refreshDue = capability.nextRetryAt.IsZero()
		return capability
	}
	if exhausted {
		capability.nextRetryAt = laterTime(capability.nextRetryAt, futureTime(retryAt, now))
		capability.refreshDue = capability.nextRetryAt.IsZero()
		return capability
	}

	if modelState.Status == StatusDisabled {
		capability.blocked = true
		capability.modelCooldown = true
		return capability
	}
	if !modelState.Quota.Exceeded || reason != "quota" || retryAt.IsZero() {
		if !modelState.Unavailable {
			capability.refreshDue = capability.blocked && capability.nextRetryAt.IsZero()
			return capability
		}
		if modelState.NextRetryAfter.After(now) {
			capability.blocked = true
			capability.modelCooldown = true
			capability.nextRetryAt = modelState.NextRetryAfter
		}
		return capability
	}
	capability.rateLimited = true
	if quotaRetryAt.After(now) {
		if retryAt.After(now) {
			capability.blocked = true
			capability.nextRetryAt = retryAt
		}
		return capability
	}
	refreshAfter := laterTime(quotaRetryAt, modelState.UpdatedAt)
	if chatGPTWebImageQuotaConfirmedAvailableAfter(auth, refreshAfter) {
		return capability
	}
	if retryAt.After(now) {
		capability.blocked = true
		capability.nextRetryAt = retryAt
		return capability
	}
	capability.blocked = true
	capability.refreshDue = true
	return capability
}

func chatGPTWebImageCapabilityWithRuntimeState(
	capability chatGPTWebImageCapabilityState,
	runtimeState chatgptwebauth.AccountInfoAuthRuntimeState,
	now time.Time,
) chatGPTWebImageCapabilityState {
	if !capability.blocked ||
		(!capability.confirmedExhausted && (!capability.rateLimited || !capability.refreshDue)) {
		return capability
	}
	if runtimeState.Refreshing {
		capability.refreshInFlight = true
		capability.refreshDue = false
	}
	if runtimeState.NextRefreshAt.After(now) {
		capability.nextRetryAt = laterTime(capability.nextRetryAt, runtimeState.NextRefreshAt)
		capability.refreshDue = false
	}
	return capability
}

func chatGPTWebImageQuotaConfirmedAvailableAfter(auth *Auth, after time.Time) bool {
	if auth == nil || auth.Metadata == nil {
		return false
	}
	if !strings.EqualFold(
		strings.TrimSpace(metadataString(auth.Metadata["quota_state"])),
		string(chatgptwebauth.QuotaStateAvailable),
	) {
		return false
	}
	stale, staleKnown := auth.Metadata["quota_stale"].(bool)
	if !staleKnown || stale {
		return false
	}
	updatedAt := metadataTime(auth.Metadata["quota_updated_at"])
	return updatedAt.After(after)
}

func clearChatGPTWebImageRateLimitAfterFreshQuota(auth *Auth, now time.Time) bool {
	state := chatGPTWebImageModelState(auth)
	if state == nil || !state.Quota.Exceeded ||
		!strings.EqualFold(strings.TrimSpace(state.Quota.Reason), "quota") {
		return false
	}
	refreshAfter := laterTime(state.Quota.NextRecoverAt, state.UpdatedAt)
	if !chatGPTWebImageQuotaConfirmedAvailableAfter(auth, refreshAfter) {
		return false
	}
	return clearModelCooldownByReasonOnAuth(auth, canonicalModelKey(chatGPTWebImageModel), "quota", now)
}

func chatGPTWebImageQuotaOwnedModelState(auth *Auth, model string, state *ModelState) bool {
	return chatGPTWebImageModelProjection(auth, model) &&
		state != nil &&
		state.Quota.Exceeded &&
		strings.EqualFold(strings.TrimSpace(state.Quota.Reason), "chatgpt_web_image_quota")
}

func chatGPTWebImageQuotaBlocksModel(auth *Auth, model string, now time.Time) (bool, time.Time) {
	if !chatGPTWebImageModelProjection(auth, model) {
		return false, time.Time{}
	}
	return chatGPTWebImageCapabilityUnavailable(auth, now)
}

func futureTime(value, now time.Time) time.Time {
	if value.After(now) {
		return value
	}
	return time.Time{}
}

func laterTime(first, second time.Time) time.Time {
	if second.After(first) {
		return second
	}
	return first
}

func metadataString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func metadataInt(value any) *int {
	var parsed int64
	switch typed := value.(type) {
	case int:
		result := typed
		return &result
	case int64:
		parsed = typed
	case float64:
		if math.Trunc(typed) != typed || typed < math.MinInt64 || typed > math.MaxInt64 {
			return nil
		}
		parsed = int64(typed)
	case json.Number:
		value, errParse := typed.Int64()
		if errParse != nil {
			return nil
		}
		parsed = value
	case string:
		value, errParse := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		if errParse != nil {
			return nil
		}
		parsed = value
	default:
		return nil
	}
	result := int(parsed)
	if int64(result) != parsed {
		return nil
	}
	return &result
}

func metadataTime(value any) time.Time {
	switch typed := value.(type) {
	case time.Time:
		return typed
	case *time.Time:
		if typed != nil {
			return *typed
		}
	case string:
		parsed, errParse := time.Parse(time.RFC3339Nano, strings.TrimSpace(typed))
		if errParse == nil {
			return parsed
		}
	}
	return time.Time{}
}

func chatGPTWebImageSelectionErrorEligible(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*modelCooldownError); ok {
		return true
	}
	var authErr *Error
	if !errors.As(err, &authErr) || authErr == nil {
		return false
	}
	return authErr.Code == "auth_unavailable" || authErr.Code == "auth_not_found"
}

func (m *Manager) preferChatGPTWebImageQuotaError(
	err error,
	providers []string,
	model string,
	opts cliproxyexecutor.Options,
	tried map[string]struct{},
	pickAllowed func(*Auth) bool,
) error {
	if m == nil || !chatGPTWebImageSelectionErrorEligible(err) {
		return err
	}
	return m.preferChatGPTWebImageQuotaErrorForCandidates(err, providers, model, opts, tried, pickAllowed, false)
}

func (m *Manager) preferChatGPTWebImageToolQuotaError(
	err error,
	providers []string,
	model string,
	opts cliproxyexecutor.Options,
	tried map[string]struct{},
	pickAllowed func(*Auth) bool,
) error {
	if m == nil || !chatGPTWebImageSelectionErrorEligible(err) {
		return err
	}
	return m.preferChatGPTWebImageQuotaErrorForCandidates(err, providers, model, opts, tried, pickAllowed, true)
}

func (m *Manager) preferChatGPTWebImageQuotaErrorForCandidates(
	err error,
	providers []string,
	model string,
	opts cliproxyexecutor.Options,
	tried map[string]struct{},
	pickAllowed func(*Auth) bool,
	imageTool bool,
) error {
	providerSet := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		provider = strings.ToLower(strings.TrimSpace(provider))
		if provider != "" {
			providerSet[provider] = struct{}{}
		}
	}
	pinnedAuthID := pinnedAuthIDFromMetadata(opts.Metadata)
	registryRef := registry.GetGlobalRegistry()
	cfg := m.currentConfig()
	now := time.Now()
	confirmedExhaustedCount := 0
	rateLimitedCount := 0
	modelCooldownCount := 0
	var earliest time.Time
	refreshDue := false
	refreshInFlight := false
	preferOriginal := false
	blockedCandidates := make([]chatGPTWebImageCapabilityCandidate, 0)

	m.mu.RLock()
	for _, candidate := range m.auths {
		if candidate == nil || candidate.Disabled || m.sessionCleanupPendingLocked(candidate.ID) || !candidate.LifecycleSelectable() {
			continue
		}
		provider := strings.ToLower(strings.TrimSpace(candidate.Provider))
		if _, allowedProvider := providerSet[provider]; !allowedProvider {
			continue
		}
		if pinnedAuthID != "" && candidate.ID != pinnedAuthID {
			continue
		}
		if _, alreadyTried := tried[candidate.ID]; alreadyTried {
			continue
		}
		if pickAllowed != nil && !pickAllowed(candidate) {
			continue
		}
		if strings.TrimSpace(model) != "" && !m.authSupportsRouteModel(registryRef, candidate, model) {
			continue
		}
		if candidate.Unavailable && strings.EqualFold(strings.TrimSpace(candidate.CooldownScope), cooldownScopeAuth) && candidate.NextRetryAfter.After(now) {
			preferOriginal = true
			continue
		}
		if !imageTool &&
			(!strings.EqualFold(provider, chatgptwebauth.Provider) ||
				!chatGPTWebImageModelProjection(candidate, m.selectionModelKeyForAuth(candidate, model))) {
			preferOriginal = true
			continue
		}
		capability := chatGPTWebImageCapabilityStateForAuth(candidate, now)
		if capability.blocked {
			blockedCandidates = append(blockedCandidates, chatGPTWebImageCapabilityCandidate{
				authID:     candidate.ID,
				capability: capability,
			})
			continue
		}
		if !imageTool {
			preferOriginal = true
			continue
		}
		if isCodexProvider(candidate, candidate.Provider) {
			if !AuthDisablesImageGeneration(cfg, candidate, candidate.Provider) {
				preferOriginal = true
				continue
			}
			continue
		}
		preferOriginal = true
		continue
	}
	m.mu.RUnlock()
	if len(blockedCandidates) == 0 {
		return err
	}
	runtimeReader := m.chatGPTWebAccountInfoRuntimeStateReader()
	for _, candidate := range blockedCandidates {
		runtimeState := chatgptwebauth.AccountInfoAuthRuntimeState{}
		if runtimeReader != nil {
			runtimeState = runtimeReader.AccountInfoAuthState(candidate.authID)
		}
		capability := chatGPTWebImageCapabilityWithRuntimeState(candidate.capability, runtimeState, now)
		if capability.confirmedExhausted {
			confirmedExhaustedCount++
		}
		if capability.rateLimited {
			rateLimitedCount++
		}
		if capability.modelCooldown {
			modelCooldownCount++
		}
		if capability.nextRetryAt.After(now) &&
			(earliest.IsZero() || capability.nextRetryAt.Before(earliest)) {
			earliest = capability.nextRetryAt
		}
		if capability.refreshDue {
			refreshDue = true
		}
		if capability.refreshInFlight {
			refreshInFlight = true
		}
	}
	if preferOriginal {
		if refreshInFlight {
			return newChatGPTWebImageRateLimitError(time.Time{}, now)
		}
		return err
	}
	if rateLimitedCount > 0 || modelCooldownCount > 0 {
		if refreshInFlight {
			return newChatGPTWebImageRateLimitError(time.Time{}, now)
		}
		if refreshDue || earliest.IsZero() {
			return err
		}
		return newChatGPTWebImageRateLimitError(earliest, now)
	}
	refreshPending := refreshDue || refreshInFlight
	if refreshPending {
		earliest = time.Time{}
	}
	quotaErr := newChatGPTWebImageQuotaExhaustedError(earliest, now)
	quotaErr.refreshPending = refreshPending
	return quotaErr
}

func (m *Manager) triggerDueChatGPTWebImageQuotaRefreshes(
	providers []string,
	model string,
	opts cliproxyexecutor.Options,
	tried map[string]struct{},
	pickAllowed func(*Auth) bool,
	imageTool bool,
) {
	if m == nil {
		return
	}
	providerSet := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		provider = strings.ToLower(strings.TrimSpace(provider))
		if provider != "" {
			providerSet[provider] = struct{}{}
		}
	}
	if _, allowed := providerSet[chatgptwebauth.Provider]; !allowed {
		return
	}

	pinnedAuthID := pinnedAuthIDFromMetadata(opts.Metadata)
	registryRef := registry.GetGlobalRegistry()
	now := time.Now()
	dueCandidates := make([]chatGPTWebImageCapabilityCandidate, 0)
	m.mu.RLock()
	for _, candidate := range m.auths {
		if candidate == nil ||
			!strings.EqualFold(strings.TrimSpace(candidate.Provider), chatgptwebauth.Provider) ||
			candidate.Disabled ||
			m.sessionCleanupPendingLocked(candidate.ID) ||
			!candidate.LifecycleSelectable() {
			continue
		}
		if pinnedAuthID != "" && candidate.ID != pinnedAuthID {
			continue
		}
		if _, alreadyTried := tried[candidate.ID]; alreadyTried {
			continue
		}
		if pickAllowed != nil && !pickAllowed(candidate) {
			continue
		}
		if strings.TrimSpace(model) != "" && !m.authSupportsRouteModel(registryRef, candidate, model) {
			continue
		}
		if candidate.Unavailable &&
			strings.EqualFold(strings.TrimSpace(candidate.CooldownScope), cooldownScopeAuth) &&
			candidate.NextRetryAfter.After(now) {
			continue
		}
		if !imageTool && !chatGPTWebImageModelProjection(candidate, m.selectionModelKeyForAuth(candidate, model)) {
			continue
		}
		capability := chatGPTWebImageCapabilityStateForAuth(candidate, now)
		if capability.refreshDue {
			dueCandidates = append(dueCandidates, chatGPTWebImageCapabilityCandidate{
				authID:     candidate.ID,
				capability: capability,
			})
		}
	}
	m.mu.RUnlock()
	refreshAuthIDs := make([]string, 0, len(dueCandidates))
	runtimeReader := m.chatGPTWebAccountInfoRuntimeStateReader()
	for _, candidate := range dueCandidates {
		runtimeState := chatgptwebauth.AccountInfoAuthRuntimeState{}
		if runtimeReader != nil {
			runtimeState = runtimeReader.AccountInfoAuthState(candidate.authID)
		}
		capability := chatGPTWebImageCapabilityWithRuntimeState(
			candidate.capability,
			runtimeState,
			now,
		)
		if capability.refreshDue {
			refreshAuthIDs = append(refreshAuthIDs, candidate.authID)
		}
	}
	m.triggerChatGPTWebAccountInfoRefresh(refreshAuthIDs)
}

func (m *Manager) triggerChatGPTWebAccountInfoRefresh(authIDs []string) {
	if m == nil || len(authIDs) == 0 {
		return
	}
	registered, ok := m.Executor(chatgptwebauth.Provider)
	if !ok || registered == nil {
		return
	}
	trigger, ok := registered.(chatGPTWebAccountInfoAutomaticRefreshTrigger)
	if !ok {
		return
	}
	for _, authID := range authIDs {
		trigger.TriggerAutomaticAccountInfoRefresh(authID)
	}
}

func (m *Manager) chatGPTWebAccountInfoRuntimeStateReader() chatGPTWebAccountInfoRuntimeStateReader {
	if m == nil {
		return nil
	}
	registered, ok := m.Executor(chatgptwebauth.Provider)
	if !ok || registered == nil {
		return nil
	}
	reader, ok := registered.(chatGPTWebAccountInfoRuntimeStateReader)
	if !ok {
		return nil
	}
	return reader
}
