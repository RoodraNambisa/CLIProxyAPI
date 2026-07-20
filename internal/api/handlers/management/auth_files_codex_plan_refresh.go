package management

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	codexPlanTypeRefreshStateIdle                = "idle"
	codexPlanTypeRefreshStateRunning             = "running"
	codexPlanTypeRefreshStatePaused              = "paused"
	codexPlanTypeRefreshStateCompleted           = "completed"
	codexPlanTypeRefreshStateCompletedWithErrors = "completed_with_errors"
	codexPlanTypeRefreshStateFailed              = "failed"
	codexPlanTypeRefreshStatusUpdated            = "updated"
	codexPlanTypeRefreshStatusUnchanged          = "unchanged"
	codexPlanTypeRefreshStatusSkipped            = "skipped"
	codexPlanTypeRefreshStatusFailed             = "failed"
	codexPlanTypeRefreshUserAgent                = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
	codexPlanTypeRefreshModeAll                  = "all"
	codexPlanTypeRefreshModeFailed               = "failed"
	codexPlanTypeRefreshActionPause              = "pause"
	codexPlanTypeRefreshActionResume             = "resume"
)

var codexPlanTypeRefreshUsageURL = "https://chatgpt.com/backend-api/wham/usage"

var errCodexPlanTypeRefreshCredentialChanged = errors.New("credential changed while plan type refresh was running")

type codexPlanTypeRefreshSummary struct {
	Eligible  int `json:"eligible"`
	Processed int `json:"processed"`
	Updated   int `json:"updated"`
	Unchanged int `json:"unchanged"`
	Skipped   int `json:"skipped"`
	Failed    int `json:"failed"`
}

type codexPlanTypeRefreshResult struct {
	Name           string `json:"name"`
	AuthID         string `json:"auth_id"`
	Status         string `json:"status"`
	PlanTypeBefore string `json:"plan_type_before,omitempty"`
	PlanTypeAfter  string `json:"plan_type_after,omitempty"`
	HTTPStatus     int    `json:"http_status,omitempty"`
	Error          string `json:"error,omitempty"`
}

type codexPlanTypeRefreshTask struct {
	State       string                       `json:"state"`
	Running     bool                         `json:"running"`
	Paused      bool                         `json:"paused,omitempty"`
	PauseWanted bool                         `json:"pause_requested,omitempty"`
	Mode        string                       `json:"mode,omitempty"`
	StartedAt   time.Time                    `json:"started_at,omitempty"`
	FinishedAt  time.Time                    `json:"finished_at,omitempty"`
	CurrentName string                       `json:"current_name,omitempty"`
	Summary     codexPlanTypeRefreshSummary  `json:"summary"`
	Results     []codexPlanTypeRefreshResult `json:"results"`

	CanRetryFailed bool `json:"can_retry_failed"`

	resumeCh        chan struct{}
	cancelCh        chan struct{}
	doneCh          chan struct{}
	cancelRequested bool
	targetAuthIDs   []string
	targetNames     []string
}

type codexPlanTypeRefreshStartRequest struct {
	Mode string `json:"mode"`
}

type codexPlanTypeRefreshControlRequest struct {
	Action string `json:"action"`
}

func (h *Handler) StartCodexPlanTypeRefresh(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler not initialized"})
		return
	}
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	manager := h.authManager
	startedAt := time.Now().UTC()
	mode, ok := parseCodexPlanTypeRefreshMode(c)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid mode"})
		return
	}

	h.codexPlanRefreshMu.Lock()
	if h.codexPlanRefresh.Running {
		snapshot := h.codexPlanTypeRefreshSnapshotLocked()
		h.codexPlanRefreshMu.Unlock()
		c.JSON(http.StatusConflict, snapshot)
		return
	}
	targetAuthIDs := []string(nil)
	targetNames := []string(nil)
	if mode == codexPlanTypeRefreshModeFailed {
		targetAuthIDs, targetNames = codexPlanTypeRefreshFailedTargets(h.codexPlanRefresh.Results)
		if len(targetAuthIDs) == 0 && len(targetNames) == 0 {
			snapshot := h.codexPlanTypeRefreshSnapshotLocked()
			h.codexPlanRefreshMu.Unlock()
			c.JSON(http.StatusOK, snapshot)
			return
		}
	}
	doneCh := make(chan struct{})
	h.codexPlanRefresh = codexPlanTypeRefreshTask{
		State:         codexPlanTypeRefreshStateRunning,
		Running:       true,
		Mode:          mode,
		StartedAt:     startedAt,
		Results:       make([]codexPlanTypeRefreshResult, 0),
		cancelCh:      make(chan struct{}),
		doneCh:        doneCh,
		targetAuthIDs: append([]string(nil), targetAuthIDs...),
		targetNames:   append([]string(nil), targetNames...),
	}
	snapshot := h.codexPlanTypeRefreshSnapshotLocked()
	h.codexPlanRefreshMu.Unlock()

	go h.runCodexPlanTypeRefresh(manager, doneCh)

	c.JSON(http.StatusAccepted, snapshot)
}

func (h *Handler) ControlCodexPlanTypeRefresh(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler not initialized"})
		return
	}

	var req codexPlanTypeRefreshControlRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	action := strings.ToLower(strings.TrimSpace(req.Action))

	h.codexPlanRefreshMu.Lock()
	if !h.codexPlanRefresh.Running {
		snapshot := h.codexPlanTypeRefreshSnapshotLocked()
		h.codexPlanRefreshMu.Unlock()
		c.JSON(http.StatusConflict, snapshot)
		return
	}
	if h.codexPlanRefresh.cancelRequested {
		snapshot := h.codexPlanTypeRefreshSnapshotLocked()
		h.codexPlanRefreshMu.Unlock()
		c.JSON(http.StatusConflict, snapshot)
		return
	}

	switch action {
	case codexPlanTypeRefreshActionPause:
		h.codexPlanRefresh.PauseWanted = true
		snapshot := h.codexPlanTypeRefreshSnapshotLocked()
		h.codexPlanRefreshMu.Unlock()
		c.JSON(http.StatusOK, snapshot)
	case codexPlanTypeRefreshActionResume:
		resumeCh := h.codexPlanRefresh.resumeCh
		h.codexPlanRefresh.resumeCh = nil
		h.codexPlanRefresh.PauseWanted = false
		h.codexPlanRefresh.Paused = false
		h.codexPlanRefresh.State = codexPlanTypeRefreshStateRunning
		snapshot := h.codexPlanTypeRefreshSnapshotLocked()
		h.codexPlanRefreshMu.Unlock()
		if resumeCh != nil {
			close(resumeCh)
		}
		c.JSON(http.StatusOK, snapshot)
	default:
		h.codexPlanRefreshMu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid action"})
	}
}

func (h *Handler) ClearCodexPlanTypeRefresh(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler not initialized"})
		return
	}

	h.codexPlanRefreshMu.Lock()
	if h.codexPlanRefresh.Running {
		if h.codexPlanRefresh.Paused && !h.codexPlanRefresh.PauseWanted {
			doneCh := h.codexPlanRefresh.doneCh
			if !h.codexPlanRefresh.cancelRequested {
				h.codexPlanRefresh.cancelRequested = true
				if h.codexPlanRefresh.cancelCh != nil {
					close(h.codexPlanRefresh.cancelCh)
				}
			}
			h.codexPlanRefreshMu.Unlock()

			if doneCh != nil {
				<-doneCh
			}

			h.codexPlanRefreshMu.Lock()
			if h.codexPlanRefresh.doneCh == doneCh && h.codexPlanRefresh.cancelRequested {
				h.codexPlanRefresh = codexPlanTypeRefreshTask{
					State:   codexPlanTypeRefreshStateIdle,
					Results: make([]codexPlanTypeRefreshResult, 0),
				}
			}
			snapshot := h.codexPlanTypeRefreshSnapshotLocked()
			h.codexPlanRefreshMu.Unlock()
			c.JSON(http.StatusOK, snapshot)
			return
		}
		snapshot := h.codexPlanTypeRefreshSnapshotLocked()
		h.codexPlanRefreshMu.Unlock()
		c.JSON(http.StatusConflict, snapshot)
		return
	}
	h.codexPlanRefresh = codexPlanTypeRefreshTask{
		State:   codexPlanTypeRefreshStateIdle,
		Results: make([]codexPlanTypeRefreshResult, 0),
	}
	snapshot := h.codexPlanTypeRefreshSnapshotLocked()
	h.codexPlanRefreshMu.Unlock()
	c.JSON(http.StatusOK, snapshot)
}

func (h *Handler) GetCodexPlanTypeRefreshStatus(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler not initialized"})
		return
	}
	c.JSON(http.StatusOK, h.codexPlanTypeRefreshSnapshot())
}

func (h *Handler) codexPlanTypeRefreshSnapshot() codexPlanTypeRefreshTask {
	h.codexPlanRefreshMu.Lock()
	defer h.codexPlanRefreshMu.Unlock()
	return h.codexPlanTypeRefreshSnapshotLocked()
}

func (h *Handler) codexPlanTypeRefreshSnapshotLocked() codexPlanTypeRefreshTask {
	snapshot := h.codexPlanRefresh
	if strings.TrimSpace(snapshot.State) == "" {
		snapshot.State = codexPlanTypeRefreshStateIdle
	}
	if len(snapshot.Results) == 0 {
		snapshot.Results = make([]codexPlanTypeRefreshResult, 0)
	} else {
		snapshot.Results = append([]codexPlanTypeRefreshResult(nil), snapshot.Results...)
	}
	snapshot.targetAuthIDs = nil
	snapshot.targetNames = nil
	snapshot.resumeCh = nil
	snapshot.cancelCh = nil
	snapshot.doneCh = nil
	snapshot.cancelRequested = false
	snapshot.CanRetryFailed = !snapshot.Running && codexPlanTypeRefreshHasFailedResult(snapshot.Results)
	return snapshot
}

func (h *Handler) runCodexPlanTypeRefresh(manager *coreauth.Manager, doneCh chan struct{}) {
	defer func() {
		if doneCh != nil {
			close(doneCh)
		}
	}()
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Errorf("management codex plan type refresh panic: %v", recovered)
			h.finishCodexPlanTypeRefresh(codexPlanTypeRefreshStateFailed)
		}
	}()

	if manager == nil {
		h.finishCodexPlanTypeRefresh(codexPlanTypeRefreshStateFailed)
		return
	}

	for _, auth := range h.codexPlanTypeRefreshTargets(manager) {
		if !h.waitIfCodexPlanTypeRefreshPaused() {
			return
		}
		name := codexPlanTypeRefreshName(auth)
		h.beginCodexPlanTypeRefreshAuth(name)
		result := h.refreshSingleCodexPlanType(manager, auth)
		h.recordCodexPlanTypeRefreshResult(result)
	}

	state := codexPlanTypeRefreshStateCompleted
	snapshot := h.codexPlanTypeRefreshSnapshot()
	if snapshot.Summary.Failed > 0 {
		state = codexPlanTypeRefreshStateCompletedWithErrors
	}
	h.finishCodexPlanTypeRefresh(state)
}

func parseCodexPlanTypeRefreshMode(c *gin.Context) (string, bool) {
	mode := codexPlanTypeRefreshModeAll
	if c == nil || c.Request == nil || c.Request.Body == nil || c.Request.ContentLength == 0 {
		return mode, true
	}

	var req codexPlanTypeRefreshStartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		return "", false
	}
	switch strings.ToLower(strings.TrimSpace(req.Mode)) {
	case "", codexPlanTypeRefreshModeAll:
		return codexPlanTypeRefreshModeAll, true
	case codexPlanTypeRefreshModeFailed:
		return codexPlanTypeRefreshModeFailed, true
	default:
		return "", false
	}
}

func codexPlanTypeRefreshFailedTargets(results []codexPlanTypeRefreshResult) ([]string, []string) {
	authIDs := make([]string, 0)
	names := make([]string, 0)
	seenAuthIDs := make(map[string]struct{})
	seenNames := make(map[string]struct{})
	for _, result := range results {
		if !strings.EqualFold(strings.TrimSpace(result.Status), codexPlanTypeRefreshStatusFailed) {
			continue
		}
		if authID := strings.TrimSpace(result.AuthID); authID != "" {
			if _, exists := seenAuthIDs[authID]; !exists {
				seenAuthIDs[authID] = struct{}{}
				authIDs = append(authIDs, authID)
			}
			continue
		}
		if name := strings.TrimSpace(result.Name); name != "" {
			if _, exists := seenNames[name]; !exists {
				seenNames[name] = struct{}{}
				names = append(names, name)
			}
		}
	}
	return authIDs, names
}

func codexPlanTypeRefreshHasFailedResult(results []codexPlanTypeRefreshResult) bool {
	for _, result := range results {
		if strings.EqualFold(strings.TrimSpace(result.Status), codexPlanTypeRefreshStatusFailed) {
			return true
		}
	}
	return false
}

func (h *Handler) codexPlanTypeRefreshTargets(manager *coreauth.Manager) []*coreauth.Auth {
	if manager == nil {
		return nil
	}

	h.codexPlanRefreshMu.Lock()
	targetAuthIDs := append([]string(nil), h.codexPlanRefresh.targetAuthIDs...)
	targetNames := append([]string(nil), h.codexPlanRefresh.targetNames...)
	h.codexPlanRefreshMu.Unlock()

	authIDSet := make(map[string]struct{}, len(targetAuthIDs))
	for _, id := range targetAuthIDs {
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			authIDSet[trimmed] = struct{}{}
		}
	}
	nameSet := make(map[string]struct{}, len(targetNames))
	for _, name := range targetNames {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			nameSet[trimmed] = struct{}{}
		}
	}
	filtered := len(authIDSet) > 0 || len(nameSet) > 0

	targets := make([]*coreauth.Auth, 0)
	for _, auth := range manager.List() {
		if !isCodexPlanTypeRefreshEligibleAuth(auth) {
			continue
		}
		if filtered {
			if _, ok := authIDSet[strings.TrimSpace(auth.ID)]; ok {
				targets = append(targets, auth)
				continue
			}
			if _, ok := nameSet[codexPlanTypeRefreshName(auth)]; !ok {
				continue
			}
		}
		targets = append(targets, auth)
	}
	return targets
}

func (h *Handler) waitIfCodexPlanTypeRefreshPaused() bool {
	for {
		h.codexPlanRefreshMu.Lock()
		if !h.codexPlanRefresh.Running {
			h.codexPlanRefreshMu.Unlock()
			return false
		}
		if h.codexPlanRefresh.cancelRequested {
			h.codexPlanRefreshMu.Unlock()
			return false
		}
		if !h.codexPlanRefresh.PauseWanted && !h.codexPlanRefresh.Paused {
			h.codexPlanRefreshMu.Unlock()
			return true
		}
		h.codexPlanRefresh.State = codexPlanTypeRefreshStatePaused
		h.codexPlanRefresh.Paused = true
		h.codexPlanRefresh.PauseWanted = false
		h.codexPlanRefresh.CurrentName = ""
		resumeCh := h.codexPlanRefresh.resumeCh
		if resumeCh == nil {
			resumeCh = make(chan struct{})
			h.codexPlanRefresh.resumeCh = resumeCh
		}
		cancelCh := h.codexPlanRefresh.cancelCh
		h.codexPlanRefreshMu.Unlock()
		if cancelCh == nil {
			<-resumeCh
			continue
		}
		select {
		case <-resumeCh:
		case <-cancelCh:
			return false
		}
	}
}

func isCodexPlanTypeRefreshEligibleAuth(auth *coreauth.Auth) bool {
	if auth == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return false
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled || coreauth.ChatGPTWebAuthRetainedForDependents(auth) {
		return false
	}
	if isRuntimeOnlyAuth(auth) {
		return false
	}
	if auth.Metadata == nil {
		return false
	}
	if auth.Attributes != nil && strings.TrimSpace(auth.Attributes["api_key"]) != "" {
		return false
	}
	path := strings.TrimSpace(authAttribute(auth, "path"))
	if path == "" {
		return false
	}
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
}

func codexPlanTypeRefreshName(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if name := strings.TrimSpace(auth.FileName); name != "" {
		return name
	}
	return strings.TrimSpace(auth.ID)
}

func (h *Handler) beginCodexPlanTypeRefreshAuth(name string) {
	h.codexPlanRefreshMu.Lock()
	defer h.codexPlanRefreshMu.Unlock()
	h.codexPlanRefresh.Summary.Eligible++
	h.codexPlanRefresh.CurrentName = strings.TrimSpace(name)
}

func (h *Handler) recordCodexPlanTypeRefreshResult(result codexPlanTypeRefreshResult) {
	h.codexPlanRefreshMu.Lock()
	defer h.codexPlanRefreshMu.Unlock()
	h.codexPlanRefresh.CurrentName = ""
	h.codexPlanRefresh.Summary.Processed++
	switch result.Status {
	case codexPlanTypeRefreshStatusUpdated:
		h.codexPlanRefresh.Summary.Updated++
	case codexPlanTypeRefreshStatusUnchanged:
		h.codexPlanRefresh.Summary.Unchanged++
	case codexPlanTypeRefreshStatusSkipped:
		h.codexPlanRefresh.Summary.Skipped++
	case codexPlanTypeRefreshStatusFailed:
		h.codexPlanRefresh.Summary.Failed++
	}
	h.codexPlanRefresh.Results = append(h.codexPlanRefresh.Results, result)
}

func (h *Handler) finishCodexPlanTypeRefresh(state string) {
	h.codexPlanRefreshMu.Lock()
	defer h.codexPlanRefreshMu.Unlock()
	if h.codexPlanRefresh.cancelRequested {
		return
	}
	if strings.TrimSpace(state) == "" {
		state = codexPlanTypeRefreshStateFailed
	}
	h.codexPlanRefresh.State = state
	h.codexPlanRefresh.Running = false
	h.codexPlanRefresh.FinishedAt = time.Now().UTC()
	h.codexPlanRefresh.CurrentName = ""
}

func (h *Handler) refreshSingleCodexPlanType(manager *coreauth.Manager, auth *coreauth.Auth) codexPlanTypeRefreshResult {
	result := codexPlanTypeRefreshResult{
		Name:           codexPlanTypeRefreshName(auth),
		AuthID:         strings.TrimSpace(auth.ID),
		PlanTypeBefore: effectiveCodexPlanType(auth),
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || !isCodexPlanTypeRefreshEligibleAuth(current) {
		result.Status = codexPlanTypeRefreshStatusSkipped
		result.Error = "credential is no longer eligible"
		return result
	}
	auth = current
	expected := current.Clone()
	result.PlanTypeBefore = effectiveCodexPlanType(auth)
	retired, errRetired := h.authBackedByRetiredGeminiCLIFile(auth)
	if errRetired != nil {
		result.Status = codexPlanTypeRefreshStatusFailed
		result.Error = "unable to verify auth file"
		return result
	}
	if retired {
		result.Status = codexPlanTypeRefreshStatusFailed
		result.Error = errGeminiCLIAuthGone.Error()
		return result
	}

	accountID := internalcodex.EffectiveAccountID(auth.Metadata)
	if accountID == "" {
		result.Status = codexPlanTypeRefreshStatusSkipped
		result.Error = "account_id not found"
		return result
	}
	resolvedAuth, errProxy := manager.ResolveProxyAuth(context.Background(), auth)
	if errProxy != nil {
		result.Status = codexPlanTypeRefreshStatusFailed
		result.Error = "proxy unavailable"
		return result
	}
	auth = resolvedAuth

	accessToken := codexAccessTokenFromMetadata(auth.Metadata)
	refreshToken := codexRefreshTokenFromMetadata(auth.Metadata)
	forcePersist := false

	if accessToken == "" && refreshToken != "" {
		var refreshErr error
		auth, refreshErr = refreshCodexPlanTypeAuth(manager, auth)
		if refreshErr != nil {
			result.Status = codexPlanTypeRefreshStatusSkipped
			result.Error = fmt.Sprintf("access token refresh failed: %v", refreshErr)
			return result
		}
		forcePersist = true
		accountID = firstNonEmptyValue(internalcodex.EffectiveAccountID(auth.Metadata), accountID)
		accessToken = codexAccessTokenFromMetadata(auth.Metadata)
	}
	if accessToken == "" {
		if forcePersist {
			if err := h.persistCodexPlanTypeRefreshAuth(context.Background(), manager, expected, auth, "", accountID, true); err != nil {
				result.Status = codexPlanTypeRefreshStatusFailed
				result.Error = fmt.Sprintf("persist refreshed auth: %v", err)
				return result
			}
		}
		result.Status = codexPlanTypeRefreshStatusSkipped
		result.Error = "access_token not found"
		return result
	}

	planType, statusCode, err := h.fetchCodexUsagePlanType(context.Background(), auth, accessToken, accountID)
	if (statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden) && refreshToken != "" {
		auth, err = refreshCodexPlanTypeAuth(manager, auth)
		if err != nil {
			if forcePersist {
				if errPersist := h.persistCodexPlanTypeRefreshAuth(context.Background(), manager, expected, auth, "", accountID, true); errPersist != nil {
					result.Status = codexPlanTypeRefreshStatusFailed
					result.HTTPStatus = statusCode
					result.Error = fmt.Sprintf("persist refreshed auth: %v", errPersist)
					return result
				}
			}
			result.Status = codexPlanTypeRefreshStatusFailed
			result.HTTPStatus = statusCode
			result.Error = fmt.Sprintf("usage request unauthorized and refresh failed: %v", err)
			return result
		}
		forcePersist = true
		accountID = firstNonEmptyValue(internalcodex.EffectiveAccountID(auth.Metadata), accountID)
		accessToken = codexAccessTokenFromMetadata(auth.Metadata)
		if accessToken == "" {
			if errPersist := h.persistCodexPlanTypeRefreshAuth(context.Background(), manager, expected, auth, "", accountID, true); errPersist != nil {
				result.Status = codexPlanTypeRefreshStatusFailed
				result.HTTPStatus = statusCode
				result.Error = fmt.Sprintf("persist refreshed auth: %v", errPersist)
				return result
			}
			result.Status = codexPlanTypeRefreshStatusSkipped
			result.HTTPStatus = statusCode
			result.Error = "access_token not found after refresh"
			return result
		}
		planType, statusCode, err = h.fetchCodexUsagePlanType(context.Background(), auth, accessToken, accountID)
	}
	if err != nil {
		if forcePersist {
			if errPersist := h.persistCodexPlanTypeRefreshAuth(context.Background(), manager, expected, auth, "", accountID, true); errPersist != nil {
				result.Status = codexPlanTypeRefreshStatusFailed
				result.HTTPStatus = statusCode
				result.Error = fmt.Sprintf("persist refreshed auth: %v", errPersist)
				return result
			}
		}
		result.Status = codexPlanTypeRefreshStatusFailed
		result.HTTPStatus = statusCode
		result.Error = err.Error()
		return result
	}

	result.HTTPStatus = statusCode
	result.PlanTypeAfter = planType
	if err = h.persistCodexPlanTypeRefreshAuth(context.Background(), manager, expected, auth, planType, accountID, forcePersist); err != nil {
		result.Status = codexPlanTypeRefreshStatusFailed
		result.Error = fmt.Sprintf("persist auth: %v", err)
		return result
	}

	if strings.EqualFold(strings.TrimSpace(result.PlanTypeBefore), strings.TrimSpace(planType)) {
		result.Status = codexPlanTypeRefreshStatusUnchanged
		return result
	}
	result.Status = codexPlanTypeRefreshStatusUpdated
	return result
}

func refreshCodexPlanTypeAuth(manager *coreauth.Manager, auth *coreauth.Auth) (*coreauth.Auth, error) {
	if manager == nil {
		return auth, fmt.Errorf("core auth manager unavailable")
	}
	executor, ok := manager.Executor("codex")
	if !ok || executor == nil {
		return auth, fmt.Errorf("codex refresh executor unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultAPICallTimeout)
	defer cancel()
	resolvedAuth, errProxy := manager.ResolveProxyAuth(ctx, auth)
	if errProxy != nil {
		return auth, errProxy
	}
	auth = resolvedAuth
	refreshed, err := executor.Refresh(ctx, auth)
	if refreshed != nil {
		if strings.TrimSpace(refreshed.ProxyURL) == "" {
			refreshed.RuntimeProxyURL = auth.RuntimeProxyURL
			refreshed.RuntimeProxyBindingID = auth.RuntimeProxyBindingID
		}
		auth = refreshed
	}
	if err != nil {
		return auth, manager.ReportProxyFailure(ctx, auth, err)
	}
	return auth, nil
}

func (h *Handler) persistCodexPlanTypeRefreshAuth(ctx context.Context, manager *coreauth.Manager, expected, refreshed *coreauth.Auth, planType string, accountID string, forcePersist bool) error {
	if h == nil || manager == nil || expected == nil || refreshed == nil {
		return fmt.Errorf("core auth manager unavailable")
	}
	h.chatGPTWebDependencyMu.Lock()
	lockedCtx, unlockAuth, errLock := manager.LockAuthMutation(ctx, expected)
	if errLock != nil {
		h.chatGPTWebDependencyMu.Unlock()
		return errLock
	}
	defer func() {
		unlockAuth()
		h.chatGPTWebDependencyMu.Unlock()
	}()
	current, ok := manager.GetByID(expected.ID)
	if !ok || current == nil || !strings.EqualFold(strings.TrimSpace(current.Provider), "codex") || coreauth.ChatGPTWebAuthRetainedForDependents(current) {
		return errCodexPlanTypeRefreshCredentialChanged
	}
	if expectedUID := coreauth.ChatGPTWebCredentialUID(expected); expectedUID != "" && coreauth.ChatGPTWebCredentialUID(current) != expectedUID {
		return errCodexPlanTypeRefreshCredentialChanged
	}
	auth := current.Clone()
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}

	changed := forcePersist
	planType = strings.TrimSpace(planType)
	accountID = strings.TrimSpace(accountID)
	if planType != "" {
		currentPlanType := strings.TrimSpace(effectiveCodexPlanType(current))
		expectedPlanType := strings.TrimSpace(effectiveCodexPlanType(expected))
		if !strings.EqualFold(currentPlanType, expectedPlanType) && !strings.EqualFold(currentPlanType, planType) {
			return errCodexPlanTypeRefreshCredentialChanged
		}
	}
	if accountID != "" {
		currentAccountID := strings.TrimSpace(internalcodex.EffectiveAccountID(current.Metadata))
		expectedAccountID := strings.TrimSpace(internalcodex.EffectiveAccountID(expected.Metadata))
		if currentAccountID != expectedAccountID && currentAccountID != accountID {
			return errCodexPlanTypeRefreshCredentialChanged
		}
	}
	if forcePersist {
		merged, conflict := mergeCodexPlanTypeRefreshMetadata(auth.Metadata, expected.Metadata, refreshed.Metadata)
		if conflict {
			return errCodexPlanTypeRefreshCredentialChanged
		}
		changed = merged || changed
	}
	if planType != "" {
		metadataChanged, conflict := setCodexPlanTypeRefreshMetadataString(auth.Metadata, current.Metadata, expected.Metadata, "plan_type", planType)
		if conflict {
			return errCodexPlanTypeRefreshCredentialChanged
		}
		attributeChanged, conflict := setCodexPlanTypeRefreshAttribute(auth.Attributes, current.Attributes, expected.Attributes, "plan_type", planType)
		if conflict {
			return errCodexPlanTypeRefreshCredentialChanged
		}
		changed = metadataChanged || attributeChanged || changed
	}
	if accountID != "" {
		metadataChanged, conflict := setCodexPlanTypeRefreshMetadataString(auth.Metadata, current.Metadata, expected.Metadata, "account_id", accountID)
		if conflict {
			return errCodexPlanTypeRefreshCredentialChanged
		}
		changed = metadataChanged || changed
	}

	if !changed {
		return nil
	}

	auth.UpdatedAt = time.Now().UTC()
	var (
		currentMatch bool
		errUpdate    error
	)
	if strings.TrimSpace(current.Attributes[coreauth.SourceHashAttributeKey]) != "" && manager.SupportsSourceConditionalSave() {
		_, currentMatch, errUpdate = manager.UpdateIfCurrentSourceHash(lockedCtx, current, auth)
	} else {
		_, currentMatch, errUpdate = manager.UpdateIfCurrent(lockedCtx, current, auth)
	}
	if errUpdate != nil {
		if outcome, explicit := coreauth.SaveOutcomeFromError(errUpdate); explicit && outcome == coreauth.SaveOutcomeRolledBack {
			return errCodexPlanTypeRefreshCredentialChanged
		}
		return errUpdate
	}
	if !currentMatch {
		return errCodexPlanTypeRefreshCredentialChanged
	}
	return nil
}

func setCodexPlanTypeRefreshMetadataString(target, current, before map[string]any, key, value string) (changed, conflict bool) {
	currentValue, currentExists := current[key]
	beforeValue, beforeExists := before[key]
	if currentExists != beforeExists || !reflect.DeepEqual(currentValue, beforeValue) {
		if strings.EqualFold(strings.TrimSpace(stringValue(current, key)), value) {
			return false, false
		}
		return false, true
	}
	if strings.EqualFold(strings.TrimSpace(stringValue(target, key)), value) {
		return false, false
	}
	target[key] = value
	return true, false
}

func setCodexPlanTypeRefreshAttribute(target, current, before map[string]string, key, value string) (changed, conflict bool) {
	currentValue, currentExists := current[key]
	beforeValue, beforeExists := before[key]
	if currentExists != beforeExists || currentValue != beforeValue {
		if strings.EqualFold(strings.TrimSpace(currentValue), value) {
			return false, false
		}
		return false, true
	}
	if strings.EqualFold(strings.TrimSpace(target[key]), value) {
		return false, false
	}
	target[key] = value
	return true, false
}

func mergeCodexPlanTypeRefreshMetadata(target, before, after map[string]any) (changed, conflict bool) {
	keys := make(map[string]struct{}, len(before)+len(after))
	for key := range before {
		keys[key] = struct{}{}
	}
	for key := range after {
		keys[key] = struct{}{}
	}
	for key := range keys {
		beforeValue, beforeExists := before[key]
		afterValue, afterExists := after[key]
		if beforeExists == afterExists && reflect.DeepEqual(beforeValue, afterValue) {
			continue
		}
		currentValue, currentExists := target[key]
		if currentExists == afterExists && reflect.DeepEqual(currentValue, afterValue) {
			continue
		}
		if currentExists != beforeExists || !reflect.DeepEqual(currentValue, beforeValue) {
			return changed, true
		}
		if afterExists {
			target[key] = afterValue
		} else {
			delete(target, key)
		}
		changed = true
	}
	return changed, false
}

func (h *Handler) fetchCodexUsagePlanType(ctx context.Context, auth *coreauth.Auth, accessToken string, accountID string) (string, int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctxRequest, cancel := context.WithTimeout(ctx, defaultAPICallTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctxRequest, http.MethodGet, codexPlanTypeRefreshUsageURL, nil)
	if err != nil {
		return "", 0, fmt.Errorf("build usage request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(accessToken))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", codexPlanTypeRefreshUserAgent)
	req.Header.Set("Chatgpt-Account-Id", strings.TrimSpace(accountID))

	httpClient := &http.Client{
		Timeout:   defaultAPICallTimeout,
		Transport: h.apiCallTransport(auth),
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		err = h.reportManagementProxyFailure(ctxRequest, auth, err)
		return "", 0, fmt.Errorf("usage request failed: %w", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Errorf("codex usage response body close error: %v", errClose)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		err = h.reportManagementProxyFailure(ctxRequest, auth, err)
		return "", resp.StatusCode, fmt.Errorf("read usage response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", resp.StatusCode, fmt.Errorf("usage request returned %d", resp.StatusCode)
	}

	var payload struct {
		PlanType  string `json:"plan_type"`
		PlanType2 string `json:"planType"`
	}
	if err = json.Unmarshal(body, &payload); err != nil {
		return "", resp.StatusCode, fmt.Errorf("decode usage response: %w", err)
	}

	planType := strings.TrimSpace(payload.PlanType)
	if planType == "" {
		planType = strings.TrimSpace(payload.PlanType2)
	}
	if planType == "" {
		return "", resp.StatusCode, fmt.Errorf("usage response missing plan_type")
	}
	return planType, resp.StatusCode, nil
}

func firstNonEmptyValue(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func codexAccessTokenFromMetadata(metadata map[string]any) string {
	if token := firstNonEmptyValue(stringValue(metadata, "access_token"), stringValue(metadata, "accessToken")); token != "" {
		return token
	}
	tokenRaw, ok := metadata["token"]
	if !ok || tokenRaw == nil {
		return ""
	}
	tokenMap, ok := tokenRaw.(map[string]any)
	if !ok {
		return ""
	}
	return firstNonEmptyValue(stringValue(tokenMap, "access_token"), stringValue(tokenMap, "accessToken"))
}

func codexRefreshTokenFromMetadata(metadata map[string]any) string {
	if token := firstNonEmptyValue(stringValue(metadata, "refresh_token"), stringValue(metadata, "refreshToken")); token != "" {
		return token
	}
	tokenRaw, ok := metadata["token"]
	if !ok || tokenRaw == nil {
		return ""
	}
	tokenMap, ok := tokenRaw.(map[string]any)
	if !ok {
		return ""
	}
	return firstNonEmptyValue(stringValue(tokenMap, "refresh_token"), stringValue(tokenMap, "refreshToken"))
}
