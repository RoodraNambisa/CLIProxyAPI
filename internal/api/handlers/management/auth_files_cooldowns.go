package management

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type clearAuthCooldownRequest struct {
	Items []clearAuthCooldownItem `json:"items"`
	Names []string                `json:"names"`
}

type clearAuthCooldownItem struct {
	Name   string   `json:"name"`
	ID     string   `json:"id"`
	Models []string `json:"models"`
}

type resolvedCooldownTarget struct {
	auth     *coreauth.Auth
	clearAll bool
	models   map[string]struct{}
}

// ClearAllAuthCooldowns clears transient cooldown state for every known auth.
func (h *Handler) ClearAllAuthCooldowns(c *gin.Context) {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	ctx := c.Request.Context()
	now := time.Now()
	auths := h.authManager.List()
	updated := 0
	for _, auth := range auths {
		if !clearFullAuthCooldownState(auth, now) {
			continue
		}
		if err := h.updateClearedAuthCooldown(ctx, auth); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to update auth %s: %v", auth.ID, err)})
			return
		}
		updated++
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "total": len(auths), "updated": updated})
}

// ClearSelectedAuthCooldowns clears transient cooldown state for selected auths or auth model states.
func (h *Handler) ClearSelectedAuthCooldowns(c *gin.Context) {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	var req clearAuthCooldownRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if len(req.Items) == 0 && len(req.Names) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "items or names is required"})
		return
	}

	targets, missing := h.resolveCooldownTargets(req)
	if len(targets) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "selected auths not found", "missing": missing})
		return
	}

	ctx := c.Request.Context()
	now := time.Now()
	updated := 0
	for _, target := range targets {
		changed := false
		if target.clearAll {
			changed = clearFullAuthCooldownState(target.auth, now)
		} else {
			changed = clearSelectedModelCooldownState(target.auth, target.models, now)
		}
		if !changed {
			continue
		}
		if err := h.updateClearedAuthCooldown(ctx, target.auth); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to update auth %s: %v", target.auth.ID, err)})
			return
		}
		updated++
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"matched": len(targets),
		"updated": updated,
		"missing": missing,
	})
}

func (h *Handler) resolveCooldownTargets(req clearAuthCooldownRequest) (map[string]*resolvedCooldownTarget, []string) {
	targets := make(map[string]*resolvedCooldownTarget)
	var missing []string
	for _, name := range req.Names {
		item := clearAuthCooldownItem{Name: name}
		h.addCooldownTarget(targets, &missing, item)
	}
	for _, item := range req.Items {
		h.addCooldownTarget(targets, &missing, item)
	}
	return targets, missing
}

func (h *Handler) addCooldownTarget(targets map[string]*resolvedCooldownTarget, missing *[]string, item clearAuthCooldownItem) {
	ref := strings.TrimSpace(item.ID)
	if ref == "" {
		ref = strings.TrimSpace(item.Name)
	}
	if ref == "" {
		*missing = append(*missing, "")
		return
	}
	auth := h.findAuthForCooldown(ref)
	if auth == nil {
		*missing = append(*missing, ref)
		return
	}
	target, ok := targets[auth.ID]
	if !ok {
		target = &resolvedCooldownTarget{
			auth:   auth,
			models: make(map[string]struct{}),
		}
		targets[auth.ID] = target
	}
	models := normalizeCooldownModels(item.Models)
	if len(models) == 0 {
		target.clearAll = true
		target.models = make(map[string]struct{})
		return
	}
	if target.clearAll {
		return
	}
	for _, model := range models {
		target.models[strings.ToLower(model)] = struct{}{}
	}
}

func (h *Handler) findAuthForCooldown(ref string) *coreauth.Auth {
	if h == nil || h.authManager == nil {
		return nil
	}
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	if auth, ok := h.authManager.GetByID(ref); ok {
		return auth
	}
	auths := h.authManager.List()
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if auth.ID == ref || auth.Index == ref || strings.TrimSpace(auth.FileName) == ref {
			return auth
		}
		if filepath.Base(strings.TrimSpace(auth.FileName)) == ref {
			return auth
		}
		if filepath.Base(strings.TrimSpace(authAttribute(auth, "path"))) == ref {
			return auth
		}
	}
	return nil
}

func normalizeCooldownModels(models []string) []string {
	if len(models) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(models))
	out := make([]string, 0, len(models))
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		key := strings.ToLower(model)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, model)
	}
	return out
}

func (h *Handler) updateClearedAuthCooldown(ctx context.Context, auth *coreauth.Auth) error {
	if h == nil || h.authManager == nil || auth == nil {
		return nil
	}
	updated, err := h.authManager.Update(coreauth.WithSkipStateCarryForward(ctx), auth)
	if err != nil {
		return err
	}
	if h.authStatusHook != nil {
		h.authStatusHook(ctx, updated.Clone())
	}
	return nil
}

func clearFullAuthCooldownState(auth *coreauth.Auth, now time.Time) bool {
	if auth == nil {
		return false
	}
	changed := clearAuthCooldownFields(auth, now)
	for _, state := range auth.ModelStates {
		if clearModelCooldownFields(state, now) {
			changed = true
		}
	}
	if changed {
		auth.UpdatedAt = now
	}
	return changed
}

func clearSelectedModelCooldownState(auth *coreauth.Auth, models map[string]struct{}, now time.Time) bool {
	if auth == nil || len(models) == 0 || len(auth.ModelStates) == 0 {
		return false
	}
	changed := false
	for model, state := range auth.ModelStates {
		if state == nil {
			continue
		}
		if _, ok := models[strings.ToLower(strings.TrimSpace(model))]; !ok {
			continue
		}
		if clearModelCooldownFields(state, now) {
			changed = true
		}
	}
	if !changed {
		return false
	}
	if refreshAuthCooldownAggregate(auth, now) {
		changed = true
	}
	auth.UpdatedAt = now
	return changed
}

func clearAuthCooldownFields(auth *coreauth.Auth, _ time.Time) bool {
	if auth == nil {
		return false
	}
	hadCooldown := auth.Unavailable || !auth.NextRetryAfter.IsZero() || auth.CooldownScope != "" || auth.Quota != (coreauth.QuotaState{})
	if !hadCooldown {
		return false
	}
	auth.Unavailable = false
	auth.NextRetryAfter = time.Time{}
	auth.CooldownScope = ""
	auth.Quota = coreauth.QuotaState{}
	if !auth.Disabled && auth.Status != coreauth.StatusDisabled {
		if auth.Status == coreauth.StatusError {
			auth.Status = coreauth.StatusActive
		}
		auth.StatusMessage = ""
		auth.LastError = nil
	}
	return true
}

func clearModelCooldownFields(state *coreauth.ModelState, now time.Time) bool {
	if state == nil {
		return false
	}
	hadCooldown := state.Unavailable || !state.NextRetryAfter.IsZero() || state.Quota != (coreauth.QuotaState{})
	if !hadCooldown {
		return false
	}
	state.Unavailable = false
	state.NextRetryAfter = time.Time{}
	state.Quota = coreauth.QuotaState{}
	if state.Status != coreauth.StatusDisabled {
		if state.Status == coreauth.StatusError {
			state.Status = coreauth.StatusActive
		}
		state.StatusMessage = ""
		state.LastError = nil
	}
	state.UpdatedAt = now
	return true
}

func refreshAuthCooldownAggregate(auth *coreauth.Auth, now time.Time) bool {
	if auth == nil || auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return false
	}
	if auth.Unavailable && auth.CooldownScope == "auth" && auth.NextRetryAfter.After(now) {
		return false
	}

	beforeUnavailable := auth.Unavailable
	beforeNextRetry := auth.NextRetryAfter
	beforeScope := auth.CooldownScope
	beforeQuota := auth.Quota

	anyState := false
	allUnavailable := true
	earliestRetry := time.Time{}
	quota := coreauth.QuotaState{}
	for _, state := range auth.ModelStates {
		if state == nil {
			continue
		}
		anyState = true
		blocked := false
		if state.Status == coreauth.StatusDisabled {
			blocked = true
		} else if state.Unavailable && state.NextRetryAfter.After(now) {
			blocked = true
			if earliestRetry.IsZero() || state.NextRetryAfter.Before(earliestRetry) {
				earliestRetry = state.NextRetryAfter
			}
		}
		if !blocked {
			allUnavailable = false
		}
		if state.Quota.Exceeded {
			quota.Exceeded = true
			quota.Reason = "quota"
			if quota.NextRecoverAt.IsZero() || (!state.Quota.NextRecoverAt.IsZero() && state.Quota.NextRecoverAt.Before(quota.NextRecoverAt)) {
				quota.NextRecoverAt = state.Quota.NextRecoverAt
			}
			if state.Quota.BackoffLevel > quota.BackoffLevel {
				quota.BackoffLevel = state.Quota.BackoffLevel
			}
			if state.Quota.StrikeCount > quota.StrikeCount {
				quota.StrikeCount = state.Quota.StrikeCount
			}
		}
	}

	if anyState && allUnavailable {
		auth.Unavailable = true
		auth.NextRetryAfter = earliestRetry
		auth.CooldownScope = "model"
		auth.Quota = quota
	} else {
		auth.Unavailable = false
		auth.NextRetryAfter = time.Time{}
		auth.CooldownScope = ""
		auth.Quota = coreauth.QuotaState{}
		if auth.Status == coreauth.StatusError {
			auth.Status = coreauth.StatusActive
		}
		auth.StatusMessage = ""
		auth.LastError = nil
	}

	return beforeUnavailable != auth.Unavailable ||
		!beforeNextRetry.Equal(auth.NextRetryAfter) ||
		beforeScope != auth.CooldownScope ||
		beforeQuota != auth.Quota
}
