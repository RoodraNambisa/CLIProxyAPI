package management

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const defaultProxyPoolCheckSample = 10

type checkProxyPoolRequest struct {
	Sample *int `json:"sample"`
}

type rebindProxyBindingsRequest struct {
	AuthIDs     []string `json:"auth_ids"`
	AuthIndexes []string `json:"auth_indexes"`
}

type proxyRebindTarget struct {
	authID    string
	authIndex string
	result    *proxypool.RebindResult
}

// GetProxyPoolStatus returns runtime health for one configured proxy pool.
func (h *Handler) GetProxyPoolStatus(c *gin.Context) {
	manager := h.proxyPoolRuntimeManager()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "proxy pool manager unavailable"})
		return
	}

	name := strings.TrimSpace(c.Param("name"))
	for _, status := range manager.PoolStatuses() {
		if strings.EqualFold(status.Name, name) {
			c.JSON(http.StatusOK, status)
			return
		}
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "proxy pool not found"})
}

// CheckProxyPool checks bound nodes and a bounded sample of unbound nodes.
func (h *Handler) CheckProxyPool(c *gin.Context) {
	manager := h.proxyPoolRuntimeManager()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "proxy pool manager unavailable"})
		return
	}

	var body checkProxyPoolRequest
	if errBind := c.ShouldBindJSON(&body); errBind != nil && !errors.Is(errBind, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	sample := defaultProxyPoolCheckSample
	if body.Sample != nil {
		sample = *body.Sample
	}
	if sample < 1 || sample > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sample must be between 1 and 100"})
		return
	}

	name := strings.TrimSpace(c.Param("name"))
	if !proxyPoolExists(manager, name) {
		c.JSON(http.StatusNotFound, gin.H{"error": "proxy pool not found"})
		return
	}
	results, errCheck := manager.CheckPool(c.Request.Context(), name, sample)
	if errCheck != nil {
		if !proxyPoolExists(manager, name) {
			c.JSON(http.StatusNotFound, gin.H{"error": "proxy pool not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check proxy pool"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"results": results})
}

// GetProxyBindings returns management-safe runtime binding details.
func (h *Handler) GetProxyBindings(c *gin.Context) {
	manager := h.proxyPoolRuntimeManager()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "proxy pool manager unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"bindings": manager.BindingStatuses()})
}

// RebindProxyBindings resolves credential references and rebinds each unique credential.
func (h *Handler) RebindProxyBindings(c *gin.Context) {
	manager := h.proxyPoolRuntimeManager()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "proxy pool manager unavailable"})
		return
	}

	var body rebindProxyBindingsRequest
	if errBind := c.ShouldBindJSON(&body); errBind != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}
	if len(body.AuthIDs) == 0 && len(body.AuthIndexes) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth_ids or auth_indexes is required"})
		return
	}

	authManager := h.coreAuthRuntimeManager()
	if len(body.AuthIndexes) > 0 && authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	targets := resolveProxyRebindTargets(body, authManager)
	if len(targets) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth_ids or auth_indexes must contain a value"})
		return
	}

	authIDs := make([]string, 0, len(targets))
	for _, target := range targets {
		if target.result == nil {
			authIDs = append(authIDs, target.authID)
		}
	}
	rebound := manager.Rebind(c.Request.Context(), authIDs)
	byAuthID := make(map[string]proxypool.RebindResult, len(rebound))
	for _, result := range rebound {
		byAuthID[result.AuthID] = result
	}

	results := make([]proxypool.RebindResult, 0, len(targets))
	allSucceeded := true
	for _, target := range targets {
		if target.result != nil {
			results = append(results, *target.result)
			allSucceeded = false
			continue
		}
		result, exists := byAuthID[target.authID]
		if !exists {
			result = proxypool.RebindResult{
				AuthID:     target.authID,
				Error:      "rebind result unavailable",
				HTTPStatus: http.StatusInternalServerError,
			}
		}
		result.AuthIndex = target.authIndex
		if !result.Updated {
			allSucceeded = false
		}
		results = append(results, result)
	}

	status := http.StatusMultiStatus
	if allSucceeded {
		status = http.StatusOK
	}
	c.JSON(status, gin.H{"results": results})
}

func (h *Handler) proxyPoolRuntimeManager() *proxypool.Manager {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	manager := h.proxyPoolManager
	h.mu.Unlock()
	return manager
}

func (h *Handler) coreAuthRuntimeManager() *coreauth.Manager {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	return manager
}

func proxyPoolExists(manager *proxypool.Manager, name string) bool {
	name = strings.TrimSpace(name)
	for _, status := range manager.PoolStatuses() {
		if strings.EqualFold(status.Name, name) {
			return true
		}
	}
	return false
}

func resolveProxyRebindTargets(body rebindProxyBindingsRequest, authManager *coreauth.Manager) []proxyRebindTarget {
	targets := make([]proxyRebindTarget, 0, len(body.AuthIDs)+len(body.AuthIndexes))
	seenAuthIDs := make(map[string]struct{}, len(body.AuthIDs)+len(body.AuthIndexes))
	for _, rawID := range body.AuthIDs {
		authID := strings.TrimSpace(rawID)
		if authID == "" {
			continue
		}
		if _, duplicate := seenAuthIDs[authID]; duplicate {
			continue
		}
		seenAuthIDs[authID] = struct{}{}
		targets = append(targets, proxyRebindTarget{authID: authID})
	}

	authIDByIndex := make(map[string]string)
	if authManager != nil {
		for _, auth := range authManager.List() {
			if auth == nil {
				continue
			}
			index := strings.TrimSpace(auth.EnsureIndex())
			if index != "" {
				authIDByIndex[index] = auth.ID
			}
		}
	}
	seenMissingIndexes := make(map[string]struct{})
	for _, rawIndex := range body.AuthIndexes {
		index := strings.TrimSpace(rawIndex)
		if index == "" {
			continue
		}
		authID := strings.TrimSpace(authIDByIndex[index])
		if authID == "" {
			if _, duplicate := seenMissingIndexes[index]; duplicate {
				continue
			}
			seenMissingIndexes[index] = struct{}{}
			result := proxypool.RebindResult{
				AuthIndex:  index,
				Error:      "auth index not found",
				HTTPStatus: http.StatusNotFound,
			}
			targets = append(targets, proxyRebindTarget{result: &result})
			continue
		}
		if _, duplicate := seenAuthIDs[authID]; duplicate {
			continue
		}
		seenAuthIDs[authID] = struct{}{}
		targets = append(targets, proxyRebindTarget{authID: authID, authIndex: index})
	}
	return targets
}
