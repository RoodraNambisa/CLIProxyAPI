package management

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	log "github.com/sirupsen/logrus"
)

// RefreshXAIModels never replaces a valid directory with a failed observation.
func (h *Handler) RefreshXAIModels(c *gin.Context) {
	var input struct {
		Name string `json:"name"`
	}
	if c.ShouldBindJSON(&input) != nil || strings.TrimSpace(input.Name) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "credential manager unavailable"})
		return
	}
	auth := h.findManagedAuthWithManager(input.Name, manager)
	if auth == nil || !strings.EqualFold(auth.Provider, "xai") {
		c.JSON(http.StatusNotFound, gin.H{"error": "Grok credential not found"})
		return
	}
	endpoint := runtimeexecutor.XAIModelsURL(auth)
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, endpoint, nil)
	var catalog *helps.XAIModelCatalog
	if err == nil {
		req.Header.Set("Accept", "application/json")
		var resp *http.Response
		resp, err = manager.HttpRequest(c.Request.Context(), auth, req)
		if resp != nil && resp.Body != nil {
			defer func() {
				if errClose := resp.Body.Close(); errClose != nil {
					log.WithError(errClose).Warn("close Grok models response")
				}
			}()
			if err == nil {
				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					err = fmt.Errorf("Grok model refresh returned HTTP %d", resp.StatusCode)
				} else {
					var body []byte
					body, err = io.ReadAll(io.LimitReader(resp.Body, 1024*1024+1))
					if err == nil {
						catalog, err = helps.ParseXAIModels(body, endpoint)
					}
				}
			}
		} else if err == nil {
			err = fmt.Errorf("Grok model refresh returned an empty response")
		}
	}
	if err != nil {
		catalog = helps.XAIModelsForAuth(auth)
		if catalog == nil || catalog.Source != endpoint {
			catalog = &helps.XAIModelCatalog{Models: registry.GetXAIModels(), Source: "builtin"}
		}
		c.JSON(http.StatusOK, gin.H{"models": catalog.Models, "updated_at": catalog.UpdatedAt, "source": catalog.Source, "using_cached": true, "error": err.Error()})
		return
	}
	updated, installed, err := manager.UpdateRuntimeMetadataIfCurrent(c.Request.Context(), auth, map[string]any{helps.XAIModelCatalogKey: catalog})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save Grok model catalog"})
		return
	}
	if !installed {
		c.JSON(http.StatusConflict, gin.H{"error": "credential changed during model refresh; retry with the current credential"})
		return
	}
	manager.Hook().OnAuthUpdated(c.Request.Context(), updated)
	c.JSON(http.StatusOK, gin.H{"models": catalog.Models, "updated_at": catalog.UpdatedAt, "source": catalog.Source, "using_cached": false})
}
