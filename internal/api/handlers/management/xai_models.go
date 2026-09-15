package management

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type xaiCatalogRefreshSource struct {
	Source      string    `json:"source"`
	UpdatedAt   time.Time `json:"updated_at"`
	UsingCached bool      `json:"using_cached"`
	ModelCount  int       `json:"model_count"`
	Error       string    `json:"error,omitempty"`
}

// RefreshXAIModels stores one snapshot per configured node. A partial failure
// preserves that node's last success without erasing other directories.
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
	cfg := h.currentConfig()
	endpoints, err := helps.XAICatalogEndpoints(auth, cfg)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	fresh := make(map[string]*helps.XAIModelCatalog)
	sources := make([]xaiCatalogRefreshSource, 0, len(endpoints))
	for _, endpoint := range endpoints {
		catalog, refreshedAuth, errRefresh := fetchXAIModelCatalog(c, manager, auth, endpoint)
		auth = refreshedAuth
		if c.Request.Context().Err() != nil {
			return
		}
		source := xaiCatalogRefreshSource{Source: endpoint}
		if errRefresh != nil {
			source.Error, source.UsingCached = errRefresh.Error(), true
		} else {
			fresh[endpoint] = catalog
		}
		sources = append(sources, source)
	}
	if len(fresh) > 0 {
		updated, installed, errSave := manager.MutateRuntimeMetadataIfCurrent(c.Request.Context(), auth, func(current *coreauth.Auth) {
			active := helps.MergeXAIModelCatalogs(current, fresh, endpoints)
			if current.Metadata == nil {
				current.Metadata = make(map[string]any)
			}
			current.Metadata[helps.XAIModelCatalogsKey] = active
			// Retain the first snapshot for older readers of the credential format.
			if len(active) > 0 {
				current.Metadata[helps.XAIModelCatalogKey] = active[0]
			}
		})
		if errSave != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save Grok model catalogs"})
			return
		}
		if !installed {
			c.JSON(http.StatusConflict, gin.H{"error": "credential changed during model refresh; retry with the current credential"})
			return
		}
		auth = updated
		manager.Hook().OnAuthUpdated(c.Request.Context(), updated)
	}
	catalogs := helps.XAIModelCatalogsForAuth(auth)
	var updatedAt time.Time
	var failures []string
	for i := range sources {
		if catalog := catalogs[sources[i].Source]; catalog != nil {
			sources[i].UpdatedAt, sources[i].ModelCount = catalog.UpdatedAt, len(catalog.Models)
			if updatedAt.Before(catalog.UpdatedAt) {
				updatedAt = catalog.UpdatedAt
			}
		}
		if sources[i].Error != "" {
			failures = append(failures, sources[i].Error)
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"models": helps.XAIMergedModelsForAuth(auth, cfg), "sources": sources,
		"source": strings.Join(endpoints, ", "), "updated_at": updatedAt,
		"using_cached": len(failures) > 0, "error": strings.Join(failures, "; "),
	})
}

func fetchXAIModelCatalog(c *gin.Context, manager *coreauth.Manager, auth *coreauth.Auth, endpoint string) (*helps.XAIModelCatalog, *coreauth.Auth, error) {
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, auth, err
	}
	req.Header.Set("Accept", "application/json")
	resp, body, auth, err := readXAIResourceQuery(c.Request.Context(), manager, auth, 1024*1024+1, func(current *coreauth.Auth) (*http.Response, error) {
		return manager.HttpRequest(c.Request.Context(), current, req.Clone(c.Request.Context()))
	})
	if err != nil {
		return nil, auth, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, auth, fmt.Errorf("Grok model refresh returned HTTP %d", resp.StatusCode)
	}
	catalog, err := helps.ParseXAIModels(body, endpoint)
	return catalog, auth, err
}
