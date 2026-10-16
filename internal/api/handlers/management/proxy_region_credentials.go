package management

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type proxyRegionCredentialOption struct {
	coreauth.ProxyCredentialSummary
	Pool  string `json:"pool,omitempty"`
	Entry string `json:"entry,omitempty"`
	Bound bool   `json:"bound"`
}

type proxyRegionPagination struct {
	Page       int `json:"page"`
	PageSize   int `json:"page_size"`
	TotalPages int `json:"total_pages"`
	Total      int `json:"total"`
}

func proxyRegionPage(c *gin.Context, defaultSize int) (int, int, error) {
	page, size := 1, defaultSize
	for key, dest := range map[string]*int{"page": &page, "page_size": &size} {
		values, exists := c.Request.URL.Query()[key]
		if !exists {
			continue
		}
		if len(values) != 1 {
			return 0, 0, fmt.Errorf("invalid %s", key)
		}
		n, err := strconv.Atoi(values[0])
		if err != nil || n < 1 || n > 1_000_000 || (key == "page_size" && n > 100) {
			return 0, 0, fmt.Errorf("invalid %s", key)
		}
		*dest = n
	}
	if len(c.Query("search")) > 256 {
		return 0, 0, fmt.Errorf("search too long")
	}
	return page, size, nil
}
func proxyRegionPageBounds(page, size, total int) (int, int, proxyRegionPagination) {
	pages := max(1, (total+size-1)/size)
	page = min(page, pages)
	start := (page - 1) * size
	return start, min(total, start+size), proxyRegionPagination{Page: page, PageSize: size, TotalPages: pages, Total: total}
}

// GetProxyRegionCredentials searches lightweight descriptors before pagination.
// Bound-only queries inspect the binding index instead of every credential.
func (h *Handler) GetProxyRegionCredentials(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	page, size, err := proxyRegionPage(c, 20)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	scope := c.DefaultQuery("scope", "bound")
	if scope != "bound" && scope != "custom" && scope != "all" {
		c.JSON(400, gin.H{"error": "invalid scope"})
		return
	}
	manager, authManager := h.proxyPoolRuntimeManager(), h.coreAuthRuntimeManager()
	if manager == nil || authManager == nil {
		c.JSON(503, gin.H{"error": "runtime unavailable"})
		return
	}
	bound := make(map[string]proxypool.BindingStatus)
	ids := make([]string, 0)
	for _, binding := range manager.BindingStatuses() {
		if binding.Direct {
			continue
		}
		bound[binding.AuthID] = binding
		ids = append(ids, binding.AuthID)
	}
	if scope != "bound" {
		ids = nil
	}
	provider := strings.TrimSpace(c.Query("provider"))
	options := filterProxyRegionCredentials(authManager.ProxyCredentialSummaries(ids), bound, scope, provider, c.Query("search"))
	start, end, pagination := proxyRegionPageBounds(page, size, len(options))
	c.JSON(200, gin.H{"credentials": options[start:end], "pagination": pagination, "capabilities": gin.H{"region_credentials": true}})
}

func filterProxyRegionCredentials(summaries []coreauth.ProxyCredentialSummary, bound map[string]proxypool.BindingStatus, scope, provider, search string) []proxyRegionCredentialOption {
	options := make([]proxyRegionCredentialOption, 0)
	for _, item := range summaries {
		if provider != "" && provider != "all" && item.Provider != provider {
			continue
		}
		binding, exists := bound[item.BindingOwnerID]
		exists = exists && !item.HasCustomProxy
		if !exists {
			binding = proxypool.BindingStatus{}
		}
		if scope == "bound" && !exists || scope == "custom" && !item.HasCustomProxy {
			continue
		}
		if !proxyRegionSearchMatch(search, item.Name, item.ID, item.Index, binding.Pool, binding.Entry) {
			continue
		}
		options = append(options, proxyRegionCredentialOption{ProxyCredentialSummary: item, Pool: binding.Pool, Entry: binding.Entry, Bound: exists})
	}
	sort.Slice(options, func(i, j int) bool {
		if options[i].Name != options[j].Name {
			return options[i].Name < options[j].Name
		}
		return options[i].ID < options[j].ID
	})
	return options
}

type proxyBindingRegionRow struct {
	proxypool.BindingStatus
	Name        string                     `json:"name,omitempty"`
	Application proxyRegionApplicationView `json:"application"`
}

func (h *Handler) proxyBindingRegions(c *gin.Context, manager *proxypool.Manager) {
	c.Header("Cache-Control", "no-store")
	page, size, err := proxyRegionPage(c, 50)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	authManager := h.coreAuthRuntimeManager()
	if authManager == nil {
		c.JSON(503, gin.H{"error": "runtime unavailable"})
		return
	}
	bindings := manager.BindingStatuses()
	ids := make([]string, 0, len(bindings))
	for _, b := range bindings {
		ids = append(ids, b.AuthID)
	}
	info := make(map[string]coreauth.ProxyCredentialSummary, len(ids))
	for _, a := range authManager.ProxyCredentialSummaries(ids) {
		info[a.ID] = a
	}
	filtered := make([]proxypool.BindingStatus, 0)
	for _, b := range bindings {
		a, ok := info[b.AuthID]
		if !ok {
			continue
		}
		if proxyRegionSearchMatch(c.Query("search"), a.Name, a.ID, a.Index, b.Pool, b.Entry, b.IP) {
			filtered = append(filtered, b)
		}
	}
	start, end, pagination := proxyRegionPageBounds(page, size, len(filtered))
	rows := make([]proxyBindingRegionRow, 0, end-start)
	for _, binding := range filtered[start:end] {
		auth, _ := authManager.GetByID(binding.AuthID)
		rows = append(rows, proxyBindingRegionRow{BindingStatus: binding, Name: info[binding.AuthID].Name, Application: h.proxyRegionApplication(c.Request.Context(), auth, binding.BindingID)})
	}
	c.JSON(200, gin.H{"bindings": rows, "pagination": pagination, "capabilities": gin.H{"region_application": true}})
}
