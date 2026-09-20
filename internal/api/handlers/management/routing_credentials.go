package management

import (
	"cmp"
	"slices"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type routingCredentialOption struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Provider string `json:"provider"`
	Priority int    `json:"priority"`
	Disabled bool   `json:"disabled"`
}

// GetRoutingCredentialOptions lists only local non-secret identity metadata.
// It includes every provider and never queries quota or refreshes credentials.
func (h *Handler) GetRoutingCredentialOptions(c *gin.Context) {
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(503, gin.H{"error": "credential manager unavailable"})
		return
	}
	_, summaries := manager.ManagementAuthCatalogSnapshot()
	options := make([]routingCredentialOption, 0, len(summaries))
	for _, a := range summaries {
		if a == nil || auth.IsRetiredGeminiCLIAuth(a) || auth.ChatGPTWebAuthRetainedForDependents(a) {
			continue
		}
		id := strings.TrimSpace(a.Index)
		if id == "" {
			id = a.ID
		}
		name := a.FileName
		if name == "" {
			name = a.Label
		}
		if name == "" {
			name = id
		}
		priority, _ := strconv.Atoi(strings.TrimSpace(a.Attributes["priority"]))
		options = append(options, routingCredentialOption{ID: id, Name: name, Provider: a.Provider, Priority: priority, Disabled: a.Disabled || a.Status == auth.StatusDisabled})
	}
	slices.SortFunc(options, func(a, b routingCredentialOption) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.ID, b.ID))
	})
	c.JSON(200, gin.H{"credentials": options})
}
