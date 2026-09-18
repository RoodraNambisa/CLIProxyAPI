package management

import (
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
)

func (h *Handler) GetCodexState(c *gin.Context) {
	a := h.findManagedAuthWithManager(c.Query("name"), h.authManager)
	if a == nil {
		c.JSON(404, gin.H{"error": "credential not found"})
		return
	}
	c.JSON(200, gin.H{"models": codexstate.Default.Snapshots(a.ID, time.Now())})
}

func (h *Handler) CodexStateAction(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	var input struct {
		Name   string `json:"name"`
		Model  string `json:"model"`
		Action string `json:"action"`
	}
	if c.ShouldBindJSON(&input) != nil || !slices.Contains([]string{"acquire", "pause", "resume", "clear"}, input.Action) {
		c.JSON(400, gin.H{"error": "invalid state action"})
		return
	}
	a := h.findManagedAuthWithManager(input.Name, h.authManager)
	if a == nil {
		c.JSON(404, gin.H{"error": "credential not found"})
		return
	}
	cfg := h.currentConfig()
	allowed := helps.ManagedStateModels(cfg, a)
	if len(allowed) == 0 {
		c.JSON(400, gin.H{"error": "credential has no eligible managed state models"})
		return
	}
	input.Model = strings.TrimSpace(input.Model)
	resolvedModel := ""
	if input.Model != "" {
		resolvedModel = resolveManagedStateActionModel(a.ID, input.Model, allowed)
		if resolvedModel == "" {
			c.JSON(400, gin.H{"error": "model is outside the configured State scope or unsupported by this credential"})
			return
		}
	}
	var previousAcquired uint64
	matched := false
	for _, candidate := range allowed {
		if resolvedModel == "" || candidate.Model == resolvedModel {
			acted, baseline := codexstate.Default.ActionWithBaseline(a.ID, candidate.Model, input.Action)
			matched = acted || matched
			previousAcquired += baseline
		}
	}
	if !matched {
		c.JSON(409, gin.H{"error": "state runtime is not ready or model is outside scope"})
		return
	}
	c.JSON(200, gin.H{"model": resolvedModel, "previous_acquired": previousAcquired, "models": codexstate.Default.Snapshots(a.ID, time.Now())})
}

// Resolve aliases against the registered catalog before checking the current scope.
func resolveManagedStateActionModel(authID, requested string, allowed []codexstate.Credential) string {
	upstream := thinking.ParseSuffix(requested).ModelName
	for _, info := range registry.GetGlobalRegistry().GetModelsForClient(authID) {
		if info != nil && thinking.ParseSuffix(info.ID).ModelName == upstream {
			if info.UpstreamID != "" {
				upstream = thinking.ParseSuffix(info.UpstreamID).ModelName
			}
			break
		}
	}
	for _, candidate := range allowed {
		if candidate.Model == upstream {
			return upstream
		}
	}
	return ""
}
