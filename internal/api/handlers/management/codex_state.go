package management

import (
	"net/http"
	"slices"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
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
	if len(helps.ManagedStateModels(cfg, a)) == 0 {
		c.JSON(400, gin.H{"error": "credential has no eligible managed state models"})
		return
	}
	if !codexstate.Default.Action(a.ID, input.Model, input.Action) {
		c.JSON(409, gin.H{"error": "state runtime is not ready or model is outside scope"})
		return
	}
	c.JSON(200, gin.H{"models": codexstate.Default.Snapshots(a.ID, time.Now())})
}
