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
	if !helps.ManagedStateCredentialEligible(cfg, a) {
		c.JSON(400, gin.H{"error": "State is disabled or this credential is outside the configured priority/credential scope"})
		return
	}
	allowed := helps.ManagedStateModels(cfg, a)
	// Explicit card actions also include diagnostic pairs already visible there.
	for _, snapshot := range codexstate.Default.Snapshots(a.ID, time.Now()) {
		if !snapshot.ManualOnly || !helps.ManagedStatePairAllowed(cfg, helps.StateCredential(a, snapshot.Model)) {
			continue
		}
		if slices.ContainsFunc(allowed, func(candidate codexstate.Credential) bool { return candidate.Model == snapshot.Model }) {
			continue
		}
		credential := helps.StateCredential(a, snapshot.Model)
		credential.Route = snapshot.Model
		allowed = append(allowed, credential)
	}
	if len(allowed) == 0 && strings.TrimSpace(input.Model) == "" {
		c.JSON(400, gin.H{"error": "credential has no eligible managed state models"})
		return
	}
	input.Model = strings.TrimSpace(input.Model)
	resolvedModel := ""
	if input.Model != "" {
		resolvedModel = resolveManagedStateActionModel(a.ID, input.Model, allowed)
		if resolvedModel == "" {
			resolvedModel = helps.ResolveStateModel(a.ID, input.Model)
			if !helps.StateTextModel(resolvedModel) || len(resolvedModel) > 256 || strings.ContainsAny(resolvedModel, "\r\n\x00") || !helps.ManagedStatePairAllowed(cfg, helps.StateCredential(a, resolvedModel)) {
				c.JSON(400, gin.H{"error": "model is outside the configured State model scope"})
				return
			}
			credential := helps.StateCredential(a, resolvedModel)
			credential.Route = resolvedModel
			var matched bool
			var previous uint64
			if input.Action == "acquire" {
				matched, previous = codexstate.Default.QueueManual(credential)
			} else if codexstate.Default.HasManual(credential) {
				matched, previous = codexstate.Default.ActionWithBaseline(a.ID, resolvedModel, input.Action)
			}
			if !matched {
				c.JSON(409, gin.H{"error": "State runtime is not ready or the manual model limit was reached"})
				return
			}
			c.JSON(200, gin.H{"model": resolvedModel, "previous_acquired": previous, "models": codexstate.Default.Snapshots(a.ID, time.Now())})
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
