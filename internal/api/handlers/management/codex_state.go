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
	manager := codexstate.Default
	if c.Query("diagnostic") == "true" {
		if !helps.StateCredentialAvailable(a) {
			c.JSON(400, gin.H{"error": "manual State acquisition requires an enabled Codex OAuth credential"})
			return
		}
		manager = codexstate.Diagnostic
	}
	c.JSON(200, gin.H{"models": manager.Snapshots(a.ID, time.Now()), "cookie": manager.CookieSnapshot(a.ID, time.Now(), helps.ResolveStateModel(a.ID, c.Query("model"))), "cookies": manager.CookieSnapshots(a.ID, time.Now())})
}

func (h *Handler) CodexStateAction(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	var input struct {
		Name       string  `json:"name"`
		Model      string  `json:"model"`
		Action     string  `json:"action"`
		Diagnostic bool    `json:"diagnostic"`
		Strategy   string  `json:"strategy"`
		CookiePool *string `json:"cookie_pool"`
	}
	if c.ShouldBindJSON(&input) != nil || input.Strategy != "" && input.Strategy != "state" && input.Strategy != "cookie-only" || !slices.Contains([]string{"acquire", "pause", "resume", "clear"}, input.Action) {
		c.JSON(400, gin.H{"error": "invalid state action"})
		return
	}
	if input.Strategy == "" {
		input.Strategy = "state"
	}
	a := h.findManagedAuthWithManager(input.Name, h.authManager)
	if a == nil {
		c.JSON(404, gin.H{"error": "credential not found"})
		return
	}
	cfg := h.currentConfig()
	if input.Diagnostic {
		if input.Action != "acquire" || !helps.StateCredentialAvailable(a) {
			c.JSON(400, gin.H{"error": "manual State acquisition requires an enabled Codex OAuth credential and the acquire action"})
			return
		}
		model := helps.ResolveStateModel(a.ID, input.Model)
		if !helps.StateTextModel(model) || len(model) > 256 || strings.ContainsAny(model, "\r\n\x00") {
			c.JSON(400, gin.H{"error": "manual State acquisition requires a text model"})
			return
		}
		for _, info := range registry.GetGlobalRegistry().GetModelsForClient(a.ID) {
			if info != nil && helps.ResolveStateModel(a.ID, info.ID) == model && len(info.SupportedOutputModalities) > 0 && !slices.ContainsFunc(info.SupportedOutputModalities, func(value string) bool { return strings.EqualFold(value, "text") }) {
				c.JSON(400, gin.H{"error": "manual State acquisition requires a text model"})
				return
			}
		}
		credential := helps.StateCredential(a, model)
		credential.Route = strings.TrimSpace(input.Model)
		matched, previous := codexstate.Diagnostic.QueueManualStrategy(credential, input.Strategy)
		if !matched {
			c.JSON(409, gin.H{"error": "State runtime is not ready or the manual model limit was reached"})
			return
		}
		c.JSON(200, gin.H{"diagnostic": true, "model": model, "previous_acquired": previous, "models": codexstate.Diagnostic.Snapshots(a.ID, time.Now()), "cookie": codexstate.Diagnostic.CookieSnapshot(a.ID, time.Now(), model), "cookies": codexstate.Diagnostic.CookieSnapshots(a.ID, time.Now())})
		return
	}
	if !helps.ManagedStateCredentialEligible(cfg, a) {
		c.JSON(400, gin.H{"error": "State is disabled or this credential is outside the configured priority/credential scope"})
		return
	}
	if input.Strategy == "cookie-only" {
		model := helps.ResolveStateModel(a.ID, input.Model)
		if input.Model == "" {
			model = ""
		}
		var pools []string
		if input.CookiePool != nil {
			pools = append(pools, *input.CookiePool)
		}
		matched, previous := codexstate.Default.CookieAction(a.ID, model, input.Action, pools...)
		if !matched {
			c.JSON(409, gin.H{"error": "Cookie runtime is not ready or model is outside scope"})
			return
		}
		c.JSON(200, gin.H{"strategy": "cookie-only", "previous_acquired": previous, "models": codexstate.Default.Snapshots(a.ID, time.Now()), "cookie": codexstate.Default.CookieSnapshot(a.ID, time.Now(), model), "cookies": codexstate.Default.CookieSnapshots(a.ID, time.Now())})
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
