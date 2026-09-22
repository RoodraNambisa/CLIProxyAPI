package management

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

// PreviewCodexResponseGuard evaluates an unsaved draft without inference,
// resource acquisition, statistics or affinity mutations.
func (h *Handler) PreviewCodexResponseGuard(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 512*1024)
	var input struct {
		Name          string                           `json:"name"`
		Model         string                           `json:"model"`
		ReturnedModel string                           `json:"returned_model"`
		StatePresent  bool                             `json:"state_present"`
		StateLength   int                              `json:"state_length"`
		Config        *config.CodexResponseGuardConfig `json:"config"`
	}
	if c.ShouldBindJSON(&input) != nil || input.Config == nil || strings.TrimSpace(input.Name) == "" || strings.TrimSpace(input.Model) == "" || len(input.Model) > 256 || len(input.ReturnedModel) > 256 || strings.ContainsAny(input.Model+input.ReturnedModel, "\r\n\x00") || input.StateLength < 0 || input.StateLength > 8192 || input.StatePresent && input.StateLength == 0 || !input.StatePresent && input.StateLength != 0 {
		c.JSON(400, gin.H{"error": "invalid response guard preview"})
		return
	}
	manager := h.coreAuthRuntimeManager()
	if manager == nil {
		c.JSON(503, gin.H{"error": "credential manager unavailable"})
		return
	}
	a := h.findManagedAuthWithManager(input.Name, manager)
	if a == nil {
		for _, candidate := range manager.List() {
			if candidate.Index == input.Name {
				a = candidate
				break
			}
		}
	}
	if a == nil {
		c.JSON(404, gin.H{"error": "credential not found"})
		return
	}
	if a.ExecutionProvider() != "codex" {
		c.JSON(400, gin.H{"error": "response guard requires Codex"})
		return
	}
	cfg, err := config.Clone(h.currentConfig())
	if err != nil || cfg == nil {
		c.JSON(500, gin.H{"error": "configuration unavailable"})
		return
	}
	cfg.Codex.ResponseGuard = *input.Config
	if err := cfg.ValidateCodexResponseGuard(); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	upstream := manager.PreviewUpstreamModel(a, input.Model)
	scope := helps.StateCredential(a, upstream).Scope()
	scope.Aliases = []string{input.Model}
	policy := cfg.Codex.ResponseGuard.PolicyFor(scope)
	verdict := policy.Evaluate(upstream, config.CodexResponseEvidence{Model: input.ReturnedModel, StatePresent: input.StatePresent, StateLength: input.StateLength}, true)
	outcome := "allowed"
	if policy.Mode == "off" {
		outcome = "disabled"
	} else if !verdict.Accepted() {
		outcome = "observed"
		if policy.Mode == "enforce" {
			outcome = "blocked"
		}
	}
	responseModel := manager.PreviewResponseModelName(a, input.Model, input.ReturnedModel)
	if outcome == "blocked" {
		responseModel = ""
	}
	state, _, managed := cfg.Codex.ManagedStateConfig().PolicyFor(scope)
	c.JSON(200, gin.H{"requested_model": input.Model, "upstream_model": upstream, "policy": policy, "verdict": verdict, "outcome": outcome, "response_model": responseModel, "managed": managed, "resource_acceptance": gin.H{"allowed_returned_models": state.AcceptedReturnedModels, "length_mode": state.ReturnedLengthMode, "lengths": state.Lengths, "invalidate_on_model_mismatch": state.InvalidateOnModelMismatch, "invalidate_on_state_length_mismatch": state.InvalidateOnStateLengthMismatch}})
}
