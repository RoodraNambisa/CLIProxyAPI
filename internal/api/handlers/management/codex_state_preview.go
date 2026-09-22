package management

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

// PreviewCodexState resolves an unsaved draft without persisting settings,
// changing State entries or making upstream requests.
func (h *Handler) PreviewCodexState(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 512*1024)
	var input struct {
		Name   string                           `json:"name"`
		Model  string                           `json:"model"`
		Config *config.CodexStateOverrideConfig `json:"config"`
	}
	if c.ShouldBindJSON(&input) != nil || input.Config == nil || strings.TrimSpace(input.Name) == "" || strings.TrimSpace(input.Model) == "" || len(input.Model) > 256 || strings.ContainsAny(input.Model, "\r\n\x00") {
		c.JSON(400, gin.H{"error": "invalid State preview"})
		return
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
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
	cfg, err := config.Clone(h.currentConfig())
	if err != nil || cfg == nil {
		c.JSON(500, gin.H{"error": "configuration unavailable"})
		return
	}
	cfg.Codex.StateOverride = *input.Config
	if err := cfg.ValidateCodexStateOverride(); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	upstream := helps.ResolveStateModel(a.ID, input.Model)
	credential := helps.StateCredential(a, upstream)
	policy, match, managed := cfg.Codex.StateOverride.PolicyFor(credential.Scope())
	if !helps.ManagedStateCredentialEligible(cfg, a) || !helps.StateTextModel(upstream) {
		managed = false
	}
	registered := len(credential.Aliases) > 0
	// Return only effective policy fields that are useful to the UI; no tokens,
	// proxy passwords, test prompt text or raw State are included.
	c.JSON(200, gin.H{"managed": managed, "registered": registered, "upstream_model": upstream, "match": match, "policy": gin.H{
		"strategy": policy.Strategy, "cookie-verify-after-acquire": policy.CookieVerifyAfterAcquire, "cookie-max-age-seconds": policy.CookieMaxAgeSeconds, "cookie-refresh-before-seconds": policy.CookieRefreshBeforeSeconds, "ttl-seconds": int(policy.StateTTL().Seconds()), "refresh-before-seconds": int(policy.StateRefreshBefore().Seconds()), "missing-returned-state": policy.MissingReturnedState,
		"lengths": policy.Lengths, "match-model": policy.MatchModel, "acquisition": policy.Acquisition,
		"mode": policy.Mode, "missing-policy": policy.MissingPolicy, "retry-seconds": policy.RetrySeconds,
		"max-attempts": policy.MaxAttempts, "retry-round-interval-minutes": policy.RetryRoundIntervalMinutes, "max-retry-rounds": policy.MaxRetryRounds, "ttl-minutes": policy.TTLMinutes, "refresh-before-minutes": policy.RefreshBeforeMinutes,
		"proxy-mode": policy.ProxyMode, "invalidate-on-state-length-mismatch": policy.InvalidateOnStateLengthMismatch, "invalidate-on-model-mismatch": policy.InvalidateOnModelMismatch,
	}})
}
