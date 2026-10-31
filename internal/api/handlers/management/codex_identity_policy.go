package management

import (
	"github.com/gin-gonic/gin"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"net/http"
	"strings"
)

// PreviewCodexIdentityPolicy uses the indexed credential snapshot only. It does
// not prepare fingerprint pools, acquire routes, or perform upstream requests.
func (h *Handler) PreviewCodexIdentityPolicy(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 32<<10)
	var input struct {
		Index string                        `json:"auth_index"`
		Draft *config.CodexIdentityOverride `json:"codex"`
	}
	if c.ShouldBindJSON(&input) != nil || input.Index == "" || len(input.Index) > 256 {
		c.JSON(400, gin.H{"error": "invalid identity preview"})
		return
	}
	cfg := h.currentConfig()
	manager := h.coreAuthRuntimeManager()
	if cfg == nil || manager == nil {
		c.JSON(503, gin.H{"error": "configuration or credential manager unavailable"})
		return
	}
	auth, ok := manager.GetByAuthIndex(input.Index)
	if !ok || auth == nil {
		c.JSON(404, gin.H{"error": "credential not found"})
		return
	}
	if auth.ExecutionProvider() != "codex" {
		c.JSON(400, gin.H{"error": "identity policy requires Codex"})
		return
	}
	priority := helps.CodexIdentityPriority(auth)
	draft := *cfg
	if input.Draft != nil {
		if err := input.Draft.Validate(); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		draft.Routing.PriorityOverrides = append([]config.RoutingPriorityOverride(nil), cfg.Routing.PriorityOverrides...)
		found := false
		for i := range draft.Routing.PriorityOverrides {
			if draft.Routing.PriorityOverrides[i].Priority == priority {
				draft.Routing.PriorityOverrides[i].Codex = input.Draft
				found = true
				break
			}
		}
		if !found {
			draft.Routing.PriorityOverrides = append(draft.Routing.PriorityOverrides, config.RoutingPriorityOverride{Priority: priority, Codex: input.Draft})
		}
	}
	policies := manager.ActiveCodexIdentityPolicies()
	if policies == nil || input.Draft != nil {
		policies = config.CompileCodexIdentityPolicies(&draft)
	}
	policy := helps.ResolveCodexIdentityPolicy(policies, auth)
	applicable := true
	reason := "active"
	if !policy.IdentityConfuse {
		reason = "disabled"
	} else if !policy.ConfuseEffective {
		reason = "session_affinity_required"
	}
	software := codexauth.ResolveSoftwareIdentity(policy.UserAgent)
	_, credentialMode := auth.Metadata[codexauth.FingerprintModeMetadataKey]
	sources := policies.Sources(priority, credentialMode, helps.CodexCredentialUserAgent(auth) != "")
	if strings.TrimSpace(auth.Attributes["api_key"]) != "" {
		for _, key := range []string{"spoof-session-identity", "fingerprint-mode", "enforce-software-identity"} {
			sources[key] = "route"
		}
	}
	c.JSON(http.StatusOK, gin.H{"auth_index": auth.Index, "priority": priority, "policy": policy, "sources": sources, "version": policies.Version, "applicable": applicable, "reason": reason, "software": software, "protected_fields": func() []string {
		if policy.PassthroughPromptCacheKey {
			return []string{"prompt_cache_key", "session_id"}
		}
		return []string{}
	}(), "migration": "fill-first requires session-affinity unless identity-confuse-require-session-affinity is false"})
}
