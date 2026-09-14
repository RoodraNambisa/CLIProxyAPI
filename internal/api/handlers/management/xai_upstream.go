package management

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

// applyXAIUpstreamInfo exposes routing state without writing resolved global
// defaults into the credential itself.
func (h *Handler) applyXAIUpstreamInfo(entry gin.H, auth *coreauth.Auth) {
	upstream := helps.ResolveXAIUpstream(auth, h.currentConfig())
	entry["base_url"] = ""
	if upstream.Source == "credential" {
		entry["base_url"] = upstream.BaseURL
	}
	entry["upstream_base_url"] = upstream.BaseURL
	entry["upstream_mode"] = upstream.Mode
	entry["upstream_source"] = upstream.Source
}
