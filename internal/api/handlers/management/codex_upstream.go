package management

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func (h *Handler) applyCodexUpstreamInfo(entry gin.H, auth *coreauth.Auth) {
	upstream := helps.ResolveCodexUpstream(auth, h.currentConfig())
	entry["base_url"] = ""
	if upstream.Source == "credential" {
		entry["base_url"] = upstream.BaseURL
	}
	entry["upstream_base_url"] = upstream.BaseURL
	entry["upstream_source"] = upstream.Source
}
