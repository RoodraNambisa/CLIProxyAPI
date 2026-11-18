package management

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func applyProviderCredentialSummary(entry gin.H, a *coreauth.Auth) {
	if a == nil || a.Attributes["config_id"] == "" {
		entry["entry_kind"] = "file"
		return
	}
	entry["entry_kind"] = "api"
	entry["display_name"] = a.Label
	entry["name"] = a.ID
	entry["runtime_only"] = false
	entry["base_url"] = a.Attributes["base_url"]
	entry["auth_mode"] = "api-key"
	entry["config_ref"] = gin.H{"id": a.Attributes["config_id"], "kind": a.Attributes["config_kind"], "parent_id": a.Attributes["config_parent_id"], "parent_name": a.Attributes["config_parent_name"], "revision": a.Attributes["config_revision"]}
	count, _ := strconv.Atoi(a.Attributes["config_models_count"])
	if registered, ok := registry.GetGlobalRegistry().ClientModelCount(a.ID); ok {
		count = registered
	}
	entry["models_count"] = count
	key := a.Attributes["api_key"]
	mask := ""
	if key != "" {
		mask = "••••"
		if len(key) > 8 {
			mask = key[:3] + "••••" + key[len(key)-4:]
		}
	}
	entry["api_key_mask"] = mask
	entry["capabilities"] = gin.H{"edit": true, "models": true, "download": false, "refresh_token": false}
}
