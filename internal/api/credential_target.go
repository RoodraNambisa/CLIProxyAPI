package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func applyCredentialTarget(c *gin.Context, manager *coreauth.Manager, result *sdkaccess.Result) bool {
	if result == nil || result.Metadata[sdkaccess.MetadataCredentialTarget] == "" {
		return true
	}
	selected, err := manager.ResolveCredentialTarget(result.Metadata[sdkaccess.MetadataCredentialTarget])
	if err != nil {
		status, code := http.StatusBadRequest, "invalid_credential_target"
		var targetError *coreauth.Error
		if errors.As(err, &targetError) {
			status, code = targetError.HTTPStatus, targetError.Code
		}
		c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error(), "type": "invalid_request_error"}})
		return false
	}
	ctx := context.WithValue(c.Request.Context(), "gin", c)
	if providers := result.Metadata[sdkaccess.MetadataAllowedProviders]; providers != "" {
		allowed := false
		for _, provider := range strings.Split(providers, ",") {
			allowed = allowed || strings.EqualFold(strings.TrimSpace(provider), selected.ExecutionProvider())
		}
		if !allowed {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": gin.H{"code": "provider_not_allowed", "message": "API key cannot access this credential provider", "type": "permission_error"}})
			return false
		}
	}
	if !manager.ClientAPIKeyAllowsCredential(ctx, selected) {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": gin.H{"code": "credential_not_allowed", "message": "API key cannot access this credential priority", "type": "permission_error"}})
		return false
	}
	c.Set(sdkaccess.CredentialTargetAuthIDContextKey, selected.ID)
	for _, option := range []string{sdkaccess.MetadataCredentialTargetRespectStatePolicy, sdkaccess.MetadataCredentialTargetRespectRequestLimit, sdkaccess.MetadataCredentialTargetResponseModelRewrite} {
		c.Set(option, result.Metadata[option])
	}
	c.Header("X-CLIProxy-Auth-ID", selected.Index)
	return true
}
