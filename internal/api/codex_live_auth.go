package api

import (
	"github.com/gin-gonic/gin"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
)

// Configured credentials keep their original precedence and access metadata.
// Only a missing or unrecognized credential may use local realtime admission.
func (s *Server) codexLiveAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		result, errAuth := s.accessManager.Authenticate(c.Request.Context(), c.Request)
		if errAuth == nil && result != nil {
			if result.Metadata[sdkaccess.MetadataCredentialTarget] != "" {
				c.AbortWithStatusJSON(400, gin.H{"error": "fixed credential tests are not supported for realtime sessions; use chat/completions or responses"})
				return
			}
			c.Set("apiKey", result.Principal)
			c.Set("accessProvider", result.Provider)
			if len(result.Metadata) > 0 {
				c.Set("accessMetadata", result.Metadata)
			}
			c.Next()
			return
		}
		if errAuth == nil || sdkaccess.IsAuthErrorCode(errAuth, sdkaccess.AuthErrorCodeNoCredentials) || sdkaccess.IsAuthErrorCode(errAuth, sdkaccess.AuthErrorCodeInvalidCredential) || sdkaccess.IsAuthErrorCode(errAuth, sdkaccess.AuthErrorCodeNotHandled) {
			grant, handled, errSecret := s.codexLive.AuthenticateClientSecret(c.Request)
			if handled {
				if errSecret != nil {
					s.codexLive.WriteClientSecretError(c, errSecret)
					return
				}
				if s.codexLive.ApplyClientSecretAuthorization(c, grant) {
					c.Next()
				}
				return
			}
		}
		if errAuth == nil {
			errAuth = sdkaccess.NewNoCredentialsError()
		}
		c.AbortWithStatusJSON(errAuth.HTTPStatusCode(), gin.H{"error": errAuth.Message})
	}
}
