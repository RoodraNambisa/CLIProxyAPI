package management

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// GetChatGPTWebCapabilities reports supported credential and transport features.
func (h *Handler) GetChatGPTWebCapabilities(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	images := config.ImagesConfig{}
	if cfg := h.currentConfig(); cfg != nil {
		images = cfg.Images
	}
	resolved := images.ResolvedChatGPTWebModelOutputFormats()
	aliases := make([]gin.H, 0, len(resolved))
	for _, model := range images.ResolvedChatGPTWebImageModels() {
		aliases = append(aliases, gin.H{"model": model, "output_formats": resolved[strings.ToLower(model)]})
	}
	defaults := make(map[string][]string)
	for _, model := range []string{"gpt-image-2.6", "gpt-image-2.6-flare", "gpt-image-2.6-sunburst"} {
		defaults[model] = config.DefaultChatGPTWebImageOutputFormats(model)
	}
	c.JSON(http.StatusOK, gin.H{
		"credential_schema_versions": []int{1, 2, 3},
		"features":                   []string{"webauthn_v1", "api798_login_v1", "advanced_account_security_v1", "tls_trust_anchors_v1", "image_model_output_formats_v1", "gif_aspect_ratio_exception_v1", "image_usage_profiles_v1", "image_n_parallel_v1"},
		"image_model_output_formats": gin.H{
			"supported_formats": config.ChatGPTWebImageOutputFormats(),
			"default_formats":   config.DefaultChatGPTWebImageOutputFormats(""),
			"default_overrides": defaults,
			"default_aliases":   (config.ImagesConfig{}).ResolvedChatGPTWebImageModels(),
			"aliases":           aliases,
		},
	})
}
