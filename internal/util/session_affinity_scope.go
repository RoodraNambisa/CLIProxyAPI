package util

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/gin-gonic/gin"
)

// AuthenticatedSessionScope partitions optional affinity inference by the
// authenticated caller and access metadata. Client headers cannot supply it.
// Empty means that a reliable shared caller scope could not be established.
func AuthenticatedSessionScope(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	c, _ := ctx.Value("gin").(*gin.Context)
	if c == nil {
		return ""
	}
	principal, provider := c.GetString("apiKey"), c.GetString("accessProvider")
	if strings.TrimSpace(principal) == "" || strings.TrimSpace(provider) == "" {
		return ""
	}
	metadata, _ := c.Get("accessMetadata")
	raw, err := json.Marshal([]any{"session-affinity-caller-v1", provider, principal, metadata})
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
