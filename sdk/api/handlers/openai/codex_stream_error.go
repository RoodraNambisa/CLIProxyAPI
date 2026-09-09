package openai

import (
	"strings"

	"github.com/gin-gonic/gin"
)

func isCodexResponsesClientRequest(c *gin.Context) bool {
	if c == nil || c.Request == nil {
		return false
	}
	userAgent := strings.TrimSpace(c.GetHeader("User-Agent"))
	if userAgent == "codex_cli_rs" || strings.HasPrefix(userAgent, "codex_cli_rs/") || strings.HasPrefix(userAgent, "codex-tui/") || strings.HasPrefix(userAgent, "Codex Desktop/") {
		return true
	}
	originator := strings.ToLower(strings.TrimSpace(c.GetHeader("Originator")))
	for _, client := range []string{"codex_cli_rs", "codex-tui", "codex desktop"} {
		if originator == client || strings.HasPrefix(originator, client+"/") {
			return true
		}
	}
	return false
}
