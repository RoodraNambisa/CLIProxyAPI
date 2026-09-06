package util

import (
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const promptCacheLogContextKey = "prompt_cache_log_redactor"

func PromptCacheLogForGin(c *gin.Context) *PromptCacheLogRedactor {
	if c == nil {
		return nil
	}
	value, _ := c.Get(promptCacheLogContextKey)
	redactor, _ := value.(*PromptCacheLogRedactor)
	return redactor
}

// RegisterPromptCacheLogPolicy updates the diagnostic policy for this request or
// WebSocket turn. It retains a key, never the source payload, and is independent
// of the wire passthrough setting. Missing keys clear the previous turn's policy.
func RegisterPromptCacheLogPolicy(c *gin.Context, payload []byte) *PromptCacheLogRedactor {
	if existing := PromptCacheLogForGin(c); existing != nil {
		key := gjson.GetBytes(payload, "prompt_cache_key")
		if key.Type == gjson.String && existing.ProtectsKey(key.Str) {
			return existing
		}
	}
	redactor := PromptCacheLogRedactorForRequest(payload)
	if c == nil {
		return redactor
	}
	c.Set(promptCacheLogContextKey, redactor)
	if writer, ok := c.Writer.(interface{ SetPromptCacheLogRedactor(*PromptCacheLogRedactor) }); ok {
		writer.SetPromptCacheLogRedactor(redactor)
	}
	return redactor
}
