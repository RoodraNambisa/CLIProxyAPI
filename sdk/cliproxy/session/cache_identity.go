package session

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/tidwall/gjson"
)

// PromptCacheIdentity reads only the explicit request-envelope cache key.
// A digest preserves exact string equality without retaining or logging the key.
func PromptCacheIdentity(payload []byte) string {
	return promptCacheIdentity(gjson.GetBytes(payload, "prompt_cache_key"))
}

func promptCacheIdentity(value gjson.Result) string {
	if value.Type != gjson.String || value.Str == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value.Str))
	return "cache:" + hex.EncodeToString(digest[:])
}
