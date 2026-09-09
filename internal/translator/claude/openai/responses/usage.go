package responses

import (
	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/tidwall/sjson"
)

func claudeResponsesUsage(u translatorcommon.ClaudeUsage, reasoningBytes int) []byte {
	input, output, total := u.Totals()
	reasoning := u.ReasoningTokens
	if !u.HasReasoning {
		// Preserve the existing response-only estimate when no count was reported.
		reasoning = int64(reasoningBytes / 4)
	}
	result := []byte(`{"input_tokens":0,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":0}`)
	result, _ = sjson.SetBytes(result, "input_tokens", input)
	result, _ = sjson.SetBytes(result, "input_tokens_details.cached_tokens", u.CacheReadTokens)
	result, _ = sjson.SetBytes(result, "input_tokens_details.cache_write_tokens", u.CacheCreationTokens)
	result, _ = sjson.SetBytes(result, "output_tokens", output)
	result, _ = sjson.SetBytes(result, "output_tokens_details.reasoning_tokens", reasoning)
	result, _ = sjson.SetBytes(result, "total_tokens", total)
	return result
}
