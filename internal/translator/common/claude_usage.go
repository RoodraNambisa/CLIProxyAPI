package common

import (
	"math"
	"strconv"

	"github.com/tidwall/gjson"
)

// ClaudeUsage preserves partial Messages API usage fields without treating
// omitted fields as zero. OutputTokens already includes reported reasoning.
type ClaudeUsage struct {
	InputTokens, OutputTokens, CacheReadTokens, CacheCreationTokens, ReasoningTokens int64
	HasUsage, HasReasoning                                                           bool
}

func (u *ClaudeUsage) Merge(node gjson.Result) bool {
	if !node.IsObject() {
		return false
	}
	merged := false
	for _, field := range []struct {
		path   string
		target *int64
	}{
		{"input_tokens", &u.InputTokens}, {"output_tokens", &u.OutputTokens},
		{"cache_read_input_tokens", &u.CacheReadTokens}, {"cache_creation_input_tokens", &u.CacheCreationTokens},
	} {
		if value, ok := claudeUsageCount(node.Get(field.path)); ok {
			*field.target = value
			merged = true
		}
	}
	for _, path := range []string{"output_tokens_details.thinking_tokens", "output_tokens_details.reasoning_tokens", "thinking_tokens"} {
		value := node.Get(path)
		if !value.Exists() {
			continue
		}
		if count, ok := claudeUsageCount(value); ok {
			u.ReasoningTokens, u.HasReasoning, merged = count, true, true
		}
		break
	}
	u.HasUsage = u.HasUsage || merged
	return merged
}

func claudeUsageCount(value gjson.Result) (int64, bool) {
	if value.Type != gjson.Number {
		return 0, false
	}
	count, err := strconv.ParseInt(value.Raw, 10, 64)
	return count, err == nil && count >= 0
}

// Totals returns inclusive input, output and total billing counts.
func (u ClaudeUsage) Totals() (input, output, total int64) {
	input = claudeUsageSum(u.InputTokens, u.CacheReadTokens, u.CacheCreationTokens)
	output = u.OutputTokens
	return input, output, claudeUsageSum(input, output)
}

func claudeUsageSum(values ...int64) int64 {
	var total int64
	for _, value := range values {
		if value > math.MaxInt64-total {
			return math.MaxInt64
		}
		total += value
	}
	return total
}
