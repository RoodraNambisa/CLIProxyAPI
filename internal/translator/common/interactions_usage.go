package common

import "github.com/tidwall/gjson"

// InteractionsUsage returns usage from the supported Interactions response envelopes.
func InteractionsUsage(root gjson.Result) gjson.Result {
	for _, path := range []string{
		"interaction.usage",
		"usage",
		"metadata.total_usage",
		"metadata.usage",
		"interaction.metadata.total_usage",
		"interaction.metadata.usage",
	} {
		if value := root.Get(path); value.Exists() {
			return value
		}
	}
	return gjson.Result{}
}

// InteractionsInputTokens folds separately reported tool prompts into OpenAI's input total.
func InteractionsInputTokens(usage gjson.Result) (int64, bool) {
	input := firstInteractionsCount(usage, "input_tokens", "total_input_tokens")
	tool := firstInteractionsCount(usage, "tool_use_tokens", "total_tool_use_tokens", "toolUseTokens", "totalToolUseTokens")
	return SumPositiveTokenCounts(input.Int(), tool.Int()), input.Exists() || tool.Exists()
}

// InteractionsOutputTokens folds reasoning into OpenAI's output total.
func InteractionsOutputTokens(usage gjson.Result) (int64, bool) {
	output := firstInteractionsCount(usage, "output_tokens", "total_output_tokens")
	reasoning := firstInteractionsCount(usage, "reasoning_tokens", "total_thought_tokens")
	return SumPositiveTokenCounts(output.Int(), reasoning.Int()), output.Exists() || reasoning.Exists()
}

// InteractionsNonReasoningTokens converts an inclusive OpenAI output total into
// the independent output bucket used alongside Interactions reasoning counts.
func InteractionsNonReasoningTokens(output, reasoning int64) int64 {
	if output <= 0 || reasoning >= output {
		return 0
	}
	return output - max(0, reasoning)
}

func firstInteractionsCount(usage gjson.Result, paths ...string) gjson.Result {
	for _, path := range paths {
		if value := usage.Get(path); value.Exists() {
			return value
		}
	}
	return gjson.Result{}
}
