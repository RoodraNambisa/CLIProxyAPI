package common

import (
	"math"
	"testing"

	"github.com/tidwall/gjson"
)

func TestInteractionsUsageCountPresenceAndBounds(t *testing.T) {
	for _, tc := range []struct {
		payload          string
		input, output    int64
		hasInput, hasOut bool
	}{
		{`{"total_tokens":9}`, 0, 0, false, false},
		{`{"tool_use_tokens":5}`, 5, 0, true, false},
		{`{"total_thought_tokens":3}`, 0, 3, false, true},
		{`{"input_tokens":0,"total_input_tokens":10,"output_tokens":0,"total_output_tokens":10,"reasoning_tokens":3}`, 0, 3, true, true},
		{`{"input_tokens":9223372036854775807,"tool_use_tokens":1,"output_tokens":9223372036854775807,"reasoning_tokens":1}`, math.MaxInt64, math.MaxInt64, true, true},
		{`{"input_tokens":-1,"tool_use_tokens":5,"output_tokens":-1,"reasoning_tokens":3}`, 5, 3, true, true},
	} {
		input, hasInput := InteractionsInputTokens(gjson.Parse(tc.payload))
		output, hasOut := InteractionsOutputTokens(gjson.Parse(tc.payload))
		if input != tc.input || hasInput != tc.hasInput || output != tc.output || hasOut != tc.hasOut {
			t.Fatal("partial counts, explicit aliases or integer bounds changed")
		}
	}
	for _, tc := range []struct{ output, reasoning, want int64 }{
		{5, 3, 2}, {5, 0, 5}, {5, -1, 5}, {5, 7, 0}, {-1, 0, 0}, {math.MaxInt64, 3, math.MaxInt64 - 3},
	} {
		if InteractionsNonReasoningTokens(tc.output, tc.reasoning) != tc.want {
			t.Fatal("inclusive output subtraction produced an invalid independent count")
		}
	}
}
