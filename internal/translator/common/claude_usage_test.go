package common

import (
	"math"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeUsageMergesPresenceAndReasoningAliases(t *testing.T) {
	var u ClaudeUsage
	u.Merge(gjson.Parse(`{"input_tokens":2,"output_tokens":0,"cache_read_input_tokens":4,"cache_creation_input_tokens":5}`))
	u.Merge(gjson.Parse(`{"output_tokens":3,"output_tokens_details":{"thinking_tokens":2,"reasoning_tokens":99},"thinking_tokens":98}`))
	if input, output, total := u.Totals(); input != 11 || output != 3 || total != 14 || u.ReasoningTokens != 2 || !u.HasReasoning || !u.HasUsage {
		t.Fatal("partial usage lost cache counts or counted thinking twice")
	}
	u.Merge(gjson.Parse(`{"input_tokens":0,"cache_read_input_tokens":0,"output_tokens_details":{"thinking_tokens":0},"thinking_tokens":9}`))
	if input, _, _ := u.Totals(); input != 5 || u.ReasoningTokens != 0 {
		t.Fatal("explicit zero did not replace previous usage")
	}
	before := u
	for _, invalid := range []string{`null`, `[]`, `{}`, `{"input_tokens":-1}`, `{"output_tokens":1.5}`, `{"cache_creation_input_tokens":9223372036854775808}`, `{"thinking_tokens":"8"}`, `{"output_tokens_details":{"thinking_tokens":null,"reasoning_tokens":99}}`} {
		if u.Merge(gjson.Parse(invalid)) || u != before {
			t.Fatal("invalid usage replaced known counts or bypassed alias precedence")
		}
	}
	for _, body := range []string{`{"output_tokens_details":{"reasoning_tokens":4}}`, `{"thinking_tokens":4}`} {
		var alias ClaudeUsage
		if !alias.Merge(gjson.Parse(body)) || alias.ReasoningTokens != 4 {
			t.Fatal("supported reasoning alias was lost")
		}
	}
	u.Merge(gjson.Parse(`{"input_tokens":9223372036854775807,"output_tokens":9223372036854775807}`))
	if input, _, total := u.Totals(); input != math.MaxInt64 || total != math.MaxInt64 {
		t.Fatal("usage sum overflowed")
	}
}
