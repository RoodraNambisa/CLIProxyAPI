package helps

import "testing"

func TestStreamUsageBufferSupportsResponseEnvelopesAndEscapedKeys(t *testing.T) {
	for _, tc := range []struct {
		payload string
		tokens  int64
		ok      bool
	}{
		{`{"usage":{"total_tokens":3}}`, 3, true},
		{`{"response":{"usage":{"total_tokens":3}}}`, 3, true},
		{`{"response":{"\u0075sage":{"total_tokens":3}}}`, 3, true},
		{`{"\u0075sage":{"total_tokens":3}}`, 3, true},
		{`{"usage":{"total_tokens":0},"response":{"usage":{"total_tokens":3}}}`, 0, true},
		{`{"usage":{},"response":{"usage":{"total_tokens":3}}}`, 3, true},
		{`{"output":[{"arguments":{"usage":{"total_tokens":3}}}]}`, 0, false},
		{`{"response":{"output":[]}}`, 0, false},
		{`{"response":{"usage":{"total_tokens":3}}`, 0, false},
		{`[DONE]`, 0, false},
	} {
		for _, prefix := range []string{"", "data: "} {
			var buffer StreamUsageBuffer
			buffer.ObserveOpenAIStream([]byte(prefix + tc.payload))
			detail, ok := buffer.Detail()
			if ok != tc.ok || detail.TotalTokens != tc.tokens {
				t.Fatal("usage envelope, precedence or validity boundary changed")
			}
		}
	}
}
