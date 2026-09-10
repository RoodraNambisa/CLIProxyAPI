package claude

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexClaudeCacheWriteUsage(t *testing.T) {
	for _, tc := range []struct {
		details string
		want    int64
	}{
		{`{"cached_tokens":2,"cache_write_tokens":3}`, 3},
		{`{"cached_tokens":2,"cache_creation_tokens":4}`, 4},
		{`{"cached_tokens":2,"cache_write_tokens":3,"cache_creation_tokens":4}`, 3},
		{`{"cached_tokens":2,"cache_write_tokens":0,"cache_creation_tokens":4}`, 4},
		{`{"cached_tokens":2,"cache_write_tokens":-1}`, 0},
		{`{"cached_tokens":2,"cache_write_tokens":0}`, 0},
		{`{"cached_tokens":2}`, 0},
	} {
		for _, status := range []string{"completed", "incomplete"} {
			t.Run(fmt.Sprintf("%s/%s", status, tc.details), func(t *testing.T) {
				event := []byte(fmt.Sprintf(`{"type":"response.%s","response":{"status":%q,"output":[],"usage":{"input_tokens":10,"output_tokens":5,"input_tokens_details":%s}}}`, status, status, tc.details))
				outputs := [][]byte{ConvertCodexResponseToClaudeNonStream(t.Context(), "", nil, nil, event, nil)}
				var state any
				for _, chunk := range ConvertCodexResponseToClaude(t.Context(), "", nil, nil, append([]byte("data: "), event...), &state) {
					for _, line := range strings.Split(string(chunk), "\n") {
						if strings.HasPrefix(line, "data: ") && gjson.Get(strings.TrimPrefix(line, "data: "), "type").String() == "message_delta" {
							outputs = append(outputs, []byte(strings.TrimPrefix(line, "data: ")))
						}
					}
				}
				if len(outputs) != 2 {
					t.Fatal("missing stream or non-stream usage")
				}
				for _, out := range outputs {
					usage := gjson.GetBytes(out, "usage")
					if usage.Get("input_tokens").Int() != 8 || usage.Get("output_tokens").Int() != 5 || usage.Get("cache_read_input_tokens").Int() != 2 || usage.Get("cache_creation_input_tokens").Int() != tc.want {
						t.Fatalf("unexpected usage: %s", usage.Raw)
					}
					if tc.want == 0 && usage.Get("cache_creation_input_tokens").Exists() {
						t.Fatal("absent or invalid cache-write count was fabricated")
					}
				}
			})
		}
	}
}
