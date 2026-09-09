package common

import (
	"math"

	"github.com/tidwall/gjson"
)

// GeminiOutputTokens includes thoughts in OpenAI's inclusive output count.
func GeminiOutputTokens(usage gjson.Result) int64 {
	var total int64
	for _, field := range []string{"candidatesTokenCount", "thoughtsTokenCount"} {
		count := usage.Get(field).Int()
		if count <= 0 {
			continue
		}
		if count > math.MaxInt64-total {
			return math.MaxInt64
		}
		total += count
	}
	return total
}
