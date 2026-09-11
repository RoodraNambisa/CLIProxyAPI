package helps

import (
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// IsCodexUsageLimitError requires an explicit quota identifier on the active
// error object. Message text and arbitrary nested request data are not evidence.
func IsCodexUsageLimitError(body []byte) bool {
	return codexUsageLimitNode(body).Exists()
}

func codexUsageLimitNode(body []byte) gjson.Result {
	if !gjson.ValidBytes(body) {
		return gjson.Result{}
	}
	root := gjson.ParseBytes(body)
	node := root
	for _, path := range []string{"response.error", "body.error", "error"} {
		if candidate := root.Get(path); candidate.Exists() && candidate.Type != gjson.Null {
			node = candidate
			break
		}
	}
	for _, field := range []string{"type", "code"} {
		value := node.Get(field)
		if value.Type == gjson.String && strings.EqualFold(strings.TrimSpace(value.String()), "usage_limit_reached") {
			return node
		}
	}
	return gjson.Result{}
}

// CodexUsageLimitRetryAfter accepts the error envelopes used by HTTP, SSE and
// WebSocket. Absolute recovery time wins; invalid or elapsed time may fall back
// to positive relative seconds. Invalid hints leave the existing backoff intact.
func CodexUsageLimitRetryAfter(body []byte, now time.Time) *time.Duration {
	node := codexUsageLimitNode(body)
	if !node.Exists() {
		return nil
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if seconds, ok := codexQuotaResetSeconds(node.Get("resets_at")); ok {
		delay := time.Unix(seconds, 0).Sub(now)
		if delay > 0 && delay < maxDuration {
			return &delay
		}
	}
	if seconds, ok := codexQuotaResetSeconds(node.Get("resets_in_seconds")); ok && seconds <= int64(maxDuration/time.Second) {
		delay := time.Duration(seconds) * time.Second
		return &delay
	}
	return nil
}

func codexQuotaResetSeconds(value gjson.Result) (int64, bool) {
	if value.Type != gjson.Number && value.Type != gjson.String {
		return 0, false
	}
	seconds, err := strconv.ParseInt(strings.TrimSpace(value.String()), 10, 64)
	return seconds, err == nil && seconds > 0
}
