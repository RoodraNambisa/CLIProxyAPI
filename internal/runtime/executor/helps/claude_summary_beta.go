package helps

import (
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

// ApplyClaudeSummaryBeta removes the redaction beta when the final request
// carries an explicit thinking display. Run it after credential header overrides.
func ApplyClaudeSummaryBeta(headers http.Header, body []byte) {
	display := gjson.GetBytes(body, "thinking.display")
	if display.Type != gjson.String || strings.TrimSpace(display.String()) == "" {
		return
	}
	values := headers.Values("Anthropic-Beta")
	filtered := make([]string, 0, len(values))
	changed := false
	for _, value := range values {
		parts := strings.Split(value, ",")
		kept := parts[:0]
		for _, part := range parts {
			if strings.TrimSpace(part) == "redact-thinking-2026-02-12" {
				changed = true
				continue
			}
			kept = append(kept, part)
		}
		if len(kept) > 0 {
			filtered = append(filtered, strings.Join(kept, ","))
		}
	}
	if changed {
		headers.Del("Anthropic-Beta")
		for _, value := range filtered {
			headers.Add("Anthropic-Beta", value)
		}
	}
}
