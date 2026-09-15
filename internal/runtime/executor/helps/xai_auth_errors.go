package helps

import (
	"strings"

	"github.com/tidwall/gjson"
)

// IsXAIBadCredentialsBody identifies invalidated tokens, not generic permission failures.
func IsXAIBadCredentialsBody(body []byte) bool {
	for _, path := range []string{"code", "error.code", "body.error.code"} {
		if strings.Contains(strings.ToLower(gjson.GetBytes(body, path).String()), "bad-credentials") {
			return true
		}
	}
	for _, path := range []string{"error", "error.message", "message", "body.error", "body.error.message"} {
		msg := strings.ToLower(gjson.GetBytes(body, path).String())
		if strings.Contains(msg, "access token could not be validated") {
			return true
		}
	}
	raw := strings.ToLower(string(body))
	return strings.Contains(raw, "bad-credentials") ||
		strings.Contains(raw, "access token could not be validated")
}
