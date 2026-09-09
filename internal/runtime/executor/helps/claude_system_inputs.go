package helps

import (
	"context"
	"fmt"
	"net/http"

	"github.com/tidwall/gjson"
)

type claudeSystemInputError struct{ index int }

func (e *claudeSystemInputError) Error() string {
	return fmt.Sprintf(`{"error":{"type":"invalid_request_error","code":"claude_system_content_unsupported","message":"system.%d must be a text block. Move non-text content into a user message."}}`, e.index)
}
func (*claudeSystemInputError) StatusCode() int      { return http.StatusBadRequest }
func (*claudeSystemInputError) SkipAuthResult() bool { return true }
func (*claudeSystemInputError) RetryOtherAuth() bool { return false }

// ValidateClaudeSystemInputs rejects unsupported operator content before local
// cloaking could silently discard it. Errors never include the content itself.
func ValidateClaudeSystemInputs(ctx context.Context, body []byte) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	system := gjson.GetBytes(body, "system")
	if !system.IsArray() {
		return nil
	}
	for index, part := range system.Array() {
		if part.Get("type").String() != "text" {
			return &claudeSystemInputError{index: index}
		}
	}
	return nil
}
