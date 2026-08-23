package helps

import "net/http"

// OpenAIImageModerationErrorBody is the OpenAI-compatible error returned when
// an image request ends with an explicit moderation signal or terminal assistant
// text without image output.
const OpenAIImageModerationErrorBody = `{"error":{"message":"Your request was rejected by the safety system.","type":"image_generation_user_error","param":null,"code":"moderation_blocked"}}`

// OpenAIImageModerationError identifies a completed image request rejected by
// an explicit moderation signal or terminal assistant text without image output.
type OpenAIImageModerationError struct{}

func (*OpenAIImageModerationError) Error() string {
	return OpenAIImageModerationErrorBody
}

func (*OpenAIImageModerationError) StatusCode() int {
	return http.StatusBadRequest
}

func (*OpenAIImageModerationError) ExecutionResultErrorCode() string {
	return "moderation_blocked"
}

func (*OpenAIImageModerationError) SkipAuthResult() bool {
	return true
}

func (*OpenAIImageModerationError) RetryOtherAuth() bool {
	return false
}

// NewOpenAIImageModerationError creates an OpenAI-compatible moderation error.
func NewOpenAIImageModerationError() error {
	return &OpenAIImageModerationError{}
}
