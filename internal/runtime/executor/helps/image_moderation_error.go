package helps

import "net/http"

// OpenAIImageModerationErrorBody is the OpenAI-compatible error returned when
// an image request completes with a text response instead of an image.
const OpenAIImageModerationErrorBody = `{"error":{"message":"Your request was rejected by the safety system.","type":"image_generation_user_error","param":null,"code":"moderation_blocked"}}`

// OpenAIImageModerationError identifies a completed image request that was
// answered with assistant text and no image output.
type OpenAIImageModerationError struct{}

func (*OpenAIImageModerationError) Error() string {
	return OpenAIImageModerationErrorBody
}

func (*OpenAIImageModerationError) StatusCode() int {
	return http.StatusBadRequest
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
