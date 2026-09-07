package executor

import "net/http"

// UpstreamWebsocketReplayRequiredError rejects unsafe incremental replay without
// changing credential health or consuming another credential attempt.
type UpstreamWebsocketReplayRequiredError struct{ cause error }

func (*UpstreamWebsocketReplayRequiredError) Error() string {
	return `{"error":{"message":"Previous response is not available on this connection; resend the full conversation input without previous_response_id","type":"invalid_request_error","code":"previous_response_not_found","param":"previous_response_id"}}`
}

func (*UpstreamWebsocketReplayRequiredError) StatusCode() int      { return http.StatusBadRequest }
func (*UpstreamWebsocketReplayRequiredError) SkipAuthResult() bool { return true }
func (*UpstreamWebsocketReplayRequiredError) RetryOtherAuth() bool { return false }
func (e *UpstreamWebsocketReplayRequiredError) Unwrap() error      { return e.cause }

func NewUpstreamWebsocketReplayRequiredError(causes ...error) error {
	err := &UpstreamWebsocketReplayRequiredError{}
	if len(causes) > 0 {
		err.cause = causes[0]
	}
	return err
}
