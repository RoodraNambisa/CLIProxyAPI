package executor

import "errors"

// PreserveErrorResponse honors deliberate local admission responses.
func PreserveErrorResponse(err error) bool {
	var marker interface{ PreserveErrorResponse() bool }
	return errors.As(err, &marker) && marker.PreserveErrorResponse()
}
