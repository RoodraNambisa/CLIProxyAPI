package middleware

import (
	"context"
	"errors"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

const statusClientCanceled = 499

func isCanceledResponseError(message *interfaces.ErrorMessage) bool {
	if message == nil {
		return true
	}
	status := message.StatusCode
	if status <= 0 {
		var coded interface{ StatusCode() int }
		if errors.As(message.Error, &coded) && coded != nil {
			status = coded.StatusCode()
		}
	}
	// A real HTTP failure remains actionable even if its cause mentions cancellation.
	if status >= http.StatusBadRequest {
		return status == statusClientCanceled
	}
	return errors.Is(message.Error, context.Canceled)
}

func hasActionableResponseError(status int, messages []*interfaces.ErrorMessage) bool {
	for _, message := range messages {
		if !isCanceledResponseError(message) {
			return true
		}
	}
	return status >= http.StatusBadRequest && status != statusClientCanceled
}
