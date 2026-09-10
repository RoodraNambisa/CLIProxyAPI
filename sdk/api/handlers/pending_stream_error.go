package handlers

import "github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"

// PendingStreamError checks queued failures before a closed data stream commits
// an empty success response. It never waits for a future error or channel close.
func PendingStreamError(errs <-chan *interfaces.ErrorMessage) (*interfaces.ErrorMessage, bool) {
	if errs != nil {
		select {
		case err, ok := <-errs:
			return err, ok
		default:
		}
	}
	return nil, false
}
