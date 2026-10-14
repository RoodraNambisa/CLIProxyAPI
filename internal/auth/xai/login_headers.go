package xai

import "net/http"

// ApplyLoginHeaders is shared by acquisition and the offline header preview.
func ApplyLoginHeaders(req *http.Request, version string, device bool) {
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-grok-client-version", version)
	if device {
		req.Header.Set("x-grok-client-surface", "ui")
	}
}
