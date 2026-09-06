package helps

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
)

// CodexWebsocketSessionLogID keeps diagnostics stable without exposing a session
// ID that may also be a cache key from an earlier turn on the same connection.
func CodexWebsocketSessionLogID(sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(sessionID))
	return fmt.Sprintf("sha256:%x", digest[:8])
}

// CodexWebsocketLogError protects a close reason before the websocket package
// adds its prefix, so a short scalar key is handled like other diagnostic values.
func CodexWebsocketLogError(err error, redactor *util.PromptCacheLogRedactor) string {
	if err == nil {
		return ""
	}
	detail := err.Error()
	var closed *websocket.CloseError
	if redactor != nil && errors.As(err, &closed) && closed != nil {
		masked := *closed
		masked.Text = redactor.Redact(closed.Text)
		detail = strings.Replace(detail, closed.Error(), masked.Error(), 1)
	}
	return redactor.Redact(detail)
}
