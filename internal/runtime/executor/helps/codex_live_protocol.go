package helps

import "net/http"

// CodexLiveNativeRouteError prevents native realtime requests from entering
// Responses conversion, token counting, compact or Responses WebSocket state.
type CodexLiveNativeRouteError struct{}

func (CodexLiveNativeRouteError) Error() string {
	return "Codex Live requires a native realtime endpoint"
}

func (CodexLiveNativeRouteError) StatusCode() int      { return http.StatusNotImplemented }
func (CodexLiveNativeRouteError) SkipAuthResult() bool { return true }
