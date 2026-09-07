package session

import (
	"net/http"

	"github.com/tidwall/gjson"
)

// A frame carrying identity metadata owns its complete lineage. In particular,
// missing parent fields must not inherit an earlier handshake's parent or fork.
func codexFrameIdentityHeaders(headers http.Header, roots []gjson.Result) http.Header {
	for _, root := range roots {
		metadata := root.Get("client_metadata")
		if !metadata.IsObject() {
			continue
		}
		frame := make(http.Header)
		present := false
		for _, field := range [][2]string{
			{"session_id", "Session-Id"},
			{"thread_id", "Thread-Id"},
			{"x-codex-turn-metadata", "X-Codex-Turn-Metadata"},
			{"x-codex-parent-thread-id", "X-Codex-Parent-Thread-Id"},
			{"x-openai-subagent", "X-Openai-Subagent"},
		} {
			value := metadata.Get(field[0])
			if !value.Exists() {
				continue
			}
			present = true
			if value.Type == gjson.String {
				frame.Set(field[1], value.Str)
			}
		}
		if present {
			return frame
		}
	}
	return headers
}
