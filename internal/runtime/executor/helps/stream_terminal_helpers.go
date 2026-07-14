package helps

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

type streamProtocolError struct {
	provider string
	message  string
}

func (e streamProtocolError) Error() string {
	return fmt.Sprintf("%s stream protocol error: %s", e.provider, e.message)
}

func (streamProtocolError) StatusCode() int { return http.StatusBadGateway }

// IncompleteStreamError reports an upstream stream that closed without a
// protocol-defined successful terminal event.
func IncompleteStreamError(provider string) error {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		provider = "upstream"
	}
	return fmt.Errorf("%s stream ended without a successful terminal event", provider)
}

// JSONStreamProtocolError preserves the upstream error message from an SSE payload.
func JSONStreamProtocolError(provider string, payload []byte) error {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		provider = "upstream"
	}
	message := ""
	if gjson.ValidBytes(payload) {
		for _, path := range []string{"error.message", "response.error.message", "message"} {
			if value := strings.TrimSpace(gjson.GetBytes(payload, path).String()); value != "" {
				message = value
				break
			}
		}
		if message == "" {
			for _, path := range []string{"error", "response.error"} {
				if value := gjson.GetBytes(payload, path); value.Exists() {
					message = strings.TrimSpace(value.Raw)
					break
				}
			}
		}
	}
	if message == "" {
		message = strings.TrimSpace(string(payload))
	}
	if message == "" {
		message = "upstream reported an error event"
	}
	return streamProtocolError{provider: provider, message: message}
}

// IsOpenAIStreamTerminal reports whether line is the explicit OpenAI SSE terminator.
func IsOpenAIStreamTerminal(line []byte) bool {
	line = bytes.TrimSpace(line)
	if bytes.HasPrefix(line, []byte("data:")) {
		line = bytes.TrimSpace(line[len("data:"):])
	}
	return bytes.Equal(line, []byte("[DONE]"))
}

// IsClaudeStreamTerminal reports whether line contains Anthropic's message_stop event.
func IsClaudeStreamTerminal(line []byte) bool {
	payload := JSONPayload(line)
	return gjson.ValidBytes(payload) && gjson.GetBytes(payload, "type").String() == "message_stop"
}

// IsGeminiStreamTerminal reports whether every returned candidate has a finish reason.
func IsGeminiStreamTerminal(payload []byte) bool {
	if IsJSONStreamProtocolError(payload) {
		return false
	}
	blockReason := gjson.GetBytes(payload, "promptFeedback.blockReason")
	if !blockReason.Exists() {
		blockReason = gjson.GetBytes(payload, "response.promptFeedback.blockReason")
	}
	if blockReason.Exists() && strings.TrimSpace(blockReason.String()) != "" {
		return true
	}
	candidates := gjson.GetBytes(payload, "candidates")
	if !candidates.IsArray() {
		candidates = gjson.GetBytes(payload, "response.candidates")
	}
	if !candidates.IsArray() {
		return false
	}
	items := candidates.Array()
	if len(items) == 0 {
		return false
	}
	for _, candidate := range items {
		finishReason := candidate.Get("finishReason")
		if !finishReason.Exists() || strings.TrimSpace(finishReason.String()) == "" {
			return false
		}
	}
	return true
}

// IsJSONStreamProtocolError reports malformed JSON and explicit upstream error events.
func IsJSONStreamProtocolError(payload []byte) bool {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return false
	}
	if !gjson.ValidBytes(payload) {
		return true
	}
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	return eventType == "error" || eventType == "response.failed" ||
		hasNonNullJSONValue(payload, "error") || hasNonNullJSONValue(payload, "response.error")
}

func hasNonNullJSONValue(payload []byte, path string) bool {
	value := gjson.GetBytes(payload, path)
	return value.Exists() && !bytes.Equal(bytes.TrimSpace([]byte(value.Raw)), []byte("null"))
}
