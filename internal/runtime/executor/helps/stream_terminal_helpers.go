package helps

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

type streamProtocolError struct {
	provider   string
	message    string
	body       string
	structured string
	status     int
}

func (e streamProtocolError) Error() string {
	if e.structured != "" {
		return e.structured
	}
	return fmt.Sprintf("%s stream protocol error: %s", e.provider, e.message)
}

func (e streamProtocolError) StatusCode() int {
	if e.status != 0 {
		return e.status
	}
	return http.StatusBadGateway
}

func (e streamProtocolError) ResponseBody() []byte { return []byte(e.body) }

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
	result := streamProtocolError{provider: provider, message: message, body: string(payload)}
	if gjson.ValidBytes(payload) {
		root := gjson.ParseBytes(payload)
		for _, path := range []string{"error.status_code", "error.http_status", "error.status", "response.error.status_code", "response.error.http_status", "response.error.status", "status_code", "http_status", "status", "error.code", "response.error.code", "code"} {
			value := root.Get(path)
			status := value.Int()
			if value.Type == gjson.Number && status >= 400 && status <= 599 && value.Float() == float64(status) {
				result.status = int(status)
				break
			}
		}
		node := root.Get("error")
		if !node.IsObject() {
			node = root.Get("response.error")
		}
		if !node.IsObject() && root.Get("type").String() == "error" {
			node = root
		}
		if node.IsObject() {
			code, kind := node.Get("code"), node.Get("type")
			hasCode := (code.Type == gjson.String && strings.TrimSpace(code.String()) != "") || code.Type == gjson.Number
			hasType := kind.Type == gjson.String && strings.TrimSpace(kind.String()) != "" && kind.String() != "error"
			if hasCode || hasType || result.status != 0 {
				// Keep structured semantics for both the SDK classifier and public errors.
				// Rules separately receive the exact original envelope through ResponseBody.
				result.structured = `{"error":` + node.Raw + `}`
			}
		}
	}
	return result
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
