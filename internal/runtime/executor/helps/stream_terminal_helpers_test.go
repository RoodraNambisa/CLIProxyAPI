package helps

import (
	"net/http"
	"testing"
)

func TestJSONStreamProtocolErrorPreservesMessageAndStatus(t *testing.T) {
	err := JSONStreamProtocolError("gemini", []byte(`{"error":{"message":"upstream failed"}}`))
	if err == nil || err.Error() != "gemini stream protocol error: upstream failed" {
		t.Fatalf("protocol error = %v", err)
	}
	status, ok := err.(interface{ StatusCode() int })
	if !ok || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("protocol error status = %v, %t", status, ok)
	}
}

func TestStreamTerminalHelpers(t *testing.T) {
	t.Run("OpenAI", func(t *testing.T) {
		if !IsOpenAIStreamTerminal([]byte("data: [DONE]")) {
			t.Fatal("data: [DONE] was not recognized")
		}
		if IsOpenAIStreamTerminal([]byte(`data: {"done":true}`)) {
			t.Fatal("JSON payload was recognized as [DONE]")
		}
	})

	t.Run("Claude", func(t *testing.T) {
		if !IsClaudeStreamTerminal([]byte(`data: {"type":"message_stop"}`)) {
			t.Fatal("message_stop was not recognized")
		}
		if IsClaudeStreamTerminal([]byte(`event: message_stop`)) {
			t.Fatal("event name without a data payload was recognized as completion")
		}
	})

	t.Run("Gemini", func(t *testing.T) {
		for _, payload := range [][]byte{
			[]byte(`{"candidates":[{"finishReason":"STOP"}]}`),
			[]byte(`{"response":{"candidates":[{"finishReason":"MAX_TOKENS"}]}}`),
			[]byte(`{"promptFeedback":{"blockReason":"SAFETY"}}`),
			[]byte(`{"response":{"promptFeedback":{"blockReason":"BLOCKLIST"}}}`),
		} {
			if !IsGeminiStreamTerminal(payload) {
				t.Fatalf("terminal payload was not recognized: %s", payload)
			}
		}
		for _, payload := range [][]byte{
			[]byte(`{"candidates":[{"finishReason":"STOP"},{}]}`),
			[]byte(`{"error":{"message":"failed"}}`),
			[]byte(`{"candidates":`),
		} {
			if IsGeminiStreamTerminal(payload) {
				t.Fatalf("non-terminal payload was recognized: %s", payload)
			}
		}
		if !GeminiTerminalAwaitsUsage([]byte(`{"candidates":[{"finishReason":"STOP"}]}`)) {
			t.Fatal("terminal payload without usage did not wait for a usage tail")
		}
		if GeminiTerminalAwaitsUsage([]byte(`{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"totalTokenCount":1}}`)) {
			t.Fatal("terminal payload with usage waited for another usage tail")
		}
		for _, payload := range [][]byte{
			[]byte(`{"candidates":[{"finishReason":"STOP"}],"usage_metadata":{"totalTokenCount":1}}`),
			[]byte(`{"response":{"candidates":[{"finishReason":"STOP"}],"usage_metadata":{"totalTokenCount":1}}}`),
		} {
			if GeminiTerminalAwaitsUsage(payload) {
				t.Fatalf("terminal payload with snake-case usage waited for another usage tail: %s", payload)
			}
		}
	})

	t.Run("JSON errors", func(t *testing.T) {
		for _, payload := range [][]byte{
			[]byte(`{"response":{"error":null}}`),
			[]byte(`{"error":null}`),
		} {
			if IsJSONStreamProtocolError(payload) {
				t.Fatalf("null error was treated as a protocol failure: %s", payload)
			}
		}
		if !IsJSONStreamProtocolError([]byte(`{"error":{"message":"failed"}}`)) {
			t.Fatal("error object was not treated as a protocol failure")
		}
	})
}
