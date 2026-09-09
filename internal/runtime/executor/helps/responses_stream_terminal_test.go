package helps

import "testing"

func TestHasResponsesStreamTerminal(t *testing.T) {
	for _, body := range []string{
		`{"type":"response.completed","response":{"status":"completed"}}`,
		"event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\"}}\n\n",
		": ping\n\ndata: {\"type\":\"response.completed\",\"response\":{}}\n",
	} {
		if !HasResponsesStreamTerminal([]byte(body)) {
			t.Fatal("valid Responses terminal was lost")
		}
	}
	for _, body := range []string{"", "data: [DONE]\n", "event: response.completed\n", `data: {"type":"response.failed","response":{}}`, `data: {"type":"response.completed"}`, `data: {"type":"response.completed","response":{`, `data: {"type":"response.function_call_arguments.delta","delta":"{\"type\":\"response.completed\",\"response\":{}}"}`, `data: {"type":"response.created","response":{}}`} {
		if HasResponsesStreamTerminal([]byte(body)) {
			t.Fatal("metadata, error or tool data counted as terminal")
		}
	}
}
