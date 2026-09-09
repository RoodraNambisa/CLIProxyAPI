package helps

import (
	"testing"

	"github.com/tidwall/sjson"
)

func TestCodexPartialResultRejectsErrorsAndMalformedTerminals(t *testing.T) {
	base := []byte(`{"type":"response.incomplete","response":{"status":"incomplete","error":null,"incomplete_details":{"reason":"max_tokens"},"output":[]}}`)
	for _, tc := range []struct {
		path  string
		value any
		want  bool
	}{
		{"type", "response.incomplete", true}, {"type", "response.completed", true}, {"type", "response.done", true},
		{"type", "response.failed", false}, {"type", "error", false}, {"type", "response.created", false},
		{"response.status", "completed", false}, {"response.status", "cancelled", false}, {"response.status", nil, false},
		{"response.incomplete_details.reason", "max_output_tokens", true}, {"response.incomplete_details.reason", "content_filter", true},
		{"response.incomplete_details.reason", "server_error", false}, {"response.incomplete_details.reason", "", false},
		{"error", map[string]any{"code": "misalignment_policy_violation"}, false}, {"response.error", "denied", false},
		{"status", 429, false}, {"response.status_code", 401, false},
	} {
		t.Run(tc.path+"/"+stringValueForPartialTest(tc.value), func(t *testing.T) {
			payload, err := sjson.SetBytes(base, tc.path, tc.value)
			if err != nil {
				t.Fatal(err)
			}
			if got := IsCodexPartialResponse(payload); got != tc.want {
				t.Fatalf("partial=%t, want %t", got, tc.want)
			}
		})
	}
	for _, raw := range []string{"", `{"type":"response.incomplete"`, `{"type":"response.incomplete","response":null}`, string(base) + " garbage"} {
		if IsCodexPartialResponse([]byte(raw)) {
			t.Fatal("malformed terminal was accepted")
		}
	}
}

func stringValueForPartialTest(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return "non-string"
}
