package helps

import "testing"

func TestCodexTerminalHTTPStatusUsesOnlyValidExplicitNumbers(t *testing.T) {
	for _, tc := range []struct {
		payload string
		want    int
	}{
		{`{"status":429,"response":{"error":{"status_code":400}}}`, 429},
		{`{"status":"failed","status_code":503}`, 503},
		{`{"response":{"status":"incomplete","status_code":429}}`, 429},
		{`{"error":{"status":402}}`, 402},
		{`{"response":{"error":{"code":401}}}`, 401},
		{`{"status":429.0}`, 429},
		{`{"status":"429"}`, 0},
		{`{"status":429.5}`, 0},
		{`{"status":200}`, 0},
		{`{"status":600}`, 0},
		{`{"status":9223372036854775808}`, 0},
		{`{"message":"429 quota reached"}`, 0},
		{`{"status":429`, 0},
	} {
		if got := CodexTerminalHTTPStatus([]byte(tc.payload)); got != tc.want {
			t.Fatalf("status = %d, want %d", got, tc.want)
		}
	}
}

func TestCodexTerminalErrorNodePreservesEnvelopePriority(t *testing.T) {
	for _, tc := range []struct{ payload, want string }{
		{`{"response":{"error":{"code":"inner"}},"error":{"code":"outer"}}`, "inner"},
		{`{"response":{"error":null},"error":{"code":"outer"}}`, "outer"},
		{`{"error":{"code":"outer"}}`, "outer"},
		{`{"response":{"error":null},"error":null}`, ""},
	} {
		if got := CodexTerminalErrorNode([]byte(tc.payload)).Get("code").String(); got != tc.want {
			t.Fatal("terminal error envelope priority changed")
		}
	}
}
