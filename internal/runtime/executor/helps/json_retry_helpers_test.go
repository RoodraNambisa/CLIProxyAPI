package helps

import (
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func TestParseRetryDelay(t *testing.T) {
	tests := []struct {
		name string
		body string
		want time.Duration
	}{
		{
			name: "retry info",
			body: `{"error":{"details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"42s"}]}}`,
			want: 42 * time.Second,
		},
		{
			name: "quota reset metadata",
			body: `{"error":{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","metadata":{"quotaResetDelay":"1m30s"}}]}}`,
			want: 90 * time.Second,
		},
		{
			name: "malformed retry info falls back to quota metadata",
			body: `{"error":{"details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"invalid"},{"@type":"type.googleapis.com/google.rpc.ErrorInfo","metadata":{"quotaResetDelay":"2m"}}]}}`,
			want: 2 * time.Minute,
		},
		{
			name: "human readable message",
			body: `{"error":{"message":"Your quota will reset after 1h43m56s."}}`,
			want: time.Hour + 43*time.Minute + 56*time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, errParse := ParseRetryDelay([]byte(tt.body))
			if errParse != nil {
				t.Fatalf("ParseRetryDelay() error = %v", errParse)
			}
			if got == nil || *got != tt.want {
				t.Fatalf("ParseRetryDelay() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseRetryDelay_ReturnsErrorWithoutHint(t *testing.T) {
	if got, errParse := ParseRetryDelay([]byte(`{"error":{"message":"quota exhausted"}}`)); errParse == nil || got != nil {
		t.Fatalf("ParseRetryDelay() = (%v, %v), want (nil, error)", got, errParse)
	}
}

func TestDeleteJSONField(t *testing.T) {
	body := []byte(`{"project":"p","request":{"safetySettings":[1],"contents":[2]}}`)
	got := DeleteJSONField(body, "request.safetySettings")
	if gjson.GetBytes(got, "request.safetySettings").Exists() {
		t.Fatalf("DeleteJSONField() retained request.safetySettings: %s", got)
	}
	if !gjson.GetBytes(got, "request.contents").Exists() || gjson.GetBytes(got, "project").String() != "p" {
		t.Fatalf("DeleteJSONField() changed unrelated fields: %s", got)
	}
}
