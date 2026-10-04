package helps

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestChatGPTWebImageRateLimitEvidence(t *testing.T) {
	for _, test := range []struct {
		message string
		limited bool
		delay   time.Duration
	}{
		{"You're generating images too quickly. Please wait for an hour before generating more images.", true, time.Hour},
		{"You are generating images too quickly. Please try again in 5 minutes.", true, 5 * time.Minute},
		{"You're generating images too quickly. Please try again later.", true, 0},
		{"You're generating images too quickly. Please try again in 999999 days.", true, 0},
		{"We experienced an error when generating images. Please wait for an hour.", false, 0},
		{"A user wrote: You're generating images too quickly. Please try again in 5 minutes.", false, 0},
		{"image generation completed without an image", false, 0},
	} {
		limited, delay := ChatGPTWebImageRateLimit(test.message)
		if limited != test.limited || (delay == nil) != (test.delay == 0) || delay != nil && *delay != test.delay {
			t.Errorf("message=%q limited=%v delay=%v", test.message, limited, delay)
		}
	}
}

func TestChatGPTWebUploadLimitWaitRequiresStructuredEvidence(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     int
		path, body string
		delay      time.Duration
	}{
		{"observed 22 hours", 429, "/backend-api/files", `{"detail":{"code":"throttled","message":"You've reached our limit of file uploads. Please try again in 22 hours."}}`, 22 * time.Hour},
		{"observed 23 hours", 429, "/backend-api/files", `{"detail":{"error_code":"throttled","message":"You've reached our limit of file uploads. Please try again in 23 hours."}}`, 23 * time.Hour},
		{"seconds", 429, "/backend-api/files/process_upload_stream", `{"error":{"type":"throttled","message":"You've reached our limit of file uploads. Please try again in 30 seconds."}}`, 30 * time.Second},
		{"missing duration", 429, "/backend-api/files", `{"detail":{"code":"throttled","message":"You've reached our limit of file uploads. Please try again later."}}`, 0},
		{"storage quota", 429, "/backend-api/files", `{"detail":{"code":"over_user_quota","message":"You've reached our limit of file uploads. Please try again in 22 hours."}}`, 0},
		{"generic rate limit", 429, "/backend-api/files", `{"detail":{"code":"throttled","message":"Too many requests. Please try again in 22 hours."}}`, 0},
		{"invalid JSON", 429, "/backend-api/files", `not JSON`, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			limited, _ := ChatGPTWebUploadRateLimit(test.status, test.path, []byte(test.body))
			if limited != (test.delay > 0 || test.name == "missing duration") {
				t.Fatalf("classification limited=%v", limited)
			}
			delay := ChatGPTWebUploadLimitRetryAfter(test.status, test.path, []byte(test.body))
			if (delay == nil) != (test.delay == 0) || delay != nil && *delay != test.delay {
				t.Fatalf("delay=%v, want %s", delay, test.delay)
			}
			for _, status := range []int{http.StatusOK, http.StatusForbidden, http.StatusServiceUnavailable} {
				if ChatGPTWebUploadLimitRetryAfter(status, test.path, []byte(test.body)) != nil {
					t.Errorf("accepted status %d", status)
				}
			}
			if ChatGPTWebUploadLimitRetryAfter(test.status, "/backend-api/f/conversation", []byte(test.body)) != nil {
				t.Error("accepted non-upload endpoint")
			}
		})
	}
	for _, wait := range []string{"0 hours", "-1 hours", "999999999999999999 hours", "999999 days"} {
		body := fmt.Sprintf(`{"detail":{"code":"throttled","message":"You've reached our limit of file uploads. Please try again in %s."}}`, wait)
		if ChatGPTWebUploadLimitRetryAfter(429, "/backend-api/files", []byte(body)) != nil {
			t.Errorf("accepted invalid wait %s", wait)
		}
	}
}
