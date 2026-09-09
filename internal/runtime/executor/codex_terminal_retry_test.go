package executor

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func TestCodexTerminalRetainsQuotaResetAndParameter(t *testing.T) {
	for _, event := range []string{"response.failed", "response.incomplete", "response.completed", "response.done", "error"} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%t", event, explicit), func(t *testing.T) {
				node := `{"type":"usage_limit_reached","code":"usage_limit_reached","message":"quota","param":"input[0]","resets_in_seconds":7}`
				status := ""
				if explicit {
					status = `,"status":500`
				}
				payload := []byte(fmt.Sprintf(`{"type":%q%s,"response":{"status":"failed","error":%s},"error":%s}`, event, status, node, node))
				err, ok := codexTerminalStreamError(payload)
				want := 429
				if explicit {
					want = 500
				}
				if !ok || err.StatusCode() != want || gjson.Get(err.Error(), "error.param").String() != "input[0]" {
					t.Fatal("terminal lost quota classification or the error parameter")
				}
				if explicit {
					if err.RetryAfter() != nil {
						t.Fatal("non-quota status acquired a quota reset")
					}
					return
				}
				if err.RetryAfter() == nil || *err.RetryAfter() != 7*time.Second || err.Headers().Get("Retry-After") != "7" {
					t.Fatal("terminal dropped the upstream's explicit reset interval")
				}
			})
		}
	}
}

func TestCodexRetryAfterDurationBounds(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	maxSeconds := int64(maxDuration / time.Second)
	for _, seconds := range []int64{-1, 0, 7, maxSeconds, maxSeconds + 1} {
		body := []byte(fmt.Sprintf(`{"error":{"type":"usage_limit_reached","resets_in_seconds":%d}}`, seconds))
		got := parseCodexRetryAfter(429, body, time.Now())
		if seconds <= 0 || seconds > maxSeconds {
			if got != nil {
				t.Fatal("invalid reset interval overflowed into a cooldown")
			}
			continue
		}
		if got == nil || *got != time.Duration(seconds)*time.Second {
			t.Fatal("valid explicit reset interval changed")
		}
	}
	for _, duration := range []time.Duration{0, time.Nanosecond, time.Second, time.Second + 1, maxDuration} {
		err := statusErr{code: 429, retryAfter: &duration}
		seconds, parseErr := strconv.ParseInt(err.Headers().Get("Retry-After"), 10, 64)
		want := int64(duration / time.Second)
		if duration%time.Second != 0 {
			want++
		}
		if parseErr != nil || seconds != want {
			t.Fatal("Retry-After header overflowed while rounding up")
		}
	}
}

func TestCodexGenericCodeDoesNotOverrideSpecificErrorType(t *testing.T) {
	for _, event := range []string{"response.failed", "error"} {
		for _, tc := range []struct {
			kind   string
			status int
		}{{"usage_limit_reached", 429}, {"authentication_error", 401}, {"invalid_request_error", 400}} {
			for _, code := range []string{"server_error", "internal_server_error"} {
				node := fmt.Sprintf(`{"type":%q,"code":%q,"message":"fixture"}`, tc.kind, code)
				payload := []byte(fmt.Sprintf(`{"type":%q,"response":{"error":%s},"error":%s}`, event, node, node))
				err, ok := codexTerminalStreamError(payload)
				if !ok || err.StatusCode() != tc.status {
					t.Fatalf("%s/%s/%s: generic code overrode error type", event, tc.kind, code)
				}
			}
		}
	}
}
