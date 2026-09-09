package executor

import (
	"fmt"
	"strings"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexCancelledTerminalPreservesStructuredErrors(t *testing.T) {
	for _, event := range []string{"response.completed", "response.done"} {
		for _, status := range []string{"cancelled", "canceled"} {
			for _, tc := range []struct {
				code, kind string
				want       int
				policy     bool
			}{
				{"misalignment_policy_violation", "server_error", 500, true},
				{"rate_limit_exceeded", "rate_limit_error", 429, false},
				{"invalid_api_key", "authentication_error", 401, false},
			} {
				t.Run(event+"/"+status+"/"+tc.code, func(t *testing.T) {
					payload := []byte(fmt.Sprintf(`{"type":%q,"response":{"status":%q,"error":{"code":%q,"type":%q,"message":"original cause"}}}`, event, status, tc.code, tc.kind))
					err, ok := codexTerminalStreamError(payload)
					if !ok || err.StatusCode() != tc.want || err.SkipAuthResult() || !strings.Contains(err.Error(), tc.code) || coreauth.IsPolicyRefusalError(err) != tc.policy {
						t.Fatal("response status erased the structured upstream cause")
					}
				})
			}
			payload := []byte(fmt.Sprintf(`{"type":%q,"response":{"status":%q,"error":null}}`, event, status))
			err, ok := codexTerminalStreamError(payload)
			if !ok || !err.SkipAuthResult() || !strings.Contains(err.Error(), "response_cancelled") {
				t.Fatal("plain cancellation lost its existing request-only behavior")
			}
		}
	}
}
