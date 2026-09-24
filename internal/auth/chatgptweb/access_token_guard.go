package chatgptweb

import (
	"net/http"
	"strings"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
)

// AccessTokenExpiredError is a local rejection, not an upstream authentication
// response. Recovery remains independent of request scheduling and cooldowns.
type AccessTokenExpiredError struct{}

func (*AccessTokenExpiredError) Error() string {
	return "chatgpt web access token expired locally; credential recovery required"
}
func (*AccessTokenExpiredError) StatusCode() int                  { return http.StatusServiceUnavailable }
func (*AccessTokenExpiredError) SkipAuthResult() bool             { return true }
func (*AccessTokenExpiredError) RetryOtherAuth() bool             { return true }
func (*AccessTokenExpiredError) ExecutionResultErrorCode() string { return "access_token_expired" }
func (*AccessTokenExpiredError) LocalPolicyReason() string        { return "access_token_expired" }

type accessTokenExpiryGuard struct {
	token     string
	expiresAt time.Time
	now       func() time.Time
}

// SetAccessTokenExpiry binds validated expiry evidence to this client's token
// snapshot. Session renewal and requests without this bearer remain unaffected.
func (client *Client) SetAccessTokenExpiry(token string, expiresAt time.Time, now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	client.accessTokenGuard.Store(&accessTokenExpiryGuard{strings.TrimSpace(token), expiresAt, now})
}

func (client *Client) checkAccessTokenExpiry(request *fhttp.Request) error {
	guard := client.accessTokenGuard.Load()
	if guard == nil || guard.token == "" || guard.expiresAt.After(guard.now()) {
		return nil
	}
	// Fingerprinted headers may deliberately retain lower-case map keys.
	for key, values := range request.Header {
		if !strings.EqualFold(key, "authorization") {
			continue
		}
		for _, value := range values {
			parts := strings.Fields(value)
			if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") && parts[1] == guard.token {
				return &AccessTokenExpiredError{}
			}
		}
	}
	return nil
}
