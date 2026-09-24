package auth

import (
	"fmt"
	"testing"
	"time"
)

func TestChatGPTWebBackgroundRefreshUsesAccessTokenExpiry(t *testing.T) {
	lead := 5 * time.Minute
	setRefreshLeadFactory(t, "chatgpt-web", func() *time.Duration { return &lead })
	now := time.Now().UTC().Truncate(time.Second)
	for _, offset := range []time.Duration{-time.Minute, 4 * time.Minute, time.Hour} {
		expiry := now.Add(offset)
		auth := &Auth{Provider: "chatgpt-web", Metadata: map[string]any{
			"access_token":    expiryTestToken(fmt.Sprint(expiry.Unix())),
			"expired":         now.Add(90 * 24 * time.Hour).Format(time.RFC3339),
			"lifecycle_state": LifecycleStateActive,
		}}
		if got, known := auth.ExpirationTime(); !known || !got.Equal(expiry) {
			t.Fatalf("expiry = %v/%t, want %v", got, known, expiry)
		}
		due, enabled := nextRefreshCheckAt(now, auth, time.Hour)
		wantDue := expiry.Add(-lead)
		if wantDue.Before(now) {
			wantDue = now
		}
		if !enabled || !due.Equal(wantDue) {
			t.Fatalf("background due = %v/%t, want %v", due, enabled, wantDue)
		}
		if (&Manager{}).shouldRefresh(auth, now) != (offset <= lead) {
			t.Fatal("refresh evaluation disagrees with scheduled JWT expiry")
		}
	}
}
